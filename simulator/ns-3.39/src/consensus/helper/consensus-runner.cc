#include "consensus-runner.h"

#include "consensus-helper.h"

#include "ns3/bidl-application.h"
#include "ns3/consensus-application.h"
#include "ns3/consensus-workload.h"
#include "ns3/enum.h"
#include "ns3/inet-socket-address.h"
#include "ns3/simulator.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <cmath>
#include <cstdio>
#include <fstream>
#include <stdexcept>
#include <vector>

namespace ns3 {

    // 匿名命名空间，仅供当前文件使用的辅助函数
    namespace {

        // 将网卡、链路速率、队列规则、交换机策略和 RDMA 背景流量连接到统计模块
        void AttachNetworkStatistics(Ptr<ConsensusStatistics> statistics, const ConsensusExperimentConfig& config,
                                     const ConsensusTopology& topology, const ApplicationContainer& background) {
            statistics->AttachNetDevices(topology.devices, DataRate(config.dataRate), topology.deviceLabels);
            statistics->AttachQueueDiscs(topology.queueDiscs);
            statistics->AttachSwitchPolicies(topology.switchPolicies);
            if (config.UsesRdmaNetwork()) {
                statistics->AttachRdmaWorkloads(background);
            }
        }

        // 安装背景流量，使用 RDMA 网络时安装 RDMA 背景流，否则安装普通的 TCP/IP 背景流
        ApplicationContainer InstallBackground(const ConsensusExperimentConfig& config, const ConsensusTopology& topology,
                                               Time applicationStart) {
            if (config.UsesRdmaNetwork()) {
                return config.InstallRdmaBackgroundTraffic(topology, applicationStart);
            }

            return config.InstallBackgroundTraffic(topology.endpoints, topology.endpointInterfaces);
        }

        // 从 ConsensusStatistics 中提取完成请求数、完成事务数、吞吐量以及 P50/P95/P99 延迟
        ConsensusRunResult CollectResult(Ptr<ConsensusStatistics> statistics, uint64_t expectedRequests) {
            ConsensusRunResult result;
            result.expectedRequests = expectedRequests;
            result.completedRequests = statistics->GetCompletedRequests();
            result.completedTransactions = statistics->GetCompletedTransactions();
            result.requestThroughput = statistics->GetRequestThroughput();
            result.transactionThroughput = statistics->GetTransactionThroughput();
            result.latencyP50Us = statistics->GetLatencyPercentile(50);
            result.latencyP95Us = statistics->GetLatencyPercentile(95);
            result.latencyP99Us = statistics->GetLatencyPercentile(99);
            return result;
        }

        std::string RunCommand(const std::string& command) {
            std::array<char, 256> buffer{};
            std::string output;
            FILE* pipe = popen(command.c_str(), "r");
            if (!pipe) {
                return "unknown";
            }
            while (fgets(buffer.data(), buffer.size(), pipe)) {
                output += buffer.data();
            }
            pclose(pipe);
            while (!output.empty() && std::isspace(static_cast<unsigned char>(output.back()))) {
                output.pop_back();
            }
            return output;
        }

        void WriteManifest(const ConsensusExperimentConfig& config, const ConsensusRunResult& result) {
            const std::string gitPrefix = "git -C '" PROJECT_SOURCE_PATH "' ";
            const std::string rawCommit = RunCommand(gitPrefix + "rev-parse HEAD 2>/dev/null");
            const std::string commit = rawCommit.empty() ? "unknown" : rawCommit;
            const std::string status = RunCommand(gitPrefix + "status --porcelain 2>/dev/null");
            std::ofstream output(config.outputPrefix + "-manifest.json");
            if (!output) {
                throw std::runtime_error("cannot write experiment manifest");
            }
            output << "{\n"
                   << "  \"git_commit\": \"" << commit << "\",\n"
                   << "  \"git_dirty\": " << (!status.empty() ? "true" : "false") << ",\n"
                   << "  \"source_path\": \"" PROJECT_SOURCE_PATH "\",\n"
                   << "  \"compiler\": \"" << __VERSION__ << "\",\n"
#ifdef NS3_BUILD_PROFILE_DEBUG
                   << "  \"build_profile\": \"debug\",\n"
#else
                   << "  \"build_profile\": \"optimized\",\n"
#endif
                   << "  \"seed\": " << config.seed << ",\n"
                   << "  \"run\": " << config.run << ",\n"
                   << "  \"application_start\": " << config.GetApplicationStart().GetSeconds() << ",\n"
                   << "  \"measurement_start\": " << config.GetMeasurementStart().GetSeconds() << ",\n"
                   << "  \"measurement_stop\": " << config.GetMeasurementStop().GetSeconds() << ",\n"
                   << "  \"simulation_stop\": " << config.simulationTime << ",\n"
                   << "  \"expected_requests\": " << result.expectedRequests << ",\n"
                   << "  \"completed_requests\": " << result.completedRequests << ",\n"
                   << "  \"status\": \"" << (result.success ? "complete" : "incomplete") << "\"\n"
                   << "}\n";
        }

        double Percentile(std::vector<double> values, double percentile) {
            if (values.empty()) {
                return 0.0;
            }

            std::sort(values.begin(), values.end());
            size_t index = static_cast<size_t>(std::ceil(percentile * values.size()) - 1.0);
            return values[std::min(index, values.size() - 1)];
        }

        // 计算一组批量实验数据的均值、P95、P99、样本标准差和 95% 置信区间半宽，并写入汇总 CSV
        void WriteAggregateMetric(std::ofstream& output, const std::string& name, const std::vector<double>& values) {
            double sum = 0.0;
            for (double value : values) {
                sum += value;
            }
            double mean = values.empty() ? 0.0 : sum / values.size();

            double variance = 0.0;
            for (double value : values) {
                variance += (value - mean) * (value - mean);
            }

            double standardDeviation = 0.0;
            if (values.size() >= 2) {
                standardDeviation = std::sqrt(variance / (values.size() - 1));
            }

            double ci95 = 0.0;
            if (!values.empty()) {
                ci95 = 1.96 * standardDeviation / std::sqrt(values.size());
            }

            output << name << ',' << mean << ',' << Percentile(values, 0.95) << ',' << Percentile(values, 0.99) << ',' << ci95 << '\n';
        }

    } // namespace

    // 单次实验的入口函数
    ConsensusRunResult ConsensusRunner::Run(const ConsensusExperimentConfig& config, bool writeResults) const {
        // 检查配置是否合法并设置随机种子
        config.Validate();
        config.ApplyRandomSeed();

        // 根据不同的架构选择不同的启动函数
        if (config.architecture == "replicated") {
            return RunReplicated(config, writeResults);
        } else if (config.architecture == "bidl") {
            return RunBidl(config, writeResults);
        }

        // 没有匹配的架构，则抛出异常
        throw std::invalid_argument("unsupported consensus architecture: " + config.architecture);
    }

    // 批次实验的入口函数
    ConsensusBatchResult ConsensusRunner::RunBatch(const ConsensusExperimentConfig& config, uint32_t runCount,
                                                   bool writeIndividualResults) const {
        if (runCount == 0) {
            throw std::invalid_argument("runCount must be positive");
        }
        ConsensusBatchResult batch;
        batch.success = true;
        const std::string basePrefix = config.outputPrefix;
        for (uint32_t index = 0; index < runCount; ++index) {
            ConsensusExperimentConfig current = config;
            current.run = config.run + index;
            current.outputPrefix = basePrefix + "-run-" + std::to_string(current.run);
            ConsensusRunResult result = Run(current, writeIndividualResults);
            batch.success = batch.success && result.success;
            batch.runs.push_back(result);
        }

        std::ofstream output(basePrefix + "-aggregate.csv");
        if (!output) {
            throw std::runtime_error("cannot write batch aggregate");
        }
        output << "metric,mean,p95,p99,ci95_half_width\n";
        std::vector<double> requestThroughput;
        std::vector<double> transactionThroughput;
        std::vector<double> latencyP50;
        std::vector<double> latencyP95;
        std::vector<double> latencyP99;
        std::vector<double> completionRatio;
        for (const auto& result : batch.runs) {
            requestThroughput.push_back(result.requestThroughput);
            transactionThroughput.push_back(result.transactionThroughput);
            latencyP50.push_back(result.latencyP50Us);
            latencyP95.push_back(result.latencyP95Us);
            latencyP99.push_back(result.latencyP99Us);

            double ratio = 0.0;
            if (result.expectedRequests > 0) {
                ratio = static_cast<double>(result.completedRequests) / result.expectedRequests;
            }
            completionRatio.push_back(ratio);
        }
        WriteAggregateMetric(output, "request_throughput", requestThroughput);
        WriteAggregateMetric(output, "transaction_throughput", transactionThroughput);
        WriteAggregateMetric(output, "latency_p50_us", latencyP50);
        WriteAggregateMetric(output, "latency_p95_us", latencyP95);
        WriteAggregateMetric(output, "latency_p99_us", latencyP99);
        WriteAggregateMetric(output, "completion_ratio", completionRatio);
        return batch;
    }

    // 启动 PBFT 一类传统副本式协议
    ConsensusRunResult ConsensusRunner::RunReplicated(const ConsensusExperimentConfig& config, bool writeResults) const {
        // 读取网络拓扑、节点部署位置、共识通信端口
        ConsensusTopology topology = config.BuildTopology(config.replicaCount + config.GetEffectiveClientCount());
        ConsensusRolePlacement placement = config.ResolvePbftPlacement(topology);
        const uint16_t port = config.GetConsensusPort(9000);

        // 建立副本和客户端地址表
        std::vector<uint32_t> replicaIds; // 保存所有逻辑副本 ID
        ConsensusEndpointMap endpoints;   // 保存逻辑「节点ID -> IP地址和端口」的映射
        NodeContainer replicaNodes;       // 保存真正安装副本应用的节点
        for (uint32_t replicaId = 0; replicaId < config.replicaCount; ++replicaId) {
            replicaIds.push_back(replicaId);
            uint32_t physicalId = placement.replicaPhysicalIds[replicaId];
            replicaNodes.Add(topology.nodes.Get(physicalId));
            endpoints.emplace(replicaId, InetSocketAddress(topology.nodeAddresses[physicalId], port));
        }
        constexpr uint32_t firstClientId = 1000; // 规定客户端逻辑 ID 从 1000 开始，避免与副本 ID 冲突
        for (uint32_t client = 0; client < placement.clientPhysicalIds.size(); ++client) {
            uint32_t physicalId = placement.clientPhysicalIds[client];
            endpoints.emplace(firstClientId + client, InetSocketAddress(topology.nodeAddresses[physicalId], port));
        }

        // 安装副本和客户端应用
        ConsensusHelper helper;
        config.ConfigureHelper(helper, &topology);
        ApplicationContainer replicas = helper.InstallReplicas(replicaNodes, replicaIds, endpoints);
        ApplicationContainer clients;
        std::vector<ConsensusWorkloadClient> workloadClients;
        for (uint32_t client = 0; client < placement.clientPhysicalIds.size(); ++client) {
            uint32_t physicalId = placement.clientPhysicalIds[client];
            ApplicationContainer installed =
                helper.InstallClient(topology.nodes.Get(physicalId), firstClientId + client, 0, replicaIds, endpoints);
            clients.Add(installed);
            workloadClients.push_back({physicalId, DynamicCast<ConsensusClientApplication>(installed.Get(0))});
        }

        // 设置仿真时间
        const Time start = config.GetApplicationStart();
        const Time measurementStart = config.GetMeasurementStart();
        const Time measurementStop = config.GetMeasurementStop();
        const Time stop = Seconds(config.simulationTime);

        // 安装背景流量并调度客户端请求
        Ptr<ConsensusWorkloadProvider> workload = CreateConsensusWorkloadProvider(config);
        uint64_t expectedRequests =
            workload->ScheduleRequests(config, workloadClients, topology, placement, measurementStart, measurementStop);
        ApplicationContainer background = InstallBackground(config, topology, start);

        // 统一启动和停止所有 Application
        replicas.Start(start);
        replicas.Stop(stop);
        clients.Start(start);
        clients.Stop(stop);
        background.Start(start);
        background.Stop(stop);

        // 设置统计窗口
        Ptr<ConsensusStatistics> statistics = CreateObject<ConsensusStatistics>();
        statistics->SetMeasurementWindow(measurementStart, measurementStop);
        for (const auto& client : workloadClients) {
            statistics->AttachClient(client.application);
        }
        statistics->AttachReplicas(replicas);
        AttachNetworkStatistics(statistics, config, topology, background);

        // 运行和清理
        Simulator::Stop(stop);
        Simulator::Run();
        ConsensusRunResult result = CollectResult(statistics, expectedRequests);
        result.success = result.completedRequests == expectedRequests;
        if (writeResults) {
            statistics->WriteResults(config.outputPrefix, config);
            WriteManifest(config, result);
        }
        Simulator::Destroy();
        return result;
    }

    ConsensusRunResult ConsensusRunner::RunBidl(const ConsensusExperimentConfig& config, bool writeResults) const {
        ConsensusTopology topology = config.BuildTopology(2 * config.replicaCount + 1 + config.GetEffectiveClientCount());
        ConsensusRolePlacement placement = config.ResolveBidlPlacement(topology);
        const uint16_t port = config.GetConsensusPort(9100);
        constexpr uint32_t firstExecutorId = 100;
        constexpr uint32_t sequencerId = 200;
        constexpr uint32_t firstClientId = 1000;

        std::vector<uint32_t> ordererIds;
        std::vector<uint32_t> executorIds;
        for (uint32_t index = 0; index < config.replicaCount; ++index) {
            ordererIds.push_back(index);
            executorIds.push_back(firstExecutorId + index);
        }
        const std::vector<uint32_t> sequencerIds{sequencerId};

        ConsensusEndpointMap endpoints;
        for (uint32_t index = 0; index < config.replicaCount; ++index) {
            uint32_t ordererPhysicalId = placement.ordererPhysicalIds[index];
            uint32_t executorPhysicalId = placement.executorPhysicalIds[index];
            endpoints.emplace(ordererIds[index], InetSocketAddress(topology.nodeAddresses[ordererPhysicalId], port));
            endpoints.emplace(executorIds[index], InetSocketAddress(topology.nodeAddresses[executorPhysicalId], port));
        }
        uint32_t sequencerPhysicalId = placement.ingressPhysicalId;
        endpoints.emplace(sequencerIds.front(), InetSocketAddress(topology.nodeAddresses[sequencerPhysicalId], port));
        for (uint32_t client = 0; client < placement.clientPhysicalIds.size(); ++client) {
            uint32_t physicalId = placement.clientPhysicalIds[client];
            endpoints.emplace(firstClientId + client, InetSocketAddress(topology.nodeAddresses[physicalId], port));
        }

        ConsensusHelper helper;
        config.ConfigureHelper(helper, &topology);
        helper.SetClientAttribute("ReplyQuorum", UintegerValue(1));
        helper.SetResourceAttribute("Mode", EnumValue(RESOURCE_QUEUED));

        ApplicationContainer applications;
        for (uint32_t index = 0; index < config.replicaCount; ++index) {
            applications.Add(helper.InstallBidlNode(topology.nodes.Get(placement.ordererPhysicalIds[index]), ordererIds[index],
                                                    BIDL_ORDERER, ordererIds, executorIds, sequencerIds, endpoints));
        }
        Ptr<BidlApplication> measuredExecutor;
        for (uint32_t index = 0; index < config.replicaCount; ++index) {
            ApplicationContainer executorApplication =
                helper.InstallBidlNode(topology.nodes.Get(placement.executorPhysicalIds[index]), executorIds[index], BIDL_EXECUTOR,
                                       ordererIds, executorIds, sequencerIds, endpoints);
            applications.Add(executorApplication);
            if (index == 0) {
                measuredExecutor = DynamicCast<BidlApplication>(executorApplication.Get(0));
            }
        }
        applications.Add(helper.InstallBidlNode(topology.nodes.Get(sequencerPhysicalId), sequencerIds.front(), BIDL_SEQUENCER, ordererIds,
                                                executorIds, sequencerIds, endpoints));
        ApplicationContainer clients;
        std::vector<ConsensusWorkloadClient> workloadClients;
        for (uint32_t client = 0; client < placement.clientPhysicalIds.size(); ++client) {
            uint32_t physicalId = placement.clientPhysicalIds[client];
            ApplicationContainer installed =
                helper.InstallClient(topology.nodes.Get(physicalId), firstClientId + client, sequencerIds.front(), sequencerIds, endpoints);
            clients.Add(installed);
            workloadClients.push_back({physicalId, DynamicCast<ConsensusClientApplication>(installed.Get(0))});
        }
        const Time start = config.GetApplicationStart();
        const Time measurementStart = config.GetMeasurementStart();
        const Time measurementStop = config.GetMeasurementStop();
        const Time stop = Seconds(config.simulationTime);

        Ptr<ConsensusWorkloadProvider> workload = CreateConsensusWorkloadProvider(config);
        uint64_t expectedRequests =
            workload->ScheduleRequests(config, workloadClients, topology, placement, measurementStart, measurementStop);
        ApplicationContainer background = InstallBackground(config, topology, start);

        applications.Start(start);
        applications.Stop(stop);
        clients.Start(start);
        clients.Stop(stop);
        background.Start(start);
        background.Stop(stop);

        Ptr<ConsensusStatistics> statistics = CreateObject<ConsensusStatistics>();
        statistics->SetMeasurementWindow(measurementStart, measurementStop);
        statistics->AttachBidlApplications(applications);
        for (const auto& client : workloadClients) {
            statistics->AttachClient(client.application);
        }
        AttachNetworkStatistics(statistics, config, topology, background);

        Simulator::Stop(stop);
        Simulator::Run();
        ConsensusRunResult result = CollectResult(statistics, expectedRequests);
        result.committedBlocks = measuredExecutor->GetCommittedBlocks();
        result.success = result.completedRequests == expectedRequests;
        if (writeResults) {
            statistics->WriteResults(config.outputPrefix, config);
            WriteManifest(config, result);
        }
        Simulator::Destroy();
        return result;
    }

} // namespace ns3
