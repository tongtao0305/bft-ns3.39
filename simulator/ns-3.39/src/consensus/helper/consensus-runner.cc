#include "consensus-runner.h"

#include "consensus-helper.h"

#include "ns3/bidl-application.h"
#include "ns3/consensus-application.h"
#include "ns3/consensus-workload.h"
#include "ns3/zyzzyva-engine.h"
#include "ns3/ezbft-engine.h"
#include "ns3/enum.h"
#include "ns3/inet-socket-address.h"
#include "ns3/simulator.h"
#include "ns3/switch-node.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <cmath>
#include <cstdio>
#include <csignal>
#include <fstream>
#include <map>
#include <set>
#include <sstream>
#include <stdexcept>
#include <vector>

namespace ns3 {
    namespace {
        volatile std::sig_atomic_t stopRequested = 0;
    }

    void RequestConsensusStop(int signal) { stopRequested = signal; }
    bool WasConsensusStopRequested() { return stopRequested != 0; }

    // 匿名命名空间，仅供当前文件使用的辅助函数
    namespace {

        // 将网卡、链路速率、队列规则、交换机策略和 RDMA 背景流量连接到统计模块
        void AttachNetworkStatistics(Ptr<ConsensusStatistics> statistics, const ConsensusExperimentConfig& config,
                                     const ConsensusTopology& topology, const ApplicationContainer& background) {
            statistics->AttachNetDevices(topology.devices, DataRate(config.dataRate), topology.deviceLabels);
            statistics->AttachQueueDiscs(topology.queueDiscs);
            statistics->AttachSwitchPolicies(topology.switchPolicies);
            statistics->AttachIorsSwitches(topology.switches);
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

        uint64_t ScheduleDirectBidlWorkload(const ConsensusExperimentConfig& config, Ptr<BidlApplication> sequencer, Time start,
                                            Time stop) {
            if (!sequencer) {
                throw std::invalid_argument("direct BIDL workload requires a sequencer");
            }
            uint64_t scheduled = 0;
            uint64_t requestId = 1;
            const Time ready = start + NanoSeconds(1);
            const bool trace = config.workload == "trace" || (config.workload == "auto" && config.HasLegacyBlockchainWorkload());
            if (trace) {
                for (const LegacyFlowRecord& flow : GetCachedLegacyFlowFile(config.flowFile)) {
                    if (flow.applicationId != 1) {
                        continue;
                    }
                    Time submission = std::max(ready, NanoSeconds(flow.startTimeNs));
                    if (submission < stop) {
                        Simulator::Schedule(submission - Simulator::Now(), &BidlApplication::InjectTransaction, sequencer, flow.bytes, 1,
                                            requestId++);
                        ++scheduled;
                    }
                }
            } else if (config.workload == "burst") {
                for (uint32_t client = 0; client < config.clientCount; ++client) {
                    for (uint32_t request = 0; request < config.requestCount; ++request) {
                        Time submission = ready + MilliSeconds((request / config.burstSize) * config.burstIntervalMs);
                        if (submission < stop) {
                            Simulator::Schedule(submission - Simulator::Now(), &BidlApplication::InjectTransaction, sequencer,
                                                config.requestSize, config.transactionsPerRequest, requestId++);
                            ++scheduled;
                        }
                    }
                }
            } else {
                Time interval = Seconds(1.0 / config.requestRate);
                for (uint32_t client = 0; client < config.clientCount; ++client) {
                    Time offset = interval * client / config.clientCount;
                    for (uint32_t request = 0; request < config.requestCount; ++request) {
                        Time submission = ready + offset + interval * request;
                        if (submission < stop) {
                            Simulator::Schedule(submission - Simulator::Now(), &BidlApplication::InjectTransaction, sequencer,
                                                config.requestSize, config.transactionsPerRequest, requestId++);
                            ++scheduled;
                        }
                    }
                }
            }
            if (scheduled == 0) {
                throw std::invalid_argument("direct BIDL workload has no transaction in the measurement window");
            }
            return scheduled;
        }

        // 从 ConsensusStatistics 中提取完成请求数、完成事务数、吞吐量以及 P50/P95/P99 延迟
        ConsensusRunResult CollectResult(Ptr<ConsensusStatistics> statistics, uint64_t expectedRequests) {
            ConsensusRunResult result;
            result.expectedRequests = expectedRequests;
            result.completedRequests = statistics->GetCompletedRequests();
            result.abortedRequests = statistics->GetAbortedRequests();
            result.completedTransactions = statistics->GetCompletedTransactions();
            result.requestThroughput = statistics->GetRequestThroughput();
            result.transactionThroughput = statistics->GetTransactionThroughput();
            result.latencyP50Us = statistics->GetLatencyPercentile(50);
            result.latencyP95Us = statistics->GetLatencyPercentile(95);
            result.latencyP99Us = statistics->GetLatencyPercentile(99);
            result.viewChanges = statistics->GetViewChanges();
            return result;
        }

        void CollectIorsResult(const ConsensusTopology& topology, ConsensusRunResult& result) {
            for (uint32_t index = 0; index < topology.switches.GetN(); ++index) {
                Ptr<SwitchNode> node = DynamicCast<SwitchNode>(topology.switches.Get(index));
                result.iorsObserved += node->GetIorsObserved();
                result.iorsMarked += node->GetIorsMarked();
                result.iorsWaited += node->GetIorsWaited();
                result.iorsAlreadyMarked += node->GetIorsAlreadyMarked();
                result.iorsCapacityBypass += node->GetIorsCapacityBypass();
                result.iorsCollisionBypass += node->GetIorsCollisionBypass();
                result.iorsUnauthorized += node->GetIorsUnauthorized();
                result.iorsEquivocation += node->GetIorsEquivocation();
                result.iorsHoldBypass += node->GetIorsHoldBypass();
                result.iorsExpired += node->GetIorsExpired();
            }
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
            const std::string protocolVersion = config.architecture == "bidl" ? (config.bidlNativeSequencing ? "bidl-pbft-leader-sequencing-v2" : "bidl-fixed-sequencer-v2") :
                (config.engine == "ezbft" ?
                    (config.ezbftVariant == 0 ? "ezbft-original-iors-chain" : "ezbft-pbft-final-tuple-research") :
                    (config.engine == "zyzzyva" ?
                        (config.zyzzyvaRecoveryRule == 0 ? "zyzzyva-original-style-recovery-v2" : "zyzzyva-velma-style-recovery-v2") :
                        config.engine));
            output << "{\n"
                   << "  \"git_commit\": \"" << commit << "\",\n"
                   << "  \"git_dirty\": " << (!status.empty() ? "true" : "false") << ",\n"
                   << "  \"protocol_version\": \"" << protocolVersion << "\",\n"
                   << "  \"simulation_boundary\": \"deterministic authentication, application results and service delays; packet communication and quorum checks modeled\",\n"
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
                   << "  \"effective_client_reply_quorum\": " <<
                       (config.architecture == "zyzzyva" || config.architecture == "ezbft" ? "null" :
                        std::to_string(config.architecture == "bidl" ? (config.bidlNativeSequencing ? config.faultTolerance + 1 : 1) :
                         (config.clientReplyQuorum ? config.clientReplyQuorum : config.faultTolerance + 1))) << ",\n"
                   << "  \"fast_path_reply_quorum\": " << (config.architecture == "zyzzyva" || config.architecture == "ezbft" ? config.replicaCount : 0) << ",\n"
                   << "  \"slow_path_reply_quorum\": " << (config.architecture == "zyzzyva" || config.architecture == "ezbft" ? 2 * config.faultTolerance + 1 : 0) << ",\n"
                   << "  \"completed_requests\": " << result.completedRequests << ",\n"
                   << "  \"aborted_requests\": " << result.abortedRequests << ",\n"
                   << "  \"iors_observed\": " << result.iorsObserved << ",\n"
                   << "  \"iors_marked\": " << result.iorsMarked << ",\n"
                   << "  \"iors_waited\": " << result.iorsWaited << ",\n"
                   << "  \"iors_already_marked\": " << result.iorsAlreadyMarked << ",\n"
                   << "  \"iors_capacity_bypass\": " << result.iorsCapacityBypass << ",\n"
                   << "  \"iors_collision_bypass\": " << result.iorsCollisionBypass << ",\n"
                   << "  \"iors_unauthorized\": " << result.iorsUnauthorized << ",\n"
                   << "  \"iors_equivocation\": " << result.iorsEquivocation << ",\n"
                   << "  \"iors_hold_bypass\": " << result.iorsHoldBypass << ",\n"
                   << "  \"iors_expired\": " << result.iorsExpired << ",\n"
                   << "  \"bidl_denylist_size\": " << result.bidlDenylistSize << ",\n"
                   << "  \"bidl_deferred_transactions\": " << result.bidlDeferredTransactions << ",\n"
                   << "  \"view_changes\": " << result.viewChanges << ",\n"
                   << "  \"zyzzyva_rollbacks\": " << result.zyzzyvaRollbacks << ",\n"
                   << "  \"zyzzyva_equivocations\": " << result.zyzzyvaEquivocations << ",\n"
                   << "  \"zyzzyva_attacks_injected\": " << result.zyzzyvaAttacksInjected << ",\n"
                   << "  \"zyzzyva_speculative_executions\": " << result.zyzzyvaSpeculativeExecutions << ",\n"
                   << "  \"zyzzyva_reexecuted_requests\": " << result.zyzzyvaReexecutedRequests << ",\n"
                   << "  \"zyzzyva_cancelled_execution_tasks\": " << result.zyzzyvaCancelledExecutions << ",\n"
                   << "  \"ezbft_equivocations\": " << result.ezbftEquivocations << ",\n"
                   << "  \"ezbft_speculative_executions\": " << result.ezbftSpeculativeExecutions << ",\n"
                   << "  \"ezbft_final_executions\": " << result.ezbftFinalExecutions << ",\n"
                   << "  \"ezbft_owner_changes\": " << result.ezbftOwnerChanges << ",\n"
                   << "  \"ezbft_reexecutions\": " << result.ezbftReexecutions << ",\n"
                   << "  \"ezbft_legal_updates\": " << result.ezbftLegalUpdates << ",\n"
                   << "  \"replica_state_agreement\": " << (result.replicaStateAgreement ? "true" : "false") << ",\n"
                   << "  \"application_history_agreement\": " << (result.applicationHistoryAgreement ? "true" : "false") << ",\n"
                   << "  \"confirmed_safety_error\": " << (result.confirmedSafetyConflict ? "true" : "false") << ",\n"
                   << "  \"replica_progress_min\": " << result.replicaProgressMin << ",\n"
                   << "  \"replica_progress_max\": " << result.replicaProgressMax << ",\n"
                   << "  \"interrupted\": " << (result.interrupted ? "true" : "false") << ",\n"
                   << "  \"status\": \"" << (result.interrupted ? "interrupted" : result.success ? "complete" : "incomplete") << "\"\n"
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
        if (config.architecture == "replicated" || config.architecture == "zyzzyva" || config.architecture == "ezbft") {
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
            if (result.interrupted) break;
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
        std::vector<double> iorsObserved, iorsMarked, iorsWaited, iorsEquivocation;
        std::vector<double> viewChanges, zyzzyvaRollbacks, zyzzyvaEquivocations;
        std::vector<double> ezbftSpeculative, ezbftFinal, ezbftOwnerChanges, ezbftReexecutions;
        for (const auto& result : batch.runs) {
            requestThroughput.push_back(result.requestThroughput);
            transactionThroughput.push_back(result.transactionThroughput);
            latencyP50.push_back(result.latencyP50Us);
            latencyP95.push_back(result.latencyP95Us);
            latencyP99.push_back(result.latencyP99Us);
            iorsObserved.push_back(result.iorsObserved);
            iorsMarked.push_back(result.iorsMarked);
            iorsWaited.push_back(result.iorsWaited);
            iorsEquivocation.push_back(result.iorsEquivocation);
            viewChanges.push_back(result.viewChanges);
            zyzzyvaRollbacks.push_back(result.zyzzyvaRollbacks);
            zyzzyvaEquivocations.push_back(result.zyzzyvaEquivocations);
            ezbftSpeculative.push_back(result.ezbftSpeculativeExecutions);
            ezbftFinal.push_back(result.ezbftFinalExecutions);
            ezbftOwnerChanges.push_back(result.ezbftOwnerChanges);
            ezbftReexecutions.push_back(result.ezbftReexecutions);

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
        WriteAggregateMetric(output, "iors_observed", iorsObserved);
        WriteAggregateMetric(output, "iors_marked", iorsMarked);
        WriteAggregateMetric(output, "iors_waited", iorsWaited);
        WriteAggregateMetric(output, "iors_equivocation", iorsEquivocation);
        WriteAggregateMetric(output, "view_changes", viewChanges);
        WriteAggregateMetric(output, "zyzzyva_rollbacks", zyzzyvaRollbacks);
        WriteAggregateMetric(output, "zyzzyva_equivocations", zyzzyvaEquivocations);
        WriteAggregateMetric(output, "ezbft_speculative_executions", ezbftSpeculative);
        WriteAggregateMetric(output, "ezbft_final_executions", ezbftFinal);
        WriteAggregateMetric(output, "ezbft_owner_changes", ezbftOwnerChanges);
        WriteAggregateMetric(output, "ezbft_reexecutions", ezbftReexecutions);
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
            // EZBFT is leaderless: spread clients across command-leaders so the
            // workload exercises distinct owner instance spaces.
            const uint32_t ingressReplica = config.architecture == "ezbft" ? client % config.replicaCount : 0;
            ApplicationContainer installed =
                helper.InstallClient(topology.nodes.Get(physicalId), firstClientId + client, ingressReplica, replicaIds, endpoints);
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
        statistics->SetDiagnosticEvents(config.diagnosticEvents);
        statistics->SetDetailedEvents(config.transactionPathEvents, config.backgroundFlowEvents, config.iorsMode != 0);
        statistics->SetEventOutputPrefix(config.outputPrefix);
        for (const auto& client : workloadClients) {
            statistics->AttachClient(client.application);
        }
        statistics->AttachReplicas(replicas);
        AttachNetworkStatistics(statistics, config, topology, background);

        // 运行和清理
        Simulator::Stop(stop);
        Simulator::Run();
        ConsensusRunResult result = CollectResult(statistics, expectedRequests);
        CollectIorsResult(topology, result);
        if (config.architecture == "zyzzyva") {
            bool first = true;
            std::vector<uint64_t> referenceCommittedHistory;
            std::vector<uint64_t> referenceApplicationHistory;
            std::set<uint32_t> malicious;
            std::stringstream ids(config.zyzzyvaMaliciousReplicaIds);
            std::string id;
            while (std::getline(ids, id, ',')) {
                try { malicious.insert(static_cast<uint32_t>(std::stoul(id))); } catch (...) {}
            }
            for (uint32_t i = 0; i < replicas.GetN(); ++i) {
                Ptr<ConsensusApplication> application = DynamicCast<ConsensusApplication>(replicas.Get(i));
                Ptr<ZyzzyvaEngine> engine = DynamicCast<ZyzzyvaEngine>(application->GetEngine());
                if (engine) result.zyzzyvaAttacksInjected += engine->GetEquivocationInjectedCount();
                if (malicious.count(application->GetReplicaId())) continue;
                if (application->GetReplicaId() == config.failedNodeId && config.failureStopMs == 0) continue;
                if (engine) result.zyzzyvaRollbacks += engine->GetRollbackCount();
                if (engine) {
                    result.zyzzyvaEquivocations += engine->GetEquivocationCount();
                    result.zyzzyvaSpeculativeExecutions += engine->GetSpeculativeExecutionCount();
                    result.zyzzyvaReexecutedRequests += engine->GetReexecutedRequestCount();
                    result.zyzzyvaCancelledExecutions += engine->GetCancelledExecutionCount();
                    uint64_t size = engine->GetExecutedSize();
                    auto committedHistory = engine->GetCommittedHistoryDigests();
                    const auto& applicationHistory = engine->GetApplicationHistory();
                    if (first) {
                        referenceCommittedHistory = std::move(committedHistory);
                        referenceApplicationHistory = applicationHistory;
                        result.replicaProgressMin = result.replicaProgressMax = size;
                        first = false;
                    } else {
                        const size_t common = std::min(referenceCommittedHistory.size(), committedHistory.size());
                        for (size_t index = 0; index < common; ++index)
                            result.replicaStateAgreement &= referenceCommittedHistory[index] == committedHistory[index];
                        const size_t applicationCommon = std::min(referenceApplicationHistory.size(), applicationHistory.size());
                        for (size_t index = 0; index < applicationCommon; ++index)
                            result.applicationHistoryAgreement &= referenceApplicationHistory[index] == applicationHistory[index];
                        result.replicaProgressMin = std::min(result.replicaProgressMin, size);
                        result.replicaProgressMax = std::max(result.replicaProgressMax, size);
                    }
                }
            }
        }
        if (config.architecture == "ezbft") {
            bool first = true;
            std::map<uint64_t, uint64_t> referenceDigests;
            for (uint32_t i = 0; i < replicas.GetN(); ++i) {
                Ptr<ConsensusApplication> application = DynamicCast<ConsensusApplication>(replicas.Get(i));
                Ptr<EzBftEngine> engine = DynamicCast<EzBftEngine>(application->GetEngine());
                if (!engine) continue;
                result.ezbftEquivocations += engine->GetEquivocationCount();
                result.ezbftSpeculativeExecutions += engine->GetSpeculativeCount();
                result.ezbftFinalExecutions += engine->GetFinalExecutionCount();
                result.ezbftOwnerChanges += engine->GetOwnerChanges();
                result.ezbftReexecutions += engine->GetReexecutionCount();
                result.ezbftLegalUpdates += engine->GetLegalUpdateCount();
                auto digests = engine->GetFinalTupleDigests();
                uint64_t count = digests.size();
                if (first) {
                    referenceDigests = std::move(digests);
                    result.replicaProgressMin = result.replicaProgressMax = count;
                    first = false;
                } else {
                    result.replicaStateAgreement &= referenceDigests == digests;
                    result.replicaProgressMin = std::min(result.replicaProgressMin, count);
                    result.replicaProgressMax = std::max(result.replicaProgressMax, count);
                }
            }
        }
        result.interrupted = WasConsensusStopRequested();
        result.success = !result.interrupted && result.completedRequests == expectedRequests;
        if (config.architecture == "zyzzyva") {
            result.success &= result.replicaStateAgreement && result.replicaProgressMin >= expectedRequests;
            if (!config.zyzzyvaEquivocationEnabled) result.success &= result.replicaProgressMax == expectedRequests;
        }
        if (writeResults) {
            result.confirmedSafetyConflict = !result.replicaStateAgreement;
            statistics->WriteResults(config.outputPrefix, config);
            WriteManifest(config, result);
        }
        Simulator::Destroy();
        return result;
    }

    ConsensusRunResult ConsensusRunner::RunBidl(const ConsensusExperimentConfig& config, bool writeResults) const {
        // 计算终端节点数量
        const uint32_t networkClients = config.bidlIngressMode == "network" ? config.GetEffectiveClientCount() : 0;
        const uint32_t maliciousNodes = config.GetEffectiveBidlMaliciousNodeCount();

        // 创建拓扑
        ConsensusTopology topology = config.BuildTopology(2 * config.replicaCount + 1 + networkClients + maliciousNodes);
        ConsensusRolePlacement placement = config.ResolveBidlPlacement(topology);
        const uint16_t port = config.GetConsensusPort(9100);
        constexpr uint32_t firstExecutorId = 100;
        constexpr uint32_t sequencerId = 200;
        constexpr uint32_t firstMaliciousId = 300;
        constexpr uint32_t firstClientId = 1000;

        // 建立逻辑 ID 到网络 endpoint 的映射
        std::vector<uint32_t> ordererIds;
        std::vector<uint32_t> executorIds;
        for (uint32_t index = 0; index < config.replicaCount; ++index) {
            ordererIds.push_back(index);
            executorIds.push_back(firstExecutorId + index);
        }
        std::vector<uint32_t> sequencerIds{sequencerId};
        if (config.bidlNativeSequencing) sequencerIds.insert(sequencerIds.end(), ordererIds.begin(), ordererIds.end());
        std::vector<uint32_t> maliciousIds;
        for (uint32_t index = 0; index < maliciousNodes; ++index) {
            maliciousIds.push_back(firstMaliciousId + index);
        }

        ConsensusEndpointMap endpoints;
        for (uint32_t index = 0; index < config.replicaCount; ++index) {
            uint32_t ordererPhysicalId = placement.ordererPhysicalIds[index];
            uint32_t executorPhysicalId = placement.executorPhysicalIds[index];
            endpoints.emplace(ordererIds[index], InetSocketAddress(topology.nodeAddresses[ordererPhysicalId], port));
            endpoints.emplace(executorIds[index], InetSocketAddress(topology.nodeAddresses[executorPhysicalId], port));
        }
        uint32_t sequencerPhysicalId = placement.ingressPhysicalId;
        endpoints.emplace(sequencerIds.front(), InetSocketAddress(topology.nodeAddresses[sequencerPhysicalId], port));
        for (uint32_t index = 0; index < maliciousIds.size(); ++index) {
            endpoints.emplace(maliciousIds[index],
                              InetSocketAddress(topology.nodeAddresses[placement.maliciousPhysicalIds[index]], port));
        }
        for (uint32_t client = 0; client < placement.clientPhysicalIds.size(); ++client) {
            uint32_t physicalId = placement.clientPhysicalIds[client];
            endpoints.emplace(firstClientId + client, InetSocketAddress(topology.nodeAddresses[physicalId], port));
        }

        // 将配置字段写入对象工厂
        ConsensusHelper helper;
        config.ConfigureHelper(helper, &topology);
        helper.SetClientAttribute("ReplyQuorum", UintegerValue(config.bidlNativeSequencing ? config.faultTolerance + 1 : 1));
        helper.SetClientAttribute("BidlClient", BooleanValue(true));
        helper.SetBidlAttribute("RequireClientAuthentication", BooleanValue(true));
        helper.SetResourceAttribute("Mode", EnumValue(RESOURCE_QUEUED));

        // 实例化并连接每一种应用
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
        ApplicationContainer sequencerApplication =
            helper.InstallBidlNode(topology.nodes.Get(sequencerPhysicalId), sequencerIds.front(), BIDL_SEQUENCER, ordererIds, executorIds,
                                   sequencerIds, endpoints);
        applications.Add(sequencerApplication);
        Ptr<BidlApplication> sequencer = DynamicCast<BidlApplication>(sequencerApplication.Get(0));
        for (uint32_t index = 0; index < maliciousIds.size(); ++index) {
            applications.Add(helper.InstallBidlNode(topology.nodes.Get(placement.maliciousPhysicalIds[index]), maliciousIds[index],
                                                    BIDL_MALICIOUS, ordererIds, executorIds, sequencerIds, endpoints));
        }
        for (uint32_t index = 0; index < applications.GetN(); ++index) {
            Ptr<BidlApplication> application = DynamicCast<BidlApplication>(applications.Get(index));
            if (application) {
                application->SetBroadcasterIds(maliciousIds);
            }
        }
        ApplicationContainer clients;
        std::vector<ConsensusWorkloadClient> workloadClients;
        if (config.bidlIngressMode == "network") {
            for (uint32_t client = 0; client < placement.clientPhysicalIds.size(); ++client) {
                uint32_t physicalId = placement.clientPhysicalIds[client];
                ApplicationContainer installed = helper.InstallClient(topology.nodes.Get(physicalId), firstClientId + client,
                                                                      sequencerIds.front(), config.bidlNativeSequencing ? ordererIds : sequencerIds, endpoints);
                clients.Add(installed);
                workloadClients.push_back({physicalId, DynamicCast<ConsensusClientApplication>(installed.Get(0))});
            }
        }
        const Time start = config.GetApplicationStart();
        const Time measurementStart = config.GetMeasurementStart();
        const Time measurementStop = config.GetMeasurementStop();
        const Time stop = Seconds(config.simulationTime);

        Ptr<ConsensusWorkloadProvider> workload;
        uint64_t expectedRequests = 0;
        if (config.bidlIngressMode == "direct") {
            expectedRequests = ScheduleDirectBidlWorkload(config, sequencer, measurementStart, measurementStop);
        } else {
            workload = CreateConsensusWorkloadProvider(config);
            expectedRequests = workload->ScheduleRequests(config, workloadClients, topology, placement, measurementStart, measurementStop);
        }
        ApplicationContainer background = InstallBackground(config, topology, start);

        applications.Start(start);
        applications.Stop(stop);
        clients.Start(start);
        clients.Stop(stop);
        background.Start(start);
        background.Stop(stop);

        Ptr<ConsensusStatistics> statistics = CreateObject<ConsensusStatistics>();
        statistics->SetMeasurementWindow(measurementStart, measurementStop);
        statistics->SetDiagnosticEvents(config.diagnosticEvents);
        statistics->SetDetailedEvents(config.transactionPathEvents, config.backgroundFlowEvents, config.iorsMode != 0);
        statistics->SetEventOutputPrefix(config.outputPrefix);
        statistics->AttachBidlApplications(applications);
        for (const auto& client : workloadClients) {
            statistics->AttachClient(client.application);
        }
        AttachNetworkStatistics(statistics, config, topology, background);

        Simulator::Stop(stop);
        Simulator::Run();
        ConsensusRunResult result = CollectResult(statistics, expectedRequests);
        CollectIorsResult(topology, result);
        result.committedBlocks = measuredExecutor->GetCommittedBlocks();
        bool firstExecutor = true;
        std::map<uint64_t, uint64_t> referenceDigest;
        for (uint32_t index = 0; index < applications.GetN(); ++index) {
            Ptr<BidlApplication> application = DynamicCast<BidlApplication>(applications.Get(index));
            result.bidlDenylistSize = std::max(result.bidlDenylistSize, application->GetDenylistSize());
            result.bidlDeferredTransactions += application->GetDenylistDeferredTransactions();
            const uint64_t progress = application->GetCommittedTransactions();
            if (!application->IsExecutor()) continue;
            if (firstExecutor) {
                referenceDigest = application->GetCommittedBlockHistory();
                result.replicaProgressMin = result.replicaProgressMax = progress;
                firstExecutor = false;
            } else {
                const auto currentHistory = application->GetCommittedBlockHistory();
                for (const auto& [blockId, digest] : referenceDigest) {
                    auto samePosition = currentHistory.find(blockId);
                    if (samePosition != currentHistory.end()) result.replicaStateAgreement &= digest == samePosition->second;
                }
                result.replicaProgressMin = std::min(result.replicaProgressMin, progress);
                result.replicaProgressMax = std::max(result.replicaProgressMax, progress);
            }
        }
        if (config.bidlIngressMode == "direct") {
            // Direct ingress has no client application. The sequencer's unique
            // committed sequence map is the completion authority; executor
            // block counters can include re-proposed ranges after view changes.
            result.completedTransactions = statistics->GetCompletedTransactions();
            result.completedRequests = result.completedTransactions / config.transactionsPerRequest;
            result.requestThroughput = statistics->GetRequestThroughput();
            result.transactionThroughput = statistics->GetTransactionThroughput();
        }
        result.success = config.bidlIngressMode == "direct"
                             ? result.completedTransactions == expectedRequests * config.transactionsPerRequest
                             : result.completedRequests == expectedRequests;
        const uint64_t expectedTransactions = expectedRequests * config.transactionsPerRequest;
        result.interrupted = WasConsensusStopRequested();
        result.success &= !result.interrupted;
        result.success &= result.replicaStateAgreement && result.replicaProgressMin >= expectedTransactions &&
                          result.replicaProgressMax >= expectedTransactions;
        if (writeResults) {
            result.confirmedSafetyConflict = !result.replicaStateAgreement;
            statistics->WriteResults(config.outputPrefix, config);
            WriteManifest(config, result);
        }
        Simulator::Destroy();
        return result;
    }

} // namespace ns3
