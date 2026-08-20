#include "consensus-workload.h"

#include "ns3/abort.h"
#include "ns3/boolean.h"
#include "ns3/consensus-application.h"
#include "ns3/consensus-experiment.h"
#include "ns3/consensus-transport.h"
#include "ns3/object-factory.h"
#include "ns3/simulator.h"

#include <algorithm>
#include <map>
#include <stdexcept>

namespace ns3 {

    NS_OBJECT_ENSURE_REGISTERED(ConsensusWorkloadProvider);
    NS_OBJECT_ENSURE_REGISTERED(SyntheticConsensusWorkloadProvider);
    NS_OBJECT_ENSURE_REGISTERED(LegacyConsensusWorkloadProvider);
    NS_OBJECT_ENSURE_REGISTERED(RdmaWorkloadApplication);

    TypeId RdmaWorkloadApplication::GetTypeId() {
        static TypeId tid = TypeId("ns3::RdmaWorkloadApplication")
                                .SetParent<Application>()
                                .SetGroupName("Consensus")
                                .AddConstructor<RdmaWorkloadApplication>()
                                .AddTraceSource("FlowStarted", "An RDMA background flow is submitted.",
                                                MakeTraceSourceAccessor(&RdmaWorkloadApplication::m_flowStartedTrace),
                                                "ns3::RdmaWorkloadApplication::FlowStartedTracedCallback")
                                .AddTraceSource("FlowCompleted", "A complete RDMA background flow is received.",
                                                MakeTraceSourceAccessor(&RdmaWorkloadApplication::m_flowCompletedTrace),
                                                "ns3::RdmaWorkloadApplication::FlowCompletedTracedCallback");
        return tid;
    }

    RdmaWorkloadApplication::RdmaWorkloadApplication() : m_nodeId(0), m_nextMessageId(1), m_running(false) {}

    void RdmaWorkloadApplication::SetNodeId(uint32_t nodeId) {
        m_nodeId = nodeId;
    }

    void RdmaWorkloadApplication::SetTransport(Ptr<ConsensusTransport> transport) {
        m_transport = transport;
    }

    void RdmaWorkloadApplication::AddFlow(const RdmaWorkloadFlow& flow) {
        m_flows.push_back(flow);
    }

    void RdmaWorkloadApplication::DoDispose() {
        m_transport = nullptr;
        Application::DoDispose();
    }

    void RdmaWorkloadApplication::StartApplication() {
        NS_ABORT_MSG_IF(!m_transport, "RdmaWorkloadApplication requires a transport");
        m_transport->SetReceiveCallback(MakeCallback(&RdmaWorkloadApplication::ReceiveFlow, this));
        m_transport->Start(GetNode());
        m_running = true;
        for (const RdmaWorkloadFlow& flow : m_flows) {
            m_events.push_back(Simulator::Schedule(flow.offset, &RdmaWorkloadApplication::SendFlow, this, flow));
        }
    }

    void RdmaWorkloadApplication::StopApplication() {
        m_running = false;
        for (EventId& event : m_events) {
            if (event.IsRunning()) {
                Simulator::Cancel(event);
            }
        }
        m_events.clear();
        if (m_transport) {
            m_transport->Stop();
        }
    }

    void RdmaWorkloadApplication::SendFlow(RdmaWorkloadFlow flow) {
        if (!m_running || flow.bytes == 0 || flow.peerId == m_nodeId) {
            return;
        }
        ConsensusMessage message;
        message.protocol = ConsensusProtocol::GENERIC;
        message.type = ConsensusMessageType::RDMA_BACKGROUND_FLOW;
        message.senderId = m_nodeId;
        message.receiverId = flow.peerId;
        message.instanceId = flow.flowId;
        message.messageId = m_nextMessageId++;
        message.view = Simulator::Now().GetNanoSeconds();
        message.payloadSize = flow.bytes;
        message.digest = flow.flowId;
        message.transactionCount = 1;
        m_flowStartedTrace(m_nodeId, flow.peerId, flow.flowId, flow.bytes);
        ConsensusTransportMetadata metadata = ConsensusTransportMetadata::FromMessage(message);
        metadata.flowId = flow.flowId;
        m_transport->Send(flow.peerId, message.ToPacket(), metadata);
    }

    void RdmaWorkloadApplication::ReceiveFlow(uint32_t peerId, Ptr<Packet> packet) {
        ConsensusMessage message;
        if (!m_running || !ConsensusMessage::FromPacket(packet, message) || message.type != ConsensusMessageType::RDMA_BACKGROUND_FLOW ||
            message.senderId != peerId || message.receiverId != m_nodeId) {
            return;
        }
        Time started = NanoSeconds(message.view);
        Time fct = Simulator::Now() >= started ? Simulator::Now() - started : Seconds(0);
        m_flowCompletedTrace(m_nodeId, peerId, message.instanceId, message.payloadSize, fct);
    }

    TypeId ConsensusWorkloadProvider::GetTypeId() {
        static TypeId tid = TypeId("ns3::ConsensusWorkloadProvider").SetParent<Object>().SetGroupName("Consensus");
        return tid;
    }

    TypeId SyntheticConsensusWorkloadProvider::GetTypeId() {
        static TypeId tid = TypeId("ns3::SyntheticConsensusWorkloadProvider")
                                .SetParent<ConsensusWorkloadProvider>()
                                .SetGroupName("Consensus")
                                .AddConstructor<SyntheticConsensusWorkloadProvider>();
        return tid;
    }

    uint64_t SyntheticConsensusWorkloadProvider::ScheduleRequests(const ConsensusExperimentConfig& config,
                                                                  const std::vector<ConsensusWorkloadClient>& clients,
                                                                  const ConsensusTopology& topology,
                                                                  const ConsensusRolePlacement& placement, Time start, Time stop) {
        (void)topology;
        (void)placement;
        if (clients.empty()) {
            throw std::invalid_argument("synthetic workload requires at least one client");
        }
        m_requestCount = config.requestCount;
        m_requestSize = config.requestSize;
        m_transactionsPerRequest = config.transactionsPerRequest;
        m_thinkTime = MicroSeconds(config.closedLoopThinkTimeUs);
        m_stop = stop;
        const Time ready = start + NanoSeconds(1);
        m_clients.clear();
        m_submitted.assign(clients.size(), 0);
        for (const auto& client : clients) {
            if (!client.application) {
                throw std::invalid_argument("synthetic workload received a null client");
            }
            client.application->SetAttribute("AutoGenerate", BooleanValue(false));
            m_clients.push_back(client.application);
        }

        if (config.workload == "closed-loop") {
            for (uint32_t index = 0; index < m_clients.size(); ++index) {
                m_clients[index]->TraceConnectWithoutContext(
                    "RequestCompleted", MakeCallback(&SyntheticConsensusWorkloadProvider::OnCompleted, this).Bind(index));
                Simulator::Schedule(ready - Simulator::Now(), &SyntheticConsensusWorkloadProvider::Submit, this, index);
            }
        } else if (config.workload == "burst") {
            for (uint32_t client = 0; client < m_clients.size(); ++client) {
                for (uint32_t request = 0; request < config.requestCount; ++request) {
                    uint32_t burst = request / config.burstSize;
                    Time submission = ready + MilliSeconds(burst * config.burstIntervalMs);
                    if (submission < stop) {
                        Simulator::Schedule(submission - Simulator::Now(), &SyntheticConsensusWorkloadProvider::Submit, this, client);
                    }
                }
            }
        } else {
            Time interval = Seconds(1.0 / config.requestRate);
            for (uint32_t client = 0; client < m_clients.size(); ++client) {
                Time offset = interval * client / m_clients.size();
                for (uint32_t request = 0; request < config.requestCount; ++request) {
                    Time submission = ready + offset + interval * request;
                    if (submission < stop) {
                        Simulator::Schedule(submission - Simulator::Now(), &SyntheticConsensusWorkloadProvider::Submit, this, client);
                    }
                }
            }
        }
        return static_cast<uint64_t>(config.requestCount) * clients.size();
    }

    void SyntheticConsensusWorkloadProvider::Submit(uint32_t clientIndex) {
        if (clientIndex >= m_clients.size() || m_submitted[clientIndex] >= m_requestCount || Simulator::Now() >= m_stop) {
            return;
        }
        m_clients[clientIndex]->SubmitRequest(m_requestSize, m_transactionsPerRequest);
        ++m_submitted[clientIndex];
    }

    void SyntheticConsensusWorkloadProvider::OnCompleted(uint32_t clientIndex, uint32_t clientId, uint64_t requestId, Time latency) {
        (void)clientId;
        (void)requestId;
        (void)latency;
        if (clientIndex < m_submitted.size() && m_submitted[clientIndex] < m_requestCount && Simulator::Now() + m_thinkTime < m_stop) {
            Simulator::Schedule(m_thinkTime, &SyntheticConsensusWorkloadProvider::Submit, this, clientIndex);
        }
    }

    TypeId LegacyConsensusWorkloadProvider::GetTypeId() {
        static TypeId tid = TypeId("ns3::LegacyConsensusWorkloadProvider")
                                .SetParent<ConsensusWorkloadProvider>()
                                .SetGroupName("Consensus")
                                .AddConstructor<LegacyConsensusWorkloadProvider>();
        return tid;
    }

    uint64_t LegacyConsensusWorkloadProvider::ScheduleRequests(const ConsensusExperimentConfig& config,
                                                               const std::vector<ConsensusWorkloadClient>& clients,
                                                               const ConsensusTopology& topology, const ConsensusRolePlacement& placement,
                                                               Time start, Time stop) {
        (void)topology;
        std::map<uint32_t, Ptr<ConsensusClientApplication>> byPhysicalId;
        for (const auto& client : clients) {
            client.application->SetAttribute("AutoGenerate", BooleanValue(false));
            byPhysicalId[client.physicalId] = client.application;
        }
        uint64_t scheduled = 0;
        for (const LegacyFlowRecord& flow : ReadLegacyFlowFile(config.flowFile)) {
            if (flow.applicationId != 1) {
                continue;
            }
            auto client = byPhysicalId.find(flow.source);
            if (client == byPhysicalId.end() || flow.destination != placement.ingressPhysicalId) {
                throw std::invalid_argument("app_id=1 flow does not match workload placement");
            }
            Time submission = std::max(start + NanoSeconds(1), NanoSeconds(flow.startTimeNs));
            if (submission < stop) {
                Simulator::Schedule(submission - Simulator::Now(), &ConsensusClientApplication::SubmitRequest, client->second, flow.bytes,
                                    1);
                ++scheduled;
            }
        }
        if (scheduled == 0) {
            throw std::invalid_argument("trace workload contains no transaction in the run window");
        }
        return scheduled;
    }

    Ptr<ConsensusWorkloadProvider> CreateConsensusWorkloadProvider(const ConsensusExperimentConfig& config) {
        std::string typeName = config.workloadProvider;
        if (typeName.empty()) {
            typeName = (config.workload == "trace" || (config.workload == "auto" && config.HasLegacyBlockchainWorkload()))
                           ? "ns3::LegacyConsensusWorkloadProvider"
                           : "ns3::SyntheticConsensusWorkloadProvider";
        }
        ObjectFactory factory;
        factory.SetTypeId(typeName);
        Ptr<ConsensusWorkloadProvider> provider = factory.Create<ConsensusWorkloadProvider>();
        if (!provider) {
            throw std::invalid_argument("workload provider is not a ConsensusWorkloadProvider: " + typeName);
        }
        return provider;
    }

} // namespace ns3
