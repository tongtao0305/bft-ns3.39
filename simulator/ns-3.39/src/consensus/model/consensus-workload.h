#ifndef CONSENSUS_WORKLOAD_H
#define CONSENSUS_WORKLOAD_H

#include "ns3/application.h"
#include "ns3/event-id.h"
#include "ns3/nstime.h"
#include "ns3/object.h"
#include "ns3/ptr.h"
#include "ns3/traced-callback.h"

#include <cstdint>
#include <vector>

namespace ns3 {

    class ConsensusClientApplication;
    class ConsensusTransport;
    class Packet;
    struct ConsensusExperimentConfig;
    struct ConsensusRolePlacement;
    struct ConsensusTopology;

    struct ConsensusWorkloadClient {
        uint32_t physicalId{0};
        Ptr<ConsensusClientApplication> application;
    };

    struct RdmaWorkloadFlow {
        uint32_t peerId{0};
        uint64_t flowId{0};
        uint32_t bytes{0};
        Time offset{Seconds(0)};
    };

    /** Generates long, short, trace-driven, and incast RDMA background flows. */
    class RdmaWorkloadApplication : public Application {
        public:
        static TypeId GetTypeId();

        RdmaWorkloadApplication();

        void SetNodeId(uint32_t nodeId);
        void SetTransport(Ptr<ConsensusTransport> transport);
        void AddFlow(const RdmaWorkloadFlow& flow);

        protected:
        void DoDispose() override;

        private:
        void StartApplication() override;
        void StopApplication() override;
        void SendFlow(RdmaWorkloadFlow flow);
        void ReceiveFlow(uint32_t peerId, Ptr<Packet> packet);

        uint32_t m_nodeId;
        uint64_t m_nextMessageId;
        Ptr<ConsensusTransport> m_transport;
        std::vector<RdmaWorkloadFlow> m_flows;
        std::vector<EventId> m_events;
        bool m_running;

        TracedCallback<uint32_t, uint32_t, uint64_t, uint32_t> m_flowStartedTrace;
        TracedCallback<uint32_t, uint32_t, uint64_t, uint32_t, Time> m_flowCompletedTrace;
    };

    /** Replaceable source of client transactions for a complete experiment. */
    class ConsensusWorkloadProvider : public Object {
        public:
        static TypeId GetTypeId();

        virtual uint64_t ScheduleRequests(const ConsensusExperimentConfig& config, const std::vector<ConsensusWorkloadClient>& clients,
                                          const ConsensusTopology& topology, const ConsensusRolePlacement& placement, Time start,
                                          Time stop) = 0;
    };

    /** Open-loop, closed-loop, and burst synthetic transaction generator. */
    class SyntheticConsensusWorkloadProvider : public ConsensusWorkloadProvider {
        public:
        static TypeId GetTypeId();

        uint64_t ScheduleRequests(const ConsensusExperimentConfig& config, const std::vector<ConsensusWorkloadClient>& clients,
                                  const ConsensusTopology& topology, const ConsensusRolePlacement& placement, Time start,
                                  Time stop) override;

        private:
        void Submit(uint32_t clientIndex);
        void OnCompleted(uint32_t clientIndex, uint32_t clientId, uint64_t requestId, Time latency);

        std::vector<Ptr<ConsensusClientApplication>> m_clients;
        std::vector<uint32_t> m_submitted;
        uint32_t m_requestCount{0};
        uint32_t m_requestSize{0};
        uint32_t m_transactionsPerRequest{0};
        Time m_thinkTime{Seconds(0)};
        Time m_stop{Seconds(0)};
    };

    /** Replays app_id=1 records as transactions from their physical source hosts. */
    class LegacyConsensusWorkloadProvider : public ConsensusWorkloadProvider {
        public:
        static TypeId GetTypeId();

        uint64_t ScheduleRequests(const ConsensusExperimentConfig& config, const std::vector<ConsensusWorkloadClient>& clients,
                                  const ConsensusTopology& topology, const ConsensusRolePlacement& placement, Time start,
                                  Time stop) override;
    };

    Ptr<ConsensusWorkloadProvider> CreateConsensusWorkloadProvider(const ConsensusExperimentConfig& config);

} // namespace ns3

#endif // CONSENSUS_WORKLOAD_H
