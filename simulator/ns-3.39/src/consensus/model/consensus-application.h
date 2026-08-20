#ifndef CONSENSUS_APPLICATION_H
#define CONSENSUS_APPLICATION_H

#include "consensus-engine.h"
#include "consensus-message.h"
#include "consensus-transport.h"

#include "ns3/address.h"
#include "ns3/application.h"
#include "ns3/event-id.h"
#include "ns3/traced-callback.h"

#include <map>
#include <set>
#include <vector>

namespace ns3 {

    /**
     * \ingroup consensus
     * Protocol-independent application installed on every consensus replica.
     */
    class ConsensusApplication : public Application {
        public:
        static TypeId GetTypeId();

        ConsensusApplication();

        void SetEngine(Ptr<ConsensusEngine> engine);
        void SetTransport(Ptr<ConsensusTransport> transport);
        void SetResourceModel(Ptr<ConsensusResourceModel> resourceModel);
        void SetReplicaId(uint32_t replicaId);
        void SetReplicaIds(const std::vector<uint32_t>& replicaIds);

        uint32_t GetReplicaId() const;
        uint64_t GetDecisionCount() const;
        Ptr<ConsensusEngine> GetEngine() const;

        protected:
        void DoDispose() override;

        private:
        void StartApplication() override;
        void StopApplication() override;

        void OnTransportReceive(uint32_t peerId, Ptr<Packet> packet);
        void OnEngineSend(uint32_t peerId, const ConsensusMessage& message);
        void OnEngineDecision(const ConsensusDecision& decision);
        void SendMessage(uint32_t peerId, ConsensusMessage message);

        Ptr<ConsensusEngine> m_engine;
        Ptr<ConsensusTransport> m_transport;
        Ptr<ConsensusResourceModel> m_resourceModel;
        uint32_t m_replicaId;
        std::vector<uint32_t> m_replicaIds;
        uint64_t m_nextMessageId;
        uint64_t m_decisionCount;
        bool m_running;

        TracedCallback<uint32_t, uint32_t, uint16_t, uint64_t, uint32_t> m_messageTxTrace;
        TracedCallback<uint32_t, uint32_t, uint16_t, uint64_t, uint32_t> m_messageRxTrace;
        TracedCallback<uint32_t, uint64_t, uint64_t, Time> m_decisionTrace;
    };

    /**
     * \ingroup consensus
     * Client workload and configurable matching-reply collector.
     */
    class ConsensusClientApplication : public Application {
        public:
        static TypeId GetTypeId();

        ConsensusClientApplication();

        void SetTransport(Ptr<ConsensusTransport> transport);
        void SetReplicaIds(const std::vector<uint32_t>& replicaIds);
        uint64_t SubmitRequest(uint32_t requestSize, uint32_t transactionCount = 1);
        uint64_t GetCompletedRequests() const;
        uint64_t GetCompletedTransactions() const;
        Time GetAverageLatency() const;
        double GetRequestThroughput() const;
        double GetTransactionThroughput() const;

        protected:
        void DoDispose() override;

        private:
        struct ReplyState {
            Time submitted{Seconds(0)};
            uint64_t digest{0};
            uint32_t payloadSize{0};
            uint32_t transactionCount{0};
            std::set<uint32_t> senders;
            uint32_t attempts{0};
            EventId timeoutEvent;
            bool completed{false};
        };

        void StartApplication() override;
        void StopApplication() override;
        void GenerateRequest();
        void SendRequest(uint64_t requestId);
        void HandleRequestTimeout(uint64_t requestId);
        void OnTransportReceive(uint32_t peerId, Ptr<Packet> packet);

        Ptr<ConsensusTransport> m_transport;
        uint32_t m_clientId;
        uint32_t m_primaryId;
        uint32_t m_faultTolerance;
        uint32_t m_replyQuorum;
        uint32_t m_requestSize;
        uint32_t m_transactionsPerRequest;
        uint32_t m_maxRequests;
        uint32_t m_maxRetransmissions;
        bool m_autoGenerate;
        Time m_interval;
        Time m_initialDelay;
        Time m_requestTimeout;
        std::vector<uint32_t> m_replicaIds;
        uint64_t m_nextRequestId;
        uint64_t m_completedRequests;
        uint64_t m_completedTransactions;
        Time m_totalLatency;
        Time m_firstSubmission;
        Time m_lastCompletion;
        bool m_hasSubmission;
        EventId m_generateEvent;
        std::map<uint64_t, ReplyState> m_requests;
        bool m_running;

        TracedCallback<uint32_t, uint64_t, Time> m_requestSubmittedTrace;
        TracedCallback<uint32_t, uint64_t, Time> m_requestCompletedTrace;
        TracedCallback<uint32_t, uint64_t, uint32_t> m_requestRetransmittedTrace;
        TracedCallback<uint32_t, uint64_t> m_requestTimedOutTrace;
    };

} // namespace ns3

#endif // CONSENSUS_APPLICATION_H
