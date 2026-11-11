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
#include <tuple>
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
        uint32_t GetVerificationBacklog() const { return m_verificationPending; }
        uint64_t GetDecisionCount() const;
        Ptr<ConsensusEngine> GetEngine() const;

        protected:
        void DoDispose() override;

        private:
        void StartApplication() override;
        void StopApplication() override;

        void OnTransportReceive(uint32_t peerId, Ptr<Packet> packet);
        void CompleteVerifiedMessage(ConsensusMessage message);
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
        Time m_verificationTime{MicroSeconds(1)};
        uint32_t m_verificationPending{0};

        TracedCallback<bool, const ConsensusMessage&> m_messageEventTrace;
        TracedCallback<uint32_t, uint32_t, uint16_t, uint64_t, uint32_t> m_messageTxTrace;
        TracedCallback<uint32_t, uint32_t, uint16_t, uint64_t, uint32_t> m_messageRxTrace;
        TracedCallback<uint32_t, uint32_t, uint16_t, uint64_t, uint64_t, uint64_t, uint32_t, uint64_t, uint8_t, uint8_t> m_candidateTrace;
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
        uint32_t GetRequestTransactionCount(uint64_t requestId) const;
        uint8_t GetRequestCompletionPath(uint64_t requestId) const;
        uint64_t GetAbortedRequests() const { return m_abortedRequests; }
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
            std::map<std::pair<uint64_t, uint64_t>, std::set<uint32_t>> abortVoters;
            using ZyzzyvaKey = std::tuple<uint64_t, uint64_t, uint64_t>;
            std::map<ZyzzyvaKey, std::set<uint32_t>> speculativeReplies;
            std::map<ZyzzyvaKey, std::map<uint32_t, std::vector<uint8_t>>> speculativeSignatures;
            std::map<uint64_t, ConsensusMessage> primaryOrderProofs;
            bool pomSent{false};
            std::map<ZyzzyvaKey, std::set<uint32_t>> localCommits;
            std::set<ZyzzyvaKey> certificatesSent;
            using EzBftKey = std::tuple<uint32_t, uint64_t, uint64_t, uint64_t>;
            std::map<EzBftKey, std::set<uint32_t>> ezSpeculativeReplies;
            std::map<EzBftKey, std::vector<uint8_t>> ezTuplePayloads;
            std::map<EzBftKey, std::map<uint32_t, std::vector<uint8_t>>> ezReplySignatures;
            std::set<EzBftKey> ezCommitsSent;
            uint32_t attempts{0};
            EventId timeoutEvent;
            EventId fastPathEvent;
            bool fastPathExpired{false};
            bool completed{false};
            uint8_t completionPath{0}; // 0=generic, 1=Zyzzyva fast, 2=Zyzzyva slow
        };

        void StartApplication() override;
        void StopApplication() override;
        void GenerateRequest();
        void SendRequest(uint64_t requestId);
        void HandleRequestTimeout(uint64_t requestId);
        void HandleFastPathTimeout(uint64_t requestId);
        void TryZyzzyvaSlowPath(uint64_t requestId);
        void SendClientMessage(uint32_t peer, const ConsensusMessage& message);
        void OnTransportReceive(uint32_t peerId, Ptr<Packet> packet);
        void CompleteRequest(uint64_t requestId, ReplyState& state, uint32_t transactionCount);

        Ptr<ConsensusTransport> m_transport;
        uint32_t m_clientId;
        uint32_t m_primaryId;
        uint32_t m_faultTolerance;
        uint32_t m_replyQuorum;
        bool m_zyzzyva{false};
        bool m_requirePrimaryProofs{false};
        uint64_t m_zyzzyvaClientView{0};
        bool m_bidl{false};
        bool m_ezbft{false};
        uint32_t m_requestSize;
        uint32_t m_transactionsPerRequest;
        uint32_t m_maxRequests;
        uint32_t m_maxRetransmissions;
        bool m_autoGenerate;
        Time m_interval;
        Time m_initialDelay;
        Time m_requestTimeout;
        Time m_fastPathTimeout{MilliSeconds(1)};
        std::vector<uint32_t> m_replicaIds;
        uint64_t m_nextRequestId;
        uint64_t m_completedRequests;
        uint64_t m_completedTransactions;
        uint64_t m_abortedRequests{0};
        Time m_totalLatency;
        Time m_firstSubmission;
        Time m_lastCompletion;
        bool m_hasSubmission;
        EventId m_generateEvent;
        std::map<uint64_t, ReplyState> m_requests;
        bool m_running;

        TracedCallback<uint32_t, uint64_t, Time> m_requestSubmittedTrace;
        TracedCallback<uint32_t, uint64_t, Time> m_requestCompletedTrace;
        TracedCallback<uint32_t, uint64_t, Time> m_requestAbortedTrace;
        TracedCallback<uint32_t, uint64_t, uint32_t> m_requestRetransmittedTrace;
        TracedCallback<uint32_t, uint64_t> m_requestTimedOutTrace;
        TracedCallback<bool, const ConsensusMessage&> m_messageEventTrace;
        TracedCallback<uint32_t, uint32_t, uint16_t, uint64_t, uint32_t> m_messageTxTrace;
        TracedCallback<uint32_t, uint32_t, uint16_t, uint64_t, uint32_t> m_messageRxTrace;
    };

} // namespace ns3

#endif // CONSENSUS_APPLICATION_H
