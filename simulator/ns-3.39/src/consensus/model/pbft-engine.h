#ifndef PBFT_ENGINE_H
#define PBFT_ENGINE_H

#include "consensus-engine.h"

#include "ns3/event-id.h"
#include "ns3/nstime.h"
#include "ns3/traced-callback.h"

#include <map>
#include <set>
#include <tuple>
#include <utility>
#include <vector>

namespace ns3 {

    enum PbftPhase : uint16_t { PBFT_PROPOSED = 0, PBFT_PREPARED = 1, PBFT_DECIDED = 2 };

    /**
     * Per-view, per-sequence state used by the PBFT normal path.
     */
    struct PbftInstance {
        uint64_t view{0};
        uint64_t sequence{0};
        uint64_t digest{0};
        uint32_t clientId{0};
        uint64_t requestId{0};
        uint32_t transactionCount{0};

        bool prePrepareAccepted{false};
        bool prepareSent{false};
        bool prepared{false};
        bool commitSent{false};
        bool decided{false};

        std::set<uint32_t> prepareSenders;
        std::set<uint32_t> commitSenders;
        std::vector<ConsensusMessage> pendingMessages;
        EventId timeoutEvent;
        uint32_t retransmissions{0};

        Time proposalTime{Seconds(0)};
        Time preparedTime{Seconds(0)};
        Time decidedTime{Seconds(0)};
    };

    struct PbftViewChangeState {
        struct PreparedRecord {
            uint64_t view{0};
            uint64_t sequence{0};
            uint64_t digest{0};
            uint32_t clientId{0};
            uint64_t requestId{0};
            uint32_t transactionCount{0};
            std::set<uint32_t> prepareSenders;
        };
        std::set<uint32_t> senders;
        std::map<uint32_t, std::vector<PreparedRecord>> reports;
        std::map<uint64_t, PreparedRecord> preparedHistory;
        bool localVoteSent{false};
        bool newViewSent{false};
        uint32_t retransmissions{0};
        EventId timeoutEvent;
    };

    /**
     * \ingroup consensus
     * PBFT performance engine with normal-path quorum processing, retransmission,
     * and prepared-history transfer during view changes. This packet-level model
     * retains quorum identities but does not model PBFT's cryptographic proofs.
     */
    class PbftEngine : public ConsensusEngine {
        public:
        static TypeId GetTypeId();

        PbftEngine();

        void Configure(uint32_t replicaId, const std::vector<uint32_t>& replicaIds) override;
        void Start() override;
        void Stop() override;
        void Submit(const ClientRequest& request) override;
        void Receive(const ConsensusMessage& message) override;

        uint64_t GetDecidedCount() const;
        uint64_t GetCurrentView() const;
        void RequestViewChange();
        uint32_t GetLeaderId() const override;
        uint32_t GetCurrentPrimary() const;
        const std::map<std::pair<uint64_t, uint64_t>, PbftInstance>& GetInstances() const;

        private:
        using InstanceKey = std::pair<uint64_t, uint64_t>;

        void ProcessMessage(ConsensusMessage message);
        void HandlePrePrepare(const ConsensusMessage& message);
        void HandlePrepare(const ConsensusMessage& message);
        void HandleCommit(const ConsensusMessage& message);
        void HandleViewChange(const ConsensusMessage& message);
        void HandleNewView(const ConsensusMessage& message);

        void SendPrepare(PbftInstance& instance);
        void TryPrepare(PbftInstance& instance);
        void SendCommit(PbftInstance& instance);
        void TryDecide(PbftInstance& instance);
        void ScheduleInstanceTimeout(PbftInstance& instance);
        void HandleInstanceTimeout(uint64_t view, uint64_t sequence);
        void StartViewChange(uint64_t newView);
        void HandleViewChangeTimeout(uint64_t view);
        void TryEnterView(uint64_t view);
        void EnterView(uint64_t view);
        std::vector<PbftViewChangeState::PreparedRecord> LocalPreparedHistory(uint64_t target) const;
        bool ValidatePreparedReport(const std::vector<PbftViewChangeState::PreparedRecord>& report, uint64_t target) const;
        bool SelectPreparedHistory(const std::map<uint32_t, std::vector<PbftViewChangeState::PreparedRecord>>& reports,
                                   std::map<uint64_t, PbftViewChangeState::PreparedRecord>& selected) const;
        static void Write32(std::vector<uint8_t>& bytes, uint32_t value);
        static void Write64(std::vector<uint8_t>& bytes, uint64_t value);
        static bool Read32(const std::vector<uint8_t>& bytes, size_t& offset, uint32_t& value);
        static bool Read64(const std::vector<uint8_t>& bytes, size_t& offset, uint64_t& value);
        static void EncodeRecord(std::vector<uint8_t>& bytes, const PbftViewChangeState::PreparedRecord& record);
        static bool DecodeRecord(const std::vector<uint8_t>& bytes, size_t& offset,
                                 PbftViewChangeState::PreparedRecord& record);
        static void EncodeReport(std::vector<uint8_t>& bytes,
                                 const std::vector<PbftViewChangeState::PreparedRecord>& report);
        static bool DecodeReport(const std::vector<uint8_t>& bytes, size_t& offset,
                                 std::vector<PbftViewChangeState::PreparedRecord>& report);
        void ProposeRequest(const ClientRequest& request);
        void ProposePendingRequests();
        void Broadcast(ConsensusMessage message);

        PbftInstance& GetInstance(uint64_t view, uint64_t sequence);
        ConsensusMessage MakeMessage(ConsensusMessageType type, const PbftInstance& instance);
        bool IsReplica(uint32_t replicaId) const;
        uint32_t GetPrimaryForView(uint64_t view) const;
        Time GetMessageProcessingTime(const ConsensusMessage& message) const;

        uint32_t m_primaryId;
        uint32_t m_faultTolerance;
        uint32_t m_controlPayloadBytes;
        Time m_messageProcessingTime;
        Time m_prePrepareProcessingTime;
        Time m_prepareProcessingTime;
        Time m_commitProcessingTime;
        Time m_viewChangeProcessingTime;
        Time m_retransmissionTimeout;
        Time m_instanceTimeout;
        uint32_t m_maxRetransmissions;
        uint64_t m_view;
        uint64_t m_nextSequence;
        uint64_t m_nextMessageId;
        uint64_t m_decidedCount;
        bool m_running;

        std::map<InstanceKey, PbftInstance> m_instances;
        std::map<uint64_t, uint64_t> m_decidedBySequence;
        std::map<uint64_t, PbftViewChangeState> m_viewChanges;
        std::map<uint64_t, ConsensusMessage> m_pendingNewViews;
        std::map<std::pair<uint32_t, uint64_t>, ClientRequest> m_pendingRequests;
        std::map<std::pair<uint32_t, uint64_t>, EventId> m_requestTimeouts;
        std::set<std::tuple<uint64_t, uint32_t, uint64_t>> m_proposedRequests;
        TracedCallback<uint32_t, uint64_t, uint32_t> m_viewChangedTrace;
        TracedCallback<uint32_t, uint64_t, uint16_t, Time> m_phaseChangedTrace;
    };

} // namespace ns3

#endif // PBFT_ENGINE_H
