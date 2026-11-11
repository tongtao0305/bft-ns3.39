#ifndef ZYZZYVA_ENGINE_H
#define ZYZZYVA_ENGINE_H

#include "consensus-engine.h"
#include "ns3/event-id.h"

#include <map>
#include <set>
#include <string>
#include <tuple>
#include <vector>

namespace ns3 {

    enum ZyzzyvaRecoveryPhase : uint16_t {
        ZYZZYVA_NEW_VIEW_INSTALLED = 14,
        ZYZZYVA_RECOVERY_STARTED = 15,
        ZYZZYVA_UNDO_COMPLETED = 16,
        ZYZZYVA_RECOVERY_COMPLETED = 17,
        ZYZZYVA_VIEW_CHANGE_STARTED = 18
    };

    /** Communication and state model of Zyzzyva's speculative replication paths. */
    class ZyzzyvaEngine : public ConsensusEngine {
        friend class ZyzzyvaRecoveryTestCase;
        friend class ZyzzyvaProgressTestCase;
      public:
        static TypeId GetTypeId();
        ZyzzyvaEngine();
        void Configure(uint32_t replicaId, const std::vector<uint32_t>& replicaIds) override;
        void Start() override;
        void Stop() override;
        void Submit(const ClientRequest& request) override;
        void Receive(const ConsensusMessage& message) override;
        uint32_t GetLeaderId() const override;
        uint64_t GetExecutionBacklog() const;
        uint64_t GetBufferedOrderCount() const { return m_futureOrders.size(); }
        bool IsRecovering() const { return m_recovering; }
        uint64_t GetView() const { return m_view; }
        uint64_t GetRollbackCount() const { return m_rollbacks; }
        uint64_t GetEquivocationCount() const { return m_equivocations; }
        uint64_t GetEquivocationInjectedCount() const { return m_equivocationInjectedViews.size(); }
        uint64_t GetSpeculativeExecutionCount() const { return m_speculativeExecutions; }
        uint64_t GetReexecutedRequestCount() const { return m_reexecutedRequests; }
        uint64_t GetCancelledExecutionCount() const { return m_cancelledExecutions; }
        uint64_t GetLogFingerprint() const;
        uint64_t GetLogSize() const { return m_log.size(); }
        uint64_t GetExecutedSize() const { return m_applicationHistory.size(); }
        uint64_t GetApplicationFingerprint() const { return m_applicationHistory.empty() ? 0 : m_applicationHistory.back(); }
        const std::vector<uint64_t>& GetApplicationHistory() const { return m_applicationHistory; }
        std::vector<uint64_t> GetCommittedHistoryDigests() const {
            std::vector<uint64_t> history;
            for (const auto& entry : m_log) {
                if (entry.sequence > m_committedThrough) break;
                history.push_back(entry.digest);
            }
            return history;
        }

      private:
        struct Entry {
            uint64_t sequence{0};
            uint64_t view{0};
            uint64_t digest{0};
            uint64_t history{0};
            uint32_t client{0};
            uint64_t request{0};
            uint32_t transactions{0};
            uint32_t payloadSize{0};
            std::vector<uint8_t> clientAuthentication;
        };
        struct Certificate {
            uint64_t view{0};
            uint64_t sequence{0};
            uint64_t digest{0};
            uint64_t history{0};
            std::set<uint32_t> voters;
            std::map<uint32_t, std::vector<uint8_t>> signatures;
        };
        struct ViewProof {
            uint32_t sender{0};
            uint64_t targetView{0};
            std::vector<Entry> log;
            Certificate certificate;
            std::map<uint32_t, std::vector<uint8_t>> hateProofs;
            std::vector<uint8_t> signature;
        };

        static uint64_t Extend(uint64_t previous, uint64_t digest, uint64_t sequence);
        static void Write32(std::vector<uint8_t>& bytes, uint32_t value);
        static void Write64(std::vector<uint8_t>& bytes, uint64_t value);
        static bool Read32(const std::vector<uint8_t>& bytes, size_t& offset, uint32_t& value);
        static bool Read64(const std::vector<uint8_t>& bytes, size_t& offset, uint64_t& value);
        static void EncodeEntry(std::vector<uint8_t>& bytes, const Entry& entry);
        static bool DecodeEntry(const std::vector<uint8_t>& bytes, size_t& offset, Entry& entry);
        static void EncodeProof(std::vector<uint8_t>& bytes, const ViewProof& proof);
        static bool DecodeProof(const std::vector<uint8_t>& bytes, size_t& offset, ViewProof& proof);
        bool ValidateLog(const std::vector<Entry>& log) const;
        bool ValidateProof(const ViewProof& proof) const;
        bool Extends(const std::vector<Entry>& longer, const std::vector<Entry>& prefix) const;
        std::vector<Entry> SelectOriginal(const std::vector<ViewProof>& proofs) const;
        std::vector<Entry> SelectVelma(const std::vector<ViewProof>& proofs) const;
        std::vector<Entry> SelectHistory(const std::vector<ViewProof>& proofs) const;
        void Propose(const ClientRequest& request);
        void AcceptOrder(const ConsensusMessage& message);
        void OnGateTimeout(uint64_t view, uint64_t sequence);
        void HandleCommitRequest(const ConsensusMessage& message);
        void HandleFillHole(const ConsensusMessage& message);
        void RequestFillHole(uint64_t view, uint64_t first, uint64_t last);
        void HandleRepair(const ConsensusMessage& message);
        void HandleCheckpoint(const ConsensusMessage& message);
        void HandleIorsEvidenceRequest(const ConsensusMessage& message);
        void HandleIorsEvidenceResponse(const ConsensusMessage& message);
        void HandlePom(const ConsensusMessage& message);
        void StartIorsEvidenceRequest(const ConsensusMessage& markedOrder);
        void RetryIorsEvidenceRequest(ConsensusMessage markedOrder, uint32_t attempt);
        void TriggerHate(const ConsensusMessage* first = nullptr, const ConsensusMessage* second = nullptr);
        void AccuseView(uint64_t view);
        void OnNewViewTimeout(uint64_t target);
        void HandleHate(const ConsensusMessage& message);
        void StartViewChange(uint64_t target);
        void HandleViewChange(const ConsensusMessage& message);
        void HandleNewView(const ConsensusMessage& message);
        void OnProgressTimeout(uint64_t view);
        void ArmProgressTimer();
        void BroadcastMessage(ConsensusMessage message);
        void SendSpecResponse(const Entry& entry);
        ConsensusMessage MakeOrderProof(const Entry& entry) const;
        void SendDelayedOrder(uint32_t peer, ConsensusMessage message);
        void QueueExecution(const Entry& entry, bool replay);
        void CompleteExecution(uint64_t generation, Entry entry, bool replay, Time queuedAt);
        void BeginReplay(uint64_t generation, uint64_t prefix);
        void FinishRecovery();

        uint32_t m_faultTolerance{1};
        uint32_t m_recoveryRule{1}; // 0: original, 1: Velma
        bool m_iorsTerminalGate{false};
        bool m_viewChangeEnabled{true};
        std::set<std::pair<uint64_t, uint64_t>> m_iorsEvidenceRequests;
        std::map<std::pair<uint64_t, uint64_t>, EventId> m_iorsEvidenceRetryEvents;
        std::map<std::pair<uint64_t, uint64_t>, Time> m_fillHoleSentAt;
        bool m_requireClientAuthentication{false};
        Time m_gateTimeout{MilliSeconds(1)};
        Time m_viewTimeout{MilliSeconds(50)};
        Time m_executionTime{MicroSeconds(15)};
        Time m_rollbackTime{MicroSeconds(15)};
        uint32_t m_checkpointInterval{10};
        bool m_equivocationEnabled{false};
        std::string m_maliciousReplicaIds;
        std::string m_equivocationPlan;
        std::map<uint64_t, std::tuple<uint32_t, Time, uint32_t>> m_attackPlan;
        std::set<uint64_t> m_equivocationInjectedViews;
        uint32_t m_equivocationTarget{1};
        Time m_equivocationDelay{MicroSeconds(20)};
        uint64_t m_equivocationStartSequence{1};
        uint64_t m_equivocationSlot{0};
        Time m_equivocationHoldUntil{Seconds(0)};
        uint64_t m_view{0};
        uint64_t m_pendingView{0};
        uint32_t m_viewTimeoutBackoff{0};
        uint64_t m_nextSequence{1};
        uint64_t m_stableCheckpoint{0};
        uint64_t m_committedThrough{0};
        bool m_running{false};
        uint64_t m_rollbacks{0};
        uint64_t m_equivocations{0};
        uint64_t m_speculativeExecutions{0};
        uint64_t m_executionGeneration{0};
        uint64_t m_reexecutedRequests{0};
        uint64_t m_cancelledExecutions{0};
        std::map<std::pair<uint32_t, uint64_t>, uint64_t> m_executedRequests;
        bool m_recovering{false};
        Time m_recoveryStartedAt{Seconds(0)};
        std::vector<uint64_t> m_applicationHistory;
        std::map<uint64_t, std::tuple<Entry, bool, Time>> m_readyExecutions;
        TracedCallback<uint32_t, uint32_t, uint16_t, uint64_t, uint64_t, uint64_t, uint32_t, uint64_t, uint8_t, uint8_t> m_executionCandidateTrace;
        std::map<uint64_t, ConsensusMessage> m_pendingCommits;
        std::map<std::pair<uint32_t, uint64_t>, ClientRequest> m_recoveryRequests;
        std::vector<Entry> m_log;
        std::map<uint64_t, Certificate> m_certificates;
        std::map<std::pair<uint64_t, uint64_t>, ConsensusMessage> m_markedPending;
        std::map<std::pair<uint64_t, uint64_t>, ConsensusMessage> m_seenOrders;
        std::set<std::pair<uint64_t, uint64_t>> m_conflictedSlots;
        std::map<std::pair<uint64_t, uint64_t>, ConsensusMessage> m_futureOrders;
        std::map<uint64_t, ConsensusMessage> m_newViews;
        std::map<std::pair<uint64_t, uint64_t>, EventId> m_gateEvents;
        std::map<uint64_t, std::set<uint32_t>> m_hateVotes;
        std::set<uint64_t> m_suspectedViews;
        std::set<uint64_t> m_equivocationEvidenceViews;
        std::set<uint64_t> m_evidenceBroadcastViews;
        std::map<uint64_t, std::map<uint32_t, std::vector<uint8_t>>> m_hateProofs;
        std::map<uint64_t, std::map<uint32_t, ViewProof>> m_viewProofs;
        std::map<std::pair<uint64_t, uint64_t>, std::set<uint32_t>> m_checkpointVotes;
        std::set<std::pair<uint32_t, uint64_t>> m_seenRequests;
        std::map<std::pair<uint32_t, uint64_t>, uint64_t> m_requestSequences;
        std::set<std::pair<uint32_t, uint64_t>> m_pendingForwards;
        EventId m_progressEvent;
        EventId m_newViewEvent;
    };

} // namespace ns3

#endif
