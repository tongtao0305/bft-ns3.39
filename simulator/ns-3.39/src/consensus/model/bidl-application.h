#ifndef BIDL_APPLICATION_H
#define BIDL_APPLICATION_H

#include "consensus-engine.h"
#include "consensus-message.h"
#include "consensus-transport.h"

#include "ns3/application.h"
#include "ns3/event-id.h"
#include "ns3/random-variable-stream.h"
#include "ns3/traced-callback.h"

#include <map>
#include <set>
#include <tuple>
#include <utility>
#include <vector>

namespace ns3 {

    // CandidateEvent phases are part of the CSV schema. Keep terminal decisions
    // distinct from ordinary transaction lifecycle events.
    enum BidlCandidateEventPhase : uint8_t {
        BIDL_CANDIDATE_SPECULATIVE_EXECUTION = 2,
        BIDL_CANDIDATE_MARKED_WAIT = 3,
        BIDL_CANDIDATE_MARKED_REPLACED = 4,
        BIDL_CANDIDATE_MARKED_TIMEOUT = 5,
        BIDL_CANDIDATE_VERIFICATION_START = 6,
        BIDL_CANDIDATE_CORRECTION_COMPLETE = 7,
        BIDL_CANDIDATE_VERIFICATION_COMPLETE = 8,
        BIDL_CANDIDATE_EXECUTION_QUEUED = 10,
        BIDL_CANDIDATE_CORRECTION_QUEUED = 11,
        BIDL_CANDIDATE_NORMAL_COMMIT = 12,
        BIDL_CANDIDATE_OLD_BRANCH_REPLACED = 13
    };

    enum BidlRole { BIDL_SEQUENCER = 0, BIDL_ORDERER = 1, BIDL_EXECUTOR = 2, BIDL_MALICIOUS = 3 };

    enum class BidlRepairReason : uint8_t { GAP_REQUEST = 0, DUPLICATE_CLIENT_REQUEST = 1 };

    enum BidlPhase : uint16_t {
        BIDL_BLOCK_PROPOSED = 0,
        BIDL_EXECUTION_COMPLETE = 1,
        BIDL_ORDERED = 2,
        BIDL_STORAGE_COMPLETE = 3,
        BIDL_BLOCK_COMMITTED = 4,
        BIDL_APPROVAL_COMPLETE = 5,
        BIDL_P1_CONSENSUS = 6,
        BIDL_P2_VERIFY_EXECUTE = 7,
        BIDL_P3_PERSIST = 8,
        BIDL_P4_EXECUTION_TOTAL = 9,
        BIDL_P5_COMMIT = 10,
        BIDL_END_TO_END = 11
    };

    struct BidlTransactionRecord {
        uint64_t sequence{0};
        ClientRequest request;
        uint64_t ingressTimeNs{0};
    };

    struct BidlBlockState {
        uint64_t blockId{0};
        uint64_t orderView{0};
        uint64_t digest{0};
        uint32_t transactionCount{0};
        Time firstSeen{Seconds(0)};
        Time executionStartedAt{Seconds(0)};
        Time executionCompletedAt{Seconds(0)};
        Time persistenceStartedAt{Seconds(0)};
        Time storageCompletedAt{Seconds(0)};
        Time reexecutionStartedAt{Seconds(0)};
        bool ordered{false};
        bool executionComplete{false};
        bool reexecutionScheduled{false};
        bool reexecutionComplete{false};
        bool requiresReexecution{false};
        bool approvalScheduled{false};
        bool approvalComplete{false};
        bool storageScheduled{false};
        bool persistSent{false};
        bool commitScheduled{false};
        bool committed{false};
        bool aborted{false};
        std::set<uint32_t> orderedSenders;
        std::set<uint32_t> persistSenders;
        std::map<uint32_t, uint64_t> persistVectorBySender;
        std::map<uint64_t, std::set<uint32_t>> persistVectorVoters;
        std::map<uint32_t, uint64_t> executionResults;
        std::vector<uint8_t> resultVector;
        uint64_t resultVectorDigest{0};
        bool resultSubmitted{false};
        bool resultVectorReceived{false};
        std::vector<BidlTransactionRecord> canonicalTransactions;
    };

    /**
     * Simplified BIDL workflow: the sequencer immediately broadcasts numbered
     * transactions, orderers agree on hash lists, and executors run transactions
     * speculatively while ordering is in progress.
     */
    class BidlApplication : public Application {
        friend class BidlAuthenticationTestCase;
        friend class BidlRepairSourceTestCase;
        friend class BidlCanonicalRecoveryTestCase;
        friend class BidlReplayAdmissionTestCase;
        public:
        static TypeId GetTypeId();

        /** A lightweight marker that tests or fault models can use for an invalid transaction. */
        static constexpr uint64_t INVALID_TRANSACTION_DIGEST = 0xbad0bad0bad0bad0ULL;

        BidlApplication();

        void SetNodeId(uint32_t nodeId);
        uint32_t GetNodeId() const { return m_nodeId; }
        void SetRole(BidlRole role);
        void SetOrdererIds(const std::vector<uint32_t>& ids);
        void SetExecutorIds(const std::vector<uint32_t>& ids);
        void SetSequencerIds(const std::vector<uint32_t>& ids);
        void SetBroadcasterIds(const std::vector<uint32_t>& ids);
        void SetEngine(Ptr<ConsensusEngine> engine);
        void SetTransport(Ptr<ConsensusTransport> transport);
        void SetResourceModel(Ptr<ConsensusResourceModel> resourceModel);

        /** Injects a transaction at the data-center-side sequencer ingress. */
        void InjectTransaction(uint32_t payloadSize, uint32_t transactionCount, uint64_t requestId);

        uint64_t GetCommittedBlocks() const;
        uint64_t GetCommittedTransactions() const;
        uint64_t GetCommittedDigest() const;
        std::map<uint64_t, uint64_t> GetCommittedBlockHistory() const;
        uint64_t GetAbortedBlocks() const;
        uint64_t GetSequencedTransactions() const;
        uint64_t GetSequencerCommittedTransactions() const;
        uint32_t GetDenylistSize() const;
        uint64_t GetDenylistDeferredTransactions() const;
        bool IsSequencer() const;
        bool IsExecutor() const { return m_role == BIDL_EXECUTOR; }
        double GetTransactionThroughput() const;
        Ptr<ConsensusEngine> GetEngine() const;
        static std::vector<uint8_t> EncodeTransactions(const std::vector<BidlTransactionRecord>& records);
        static bool DecodeTransactions(const std::vector<uint8_t>& bytes, std::vector<BidlTransactionRecord>& records);
        static void SignRequest(ClientRequest& request);
        static bool VerifyRequest(const ClientRequest& request);

        protected:
        void DoDispose() override;

        private:
        void StartApplication() override;
        void StopApplication() override;
        void SampleExecutionBacklog();

        void EnqueueTransaction(const ClientRequest& transaction);
        void CompleteSequencerService(uint32_t clientId, uint64_t requestId, uint32_t payloadSize, uint32_t transactionCount,
                                      uint64_t digest, std::vector<uint8_t> authentication);
        bool ReadRequest(const ConsensusMessage& message, ClientRequest& request) const;
        void OnOrderingViewChanged(uint32_t replica, uint64_t view, uint32_t leader);
        void HandleSequenceEpoch(const ConsensusMessage& message);
        void RetryIngress();
        void ForwardIngressRequest(const ClientRequest& request);
        void OnSequencerTimeout(uint64_t view);
        void ResumeOrdering();
        bool CanSequence() const;
        void SequenceTransaction(const ClientRequest& transaction);
        void ReplyToClient(uint64_t sequence, uint64_t blockId);
        void VerifyTransaction(ConsensusMessage message);
        bool IsTransactionReplay(const ConsensusMessage& message) const;
        void CompleteTransactionVerification(ConsensusMessage message);
        void TryProcessPendingExecutorMessages(uint64_t blockId);
        void RequestMissingTransactions(uint64_t first, uint64_t last);
        bool IsTransactionAvailableForGap(uint64_t sequence) const;
        void ObserveTransactionSequence(uint64_t sequence);
        void AdvanceGapProgress();
        void RetryMissingTransaction(uint64_t sequence, bool timerExpired = false);
        void SendSequencedTransaction(uint32_t receiver, uint64_t sequence, BidlRepairReason reason);
        bool StoreTransaction(const ConsensusMessage& message);
        bool ValidateBlock(uint64_t firstSequence, uint32_t count, uint64_t digest) const;
        uint64_t ComputeBlockDigest(uint64_t firstSequence, uint32_t count) const;
        void TryProposeBlocks(bool flushPartial);
        void ProposeBlock(uint64_t firstSequence, uint32_t count);
        void FlushPartialBlock();
        void TryProcessPendingProposals(uint64_t availableSequence);
        void IndexPendingProposal(uint64_t blockId);

        void OnTransportReceive(uint32_t peerId, Ptr<Packet> packet);
        void OnEngineSend(uint32_t peerId, const ConsensusMessage& message);
        void OnEngineDecision(const ConsensusDecision& decision);
        void CompleteEngineDecision(uint64_t requestId);
        void HandleSequencerMessage(const ConsensusMessage& message);
        void HandleOrdererMessage(const ConsensusMessage& message);
        void HandleExecutorMessage(const ConsensusMessage& message);
        void OnIorsGateTimeout(uint64_t sequence);
        void HandleMaliciousMessage(const ConsensusMessage& message);
        void ForgeTransaction(ConsensusMessage message);
        void TraceRejectedConflict(const ConsensusMessage& message);
        void HandleConflictReport(const ConsensusMessage& message);
        void HandleDenylistVote(const ConsensusMessage& message);
        void HandleExecutionResult(const ConsensusMessage& message);
        void HandleResultPersist(const ConsensusMessage& message);
        void CompleteOrdererPersist(uint64_t blockId, uint32_t transactionCount, uint64_t blockDigest, uint64_t resultDigest,
                                    const std::vector<uint8_t>& resultVector);
        void SubmitExecutionResult(BidlBlockState& block);
        void RequestCanonicalBlock(uint64_t blockId);

        void TryExecuteNextTransaction();
        void CompleteTransaction(uint64_t sequence, uint64_t generation);
        void TraceTransaction(uint64_t sequence, uint8_t phase);
        bool AllTransactionsExecuted(const BidlBlockState& block) const;
        bool BlockContainsInvalidTransaction(const BidlBlockState& block) const;
        void CompleteBlockReexecution(uint64_t blockId);
        void CompleteApproval(uint64_t blockId);
        void TryPersist(BidlBlockState& block);
        void CompleteStorage(uint64_t blockId);
        void TryCommit(BidlBlockState& block);
        void CompleteCommit(uint64_t blockId);
        void Send(uint32_t peerId, ConsensusMessage message);
        void Broadcast(const std::vector<uint32_t>& peers, ConsensusMessage message);

        Ptr<ConsensusEngine> m_engine;
        Ptr<ConsensusTransport> m_transport;
        Ptr<ConsensusResourceModel> m_resourceModel;
        uint32_t m_nodeId;
        BidlRole m_role;
        uint32_t m_faultTolerance;
        uint32_t m_batchSize;
        Time m_batchTimeout;
        Time m_sequencerServiceTime;
        Time m_transactionVerificationTime;
        Time m_executionTimePerTransaction;
        Time m_approvalTimePerBlock;
        Time m_storageTime;
        Time m_consensusSoftwareTime;
        Time m_commitProcessingTime;
        double m_attackProbability;
        Time m_attackStart;
        Time m_attackStop;
        Time m_attackProcessingTime;
        uint32_t m_attackTargetExecutors;
        bool m_iorsTerminalGate{false};
        bool m_denylistEnabled{false};
        bool m_requireClientAuthentication{false};
        uint64_t m_sequenceView{0};
        bool m_nativeSequencing{false};
        bool m_compactOrderNotifications{false};
        uint32_t m_attackMode{0};
        std::map<std::pair<uint32_t, uint64_t>, std::set<uint32_t>> m_metadataConflictReporters;
        Time m_sequencerTimeout{MilliSeconds(50)};
        EventId m_ingressRetryEvent;
        EventId m_sequencerTimeoutEvent;
        std::map<std::pair<uint32_t, uint64_t>, ClientRequest> m_pendingIngress;
        std::map<std::pair<uint32_t, uint64_t>, ConsensusMessage> m_terminalAborts;
        std::map<std::pair<uint64_t, uint32_t>, std::set<uint32_t>> m_sequenceEpochVotes;
        Time m_iorsGateTimeout{MilliSeconds(1)};
        std::map<uint64_t, ConsensusMessage> m_iorsPendingMarked;
        std::map<uint64_t, EventId> m_iorsGateEvents;
        uint64_t m_observedOrderView{0};
        std::map<uint64_t, uint32_t> m_transactionSource;
        std::map<std::pair<uint32_t, uint64_t>, std::set<uint32_t>> m_conflictReporters;
        std::map<uint32_t, std::set<uint32_t>> m_suspectLeaders;
        std::map<uint32_t, std::set<uint32_t>> m_denylistVotes;
        std::set<std::pair<uint64_t, uint32_t>> m_sentDenylistViewRecords;
        std::set<uint32_t> m_denylist;
        std::set<uint64_t> m_deferredTransactions;
        uint64_t m_deferredTotal{0};
        uint64_t m_nextTransactionSequence;
        uint64_t m_nextBlockSequence;
        uint64_t m_nextExecutionSequence;
        uint64_t m_nextMessageId;
        std::vector<uint32_t> m_ordererIds;
        std::vector<uint32_t> m_executorIds;
        std::vector<uint32_t> m_sequencerIds;
        std::vector<uint32_t> m_broadcasterIds;
        std::map<uint64_t, ClientRequest> m_transactions;
        std::map<std::pair<uint32_t, uint64_t>, uint64_t> m_clientRequestSequences;
        std::map<uint64_t, uint64_t> m_committedTransactionBlocks;
        std::map<uint64_t, std::set<uint32_t>> m_abortVotes;
        std::map<std::tuple<uint64_t, uint32_t, uint64_t>, std::set<uint32_t>> m_commitVotes;
        std::set<uint64_t> m_abortedBlockIds;
        std::map<uint64_t, uint64_t> m_digestSequences;
        std::map<uint64_t, Time> m_transactionArrivalTimes;
        std::map<uint64_t, Time> m_transactionReceiveTimes;
        std::map<uint64_t, Time> m_transactionExecutionTimes;
        std::map<uint64_t, Time> m_consensusStartTimes;
        std::map<uint64_t, ConsensusDecision> m_pendingConsensusDecisions;
        std::map<std::pair<uint32_t, uint64_t>, Time> m_ingressArrivalTimes;
        std::set<std::pair<uint32_t, uint64_t>> m_queuedIngressRequests;
        std::map<uint64_t, BidlBlockState> m_blocks;
        std::map<std::tuple<uint64_t, uint32_t, uint64_t, uint64_t>, BidlBlockState> m_orderedCandidates;
        std::map<uint64_t, uint64_t> m_orderedSequenceBlocks;
        std::map<uint64_t, uint64_t> m_transactionEpoch;
        std::map<uint64_t, ConsensusMessage> m_decidedBlocks;
        std::map<uint64_t, EventId> m_blockFetchEvents;
        uint64_t m_executionGeneration{0};
        std::map<uint64_t, std::map<uint32_t, uint64_t>> m_ordererExecutionResults;
        std::map<uint64_t, uint64_t> m_ordererPersistedResultDigests;
        std::map<uint64_t, ConsensusMessage> m_pendingBlockProposals;
        std::map<uint64_t, std::set<uint64_t>> m_proposalsWaitingForTransaction;
        std::map<uint64_t, uint64_t> m_proposalMissingTransaction;
        std::multimap<uint64_t, ConsensusMessage> m_pendingExecutorMessages;
        std::set<uint64_t> m_executedTransactions;
        std::set<uint64_t> m_invalidTransactions;
        std::set<uint64_t> m_verificationPending;
        std::map<uint64_t, uint64_t> m_verificationDigests;
        std::map<uint64_t, uint64_t> m_verificationDigestSequences;
        std::map<uint64_t, EventId> m_gapRequestEvents;
        std::map<uint64_t, uint32_t> m_gapRequestAttempts;
        uint64_t m_highestObservedTransactionSequence{0};
        uint64_t m_nextGapSequence{1};
        std::set<uint64_t> m_committedBlockIds;
        std::set<uint64_t> m_tracedBlocks;
        EventId m_batchEvent;
        EventId m_backlogEvent;
        bool m_executionBusy;
        uint64_t m_committedBlocks;
        uint64_t m_committedTransactions;
        Time m_firstCommitWindowStart;
        Time m_lastCommit;
        bool m_hasCommitWindow;
        bool m_running;
        Ptr<UniformRandomVariable> m_attackRandom;

        TracedCallback<uint32_t, uint64_t, uint32_t> m_batchCreatedTrace;
        TracedCallback<uint32_t, uint64_t, uint32_t, Time> m_blockCommittedTrace;
        TracedCallback<uint32_t, uint64_t, uint64_t, uint32_t, uint64_t, uint32_t> m_transactionFinalizedTrace;
        TracedCallback<uint32_t, uint64_t, Time> m_transactionCommittedTrace;
        TracedCallback<uint32_t, uint64_t, uint32_t, Time, Time> m_blockCommitTimeTrace;
        TracedCallback<bool, const ConsensusMessage&> m_messageEventTrace;
        TracedCallback<uint32_t, uint32_t, uint16_t, uint64_t, uint32_t> m_messageTxTrace;
        TracedCallback<uint32_t, uint32_t, uint16_t, uint64_t, uint32_t> m_messageRxTrace;
        TracedCallback<uint32_t, uint32_t, uint64_t, uint8_t, uint32_t> m_repairTxTrace;
        TracedCallback<uint32_t, uint32_t, uint16_t, uint64_t, uint64_t, uint64_t, uint32_t, uint64_t, uint8_t, uint8_t> m_candidateTrace;
        TracedCallback<uint32_t, uint64_t, uint16_t, Time> m_phaseChangedTrace;
        TracedCallback<uint32_t, uint64_t, uint32_t, uint64_t> m_forgedTransactionTrace;
        TracedCallback<uint32_t, uint64_t, uint32_t> m_conflictRejectedTrace;
        TracedCallback<uint32_t, uint64_t, uint32_t, Time> m_blockReexecutionTrace;
        TracedCallback<uint32_t, uint32_t, uint32_t, uint32_t> m_executionBacklogTrace;
    };

} // namespace ns3

#endif // BIDL_APPLICATION_H
