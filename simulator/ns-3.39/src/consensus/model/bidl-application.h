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
#include <utility>
#include <vector>

namespace ns3 {

    enum BidlRole { BIDL_SEQUENCER = 0, BIDL_ORDERER = 1, BIDL_EXECUTOR = 2, BIDL_MALICIOUS = 3 };

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

    struct BidlBlockState {
        uint64_t blockId{0};
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
        std::set<uint32_t> orderedSenders;
        std::set<uint32_t> persistSenders;
    };

    /**
     * Simplified BIDL workflow: the sequencer immediately broadcasts numbered
     * transactions, orderers agree on hash lists, and executors run transactions
     * speculatively while ordering is in progress.
     */
    class BidlApplication : public Application {
        public:
        static TypeId GetTypeId();

        /** A lightweight marker that tests or fault models can use for an invalid transaction. */
        static constexpr uint64_t INVALID_TRANSACTION_DIGEST = 0xbad0bad0bad0bad0ULL;

        BidlApplication();

        void SetNodeId(uint32_t nodeId);
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
        uint64_t GetSequencedTransactions() const;
        uint64_t GetSequencerCommittedTransactions() const;
        bool IsSequencer() const;
        double GetTransactionThroughput() const;
        Ptr<ConsensusEngine> GetEngine() const;

        protected:
        void DoDispose() override;

        private:
        void StartApplication() override;
        void StopApplication() override;
        void SampleExecutionBacklog();

        void EnqueueTransaction(const ClientRequest& transaction);
        void CompleteSequencerService(uint32_t clientId, uint64_t requestId, uint32_t payloadSize, uint32_t transactionCount,
                                      uint64_t digest);
        void SequenceTransaction(const ClientRequest& transaction);
        void ReplyToClient(uint64_t sequence, uint64_t blockId);
        void VerifyTransaction(ConsensusMessage message);
        void CompleteTransactionVerification(ConsensusMessage message);
        void TryProcessPendingExecutorMessages();
        bool StoreTransaction(const ConsensusMessage& message);
        bool ValidateBlock(uint64_t firstSequence, uint32_t count, uint64_t digest) const;
        uint64_t ComputeBlockDigest(uint64_t firstSequence, uint32_t count) const;
        void TryProposeBlocks(bool flushPartial);
        void ProposeBlock(uint64_t firstSequence, uint32_t count);
        void FlushPartialBlock();
        void TryProcessPendingProposals();

        void OnTransportReceive(uint32_t peerId, Ptr<Packet> packet);
        void OnEngineSend(uint32_t peerId, const ConsensusMessage& message);
        void OnEngineDecision(const ConsensusDecision& decision);
        void CompleteEngineDecision(uint64_t requestId);
        void HandleSequencerMessage(const ConsensusMessage& message);
        void HandleOrdererMessage(const ConsensusMessage& message);
        void HandleExecutorMessage(const ConsensusMessage& message);
        void HandleMaliciousMessage(const ConsensusMessage& message);
        void ForgeTransaction(ConsensusMessage message);
        void TraceRejectedConflict(const ConsensusMessage& message);

        void TryExecuteNextTransaction();
        void CompleteTransaction(uint64_t sequence);
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
        std::map<uint64_t, uint64_t> m_digestSequences;
        std::map<uint64_t, Time> m_transactionArrivalTimes;
        std::map<uint64_t, Time> m_transactionReceiveTimes;
        std::map<uint64_t, Time> m_transactionExecutionTimes;
        std::map<uint64_t, Time> m_consensusStartTimes;
        std::map<uint64_t, ConsensusDecision> m_pendingConsensusDecisions;
        std::map<std::pair<uint32_t, uint64_t>, Time> m_ingressArrivalTimes;
        std::map<uint64_t, BidlBlockState> m_blocks;
        std::map<uint64_t, ConsensusMessage> m_pendingBlockProposals;
        std::multimap<uint64_t, ConsensusMessage> m_pendingExecutorMessages;
        std::set<uint64_t> m_executedTransactions;
        std::set<uint64_t> m_invalidTransactions;
        std::set<uint64_t> m_verificationPending;
        std::map<uint64_t, uint64_t> m_verificationDigests;
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
        TracedCallback<uint32_t, uint64_t, Time> m_transactionCommittedTrace;
        TracedCallback<uint32_t, uint64_t, uint32_t, Time, Time> m_blockCommitTimeTrace;
        TracedCallback<uint32_t, uint32_t, uint16_t, uint64_t, uint32_t> m_messageTxTrace;
        TracedCallback<uint32_t, uint32_t, uint16_t, uint64_t, uint32_t> m_messageRxTrace;
        TracedCallback<uint32_t, uint64_t, uint16_t, Time> m_phaseChangedTrace;
        TracedCallback<uint32_t, uint64_t, uint32_t, uint64_t> m_forgedTransactionTrace;
        TracedCallback<uint32_t, uint64_t, uint32_t> m_conflictRejectedTrace;
        TracedCallback<uint32_t, uint64_t, uint32_t, Time> m_blockReexecutionTrace;
        TracedCallback<uint32_t, uint32_t, uint32_t, uint32_t> m_executionBacklogTrace;
    };

} // namespace ns3

#endif // BIDL_APPLICATION_H
