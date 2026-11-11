#ifndef CONSENSUS_RUNNER_H
#define CONSENSUS_RUNNER_H

#include "consensus-experiment.h"

#include <cstdint>
#include <vector>

namespace ns3 {

    /** Summary returned by a complete consensus experiment run. */
    struct ConsensusRunResult {
        bool success{false};
        uint64_t expectedRequests{0};
        uint64_t completedRequests{0};
        uint64_t completedTransactions{0};
        uint64_t committedBlocks{0};
        double requestThroughput{0.0};
        double transactionThroughput{0.0};
        double latencyP50Us{0.0};
        double latencyP95Us{0.0};
        double latencyP99Us{0.0};
        uint64_t iorsObserved{0};
        uint64_t iorsMarked{0};
        uint64_t iorsWaited{0};
        uint64_t iorsAlreadyMarked{0};
        uint64_t iorsCapacityBypass{0};
        uint64_t iorsCollisionBypass{0};
        uint64_t iorsUnauthorized{0};
        uint64_t iorsEquivocation{0};
        uint64_t iorsHoldBypass{0};
        uint64_t iorsExpired{0};
        uint32_t bidlDenylistSize{0};
        uint64_t bidlDeferredTransactions{0};
        uint64_t viewChanges{0};
        uint64_t zyzzyvaRollbacks{0};
        uint64_t zyzzyvaEquivocations{0};
        uint64_t zyzzyvaAttacksInjected{0};
        uint64_t zyzzyvaSpeculativeExecutions{0};
        uint64_t zyzzyvaReexecutedRequests{0};
        uint64_t abortedRequests{0};
        uint64_t zyzzyvaCancelledExecutions{0};
        uint64_t ezbftEquivocations{0};
        uint64_t ezbftSpeculativeExecutions{0};
        uint64_t ezbftFinalExecutions{0};
        uint64_t ezbftOwnerChanges{0};
        uint64_t ezbftReexecutions{0};
        uint64_t ezbftLegalUpdates{0};
        bool replicaStateAgreement{true};
        bool applicationHistoryAgreement{true};
        bool confirmedSafetyConflict{false};
        uint64_t replicaProgressMin{0};
        uint64_t replicaProgressMax{0};
    };

    struct ConsensusBatchResult {
        bool success{false};
        std::vector<ConsensusRunResult> runs;
    };

    /** Builds, runs, measures, and validates one configured experiment. */
    class ConsensusRunner {
        public:
        ConsensusRunResult Run(const ConsensusExperimentConfig& config, bool writeResults = true) const;
        ConsensusBatchResult RunBatch(const ConsensusExperimentConfig& config, uint32_t runCount, bool writeIndividualResults = true) const;

        private:
        ConsensusRunResult RunReplicated(const ConsensusExperimentConfig& config, bool writeResults) const;
        ConsensusRunResult RunBidl(const ConsensusExperimentConfig& config, bool writeResults) const;
    };

} // namespace ns3

#endif // CONSENSUS_RUNNER_H
