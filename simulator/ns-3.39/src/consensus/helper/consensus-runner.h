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
