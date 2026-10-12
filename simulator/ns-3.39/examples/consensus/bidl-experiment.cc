#include "ns3/consensus-experiment.h"
#include "ns3/consensus-runner.h"

#include <iomanip>
#include <iostream>

using namespace ns3;

int main(int argc, char* argv[]) {
    ConsensusExperimentConfig config;
    config.ApplyPreset("pbft-tcp");
    config.outputPrefix = "consensus-results";
    uint32_t batchRuns = 1;
    bool writeIndividualResults = true;

    config.PrepareFromCommandLine(argc, argv);
    CommandLine command(__FILE__);
    config.AddCommandLineOptions(command);
    command.AddValue("batchRuns", "Number of independent runs; one runs a single experiment", batchRuns);
    command.AddValue("writeIndividualResults", "Write each run when batchRuns is greater than one", writeIndividualResults);
    command.Parse(argc, argv);

    ConsensusRunner runner;
    if (batchRuns > 1) {
        ConsensusBatchResult result = runner.RunBatch(config, batchRuns, writeIndividualResults);
        std::cout << "runs=" << result.runs.size() << " status=" << (result.success ? "complete" : "incomplete")
                  << " aggregate=" << config.outputPrefix << "-aggregate.csv" << std::endl;
        return result.success ? 0 : 1;
    }

    ConsensusRunResult result = runner.Run(config);
    std::cout << "completed=" << result.completedRequests << " transactions=" << result.completedTransactions
              << " blocks=" << result.committedBlocks << " p50=" << std::fixed << std::setprecision(2) << result.latencyP50Us << " us"
              << " p95=" << result.latencyP95Us << " us p99=" << result.latencyP99Us << " us"
              << " request_throughput=" << result.requestThroughput << " req/s"
              << " transaction_throughput=" << result.transactionThroughput << " tx/s"
              << " output=" << config.outputPrefix << "-*.csv" << std::endl;
    return result.success ? 0 : 1;
}
