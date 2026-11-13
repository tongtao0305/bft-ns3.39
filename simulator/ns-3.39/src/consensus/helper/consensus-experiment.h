#ifndef CONSENSUS_EXPERIMENT_H
#define CONSENSUS_EXPERIMENT_H

#include "ns3/application-container.h"
#include "ns3/command-line.h"
#include "ns3/data-rate.h"
#include "ns3/ipv4-interface-container.h"
#include "ns3/net-device-container.h"
#include "ns3/node-container.h"
#include "ns3/nstime.h"
#include "ns3/object.h"
#include "ns3/queue-disc-container.h"

#include <cstdint>
#include <chrono>
#include <array>
#include <fstream>
#include <limits>
#include <map>
#include <set>
#include <string>
#include <tuple>
#include <vector>

namespace ns3 {

    class BidlApplication;
    class ConsensusApplication;
    struct ConsensusMessage;
    class ConsensusClientApplication;
    class ConsensusHelper;
    class Packet;
    class QueueDiscItem;
    class RdmaWorkloadApplication;
    class SwitchNode;

    enum class ConsensusSwitchEventType : uint8_t { ENQUEUE, DEQUEUE, MARK, DROP };

    struct ConsensusSwitchEvent {
        ConsensusSwitchEventType type{ConsensusSwitchEventType::ENQUEUE};
        uint32_t switchId{0};
        uint32_t deviceId{0};
        uint32_t queueId{0};
        uint16_t messageType{0};
        uint64_t flowId{0};
        uint8_t priorityGroup{0};
        uint32_t bytes{0};
        Time time{Seconds(0)};
    };

    struct LegacyTopologyLink {
        uint32_t source{0};
        uint32_t destination{0};
        std::string dataRate;
        std::string delay;
        double errorRate{0.0};
    };

    struct LegacyTopologySpec {
        uint32_t nodeCount{0};
        std::set<uint32_t> switchIds;
        std::vector<LegacyTopologyLink> links;
    };

    struct LegacyFlowRecord {
        uint32_t source{0};
        uint32_t destination{0};
        uint16_t priorityGroup{0};
        uint16_t destinationPort{0};
        uint32_t bytes{0};
        uint64_t startTimeNs{0};
        uint32_t applicationId{0};
    };

    LegacyTopologySpec ReadLegacyTopologyFile(const std::string& path);
    std::vector<LegacyFlowRecord> ReadLegacyFlowFile(const std::string& path);
    const std::vector<LegacyFlowRecord>& GetCachedLegacyFlowFile(const std::string& path);

    /** Configures switch-local queue, ECN, PFC headroom, and CC behavior. */
    class ConsensusSwitchPolicy : public Object {
        public:
        static TypeId GetTypeId();
        virtual void Configure(Ptr<Node> switchNode) = 0;
        virtual void ObserveEnqueue(const ConsensusSwitchEvent& event);
        virtual void ObserveDequeue(const ConsensusSwitchEvent& event);
        virtual void ObserveMark(const ConsensusSwitchEvent& event);
        virtual void ObserveDrop(const ConsensusSwitchEvent& event);
        uint64_t GetEnqueueEvents() const;
        uint64_t GetDequeueEvents() const;
        uint64_t GetMarkEvents() const;
        uint64_t GetDropEvents() const;
        uint64_t GetTaggedEvents() const;

        protected:
        void AttachRuntimeHooks(Ptr<Node> switchNode);

        private:
        ConsensusSwitchEvent MakeEvent(ConsensusSwitchEventType type, Ptr<const Packet> packet, uint32_t deviceId, uint32_t queueId) const;
        void OnEnqueue(uint32_t deviceId, Ptr<const Packet> packet, uint32_t queueId);
        void OnDequeue(uint32_t deviceId, Ptr<const Packet> packet, uint32_t queueId);
        void OnDrop(uint32_t deviceId, Ptr<const Packet> packet, uint32_t queueId);
        void OnMark(Ptr<const Packet> packet, uint32_t deviceId, uint32_t queueId);
        uint32_t m_switchId{0};
        uint64_t m_enqueueEvents{0};
        uint64_t m_dequeueEvents{0};
        uint64_t m_markEvents{0};
        uint64_t m_dropEvents{0};
        uint64_t m_taggedEvents{0};
    };

    class QbbSwitchPolicy : public ConsensusSwitchPolicy {
        public:
        static TypeId GetTypeId();
        QbbSwitchPolicy();
        void Configure(Ptr<Node> switchNode) override;
        void SetEcnThresholdMaps(const std::map<uint64_t, uint32_t>& kmin, const std::map<uint64_t, uint32_t>& kmax,
                                 const std::map<uint64_t, double>& pmax);

        private:
        bool m_enableEcn;
        uint32_t m_ccMode;
        uint32_t m_priorityGroup;
        uint32_t m_bufferBytes;
        uint32_t m_ecnKminBytes;
        uint32_t m_ecnKmaxBytes;
        double m_ecnPmax;
        uint32_t m_headroomBytes;
        DataRate m_linkRate;
        Time m_linkDelay;
        std::map<uint64_t, uint32_t> m_ecnKminByRate;
        std::map<uint64_t, uint32_t> m_ecnKmaxByRate;
        std::map<uint64_t, double> m_ecnPmaxByRate;
    };

    struct ConsensusTopology {
        // All hosts and switches in physical node ID order, like the legacy NodeContainer n.
        NodeContainer nodes;
        // Host addresses indexed by physical node ID. Switch entries remain 0.0.0.0.
        std::vector<Ipv4Address> nodeAddresses;
        NodeContainer endpoints;
        NodeContainer switches;
        NetDeviceContainer devices;
        Ipv4InterfaceContainer endpointInterfaces;
        QueueDiscContainer queueDiscs;
        std::vector<std::string> deviceLabels;
        std::vector<uint32_t> endpointPhysicalIds;
        std::map<uint32_t, uint32_t> physicalToEndpoint;
        std::map<std::pair<uint32_t, uint32_t>, uint64_t> pairRttNs;
        std::map<std::pair<uint32_t, uint32_t>, uint64_t> pairBdpBytes;
        std::vector<Ptr<ConsensusSwitchPolicy>> switchPolicies;
        uint64_t maxRttNs{0};
        uint64_t maxBdpBytes{0};
    };

    struct ConsensusRolePlacement {
        uint32_t clientPhysicalId{std::numeric_limits<uint32_t>::max()};
        std::vector<uint32_t> clientPhysicalIds;
        uint32_t ingressPhysicalId{std::numeric_limits<uint32_t>::max()};
        std::vector<uint32_t> replicaPhysicalIds;
        std::vector<uint32_t> ordererPhysicalIds;
        std::vector<uint32_t> executorPhysicalIds;
        std::vector<uint32_t> maliciousPhysicalIds;
    };

    /**
     * Common command-line configuration used by consensus experiments.
     */
    struct ConsensusExperimentConfig {
        std::string preset;
        std::string configFile;
        std::string architecture{"replicated"};
        std::string bidlIngressMode{"direct"};
        std::string workload{"auto"};
        std::string workloadProvider;
        std::string scenario{"consensus"};
        std::string engine{"pbft"};
        std::string topology{"csma"};
        std::string transport{"tcp"};
        std::string dataRate{"10Gbps"};
        std::string linkDelay{"10us"};
        std::string backgroundRate{"1Gbps"};
        std::string queueDisc{"ns3::FqCoDelQueueDisc"};
        std::string switchPolicy{"ns3::QbbSwitchPolicy"};
        std::string topologyFile;
        std::string flowFile;
        std::string clientPhysicalIds;
        std::string replicaPhysicalIds;
        std::string ordererPhysicalIds;
        std::string executorPhysicalIds;
        std::string bidlMaliciousPhysicalIds;
        std::string ecnKminMap;
        std::string ecnKmaxMap;
        std::string ecnPmaxMap;
        std::string zyzzyvaEquivocationPlan;
        std::string zyzzyvaMaliciousReplicaIds{"0"};
        std::string outputPrefix{"consensus-results"};
        bool diagnosticEvents{false};
        bool transactionPathEvents{false};
        bool backgroundFlowEvents{false};
        uint32_t replicaCount{4};
        uint32_t clientCount{1};
        uint32_t clientPhysicalId{std::numeric_limits<uint32_t>::max()};
        uint32_t ingressPhysicalId{std::numeric_limits<uint32_t>::max()};
        uint32_t faultTolerance{1};
        uint32_t clientReplyQuorum{0};
        uint32_t requestCount{10};
        uint32_t requestSize{1024};
        uint32_t transactionsPerRequest{1};
        uint32_t batchSize{2};
        uint32_t resourceCpuCores{1};
        uint32_t resourceStorageChannels{1};
        uint32_t bidlMaliciousNodeCount{1};
        uint32_t bidlAttackTargetExecutors{1};
        uint32_t burstSize{10};
        uint32_t backgroundFlows{0};
        uint32_t backgroundPacketSize{1400};
        uint32_t rdmaCcMode{0};
        uint32_t rdmaPriorityGroup{3};
        uint32_t rdmaMtu{1000};
        uint32_t iorsMode{0};
        bool iorsDetectOnly{false};
        uint32_t zyzzyvaRecoveryRule{1};
        bool bidlNativeSequencing{true};
        bool bidlCompactOrderNotifications{false};
        uint32_t bidlAttackMode{0};
        bool zyzzyvaEquivocationEnabled{false};
        bool zyzzyvaViewChangeEnabled{true};
        uint32_t ezbftVariant{0};
        bool ezbftEquivocationEnabled{false};
        uint32_t ezbftEquivocationTarget{1};
        double ezbftEquivocationDelayUs{20.0};
        uint32_t zyzzyvaEquivocationTarget{1};
        double zyzzyvaEquivocationDelayUs{20.0};
        uint64_t zyzzyvaEquivocationStartSequence{1};
        uint32_t iorsCapacity{4096};
        uint32_t iorsFingerprintBits{64};
        uint32_t iorsHoldCapacity{128};
        double iorsTtlUs{10000.0};
        double iorsFixedWaitUs{10.0};
        double iorsMaxWaitUs{100.0};
        bool iorsTerminalGate{false};
        double iorsGateTimeoutUs{1000.0};
        bool bidlDenylistEnabled{false};
        uint32_t rdmaAckInterval{1};
        uint32_t rdmaL2ChunkSize{4000};
        uint32_t rdmaWindow{0};
        bool rdmaHasWindow{true};
        bool rdmaGlobalRtt{true};
        bool rdmaL2BackToZero{false};
        bool rdmaVarWindow{true};
        bool rdmaFastReact{true};
        bool rdmaMultiRate{false};
        bool rdmaSampleFeedback{false};
        bool rdmaRateBound{true};
        bool rdmaClampTargetRate{false};
        bool ackHighPriority{false};
        bool qbbQcnEnabled{true};
        bool qbbPfcEnabled{true};
        bool qbbDynamicThreshold{true};
        uint32_t qbbPauseTime{5};
        uint32_t rdmaFastRecoveryTimes{1};
        uint32_t rdmaMiThreshold{0};
        uint32_t intMulti{1};
        double rdmaAlphaResumeInterval{1.0};
        double rdmaRateDecreaseInterval{4.0};
        double rdmaRpTimer{300.0};
        double rdmaEwmaGain{0.00390625};
        double rdmaTargetUtilization{0.95};
        double pintLogBase{1.01};
        double pintProbability{1.0};
        double linkErrorRate{0.0};
        std::string rdmaMinRate{"1000Mb/s"};
        std::string rdmaRateAi{"50Mb/s"};
        std::string rdmaRateHai{"50Mb/s"};
        std::string dctcpRateAi{"1000Mb/s"};
        uint32_t leafCount{2};
        uint32_t spineCount{2};
        uint32_t switchBufferBytes{24 * 1024 * 1024};
        uint32_t ecnKminBytes{100 * 1024};
        uint32_t ecnKmaxBytes{400 * 1024};
        uint32_t pfcHeadroomBytes{0};
        uint32_t rdmaBackgroundPriorityGroup{4};
        uint32_t rdmaLongFlows{0};
        uint32_t rdmaLongFlowBytes{64 * 1024 * 1024};
        double rdmaLongStartMs{1.0};
        uint32_t rdmaShortFlows{0};
        uint32_t rdmaShortFlowBytes{64 * 1024};
        double rdmaShortStartMs{1.0};
        double rdmaShortIntervalUs{100.0};
        uint32_t rdmaIncastSenders{0};
        uint32_t rdmaIncastBursts{1};
        uint32_t rdmaIncastFlowBytes{256 * 1024};
        double rdmaIncastStartMs{10.0};
        double rdmaIncastIntervalUs{1000.0};
        double rdmaConstantRateGbps{0.0};
        double ecnPmax{0.2};
        double messageDropProbability{0.0};
        double messageDelayUs{0.0};
        uint32_t failedNodeId{std::numeric_limits<uint32_t>::max()};
        double failureStartMs{0.0};
        double failureStopMs{0.0};
        uint16_t faultMessageType{0};
        bool faultAffectSend{false};
        bool enableEcn{true};
        bool bidlAttackEnabled{false};
        double requestRate{1000.0};
        double requestTimeoutMs{500.0};
        double retransmissionTimeoutMs{50.0};
        double viewChangeTimeoutMs{3000.0};
        double batchTimeoutMs{5.0};
        double sequencerServiceUs{20.0};
        double transactionVerificationUs{10.0};
        double pbftMessageProcessingUs{5.0};
        double pbftPrePrepareProcessingUs{8.0};
        double pbftPrepareProcessingUs{6.0};
        double pbftCommitProcessingUs{6.0};
        double pbftViewChangeProcessingUs{10.0};
        double executionTimePerTransactionUs{15.0};
        double approvalTimePerBlockUs{100.0};
        double storageTimePerBlockUs{500.0};
        double consensusSoftwareTimePerBlockUs{10000.0};
        double commitProcessingTimePerBlockUs{2500.0};
        double bidlAttackProbability{1.0};
        double bidlAttackStartMs{0.0};
        double bidlAttackStopMs{0.0};
        double bidlAttackProcessingUs{20.0};
        double burstIntervalMs{10.0};
        double closedLoopThinkTimeUs{0.0};
        double applicationStartTime{1.0};
        double warmupTime{0.0};
        double measurementTime{0.0};
        double cooldownTime{0.0};
        double simulationTime{2.0};
        uint32_t seed{1};
        uint64_t run{1};

        void PrepareFromCommandLine(int argc, char* argv[]);
        void ApplyPreset(const std::string& name);
        void LoadConfigFile(const std::string& path);
        static std::vector<std::string> GetAvailablePresets();
        void AddCommandLineOptions(CommandLine& command);
        void Validate() const;
        void ApplyRandomSeed() const;
        void ConfigureHelper(ConsensusHelper& helper, const ConsensusTopology* topologyResult = nullptr) const;
        std::string GetTransportType() const;
        std::string GetEngineType() const;
        bool UsesRdmaNetwork() const;
        void WriteJson(const std::string& path) const;
        ConsensusTopology BuildTopology(uint32_t endpointCount) const;
        ConsensusTopology BuildTcpTopology(uint32_t endpointCount) const;
        ConsensusTopology BuildRdmaTopology(uint32_t endpointCount) const;
        bool HasLegacyBlockchainWorkload() const;
        uint32_t GetEffectiveClientCount() const;
        uint32_t GetEffectiveBidlMaliciousNodeCount() const;
        uint16_t GetConsensusPort(uint16_t fallback) const;
        uint16_t GetConsensusPriorityGroup() const;
        Time GetApplicationStart() const;
        Time GetMeasurementStart() const;
        Time GetMeasurementStop() const;
        ConsensusRolePlacement ResolvePbftPlacement(const ConsensusTopology& topologyResult) const;
        ConsensusRolePlacement ResolveBidlPlacement(const ConsensusTopology& topologyResult) const;
        uint32_t ScheduleLegacyBlockchainWorkload(Ptr<ConsensusClientApplication> client, const ConsensusTopology& topologyResult,
                                                  const ConsensusRolePlacement& placement, Time start, Time stop) const;

        ApplicationContainer InstallBackgroundTraffic(const NodeContainer& nodes, const Ipv4InterfaceContainer& interfaces) const;
        ApplicationContainer InstallRdmaBackgroundTraffic(const ConsensusTopology& topologyResult, Time applicationStart) const;
    };

    /**
     * Aggregates protocol, client, queue, and link traces and writes analysis-ready
     * CSV files.
     */
    class ConsensusStatistics : public Object {
        public:
        static TypeId GetTypeId();

        ConsensusStatistics();

        void SetMeasurementWindow(Time start, Time stop);
        void SetDiagnosticEvents(bool enabled) { m_diagnosticEvents = enabled; }
        void SetDetailedEvents(bool paths, bool background, bool trackIors) {
            m_transactionPathEvents = paths;
            m_backgroundFlowEvents = background;
            m_trackIorsCandidates = trackIors;
        }
        void SetEventOutputPrefix(const std::string& prefix);
        void AttachClient(Ptr<ConsensusClientApplication> client);
        void AttachReplicas(const ApplicationContainer& applications);
        void AttachBidlApplications(const ApplicationContainer& applications);
        void AttachRdmaWorkloads(const ApplicationContainer& applications);
        void AttachNetDevices(const NetDeviceContainer& devices, const DataRate& linkRate, const std::vector<std::string>& labels = {});
        void AttachQueueDiscs(const QueueDiscContainer& queueDiscs);
        void AttachSwitchPolicies(const std::vector<Ptr<ConsensusSwitchPolicy>>& policies);
        void AttachIorsSwitches(const NodeContainer& switches);

        uint64_t GetCompletedRequests() const;
        uint64_t GetAbortedRequests() const { return m_abortedRequests; }
        uint64_t GetMeasuredRequests() const;
        uint64_t GetCompletedTransactions() const;
        uint64_t GetRetransmissions() const;
        uint64_t GetTimedOutRequests() const;
        uint64_t GetViewChanges() const;
        double GetRequestThroughput() const;
        double GetTransactionThroughput() const;
        double GetWindowTransactionThroughput() const;
        double GetLatencyPercentile(double percentile) const;
        double GetPhaseLatencyPercentile(const std::string& protocol, uint16_t phase, double percentile) const;

        void WriteResults(const std::string& outputPrefix, const ConsensusExperimentConfig& config) const;

        private:
        struct RequestSample {
            uint32_t clientId{0};
            uint64_t requestId{0};
            Time latency{Seconds(0)};
        };

        struct PhaseSample {
            std::string protocol;
            uint32_t nodeId{0};
            uint64_t instanceId{0};
            uint16_t phase{0};
            Time elapsed{Seconds(0)};
            Time time{Seconds(0)};
            uint64_t view{0};
        };

        struct DeviceStats {
            uint64_t fullTxPackets{0};
            uint64_t fullTxBytes{0};
            uint64_t fullRxPackets{0};
            uint64_t fullRxBytes{0};
            uint64_t fullDrops{0};
            uint64_t fullPfcPause{0};
            uint64_t fullPfcResume{0};
            uint64_t txPackets{0};
            uint64_t txBytes{0};
            uint64_t rxPackets{0};
            uint64_t rxBytes{0};
            uint64_t drops{0};
            uint64_t pfcPause{0};
            uint64_t pfcResume{0};
            DataRate rate{0};
            std::string label;
        };

        struct BackgroundFlowSample {
            uint32_t receiverId{0};
            uint32_t senderId{0};
            uint64_t flowId{0};
            uint32_t bytes{0};
            Time fct{Seconds(0)};
        };

        struct QueueStats {
            uint32_t packets{0};
            uint32_t fullMaxPackets{0};
            uint32_t maxPackets{0};
            uint32_t bytes{0};
            uint32_t fullMaxBytes{0};
            uint32_t maxBytes{0};
            uint64_t drops{0};
            uint64_t fullDrops{0};
            uint64_t marks{0};
            uint64_t fullMarks{0};
            Time totalSojourn{Seconds(0)};
            uint64_t sojournSamples{0};
            Time fullTotalSojourn{Seconds(0)};
            uint64_t fullSojournSamples{0};
        };

        void OnRequestSubmitted(ConsensusClientApplication* client, uint32_t clientId, uint64_t requestId, Time time);
        void OnRequestCompleted(ConsensusClientApplication* client, uint32_t clientId, uint64_t requestId, Time latency);
        void OnRequestAborted(uint32_t clientId, uint64_t requestId, Time latency);
        void OnRequestRetransmitted(uint32_t clientId, uint64_t requestId, uint32_t targetId);
        void OnRequestTimedOut(uint32_t clientId, uint64_t requestId);
        void OnMessage(uint32_t senderId, uint32_t receiverId, uint16_t type, uint64_t instanceId, uint32_t bytes);
        void OnCandidate(uint32_t senderId, uint32_t receiverId, uint16_t type, uint64_t view,
                         uint64_t instanceId, uint64_t digest, uint32_t clientId, uint64_t requestId, uint8_t phase, uint8_t marked);
        void OnBidlMessageTx(uint32_t senderId, uint32_t receiverId, uint16_t type, uint64_t instanceId, uint32_t bytes);
        void OnBidlMessageRx(uint32_t senderId, uint32_t receiverId, uint16_t type, uint64_t instanceId, uint32_t bytes);
        void OnBidlRepairTx(uint32_t senderId, uint32_t receiverId, uint64_t sequence, uint8_t reason, uint32_t bytes);
        void OnDecision(uint32_t replicaId, uint64_t instanceId, uint64_t digest, Time time);
        void OnViewChanged(uint32_t replicaId, uint64_t view, uint32_t primaryId);
        void OnPbftPhase(uint32_t replicaId, uint64_t instanceId, uint16_t phase, Time elapsed);
        void OnEnginePhase(uint32_t replicaId, uint8_t protocol, uint64_t instanceId, uint16_t phase, Time elapsed);
        void OnIorsConflict(uint32_t switchId, uint8_t protocol, uint32_t senderId, uint64_t epoch,
                           uint64_t instance, uint64_t digest, uint8_t evidence, Time time);
        void OnBidlPhase(uint32_t nodeId, uint64_t blockId, uint16_t phase, Time elapsed);
        void OnBidlBatch(uint32_t nodeId, uint64_t blockId, uint32_t transactions);
        void OnBidlCommit(uint32_t nodeId, uint64_t blockId, uint32_t transactions, Time latency);
        void OnBidlTransactionCommit(uint32_t nodeId, uint64_t sequence, Time latency);
        void OnBidlBlockCommitTime(uint32_t nodeId, uint64_t blockId, uint32_t transactions, Time commitTime, Time latency);
        void OnExecutionBacklog(uint32_t nodeId, uint32_t verification, uint32_t execution, uint32_t total);
        void OnBidlForged(uint32_t nodeId, uint64_t sequence, uint32_t messages, uint64_t bytes);
        void OnBidlConflictRejected(uint32_t nodeId, uint64_t sequence, uint32_t senderId);
        void OnBidlReexecution(uint32_t nodeId, uint64_t blockId, uint32_t transactions, Time latency);
        void OnMessageEvent(bool tx, const ConsensusMessage& message);
        void OnTransactionFinalized(uint32_t node, uint64_t sequence, uint64_t block, uint32_t client, uint64_t request, uint32_t transactions);
        void OnBackgroundFlowStarted(uint32_t sender, uint32_t receiver, uint64_t flow, uint32_t bytes);
        void SampleReplicaBacklog();
        void SampleRuntime();
        void FlushRuntimeCheckpoint() const;
        void DoDispose() override;
        EventId m_replicaBacklogEvent;
        std::vector<Ptr<ConsensusApplication>> m_replicaApplications;
        std::map<std::pair<uint32_t, uint64_t>, uint8_t> m_completionPaths;
        mutable std::ofstream m_finalizedStream, m_backgroundEventStream, m_replicaBacklogStream;
        void OnBackgroundFlowCompleted(uint32_t receiverId, uint32_t senderId, uint64_t flowId, uint32_t bytes, Time fct);
        void OnDeviceTx(uint32_t device, Ptr<const Packet> packet);
        void OnDeviceRx(uint32_t device, Ptr<const Packet> packet);
        void OnDeviceDrop(uint32_t device, Ptr<const Packet> packet);
        void OnQbbTx(uint32_t device, Ptr<const Packet> packet, uint32_t queue);
        void OnQbbDrop(uint32_t device, Ptr<const Packet> packet, uint32_t queue);
        void OnPfc(uint32_t device, uint32_t event);
        void OnQueuePackets(uint32_t queue, uint32_t oldValue, uint32_t newValue);
        void OnQueueBytes(uint32_t queue, uint32_t oldValue, uint32_t newValue);
        void OnQueueDropStored(uint32_t queue, Ptr<const QueueDiscItem> item);
        void OnQueueDrop(uint32_t queue, Ptr<const QueueDiscItem> item, const char* reason);
        void OnQueueMark(uint32_t queue, Ptr<const QueueDiscItem> item, const char* reason);
        void OnSojourn(uint32_t queue, Time value);
        bool IsMeasuring() const;

        std::vector<Ptr<ConsensusClientApplication>> m_clients;
        std::vector<Ptr<BidlApplication>> m_bidlApplications;
        std::vector<RequestSample> m_requestSamples;
        std::vector<PhaseSample> m_phaseSamples;
        std::vector<BackgroundFlowSample> m_backgroundFlows;
        uint64_t m_backgroundCompletedCount{0};
        std::map<int64_t, uint64_t> m_backgroundFctHistogram;
        struct BackgroundAggregate {
            uint64_t started{0};
            uint64_t completed{0};
            uint64_t totalLatencyNs{0};
            uint64_t maxLatencyNs{0};
            std::map<int64_t, uint64_t> histogram;
        };
        std::array<BackgroundAggregate, 3> m_backgroundAggregates;
        struct IorsEventSample { uint32_t switchId; uint8_t protocol; uint32_t senderId; uint64_t epoch; uint64_t instance; uint64_t digest; uint8_t evidence; Time time; };
        std::vector<IorsEventSample> m_iorsEvents;
        struct CandidateEventSample { Time time; uint32_t senderId; uint32_t receiverId; uint16_t type;
                                      uint64_t view; uint64_t instanceId; uint64_t digest; uint32_t clientId; uint64_t requestId; uint8_t phase; uint8_t marked; };
        void RecordCandidateEvent(const CandidateEventSample& event);
        std::map<std::tuple<uint64_t, uint64_t, uint32_t>, CandidateEventSample> m_firstCandidateBySlot;
        std::set<std::tuple<uint64_t, uint64_t, uint32_t>> m_conflictingCandidateSlots;
        std::set<std::pair<uint64_t, uint64_t>> m_divergentCandidateInstances;
        uint64_t m_parsedZyzzyvaCandidates{0};
        using BidlMarkedKey = std::tuple<uint32_t, uint64_t, uint64_t, uint64_t, uint32_t, uint64_t>;
        std::map<BidlMarkedKey, Time> m_bidlMarkedAt;
        std::set<BidlMarkedKey> m_bidlMarkedExecuted;
        std::set<BidlMarkedKey> m_bidlExecutedCandidates;
        uint64_t m_bidlSpeculativeExecutions{0};
        struct RepairSourceCounts {
            uint64_t measuredCount{0}, measuredBytes{0}, fullCount{0}, fullBytes{0};
        };
        std::map<std::tuple<uint8_t, uint32_t, uint32_t>, RepairSourceCounts> m_bidlRepairSources;
        uint64_t m_bidlMarkedWaits{0};
        uint64_t m_bidlMarkedReplacements{0};
        uint64_t m_bidlMarkedTimeouts{0};
        uint64_t m_bidlVerificationStarts{0};
        uint64_t m_bidlCorrections{0};
        uint64_t m_bidlAvoidedExecutions{0};
        uint64_t m_bidlDelayedFirstExecutions{0};
        double m_bidlDelayedExecutionUs{0.0};
        struct BacklogSample { uint32_t nodeId; Time time; uint32_t verification; uint32_t execution; uint32_t total; };
        std::vector<BacklogSample> m_backlogSamples;
        std::vector<DeviceStats> m_devices;
        std::vector<QueueStats> m_queues;
        std::vector<Ptr<ConsensusSwitchPolicy>> m_switchPolicies;
        std::map<uint16_t, uint64_t> m_messagesByType;
        std::map<uint16_t, uint64_t> m_messageBytesByType;
        std::map<uint16_t, uint64_t> m_allMessagesByType;
        std::map<uint16_t, uint64_t> m_allMessageBytesByType;
        std::map<std::pair<uint64_t, uint16_t>, std::pair<uint64_t, uint64_t>> m_messageTimelineBins;
        uint64_t m_allViewChanges{0};
        uint64_t m_allRetransmissions{0};
        uint64_t m_allTimedOutRequests{0};
        uint64_t m_submittedRequests;
        uint64_t m_completedRequests;
        std::map<std::pair<uint32_t, uint64_t>, Time> m_submissionTimes;
        std::map<std::pair<uint32_t, uint64_t>, uint32_t> m_requestTransactionCounts;
        std::map<std::pair<uint32_t, uint64_t>, Time> m_completionTimes;
        std::map<std::pair<uint32_t, uint64_t>, Time> m_abortTimes;
        uint64_t m_abortedRequests{0};
        uint64_t m_retransmissions;
        uint64_t m_timedOutRequests;
        uint64_t m_decisions;
        uint64_t m_viewChanges;
        uint64_t m_bidlBlocks;
        uint64_t m_bidlTransactions;
        uint64_t m_bidlForgedTransactions;
        uint64_t m_bidlForgedMessages;
        uint64_t m_bidlForgedBytes;
        uint64_t m_bidlRejectedConflicts;
        uint64_t m_bidlReexecutionEvents;
        uint64_t m_bidlReexecutedTransactions;
        std::set<uint64_t> m_bidlAffectedBlocks;
        std::vector<Time> m_bidlReexecutionLatencies;
        std::set<uint64_t> m_views;
        std::set<uint64_t> m_committedBidlBlocks;
        std::set<uint64_t> m_committedBidlSequences;
        std::map<uint64_t, Time> m_bidlTransactionLatencies;
        struct BidlTransactionCommitSample { Time commitTime; Time latency; };
        std::map<uint64_t, BidlTransactionCommitSample> m_bidlTransactionCommitSamples;
        struct BidlBlockCommitSample { uint32_t nodeId; uint64_t blockId; uint32_t transactions; Time commitTime; Time latency; };
        std::map<uint64_t, BidlBlockCommitSample> m_bidlBlockCommitSamples;
        Time m_firstSubmission;
        Time m_lastCompletion;
        Time m_measurementStart;
        Time m_measurementStop;
        bool m_hasSubmission;
        bool m_diagnosticEvents{false};
        bool m_transactionPathEvents{false};
        bool m_backgroundFlowEvents{false};
        bool m_trackIorsCandidates{false};
        EventId m_runtimeEvent;
        std::chrono::steady_clock::time_point m_runtimeStarted;
        mutable std::ofstream m_runtimeStream;
        mutable std::ofstream m_replicaCommitStream;
        mutable std::ofstream m_candidateEventStream;
        mutable std::ofstream m_bidlPathStream;
        mutable std::ofstream m_bidlLiveProgressStream;
        mutable std::ofstream m_bidlAttackEventStream;
        mutable std::ofstream m_bidlReexecutionEventStream;
    };

} // namespace ns3

#endif // CONSENSUS_EXPERIMENT_H
