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
#include <limits>
#include <map>
#include <set>
#include <string>
#include <vector>

namespace ns3 {

    class BidlApplication;
    class ConsensusClientApplication;
    class ConsensusHelper;
    class Packet;
    class QueueDiscItem;
    class RdmaWorkloadApplication;

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
    };

    /**
     * Common command-line configuration used by consensus experiments.
     */
    struct ConsensusExperimentConfig {
        std::string preset;
        std::string configFile;
        std::string architecture{"replicated"};
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
        std::string ecnKminMap;
        std::string ecnKmaxMap;
        std::string ecnPmaxMap;
        std::string outputPrefix{"consensus-results"};
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
        uint32_t burstSize{10};
        uint32_t backgroundFlows{0};
        uint32_t backgroundPacketSize{1400};
        uint32_t rdmaCcMode{0};
        uint32_t rdmaPriorityGroup{3};
        uint32_t rdmaMtu{1000};
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
        double ecnPmax{0.2};
        double messageDropProbability{0.0};
        double messageDelayUs{0.0};
        uint32_t failedNodeId{std::numeric_limits<uint32_t>::max()};
        double failureStartMs{0.0};
        double failureStopMs{0.0};
        uint16_t faultMessageType{0};
        bool faultAffectSend{false};
        bool enableEcn{true};
        double requestRate{1000.0};
        double requestTimeoutMs{500.0};
        double retransmissionTimeoutMs{50.0};
        double viewChangeTimeoutMs{200.0};
        double batchTimeoutMs{5.0};
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
        void AttachClient(Ptr<ConsensusClientApplication> client);
        void AttachReplicas(const ApplicationContainer& applications);
        void AttachBidlApplications(const ApplicationContainer& applications);
        void AttachRdmaWorkloads(const ApplicationContainer& applications);
        void AttachNetDevices(const NetDeviceContainer& devices, const DataRate& linkRate, const std::vector<std::string>& labels = {});
        void AttachQueueDiscs(const QueueDiscContainer& queueDiscs);
        void AttachSwitchPolicies(const std::vector<Ptr<ConsensusSwitchPolicy>>& policies);

        uint64_t GetCompletedRequests() const;
        uint64_t GetMeasuredRequests() const;
        uint64_t GetCompletedTransactions() const;
        uint64_t GetRetransmissions() const;
        uint64_t GetTimedOutRequests() const;
        uint64_t GetViewChanges() const;
        double GetRequestThroughput() const;
        double GetTransactionThroughput() const;
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
        };

        struct DeviceStats {
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
            uint32_t maxPackets{0};
            uint32_t bytes{0};
            uint32_t maxBytes{0};
            uint64_t drops{0};
            uint64_t marks{0};
            Time totalSojourn{Seconds(0)};
            uint64_t sojournSamples{0};
        };

        void OnRequestSubmitted(uint32_t clientId, uint64_t requestId, Time time);
        void OnRequestCompleted(uint32_t clientId, uint64_t requestId, Time latency);
        void OnRequestRetransmitted(uint32_t clientId, uint64_t requestId, uint32_t targetId);
        void OnRequestTimedOut(uint32_t clientId, uint64_t requestId);
        void OnMessage(uint32_t senderId, uint32_t receiverId, uint16_t type, uint64_t instanceId, uint32_t bytes);
        void OnDecision(uint32_t replicaId, uint64_t instanceId, uint64_t digest, Time time);
        void OnViewChanged(uint32_t replicaId, uint64_t view, uint32_t primaryId);
        void OnPbftPhase(uint32_t replicaId, uint64_t instanceId, uint16_t phase, Time elapsed);
        void OnEnginePhase(uint32_t replicaId, uint8_t protocol, uint64_t instanceId, uint16_t phase, Time elapsed);
        void OnBidlPhase(uint32_t nodeId, uint64_t blockId, uint16_t phase, Time elapsed);
        void OnBidlBatch(uint32_t nodeId, uint64_t blockId, uint32_t transactions);
        void OnBidlCommit(uint32_t nodeId, uint64_t blockId, Time latency);
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
        std::vector<DeviceStats> m_devices;
        std::vector<QueueStats> m_queues;
        std::vector<Ptr<ConsensusSwitchPolicy>> m_switchPolicies;
        std::map<uint16_t, uint64_t> m_messagesByType;
        std::map<uint16_t, uint64_t> m_messageBytesByType;
        uint64_t m_submittedRequests;
        uint64_t m_completedRequests;
        std::map<std::pair<uint32_t, uint64_t>, Time> m_submissionTimes;
        uint64_t m_retransmissions;
        uint64_t m_timedOutRequests;
        uint64_t m_decisions;
        uint64_t m_viewChanges;
        uint64_t m_bidlBlocks;
        uint64_t m_bidlTransactions;
        std::set<uint64_t> m_views;
        std::set<uint64_t> m_committedBidlBlocks;
        Time m_firstSubmission;
        Time m_lastCompletion;
        Time m_measurementStart;
        Time m_measurementStop;
        bool m_hasSubmission;
    };

} // namespace ns3

#endif // CONSENSUS_EXPERIMENT_H
