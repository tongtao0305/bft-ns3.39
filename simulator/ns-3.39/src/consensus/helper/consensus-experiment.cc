#include "consensus-experiment.h"

#include "consensus-helper.h"

#include "ns3/abort.h"
#include "ns3/bidl-application.h"
#include "ns3/boolean.h"
#include "ns3/config.h"
#include "ns3/consensus-application.h"
#include "ns3/consensus-message.h"
#include "ns3/consensus-workload.h"
#include "ns3/csma-helper.h"
#include "ns3/data-rate.h"
#include "ns3/double.h"
#include "ns3/error-model.h"
#include "ns3/inet-socket-address.h"
#include "ns3/int-header.h"
#include "ns3/internet-stack-helper.h"
#include "ns3/ipv4-address-helper.h"
#include "ns3/ipv4-global-routing-helper.h"
#include "ns3/log.h"
#include "ns3/object-factory.h"
#include "ns3/on-off-helper.h"
#include "ns3/packet-sink-helper.h"
#include "ns3/pbft-engine.h"
#include "ns3/pint.h"
#include "ns3/point-to-point-helper.h"
#include "ns3/qbb-channel.h"
#include "ns3/qbb-helper.h"
#include "ns3/qbb-net-device.h"
#include "ns3/queue-item.h"
#include "ns3/rdma-driver.h"
#include "ns3/rdma-hw.h"
#include "ns3/rng-seed-manager.h"
#include "ns3/simulator.h"
#include "ns3/string.h"
#include "ns3/switch-node.h"
#include "ns3/traffic-control-helper.h"
#include "ns3/uinteger.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <numeric>
#include <queue>
#include <sstream>
#include <stdexcept>

namespace ns3 {

    NS_LOG_COMPONENT_DEFINE("ConsensusExperiment");
    NS_OBJECT_ENSURE_REGISTERED(ConsensusStatistics);
    NS_OBJECT_ENSURE_REGISTERED(ConsensusSwitchPolicy);
    NS_OBJECT_ENSURE_REGISTERED(QbbSwitchPolicy);

    namespace {

        std::string Trim(const std::string& value) {
            auto first = std::find_if_not(value.begin(), value.end(), [](unsigned char character) { return std::isspace(character); });
            auto last =
                std::find_if_not(value.rbegin(), value.rend(), [](unsigned char character) { return std::isspace(character); }).base();
            return first < last ? std::string(first, last) : std::string();
        }

        std::vector<std::pair<std::string, std::string>> ReadConfigurationEntries(const std::string& path) {
            std::ifstream input(path);
            if (!input) {
                throw std::runtime_error("cannot open consensus configuration file: " + path);
            }
            std::vector<std::pair<std::string, std::string>> entries;
            std::string line;
            uint32_t lineNumber = 0;
            while (std::getline(input, line)) {
                ++lineNumber;
                std::string text = Trim(line);
                if (text.empty() || text.front() == '#') {
                    continue;
                }
                size_t separator = text.find('=');
                if (separator == std::string::npos) {
                    throw std::invalid_argument("invalid configuration entry at " + path + ":" + std::to_string(lineNumber));
                }
                std::string key = Trim(text.substr(0, separator));
                std::string value = Trim(text.substr(separator + 1));
                if (key.empty()) {
                    throw std::invalid_argument("empty configuration key at " + path + ":" + std::to_string(lineNumber));
                }
                if (value.size() >= 2 &&
                    ((value.front() == '"' && value.back() == '"') || (value.front() == '\'' && value.back() == '\''))) {
                    value = value.substr(1, value.size() - 2);
                }
                entries.emplace_back(key, value);
            }
            return entries;
        }

        std::string FindArgumentValue(int argc, char* argv[], const std::string& name) {
            const std::string prefix = "--" + name + "=";
            for (int index = 1; index < argc; ++index) {
                std::string argument(argv[index]);
                if (argument.rfind(prefix, 0) == 0) {
                    return argument.substr(prefix.size());
                }
                if (argument == "--" + name && index + 1 < argc) {
                    return argv[index + 1];
                }
            }
            return {};
        }

        std::string EscapeJson(const std::string& value) {
            std::string escaped;
            for (char character : value) {
                if (character == '\\' || character == '"') {
                    escaped.push_back('\\');
                }
                escaped.push_back(character);
            }
            return escaped;
        }

        template <typename Value> std::map<uint64_t, Value> ParseRateMap(const std::string& text) {
            std::map<uint64_t, Value> result;
            if (text.empty()) {
                return result;
            }
            std::string normalized = text;
            std::replace(normalized.begin(), normalized.end(), ',', ' ');
            std::replace(normalized.begin(), normalized.end(), ':', ' ');
            std::istringstream input(normalized);
            std::vector<std::string> tokens;
            std::string token;
            while (input >> token) {
                tokens.push_back(token);
            }
            size_t offset = 0;
            if (tokens.size() % 2 == 1) {
                size_t declared = std::stoul(tokens.front());
                if (tokens.size() != 1 + 2 * declared) {
                    throw std::invalid_argument("invalid legacy rate map: " + text);
                }
                offset = 1;
            }
            if ((tokens.size() - offset) % 2 != 0) {
                throw std::invalid_argument("invalid rate/value map: " + text);
            }
            for (size_t index = offset; index < tokens.size(); index += 2) {
                uint64_t rate = std::stoull(tokens[index]);
                std::istringstream valueInput(tokens[index + 1]);
                Value value{};
                valueInput >> value;
                if (!valueInput || rate == 0) {
                    throw std::invalid_argument("invalid rate/value map entry: " + text);
                }
                result[rate] = value;
            }
            return result;
        }

        Ptr<ConsensusSwitchPolicy> CreateSwitchPolicy(const ConsensusExperimentConfig& config) {
            ObjectFactory factory;
            factory.SetTypeId(config.switchPolicy);
            factory.Set("EnableEcn", BooleanValue(config.enableEcn));
            factory.Set("CcMode", UintegerValue(config.rdmaCcMode));
            factory.Set("PriorityGroup", UintegerValue(config.GetConsensusPriorityGroup()));
            factory.Set("BufferBytes", UintegerValue(config.switchBufferBytes));
            factory.Set("EcnKminBytes", UintegerValue(config.ecnKminBytes));
            factory.Set("EcnKmaxBytes", UintegerValue(config.ecnKmaxBytes));
            factory.Set("EcnPmax", DoubleValue(config.ecnPmax));
            factory.Set("HeadroomBytes", UintegerValue(config.pfcHeadroomBytes));
            factory.Set("LinkRate", DataRateValue(DataRate(config.dataRate)));
            factory.Set("LinkDelay", TimeValue(Time(config.linkDelay)));
            Ptr<ConsensusSwitchPolicy> policy = factory.Create<ConsensusSwitchPolicy>();
            Ptr<QbbSwitchPolicy> qbbPolicy = DynamicCast<QbbSwitchPolicy>(policy);
            if (qbbPolicy) {
                qbbPolicy->SetEcnThresholdMaps(ParseRateMap<uint32_t>(config.ecnKminMap), ParseRateMap<uint32_t>(config.ecnKmaxMap),
                                               ParseRateMap<double>(config.ecnPmaxMap));
            }
            return policy;
        }

        std::vector<uint32_t> ParseIdList(const std::string& text) {
            std::vector<uint32_t> result;
            if (text.empty()) {
                return result;
            }
            std::string normalized = text;
            std::replace(normalized.begin(), normalized.end(), ',', ' ');
            std::istringstream input(normalized);
            uint32_t id = 0;
            while (input >> id) {
                result.push_back(id);
            }
            if (!input.eof()) {
                throw std::invalid_argument("invalid physical node list: " + text);
            }
            std::set<uint32_t> unique(result.begin(), result.end());
            if (unique.size() != result.size()) {
                throw std::invalid_argument("duplicate physical node id: " + text);
            }
            return result;
        }

        std::vector<LegacyFlowRecord> GetLegacyBlockchainFlows(const std::string& path) {
            std::vector<LegacyFlowRecord> result;
            if (path.empty()) {
                return result;
            }
            for (const LegacyFlowRecord& flow : ReadLegacyFlowFile(path)) {
                if (flow.applicationId == 1) {
                    result.push_back(flow);
                }
            }
            return result;
        }

        void ValidatePhysicalHost(const ConsensusTopology& topology, uint32_t physicalId, const std::string& role) {
            if (topology.physicalToEndpoint.count(physicalId) == 0) {
                throw std::invalid_argument(role + " physical node is not a topology host: " + std::to_string(physicalId));
            }
        }

        void AppendAvailableHosts(std::vector<uint32_t>& output, uint32_t count, const ConsensusTopology& topology,
                                  std::set<uint32_t>& used) {
            for (uint32_t physicalId : topology.endpointPhysicalIds) {
                if (output.size() >= count) {
                    break;
                }
                if (used.insert(physicalId).second) {
                    output.push_back(physicalId);
                }
            }
            if (output.size() != count) {
                throw std::invalid_argument("topology does not contain enough unassigned hosts");
            }
        }

        void SubmitLegacyRequest(Ptr<ConsensusClientApplication> client, uint32_t bytes) {
            client->SubmitRequest(bytes, 1);
        }

        Ipv4Address LegacyNodeAddress(uint32_t physicalId) {
            return Ipv4Address(0x0b000001 + ((physicalId / 256) * 0x00010000) + ((physicalId % 256) * 0x00000100));
        }

        void ConfigureRdmaGlobals(const ConsensusExperimentConfig& config) {
            Config::SetDefault("ns3::QbbNetDevice::PauseTime", UintegerValue(config.qbbPauseTime));
            Config::SetDefault("ns3::QbbNetDevice::QcnEnabled", BooleanValue(config.qbbQcnEnabled));
            Config::SetDefault("ns3::QbbNetDevice::QbbEnabled", BooleanValue(config.qbbPfcEnabled));
            RdmaEgressQueue::ack_q_idx = config.ackHighPriority ? 0 : 3;
            IntHop::multi = config.intMulti;
            IntHeader::mode = config.rdmaCcMode == 3    ? IntHeader::NORMAL
                              : config.rdmaCcMode == 7  ? IntHeader::TS
                              : config.rdmaCcMode == 10 ? IntHeader::PINT
                                                        : IntHeader::NONE;
            Pint::set_log_base(config.pintLogBase);
            if (IntHeader::mode == IntHeader::PINT) {
                IntHeader::pint_bytes = Pint::get_n_bytes();
            }
        }

        void ConfigureRdmaHardware(Ptr<RdmaHw> hardware, const ConsensusExperimentConfig& config, uint64_t topologyBdp) {
            hardware->SetAttribute("Mtu", UintegerValue(config.rdmaMtu));
            hardware->SetAttribute("CcMode", UintegerValue(config.rdmaCcMode));
            hardware->SetAttribute("L2ChunkSize", UintegerValue(config.rdmaL2ChunkSize));
            hardware->SetAttribute("L2AckInterval", UintegerValue(config.rdmaAckInterval));
            hardware->SetAttribute("L2BackToZero", BooleanValue(config.rdmaL2BackToZero));
            hardware->SetAttribute("MinRate", DataRateValue(DataRate(config.rdmaMinRate)));
            hardware->SetAttribute("ClampTargetRate", BooleanValue(config.rdmaClampTargetRate));
            hardware->SetAttribute("AlphaResumInterval", DoubleValue(config.rdmaAlphaResumeInterval));
            hardware->SetAttribute("RPTimer", DoubleValue(config.rdmaRpTimer));
            hardware->SetAttribute("FastRecoveryTimes", UintegerValue(config.rdmaFastRecoveryTimes));
            hardware->SetAttribute("EwmaGain", DoubleValue(config.rdmaEwmaGain));
            hardware->SetAttribute("RateAI", DataRateValue(DataRate(config.rdmaRateAi)));
            hardware->SetAttribute("RateHAI", DataRateValue(DataRate(config.rdmaRateHai)));
            hardware->SetAttribute("RateDecreaseInterval", DoubleValue(config.rdmaRateDecreaseInterval));
            hardware->SetAttribute("MiThresh", UintegerValue(config.rdmaMiThreshold));
            hardware->SetAttribute("VarWin", BooleanValue(config.rdmaVarWindow));
            hardware->SetAttribute("FastReact", BooleanValue(config.rdmaFastReact));
            hardware->SetAttribute("MultiRate", BooleanValue(config.rdmaMultiRate));
            hardware->SetAttribute("SampleFeedback", BooleanValue(config.rdmaSampleFeedback));
            hardware->SetAttribute("TargetUtil", DoubleValue(config.rdmaTargetUtilization));
            hardware->SetAttribute("RateBound", BooleanValue(config.rdmaRateBound));
            hardware->SetAttribute("DctcpRateAI", DataRateValue(DataRate(config.dctcpRateAi)));
            (void)topologyBdp;
            hardware->SetPintSmplThresh(config.pintProbability);
        }

    } // namespace

    LegacyTopologySpec ReadLegacyTopologyFile(const std::string& path) {
        std::ifstream input(path);
        if (!input) {
            throw std::runtime_error("cannot open legacy topology file: " + path);
        }
        LegacyTopologySpec spec;
        uint32_t switchCount = 0;
        uint32_t linkCount = 0;
        if (!(input >> spec.nodeCount >> switchCount >> linkCount) || spec.nodeCount < 2) {
            throw std::runtime_error("invalid legacy topology header: " + path);
        }
        for (uint32_t index = 0; index < switchCount; ++index) {
            uint32_t id = 0;
            if (!(input >> id) || id >= spec.nodeCount || !spec.switchIds.insert(id).second) {
                throw std::runtime_error("invalid or duplicate switch id in: " + path);
            }
        }
        std::set<std::pair<uint32_t, uint32_t>> links;
        for (uint32_t index = 0; index < linkCount; ++index) {
            LegacyTopologyLink link;
            if (!(input >> link.source >> link.destination >> link.dataRate >> link.delay >> link.errorRate) ||
                link.source >= spec.nodeCount || link.destination >= spec.nodeCount || link.source == link.destination ||
                link.errorRate < 0.0 || link.errorRate > 1.0) {
                throw std::runtime_error("invalid link in legacy topology: " + path);
            }
            auto key = std::minmax(link.source, link.destination);
            if (!links.insert(key).second) {
                throw std::runtime_error("duplicate link in legacy topology: " + path);
            }
            DataRate(link.dataRate);
            Time(link.delay);
            spec.links.push_back(link);
        }
        return spec;
    }

    std::vector<LegacyFlowRecord> ReadLegacyFlowFile(const std::string& path) {
        std::ifstream input(path);
        if (!input) {
            throw std::runtime_error("cannot open legacy flow file: " + path);
        }
        uint32_t count = 0;
        if (!(input >> count)) {
            throw std::runtime_error("invalid legacy flow header: " + path);
        }
        std::vector<LegacyFlowRecord> flows;
        flows.reserve(count);
        for (uint32_t index = 0; index < count; ++index) {
            LegacyFlowRecord flow;
            uint32_t priorityGroup = 0;
            uint32_t destinationPort = 0;
            if (!(input >> flow.source >> flow.destination >> priorityGroup >> destinationPort >> flow.bytes >> flow.startTimeNs >>
                  flow.applicationId) ||
                flow.source == flow.destination || priorityGroup > 7 || destinationPort == 0 || destinationPort > 65535 ||
                flow.bytes == 0) {
                throw std::runtime_error("invalid legacy flow record in: " + path);
            }
            flow.priorityGroup = static_cast<uint16_t>(priorityGroup);
            flow.destinationPort = static_cast<uint16_t>(destinationPort);
            flows.push_back(flow);
        }
        return flows;
    }

    TypeId ConsensusSwitchPolicy::GetTypeId() {
        static TypeId tid = TypeId("ns3::ConsensusSwitchPolicy").SetParent<Object>().SetGroupName("Consensus");
        return tid;
    }

    void ConsensusSwitchPolicy::AttachRuntimeHooks(Ptr<Node> switchNode) {
        if (!switchNode) {
            return;
        }
        m_switchId = switchNode->GetId();
        for (uint32_t deviceId = 0; deviceId < switchNode->GetNDevices(); ++deviceId) {
            Ptr<NetDevice> device = switchNode->GetDevice(deviceId);
            if (!device || !device->IsQbb()) {
                continue;
            }
            device->TraceConnectWithoutContext("QbbEnqueue", MakeCallback(&ConsensusSwitchPolicy::OnEnqueue, this).Bind(deviceId));
            device->TraceConnectWithoutContext("QbbDequeue", MakeCallback(&ConsensusSwitchPolicy::OnDequeue, this).Bind(deviceId));
            device->TraceConnectWithoutContext("QbbDrop", MakeCallback(&ConsensusSwitchPolicy::OnDrop, this).Bind(deviceId));
        }
        switchNode->TraceConnectWithoutContext("EcnMark", MakeCallback(&ConsensusSwitchPolicy::OnMark, this));
    }

    ConsensusSwitchEvent ConsensusSwitchPolicy::MakeEvent(ConsensusSwitchEventType type, Ptr<const Packet> packet, uint32_t deviceId,
                                                          uint32_t queueId) const {
        ConsensusSwitchEvent event;
        event.type = type;
        event.switchId = m_switchId;
        event.deviceId = deviceId;
        event.queueId = queueId;
        event.bytes = packet ? packet->GetSize() : 0;
        event.time = Simulator::Now();
        ConsensusMetadataTag tag;
        if (packet && packet->PeekPacketTag(tag)) {
            event.messageType = tag.GetMessageType();
            event.flowId = tag.GetFlowId();
            event.priorityGroup = tag.GetPriorityGroup();
        } else {
            event.priorityGroup = static_cast<uint8_t>(std::min(queueId, 255u));
        }
        return event;
    }

    void ConsensusSwitchPolicy::OnEnqueue(uint32_t deviceId, Ptr<const Packet> packet, uint32_t queueId) {
        ConsensusSwitchEvent event = MakeEvent(ConsensusSwitchEventType::ENQUEUE, packet, deviceId, queueId);
        ++m_enqueueEvents;
        m_taggedEvents += event.messageType != 0;
        ObserveEnqueue(event);
    }

    void ConsensusSwitchPolicy::OnDequeue(uint32_t deviceId, Ptr<const Packet> packet, uint32_t queueId) {
        ConsensusSwitchEvent event = MakeEvent(ConsensusSwitchEventType::DEQUEUE, packet, deviceId, queueId);
        ++m_dequeueEvents;
        m_taggedEvents += event.messageType != 0;
        ObserveDequeue(event);
    }

    void ConsensusSwitchPolicy::OnDrop(uint32_t deviceId, Ptr<const Packet> packet, uint32_t queueId) {
        ConsensusSwitchEvent event = MakeEvent(ConsensusSwitchEventType::DROP, packet, deviceId, queueId);
        ++m_dropEvents;
        m_taggedEvents += event.messageType != 0;
        ObserveDrop(event);
    }

    void ConsensusSwitchPolicy::OnMark(Ptr<const Packet> packet, uint32_t deviceId, uint32_t queueId) {
        ConsensusSwitchEvent event = MakeEvent(ConsensusSwitchEventType::MARK, packet, deviceId, queueId);
        ++m_markEvents;
        m_taggedEvents += event.messageType != 0;
        ObserveMark(event);
    }

    void ConsensusSwitchPolicy::ObserveEnqueue(const ConsensusSwitchEvent&) {}
    void ConsensusSwitchPolicy::ObserveDequeue(const ConsensusSwitchEvent&) {}
    void ConsensusSwitchPolicy::ObserveMark(const ConsensusSwitchEvent&) {}
    void ConsensusSwitchPolicy::ObserveDrop(const ConsensusSwitchEvent&) {}
    uint64_t ConsensusSwitchPolicy::GetEnqueueEvents() const {
        return m_enqueueEvents;
    }
    uint64_t ConsensusSwitchPolicy::GetDequeueEvents() const {
        return m_dequeueEvents;
    }
    uint64_t ConsensusSwitchPolicy::GetMarkEvents() const {
        return m_markEvents;
    }
    uint64_t ConsensusSwitchPolicy::GetDropEvents() const {
        return m_dropEvents;
    }
    uint64_t ConsensusSwitchPolicy::GetTaggedEvents() const {
        return m_taggedEvents;
    }

    TypeId QbbSwitchPolicy::GetTypeId() {
        static TypeId tid =
            TypeId("ns3::QbbSwitchPolicy")
                .SetParent<ConsensusSwitchPolicy>()
                .SetGroupName("Consensus")
                .AddConstructor<QbbSwitchPolicy>()
                .AddAttribute("EnableEcn", "Enable ECN marking in SwitchNode.", BooleanValue(true),
                              MakeBooleanAccessor(&QbbSwitchPolicy::m_enableEcn), MakeBooleanChecker())
                .AddAttribute("CcMode", "ns3-datacenter switch congestion-control mode.", UintegerValue(0),
                              MakeUintegerAccessor(&QbbSwitchPolicy::m_ccMode), MakeUintegerChecker<uint32_t>())
                .AddAttribute("PriorityGroup", "Lossless priority group receiving PFC headroom.", UintegerValue(3),
                              MakeUintegerAccessor(&QbbSwitchPolicy::m_priorityGroup), MakeUintegerChecker<uint32_t>(0, 7))
                .AddAttribute("BufferBytes", "Shared switch buffer size.", UintegerValue(24 * 1024 * 1024),
                              MakeUintegerAccessor(&QbbSwitchPolicy::m_bufferBytes), MakeUintegerChecker<uint32_t>(1))
                .AddAttribute("EcnKminBytes", "Minimum ECN marking threshold.", UintegerValue(100 * 1024),
                              MakeUintegerAccessor(&QbbSwitchPolicy::m_ecnKminBytes), MakeUintegerChecker<uint32_t>())
                .AddAttribute("EcnKmaxBytes", "Maximum ECN marking threshold.", UintegerValue(400 * 1024),
                              MakeUintegerAccessor(&QbbSwitchPolicy::m_ecnKmaxBytes), MakeUintegerChecker<uint32_t>())
                .AddAttribute("EcnPmax", "Maximum ECN marking probability.", DoubleValue(0.2),
                              MakeDoubleAccessor(&QbbSwitchPolicy::m_ecnPmax), MakeDoubleChecker<double>(0.0, 1.0))
                .AddAttribute("HeadroomBytes", "PFC headroom per configured port and PG; zero derives it from BDP.", UintegerValue(0),
                              MakeUintegerAccessor(&QbbSwitchPolicy::m_headroomBytes), MakeUintegerChecker<uint32_t>())
                .AddAttribute("LinkRate", "Link rate used to derive automatic PFC headroom.", DataRateValue(DataRate("10Gbps")),
                              MakeDataRateAccessor(&QbbSwitchPolicy::m_linkRate), MakeDataRateChecker())
                .AddAttribute("LinkDelay", "One-way link delay used to derive automatic PFC headroom.", TimeValue(MicroSeconds(10)),
                              MakeTimeAccessor(&QbbSwitchPolicy::m_linkDelay), MakeTimeChecker());
        return tid;
    }

    QbbSwitchPolicy::QbbSwitchPolicy()
        : m_enableEcn(true), m_ccMode(0), m_priorityGroup(3), m_bufferBytes(24 * 1024 * 1024), m_ecnKminBytes(100 * 1024),
          m_ecnKmaxBytes(400 * 1024), m_ecnPmax(0.2), m_headroomBytes(0), m_linkRate("10Gbps"), m_linkDelay(MicroSeconds(10)) {}

    void QbbSwitchPolicy::SetEcnThresholdMaps(const std::map<uint64_t, uint32_t>& kmin, const std::map<uint64_t, uint32_t>& kmax,
                                              const std::map<uint64_t, double>& pmax) {
        m_ecnKminByRate = kmin;
        m_ecnKmaxByRate = kmax;
        m_ecnPmaxByRate = pmax;
    }

    void QbbSwitchPolicy::Configure(Ptr<Node> node) {
        Ptr<SwitchNode> switchNode = DynamicCast<SwitchNode>(node);
        NS_ABORT_MSG_IF(!switchNode, "QbbSwitchPolicy requires a SwitchNode");
        NS_ABORT_MSG_IF(m_ecnKminBytes > m_ecnKmaxBytes, "QbbSwitchPolicy ECN thresholds are inconsistent");
        switchNode->SetAttribute("EcnEnabled", BooleanValue(m_enableEcn));
        switchNode->SetAttribute("CcMode", UintegerValue(m_ccMode));
        switchNode->m_mmu->SetBufferPool(m_bufferBytes);
        switchNode->m_mmu->SetIngressPool(m_bufferBytes);
        switchNode->m_mmu->SetEgressLosslessPool(m_bufferBytes);
        switchNode->m_mmu->SetPortCount(switchNode->GetNDevices());
        switchNode->m_mmu->node_id = switchNode->GetId();

        for (uint32_t device = 1; device < switchNode->GetNDevices(); ++device) {
            Ptr<QbbNetDevice> qbb = DynamicCast<QbbNetDevice>(switchNode->GetDevice(device));
            uint64_t rate = qbb ? qbb->GetDataRate().GetBitRate() : m_linkRate.GetBitRate();
            Time delay = m_linkDelay;
            if (qbb) {
                Ptr<QbbChannel> channel = DynamicCast<QbbChannel>(qbb->GetChannel());
                if (channel) {
                    delay = channel->GetDelay();
                }
            }
            // SwitchMmu::ConfigEcn keeps the legacy ns3-datacenter convention:
            // threshold arguments are in kB and are converted to bytes internally.
            uint32_t kmin = (m_ecnKminBytes + 999) / 1000;
            uint32_t kmax = (m_ecnKmaxBytes + 999) / 1000;
            double pmax = m_ecnPmax;
            if (m_ecnKminByRate.count(rate) != 0) {
                kmin = m_ecnKminByRate.at(rate);
            }
            if (m_ecnKmaxByRate.count(rate) != 0) {
                kmax = m_ecnKmaxByRate.at(rate);
            }
            if (m_ecnPmaxByRate.count(rate) != 0) {
                pmax = m_ecnPmaxByRate.at(rate);
            }
            NS_ABORT_MSG_IF(kmin > kmax, "per-rate Qbb ECN thresholds are inconsistent");
            switchNode->m_mmu->ConfigEcn(device, kmin, kmax, pmax);
            uint64_t derivedHeadroom = rate * delay.GetNanoSeconds() * 3 / 8000000000ULL;
            uint32_t headroom = m_headroomBytes == 0 ? static_cast<uint32_t>(std::max<uint64_t>(2048, derivedHeadroom)) : m_headroomBytes;
            switchNode->m_mmu->SetHeadroom(headroom, device, m_priorityGroup);
        }
        AttachRuntimeHooks(switchNode);
    }

    void ConsensusExperimentConfig::PrepareFromCommandLine(int argc, char* argv[]) {
        const std::string requestedFile = FindArgumentValue(argc, argv, "configFile");
        const std::string requestedPreset = FindArgumentValue(argc, argv, "preset");

        std::string filePreset;
        if (!requestedFile.empty()) {
            for (const auto& [key, value] : ReadConfigurationEntries(requestedFile)) {
                if (key == "preset") {
                    filePreset = value;
                }
            }
        }
        const std::string selectedPreset = requestedPreset.empty() ? filePreset : requestedPreset;
        if (!selectedPreset.empty()) {
            ApplyPreset(selectedPreset);
        }
        if (!requestedFile.empty()) {
            LoadConfigFile(requestedFile);
            configFile = requestedFile;
        }
    }

    void ConsensusExperimentConfig::ApplyPreset(const std::string& name) {
        if (name.empty()) {
            preset.clear();
            return;
        }
        preset = name;
        if (name == "pbft-tcp") {
            architecture = "replicated";
            scenario = "pbft-tcp";
            engine = "pbft";
            transport = "tcp";
            topology = "csma";
            queueDisc = "ns3::FqCoDelQueueDisc";
        } else if (name == "pbft-rdma") {
            architecture = "replicated";
            scenario = "pbft-rdma";
            engine = "pbft";
            transport = "rdma";
            topology = "qbb-leaf-spine";
            queueDisc = "none";
        } else if (name == "bidl-tcp") {
            architecture = "bidl";
            scenario = "bidl-tcp";
            engine = "pbft";
            transport = "tcp";
            topology = "leaf-spine";
            queueDisc = "ns3::FqCoDelQueueDisc";
        } else if (name == "bidl-rdma") {
            architecture = "bidl";
            scenario = "bidl-rdma";
            engine = "pbft";
            transport = "rdma";
            topology = "qbb-leaf-spine";
            queueDisc = "none";
        } else {
            throw std::invalid_argument("unknown consensus preset: " + name);
        }
    }

    void ConsensusExperimentConfig::LoadConfigFile(const std::string& path) {
        std::vector<std::pair<std::string, std::string>> entries = ReadConfigurationEntries(path);
        std::vector<std::string> arguments{"consensus-config"};
        arguments.reserve(entries.size() + 1);
        for (const auto& [key, value] : entries) {
            arguments.push_back("--" + key + "=" + value);
        }
        std::vector<char*> rawArguments;
        rawArguments.reserve(arguments.size());
        for (std::string& argument : arguments) {
            rawArguments.push_back(argument.data());
        }
        CommandLine command;
        AddCommandLineOptions(command);
        command.Parse(static_cast<int>(rawArguments.size()), rawArguments.data());
        configFile = path;
    }

    std::vector<std::string> ConsensusExperimentConfig::GetAvailablePresets() {
        return {"pbft-tcp", "pbft-rdma", "bidl-tcp", "bidl-rdma"};
    }

    void ConsensusExperimentConfig::AddCommandLineOptions(CommandLine& command) {
        command.AddValue("preset", "Built-in experiment preset", preset);
        command.AddValue("configFile", "key=value experiment configuration file", configFile);
        command.AddValue("architecture", "Application architecture: replicated or bidl", architecture);
        command.AddValue("workload", "Workload mode: auto, open-loop, closed-loop, burst, or trace", workload);
        command.AddValue("workloadProvider", "Optional ns-3 TypeId implementing ConsensusWorkloadProvider", workloadProvider);
        command.AddValue("scenario", "Scenario name stored with the results", scenario);
        command.AddValue("engine", "Consensus engine alias or ns-3 TypeId", engine);
        command.AddValue("topology", "Topology: csma, leaf-spine, qbb-star, qbb-leaf-spine, or qbb-file", topology);
        command.AddValue("topologyFile", "Legacy ns3-datacenter topology file", topologyFile);
        command.AddValue("flowFile", "Legacy flow file: app_id=0 is background RDMA and app_id=1 is a client transaction", flowFile);
        command.AddValue("clientPhysicalIds", "Comma-separated physical hosts for multiple clients", clientPhysicalIds);
        command.AddValue("clientPhysicalId", "Physical host for the blockchain client", clientPhysicalId);
        command.AddValue("ingressPhysicalId", "Physical host for PBFT primary or BIDL sequencer", ingressPhysicalId);
        command.AddValue("replicaPhysicalIds", "Comma-separated PBFT replica physical hosts; primary first", replicaPhysicalIds);
        command.AddValue("ordererPhysicalIds", "Comma-separated BIDL orderer physical hosts", ordererPhysicalIds);
        command.AddValue("executorPhysicalIds", "Comma-separated BIDL executor physical hosts", executorPhysicalIds);
        command.AddValue("transport", "Consensus transport: tcp or rdma", transport);
        command.AddValue("dataRate", "Link data rate", dataRate);
        command.AddValue("linkDelay", "Link propagation delay", linkDelay);
        command.AddValue("replicaCount", "Number of consensus replicas", replicaCount);
        command.AddValue("clientCount", "Number of synthetic clients", clientCount);
        command.AddValue("faultTolerance", "Fault threshold f", faultTolerance);
        command.AddValue("clientReplyQuorum", "Matching client replies required; zero selects a protocol default", clientReplyQuorum);
        command.AddValue("requestCount", "Requests generated by each client", requestCount);
        command.AddValue("requestSize", "Bytes in each request", requestSize);
        command.AddValue("transactionsPerRequest", "Transactions represented by one request", transactionsPerRequest);
        command.AddValue("requestRate", "Offered request rate per second", requestRate);
        command.AddValue("requestTimeoutMs", "Client retransmission timeout in ms", requestTimeoutMs);
        command.AddValue("retransmissionTimeoutMs", "PBFT phase retransmission timeout in ms", retransmissionTimeoutMs);
        command.AddValue("viewChangeTimeoutMs", "PBFT unresolved-instance timeout in ms", viewChangeTimeoutMs);
        command.AddValue("batchTimeoutMs", "Maximum BIDL partial-batch wait in ms", batchTimeoutMs);
        command.AddValue("simulationTime", "Simulation stop time in seconds", simulationTime);
        command.AddValue("applicationStartTime", "Application start time in seconds", applicationStartTime);
        command.AddValue("warmupTime", "Warm-up duration excluded from metrics", warmupTime);
        command.AddValue("measurementTime", "Measurement duration; zero uses the remaining interval", measurementTime);
        command.AddValue("cooldownTime", "Cooldown duration after the measurement window", cooldownTime);
        command.AddValue("batchSize", "Transactions in one BIDL batch", batchSize);
        command.AddValue("burstSize", "Requests per client in each synthetic burst", burstSize);
        command.AddValue("burstIntervalMs", "Interval between request bursts in ms", burstIntervalMs);
        command.AddValue("closedLoopThinkTimeUs", "Think time after each completed closed-loop request", closedLoopThinkTimeUs);
        command.AddValue("backgroundFlows", "Number of TCP background flows", backgroundFlows);
        command.AddValue("backgroundRate", "Rate of each background flow", backgroundRate);
        command.AddValue("queueDisc", "Root queue disc type, or none to disable it", queueDisc);
        command.AddValue("switchPolicy", "ns-3 TypeId implementing ConsensusSwitchPolicy", switchPolicy);
        command.AddValue("backgroundPacketSize", "TCP background packet size", backgroundPacketSize);
        command.AddValue("rdmaCcMode", "ns3-datacenter RDMA congestion-control mode", rdmaCcMode);
        command.AddValue("rdmaPriorityGroup", "Priority group used by consensus RDMA QPs", rdmaPriorityGroup);
        command.AddValue("rdmaMtu", "RDMA payload MTU in bytes", rdmaMtu);
        command.AddValue("rdmaAckInterval", "RDMA L2 ACK interval in packets", rdmaAckInterval);
        command.AddValue("rdmaL2ChunkSize", "RDMA L2 chunk size in bytes", rdmaL2ChunkSize);
        command.AddValue("rdmaWindow", "Maximum RDMA bytes in flight; zero uses CC defaults", rdmaWindow);
        command.AddValue("rdmaHasWindow", "Use the configured/global BDP RDMA window", rdmaHasWindow);
        command.AddValue("rdmaGlobalRtt", "Use topology-wide maximum RTT/BDP", rdmaGlobalRtt);
        command.AddValue("rdmaL2BackToZero", "Use RDMA go-back-zero recovery", rdmaL2BackToZero);
        command.AddValue("rdmaVarWindow", "Enable variable RDMA window", rdmaVarWindow);
        command.AddValue("rdmaFastReact", "Enable per-feedback HPCC reaction", rdmaFastReact);
        command.AddValue("rdmaMultiRate", "Maintain one HPCC rate per hop", rdmaMultiRate);
        command.AddValue("rdmaSampleFeedback", "Sample HPCC feedback instead of every packet", rdmaSampleFeedback);
        command.AddValue("rdmaRateBound", "Enable RDMA rate limiter", rdmaRateBound);
        command.AddValue("rdmaClampTargetRate", "Clamp DCQCN target rate", rdmaClampTargetRate);
        command.AddValue("rdmaFastRecoveryTimes", "DCQCN fast-recovery iterations", rdmaFastRecoveryTimes);
        command.AddValue("rdmaMiThreshold", "HPCC multiplicative-increase threshold", rdmaMiThreshold);
        command.AddValue("rdmaAlphaResumeInterval", "DCQCN alpha resume interval", rdmaAlphaResumeInterval);
        command.AddValue("rdmaRateDecreaseInterval", "DCQCN rate decrease interval", rdmaRateDecreaseInterval);
        command.AddValue("rdmaRpTimer", "DCQCN rate increase timer", rdmaRpTimer);
        command.AddValue("rdmaEwmaGain", "DCQCN/DCTCP EWMA gain", rdmaEwmaGain);
        command.AddValue("rdmaTargetUtilization", "HPCC target utilization", rdmaTargetUtilization);
        command.AddValue("rdmaMinRate", "Minimum RDMA flow rate", rdmaMinRate);
        command.AddValue("rdmaRateAi", "DCQCN additive-increase rate", rdmaRateAi);
        command.AddValue("rdmaRateHai", "DCQCN hyper-additive-increase rate", rdmaRateHai);
        command.AddValue("dctcpRateAi", "DCTCP additive-increase rate", dctcpRateAi);
        command.AddValue("intMulti", "HPCC INT counter multiplier", intMulti);
        command.AddValue("pintLogBase", "PINT logarithmic encoding base", pintLogBase);
        command.AddValue("pintProbability", "Fraction of packets carrying PINT feedback", pintProbability);
        command.AddValue("ackHighPriority", "Place RDMA ACK/NACK in the highest-priority queue", ackHighPriority);
        command.AddValue("qbbQcnEnabled", "Enable QCN processing on Qbb devices", qbbQcnEnabled);
        command.AddValue("qbbPfcEnabled", "Enable IEEE 802.1Qbb PFC pause processing", qbbPfcEnabled);
        command.AddValue("qbbDynamicThreshold", "Enable dynamic PFC threshold on Qbb devices", qbbDynamicThreshold);
        command.AddValue("qbbPauseTime", "Qbb pause time attribute", qbbPauseTime);
        command.AddValue("linkErrorRate", "Fallback packet error rate for links without an explicit error rate", linkErrorRate);
        command.AddValue("leafCount", "Number of Qbb leaf switches", leafCount);
        command.AddValue("spineCount", "Number of Qbb spine switches", spineCount);
        command.AddValue("switchBufferBytes", "Qbb switch shared-buffer size", switchBufferBytes);
        command.AddValue("enableEcn", "Enable ECN marking in Qbb switches", enableEcn);
        command.AddValue("ecnKminBytes", "Qbb ECN minimum threshold", ecnKminBytes);
        command.AddValue("ecnKmaxBytes", "Qbb ECN maximum threshold", ecnKmaxBytes);
        command.AddValue("ecnPmax", "Qbb ECN maximum marking probability", ecnPmax);
        command.AddValue("ecnKminMap", "Per-rate ECN kmin map: rate:value,rate:value", ecnKminMap);
        command.AddValue("ecnKmaxMap", "Per-rate ECN kmax map: rate:value,rate:value", ecnKmaxMap);
        command.AddValue("ecnPmaxMap", "Per-rate ECN pmax map: rate:value,rate:value", ecnPmaxMap);
        command.AddValue("pfcHeadroomBytes", "Per-port PFC headroom; zero derives it from link BDP", pfcHeadroomBytes);
        command.AddValue("rdmaBackgroundPriorityGroup", "Priority group for RDMA background workloads", rdmaBackgroundPriorityGroup);
        command.AddValue("rdmaLongFlows", "Number of long RDMA background flows", rdmaLongFlows);
        command.AddValue("rdmaLongFlowBytes", "Bytes in each long RDMA flow", rdmaLongFlowBytes);
        command.AddValue("rdmaLongStartMs", "Relative start time of long RDMA flows", rdmaLongStartMs);
        command.AddValue("rdmaShortFlows", "Number of short RDMA background flows", rdmaShortFlows);
        command.AddValue("rdmaShortFlowBytes", "Bytes in each short RDMA flow", rdmaShortFlowBytes);
        command.AddValue("rdmaShortStartMs", "Relative start time of short RDMA flows", rdmaShortStartMs);
        command.AddValue("rdmaShortIntervalUs", "Spacing between short RDMA flow arrivals", rdmaShortIntervalUs);
        command.AddValue("rdmaIncastSenders", "Synchronized RDMA incast senders", rdmaIncastSenders);
        command.AddValue("rdmaIncastBursts", "Number of synchronized RDMA incast bursts", rdmaIncastBursts);
        command.AddValue("rdmaIncastFlowBytes", "Bytes per RDMA incast sender", rdmaIncastFlowBytes);
        command.AddValue("rdmaIncastStartMs", "Relative start time of RDMA incast", rdmaIncastStartMs);
        command.AddValue("rdmaIncastIntervalUs", "Spacing between synchronized RDMA incast bursts", rdmaIncastIntervalUs);
        command.AddValue("messageDropProbability", "Independent receive-side complete-message drop probability", messageDropProbability);
        command.AddValue("messageDelayUs", "Additional receive-side complete-message delay in microseconds", messageDelayUs);
        command.AddValue("failedNodeId", "Logical node isolated by FaultModel; UINT32_MAX disables it", failedNodeId);
        command.AddValue("failureStartMs", "FaultModel node-isolation start time", failureStartMs);
        command.AddValue("failureStopMs", "FaultModel node-isolation stop time; zero means simulation end", failureStopMs);
        command.AddValue("faultMessageType", "Consensus message type impaired by FaultModel; zero matches all", faultMessageType);
        command.AddValue("faultAffectSend", "Apply probabilistic FaultModel impairment on egress too", faultAffectSend);
        command.AddValue("seed", "ns-3 random seed", seed);
        command.AddValue("run", "ns-3 independent run number", run);
        command.AddValue("outputPrefix", "Prefix for JSON and CSV result files", outputPrefix);
    }

    void ConsensusExperimentConfig::Validate() const {
        if (architecture != "replicated" && architecture != "bidl") {
            throw std::invalid_argument("architecture must be replicated or bidl");
        }
        if (engine.empty()) {
            throw std::invalid_argument("engine must not be empty");
        }
        if (workload != "auto" && workload != "open-loop" && workload != "closed-loop" && workload != "burst" && workload != "trace") {
            throw std::invalid_argument("unsupported consensus workload mode: " + workload);
        }
        if (workload == "trace" && !HasLegacyBlockchainWorkload()) {
            throw std::invalid_argument("workload=trace requires app_id=1 records in flowFile");
        }
        if (replicaCount == 0 || faultTolerance >= replicaCount) {
            throw std::invalid_argument("replica and fault counts are inconsistent");
        }
        if (requestRate <= 0.0 || requestTimeoutMs <= 0.0 || retransmissionTimeoutMs <= 0.0 || viewChangeTimeoutMs <= 0.0 ||
            batchTimeoutMs <= 0.0 || simulationTime <= 0.0 || applicationStartTime < 0.0 || warmupTime < 0.0 ||
            measurementTime < 0.0 || cooldownTime < 0.0 || requestCount == 0 || requestSize == 0 || transactionsPerRequest == 0 ||
            clientCount == 0 || batchSize == 0 || burstSize == 0 || burstIntervalMs <= 0.0 || closedLoopThinkTimeUs < 0.0 || rdmaMtu == 0 ||
            rdmaAckInterval == 0 || rdmaL2ChunkSize == 0 || switchBufferBytes == 0 || leafCount == 0 || spineCount == 0 ||
            switchPolicy.empty() || rdmaBackgroundPriorityGroup > 7 || rdmaLongFlowBytes == 0 || rdmaShortFlowBytes == 0 ||
            rdmaIncastFlowBytes == 0 || rdmaLongStartMs < 0.0 || rdmaShortStartMs < 0.0 || rdmaShortIntervalUs <= 0.0 ||
            rdmaIncastStartMs < 0.0 || rdmaIncastIntervalUs <= 0.0 || ecnKminBytes > ecnKmaxBytes || ecnPmax < 0.0 || ecnPmax > 1.0 ||
            rdmaAlphaResumeInterval <= 0.0 || rdmaRateDecreaseInterval <= 0.0 || rdmaRpTimer <= 0.0 || rdmaEwmaGain < 0.0 ||
            rdmaEwmaGain > 1.0 || rdmaTargetUtilization <= 0.0 || rdmaTargetUtilization > 1.0 || pintLogBase <= 1.0 ||
            pintProbability < 0.0 || pintProbability > 1.0 || linkErrorRate < 0.0 || linkErrorRate > 1.0 || messageDropProbability < 0.0 ||
            messageDropProbability > 1.0 || messageDelayUs < 0.0 || failureStartMs < 0.0 || failureStopMs < 0.0 ||
            (failureStopMs > 0.0 && failureStopMs <= failureStartMs)) {
            throw std::invalid_argument("request and batch parameters must be positive");
        }
        if (GetMeasurementStop() <= GetMeasurementStart() || GetMeasurementStop() > Seconds(simulationTime)) {
            throw std::invalid_argument("warmup, measurement, and cooldown times do not fit simulationTime");
        }
        const bool pbftEngine = engine == "pbft" || engine == "ns3::PbftEngine";
        const bool tcpTransport = transport == "tcp" || transport == "ns3::TcpConsensusTransport";
        const bool rdmaTransport = transport == "rdma" || transport == "ns3::RdmaConsensusTransport";
        const bool tcpTopology = topology == "csma" || topology == "leaf-spine";
        const bool rdmaTopology = topology == "qbb-star" || topology == "qbb-leaf-spine" || topology == "qbb-file";
        if (!pbftEngine) {
            throw std::invalid_argument("unsupported consensus engine: " + engine);
        }
        if (!tcpTransport && !rdmaTransport) {
            throw std::invalid_argument("unsupported consensus transport: " + transport);
        }
        if (!tcpTopology && !rdmaTopology) {
            throw std::invalid_argument("unsupported consensus topology: " + topology);
        }
        if (tcpTransport != tcpTopology || rdmaTransport != rdmaTopology) {
            throw std::invalid_argument("transport " + transport + " is incompatible with " + topology);
        }
        if (rdmaTransport && switchPolicy != "ns3::QbbSwitchPolicy") {
            throw std::invalid_argument("unsupported RDMA switch policy: " + switchPolicy);
        }
        if (rdmaTransport && GetConsensusPriorityGroup() > 7) {
            throw std::invalid_argument("RDMA priority group must be between 0 and 7");
        }
        if (replicaCount < 3 * faultTolerance + 1) {
            throw std::invalid_argument("replicaCount must be at least 3f+1 for PBFT");
        }
        if (topology == "qbb-file" && topologyFile.empty()) {
            throw std::invalid_argument("topology=qbb-file requires topologyFile");
        }
        if ((!topologyFile.empty() || !flowFile.empty()) && !UsesRdmaNetwork()) {
            throw std::invalid_argument("legacy topology and flow files require transport=rdma");
        }
        auto kmin = ParseRateMap<uint32_t>(ecnKminMap);
        auto kmax = ParseRateMap<uint32_t>(ecnKmaxMap);
        auto pmax = ParseRateMap<double>(ecnPmaxMap);
        auto clients = ParseIdList(clientPhysicalIds);
        auto replicas = ParseIdList(replicaPhysicalIds);
        auto orderers = ParseIdList(ordererPhysicalIds);
        auto executors = ParseIdList(executorPhysicalIds);
        if (!replicas.empty() && replicas.size() != replicaCount) {
            throw std::invalid_argument("replicaPhysicalIds must contain replicaCount hosts");
        }
        if (!clients.empty() && clients.size() != GetEffectiveClientCount()) {
            throw std::invalid_argument("clientPhysicalIds must contain the effective client count");
        }
        if (!clients.empty() && clientPhysicalId != std::numeric_limits<uint32_t>::max() && clients.front() != clientPhysicalId) {
            throw std::invalid_argument("clientPhysicalId conflicts with clientPhysicalIds");
        }
        if ((!orderers.empty() && orderers.size() != replicaCount) || (!executors.empty() && executors.size() != replicaCount)) {
            throw std::invalid_argument("BIDL physical role lists must contain replicaCount hosts");
        }
        std::vector<LegacyFlowRecord> blockchainFlows = GetLegacyBlockchainFlows(flowFile);
        if (!blockchainFlows.empty()) {
            const LegacyFlowRecord& first = blockchainFlows.front();
            for (const LegacyFlowRecord& flow : blockchainFlows) {
                if (flow.destination != first.destination || flow.priorityGroup != first.priorityGroup ||
                    flow.destinationPort != first.destinationPort) {
                    throw std::invalid_argument("app_id=1 requires one ingress, PG, and destination port");
                }
            }
            std::set<uint32_t> traceClients;
            for (const auto& flow : blockchainFlows) {
                traceClients.insert(flow.source);
            }
            if ((clientPhysicalId != std::numeric_limits<uint32_t>::max() && traceClients.count(clientPhysicalId) == 0) ||
                (!clients.empty() && std::set<uint32_t>(clients.begin(), clients.end()) != traceClients) ||
                (ingressPhysicalId != std::numeric_limits<uint32_t>::max() && ingressPhysicalId != first.destination)) {
                throw std::invalid_argument("configured client/ingress physical IDs conflict with app_id=1 records");
            }
        }
        if ((!kmin.empty() || !kmax.empty() || !pmax.empty()) && (kmin.size() != kmax.size() || kmin.size() != pmax.size())) {
            throw std::invalid_argument("ECN rate maps must contain the same link rates");
        }
        for (const auto& [rate, minimum] : kmin) {
            if (kmax.count(rate) == 0 || pmax.count(rate) == 0 || minimum > kmax.at(rate) || pmax.at(rate) < 0.0 || pmax.at(rate) > 1.0) {
                throw std::invalid_argument("invalid per-rate ECN threshold map");
            }
        }
        if (backgroundFlows > 0 && (backgroundRate.empty() || backgroundPacketSize == 0)) {
            throw std::invalid_argument("background traffic parameters must be positive");
        }
        if (UsesRdmaNetwork() && backgroundFlows > 0) {
            throw std::invalid_argument("RDMA background flows are not implemented; do not silently substitute TCP traffic");
        }
        if (!UsesRdmaNetwork() && (rdmaLongFlows > 0 || rdmaShortFlows > 0 || rdmaIncastSenders > 0)) {
            throw std::invalid_argument("RDMA workload options require transport=rdma");
        }
        if (UsesRdmaNetwork() && (rdmaLongFlows > 0 || rdmaShortFlows > 0 || rdmaIncastSenders > 0) &&
            rdmaBackgroundPriorityGroup == GetConsensusPriorityGroup()) {
            throw std::invalid_argument("RDMA consensus and background workloads must use different priority groups");
        }
    }

    void ConsensusExperimentConfig::ApplyRandomSeed() const {
        RngSeedManager::SetSeed(seed);
        RngSeedManager::SetRun(run);
    }

    void ConsensusExperimentConfig::ConfigureHelper(ConsensusHelper& helper, const ConsensusTopology* topologyResult) const {
        Validate();
        helper.SetEngine(GetEngineType());
        helper.SetEngineAttribute("PrimaryId", UintegerValue(0));
        helper.SetEngineAttribute("FaultTolerance", UintegerValue(faultTolerance));
        helper.SetEngineAttribute("RetransmissionTimeout", TimeValue(MilliSeconds(retransmissionTimeoutMs)));
        helper.SetEngineAttribute("InstanceTimeout", TimeValue(MilliSeconds(viewChangeTimeoutMs)));

        helper.SetTransport(GetTransportType());
        if (UsesRdmaNetwork()) {
            helper.SetTransportAttribute("PriorityGroup", UintegerValue(GetConsensusPriorityGroup()));
            helper.SetTransportAttribute("Window", UintegerValue(rdmaWindow));
            uint64_t baseRtt = topologyResult && topologyResult->maxRttNs > 0 ? topologyResult->maxRttNs
                                                                              : static_cast<uint64_t>(Time(linkDelay).GetNanoSeconds()) * 4;
            helper.SetTransportAttribute("BaseRttNs", UintegerValue(baseRtt));
        }
        helper.SetFaultModel("ns3::ConfigurableConsensusFaultModel");
        helper.SetFaultAttribute("DropProbability", DoubleValue(messageDropProbability));
        helper.SetFaultAttribute("Delay", TimeValue(MicroSeconds(messageDelayUs)));
        helper.SetFaultAttribute("FailedNodeId", UintegerValue(failedNodeId));
        helper.SetFaultAttribute("FailureStart", TimeValue(MilliSeconds(failureStartMs)));
        helper.SetFaultAttribute("FailureStop", TimeValue(MilliSeconds(failureStopMs)));
        helper.SetFaultAttribute("MessageType", UintegerValue(faultMessageType));
        helper.SetFaultAttribute("AffectSend", BooleanValue(faultAffectSend));
        helper.SetClientAttribute("FaultTolerance", UintegerValue(faultTolerance));
        helper.SetClientAttribute("ReplyQuorum", UintegerValue(clientReplyQuorum));
        helper.SetClientAttribute("RequestSize", UintegerValue(requestSize));
        helper.SetClientAttribute("TransactionsPerRequest", UintegerValue(transactionsPerRequest));
        helper.SetClientAttribute("MaxRequests", UintegerValue(requestCount));
        helper.SetClientAttribute("Interval", TimeValue(Seconds(1.0 / requestRate)));
        helper.SetClientAttribute("RequestTimeout", TimeValue(MilliSeconds(requestTimeoutMs)));
        helper.SetClientAttribute("AutoGenerate", BooleanValue(false));
        helper.SetBidlAttribute("FaultTolerance", UintegerValue(faultTolerance));
        helper.SetBidlAttribute("BatchSize", UintegerValue(batchSize));
        helper.SetBidlAttribute("BatchTimeout", TimeValue(MilliSeconds(batchTimeoutMs)));
    }

    std::string ConsensusExperimentConfig::GetEngineType() const {
        if (engine == "pbft" || engine == "ns3::PbftEngine") {
            return "ns3::PbftEngine";
        }
        throw std::invalid_argument("unsupported consensus engine: " + engine);
    }

    std::string ConsensusExperimentConfig::GetTransportType() const {
        if (transport == "tcp" || transport == "ns3::TcpConsensusTransport") {
            return "ns3::TcpConsensusTransport";
        }
        if (transport == "rdma" || transport == "ns3::RdmaConsensusTransport") {
            return "ns3::RdmaConsensusTransport";
        }
        throw std::invalid_argument("unsupported consensus transport: " + transport);
    }

    bool ConsensusExperimentConfig::UsesRdmaNetwork() const {
        if (transport == "rdma" || transport == "ns3::RdmaConsensusTransport") {
            return true;
        }
        if (transport == "tcp" || transport == "ns3::TcpConsensusTransport") {
            return false;
        }
        throw std::invalid_argument("unsupported consensus transport: " + transport);
    }

    void ConsensusExperimentConfig::WriteJson(const std::string& path) const {
        std::ofstream output(path);
        if (!output) {
            throw std::runtime_error("cannot write experiment config: " + path);
        }
        output << "{\n"
               << "  \"preset\": \"" << EscapeJson(preset) << "\",\n"
               << "  \"config_file\": \"" << EscapeJson(configFile) << "\",\n"
               << "  \"architecture\": \"" << EscapeJson(architecture) << "\",\n"
               << "  \"workload\": \"" << EscapeJson(workload) << "\",\n"
               << "  \"workload_provider\": \"" << EscapeJson(workloadProvider) << "\",\n"
               << "  \"scenario\": \"" << EscapeJson(scenario) << "\",\n"
               << "  \"engine\": \"" << EscapeJson(engine) << "\",\n"
               << "  \"topology\": \"" << EscapeJson(topology) << "\",\n"
               << "  \"topology_file\": \"" << EscapeJson(topologyFile) << "\",\n"
               << "  \"flow_file\": \"" << EscapeJson(flowFile) << "\",\n"
               << "  \"legacy_blockchain_transactions\": " << GetLegacyBlockchainFlows(flowFile).size() << ",\n"
               << "  \"client_physical_id\": " << clientPhysicalId << ",\n"
               << "  \"client_physical_ids\": \"" << EscapeJson(clientPhysicalIds) << "\",\n"
               << "  \"ingress_physical_id\": " << ingressPhysicalId << ",\n"
               << "  \"replica_physical_ids\": \"" << EscapeJson(replicaPhysicalIds) << "\",\n"
               << "  \"orderer_physical_ids\": \"" << EscapeJson(ordererPhysicalIds) << "\",\n"
               << "  \"executor_physical_ids\": \"" << EscapeJson(executorPhysicalIds) << "\",\n"
               << "  \"transport\": \"" << EscapeJson(transport) << "\",\n"
               << "  \"data_rate\": \"" << EscapeJson(dataRate) << "\",\n"
               << "  \"link_delay\": \"" << EscapeJson(linkDelay) << "\",\n"
               << "  \"replica_count\": " << replicaCount << ",\n"
               << "  \"client_count\": " << GetEffectiveClientCount() << ",\n"
               << "  \"fault_tolerance\": " << faultTolerance << ",\n"
               << "  \"client_reply_quorum\": " << clientReplyQuorum << ",\n"
               << "  \"request_count\": " << requestCount << ",\n"
               << "  \"request_size\": " << requestSize << ",\n"
               << "  \"transactions_per_request\": " << transactionsPerRequest << ",\n"
               << "  \"request_rate\": " << requestRate << ",\n"
               << "  \"request_timeout_ms\": " << requestTimeoutMs << ",\n"
               << "  \"retransmission_timeout_ms\": " << retransmissionTimeoutMs << ",\n"
               << "  \"view_change_timeout_ms\": " << viewChangeTimeoutMs << ",\n"
               << "  \"batch_timeout_ms\": " << batchTimeoutMs << ",\n"
               << "  \"simulation_time\": " << simulationTime << ",\n"
               << "  \"application_start_time\": " << applicationStartTime << ",\n"
               << "  \"warmup_time\": " << warmupTime << ",\n"
               << "  \"measurement_time\": " << measurementTime << ",\n"
               << "  \"cooldown_time\": " << cooldownTime << ",\n"
               << "  \"batch_size\": " << batchSize << ",\n"
               << "  \"burst_size\": " << burstSize << ",\n"
               << "  \"burst_interval_ms\": " << burstIntervalMs << ",\n"
               << "  \"closed_loop_think_time_us\": " << closedLoopThinkTimeUs << ",\n"
               << "  \"background_flows\": " << backgroundFlows << ",\n"
               << "  \"background_rate\": \"" << EscapeJson(backgroundRate) << "\",\n"
               << "  \"queue_disc\": \"" << EscapeJson(queueDisc) << "\",\n"
               << "  \"switch_policy\": \"" << EscapeJson(switchPolicy) << "\",\n"
               << "  \"background_packet_size\": " << backgroundPacketSize << ",\n"
               << "  \"rdma_cc_mode\": " << rdmaCcMode << ",\n"
               << "  \"rdma_priority_group\": " << rdmaPriorityGroup << ",\n"
               << "  \"effective_consensus_priority_group\": " << GetConsensusPriorityGroup() << ",\n"
               << "  \"rdma_mtu\": " << rdmaMtu << ",\n"
               << "  \"rdma_ack_interval\": " << rdmaAckInterval << ",\n"
               << "  \"rdma_l2_chunk_size\": " << rdmaL2ChunkSize << ",\n"
               << "  \"rdma_window\": " << rdmaWindow << ",\n"
               << "  \"rdma_has_window\": " << (rdmaHasWindow ? "true" : "false") << ",\n"
               << "  \"rdma_global_rtt\": " << (rdmaGlobalRtt ? "true" : "false") << ",\n"
               << "  \"rdma_l2_back_to_zero\": " << (rdmaL2BackToZero ? "true" : "false") << ",\n"
               << "  \"rdma_var_window\": " << (rdmaVarWindow ? "true" : "false") << ",\n"
               << "  \"rdma_fast_react\": " << (rdmaFastReact ? "true" : "false") << ",\n"
               << "  \"rdma_multi_rate\": " << (rdmaMultiRate ? "true" : "false") << ",\n"
               << "  \"rdma_sample_feedback\": " << (rdmaSampleFeedback ? "true" : "false") << ",\n"
               << "  \"rdma_rate_bound\": " << (rdmaRateBound ? "true" : "false") << ",\n"
               << "  \"rdma_clamp_target_rate\": " << (rdmaClampTargetRate ? "true" : "false") << ",\n"
               << "  \"rdma_fast_recovery_times\": " << rdmaFastRecoveryTimes << ",\n"
               << "  \"rdma_mi_threshold\": " << rdmaMiThreshold << ",\n"
               << "  \"rdma_alpha_resume_interval\": " << rdmaAlphaResumeInterval << ",\n"
               << "  \"rdma_rate_decrease_interval\": " << rdmaRateDecreaseInterval << ",\n"
               << "  \"rdma_rp_timer\": " << rdmaRpTimer << ",\n"
               << "  \"rdma_ewma_gain\": " << rdmaEwmaGain << ",\n"
               << "  \"rdma_target_utilization\": " << rdmaTargetUtilization << ",\n"
               << "  \"rdma_min_rate\": \"" << EscapeJson(rdmaMinRate) << "\",\n"
               << "  \"rdma_rate_ai\": \"" << EscapeJson(rdmaRateAi) << "\",\n"
               << "  \"rdma_rate_hai\": \"" << EscapeJson(rdmaRateHai) << "\",\n"
               << "  \"dctcp_rate_ai\": \"" << EscapeJson(dctcpRateAi) << "\",\n"
               << "  \"int_multi\": " << intMulti << ",\n"
               << "  \"pint_log_base\": " << pintLogBase << ",\n"
               << "  \"pint_probability\": " << pintProbability << ",\n"
               << "  \"ack_high_priority\": " << (ackHighPriority ? "true" : "false") << ",\n"
               << "  \"qbb_qcn_enabled\": " << (qbbQcnEnabled ? "true" : "false") << ",\n"
               << "  \"qbb_pfc_enabled\": " << (qbbPfcEnabled ? "true" : "false") << ",\n"
               << "  \"qbb_dynamic_threshold\": " << (qbbDynamicThreshold ? "true" : "false") << ",\n"
               << "  \"qbb_pause_time\": " << qbbPauseTime << ",\n"
               << "  \"link_error_rate\": " << linkErrorRate << ",\n"
               << "  \"leaf_count\": " << leafCount << ",\n"
               << "  \"spine_count\": " << spineCount << ",\n"
               << "  \"switch_buffer_bytes\": " << switchBufferBytes << ",\n"
               << "  \"enable_ecn\": " << (enableEcn ? "true" : "false") << ",\n"
               << "  \"ecn_kmin_bytes\": " << ecnKminBytes << ",\n"
               << "  \"ecn_kmax_bytes\": " << ecnKmaxBytes << ",\n"
               << "  \"ecn_pmax\": " << ecnPmax << ",\n"
               << "  \"ecn_kmin_map\": \"" << EscapeJson(ecnKminMap) << "\",\n"
               << "  \"ecn_kmax_map\": \"" << EscapeJson(ecnKmaxMap) << "\",\n"
               << "  \"ecn_pmax_map\": \"" << EscapeJson(ecnPmaxMap) << "\",\n"
               << "  \"pfc_headroom_bytes\": " << pfcHeadroomBytes << ",\n"
               << "  \"rdma_background_priority_group\": " << rdmaBackgroundPriorityGroup << ",\n"
               << "  \"rdma_long_flows\": " << rdmaLongFlows << ",\n"
               << "  \"rdma_long_flow_bytes\": " << rdmaLongFlowBytes << ",\n"
               << "  \"rdma_long_start_ms\": " << rdmaLongStartMs << ",\n"
               << "  \"rdma_short_flows\": " << rdmaShortFlows << ",\n"
               << "  \"rdma_short_flow_bytes\": " << rdmaShortFlowBytes << ",\n"
               << "  \"rdma_short_start_ms\": " << rdmaShortStartMs << ",\n"
               << "  \"rdma_short_interval_us\": " << rdmaShortIntervalUs << ",\n"
               << "  \"rdma_incast_senders\": " << rdmaIncastSenders << ",\n"
               << "  \"rdma_incast_bursts\": " << rdmaIncastBursts << ",\n"
               << "  \"rdma_incast_flow_bytes\": " << rdmaIncastFlowBytes << ",\n"
               << "  \"rdma_incast_start_ms\": " << rdmaIncastStartMs << ",\n"
               << "  \"rdma_incast_interval_us\": " << rdmaIncastIntervalUs << ",\n"
               << "  \"message_drop_probability\": " << messageDropProbability << ",\n"
               << "  \"message_delay_us\": " << messageDelayUs << ",\n"
               << "  \"failed_node_id\": " << failedNodeId << ",\n"
               << "  \"failure_start_ms\": " << failureStartMs << ",\n"
               << "  \"failure_stop_ms\": " << failureStopMs << ",\n"
               << "  \"fault_message_type\": " << faultMessageType << ",\n"
               << "  \"fault_affect_send\": " << (faultAffectSend ? "true" : "false") << ",\n"
               << "  \"seed\": " << seed << ",\n"
               << "  \"run\": " << run << "\n"
               << "}\n";
    }

    ConsensusTopology ConsensusExperimentConfig::BuildTopology(uint32_t endpointCount) const {
        if (topology == "csma" || topology == "leaf-spine") {
            return BuildTcpTopology(endpointCount);
        }
        if (topology == "qbb-star" || topology == "qbb-leaf-spine" || topology == "qbb-file") {
            return BuildRdmaTopology(endpointCount);
        }
        throw std::invalid_argument("unsupported consensus topology: " + topology);
    }

    bool ConsensusExperimentConfig::HasLegacyBlockchainWorkload() const {
        return !GetLegacyBlockchainFlows(flowFile).empty();
    }

    uint32_t ConsensusExperimentConfig::GetEffectiveClientCount() const {
        if ((workload == "auto" || workload == "trace") && HasLegacyBlockchainWorkload()) {
            std::set<uint32_t> sources;
            for (const auto& flow : GetLegacyBlockchainFlows(flowFile)) {
                sources.insert(flow.source);
            }
            return sources.size();
        }
        return clientCount;
    }

    uint16_t ConsensusExperimentConfig::GetConsensusPort(uint16_t fallback) const {
        std::vector<LegacyFlowRecord> flows = GetLegacyBlockchainFlows(flowFile);
        return flows.empty() ? fallback : flows.front().destinationPort;
    }

    uint16_t ConsensusExperimentConfig::GetConsensusPriorityGroup() const {
        std::vector<LegacyFlowRecord> flows = GetLegacyBlockchainFlows(flowFile);
        return flows.empty() ? static_cast<uint16_t>(rdmaPriorityGroup) : flows.front().priorityGroup;
    }

    Time ConsensusExperimentConfig::GetApplicationStart() const {
        return Seconds(applicationStartTime);
    }

    Time ConsensusExperimentConfig::GetMeasurementStart() const {
        return Seconds(applicationStartTime + warmupTime);
    }

    Time ConsensusExperimentConfig::GetMeasurementStop() const {
        return measurementTime > 0.0 ? Seconds(applicationStartTime + warmupTime + measurementTime)
                                     : Seconds(simulationTime - cooldownTime);
    }

    ConsensusRolePlacement ConsensusExperimentConfig::ResolvePbftPlacement(const ConsensusTopology& topologyResult) const {
        const uint32_t effectiveClients = GetEffectiveClientCount();
        if (topologyResult.endpointPhysicalIds.size() < replicaCount + effectiveClients) {
            throw std::invalid_argument("PBFT placement does not have enough replica and client hosts");
        }
        std::vector<LegacyFlowRecord> flows = GetLegacyBlockchainFlows(flowFile);
        ConsensusRolePlacement placement;
        placement.clientPhysicalIds = ParseIdList(clientPhysicalIds);
        if (placement.clientPhysicalIds.empty() && clientPhysicalId != std::numeric_limits<uint32_t>::max()) {
            placement.clientPhysicalIds.push_back(clientPhysicalId);
        }
        if (placement.clientPhysicalIds.empty() && !flows.empty() && (workload == "auto" || workload == "trace")) {
            std::set<uint32_t> sources;
            for (const auto& flow : flows) {
                if (sources.insert(flow.source).second) {
                    placement.clientPhysicalIds.push_back(flow.source);
                }
            }
        }
        placement.ingressPhysicalId = ingressPhysicalId != std::numeric_limits<uint32_t>::max()
                                          ? ingressPhysicalId
                                          : (!flows.empty() ? flows.front().destination : std::numeric_limits<uint32_t>::max());
        placement.replicaPhysicalIds = ParseIdList(replicaPhysicalIds);
        if (placement.ingressPhysicalId == std::numeric_limits<uint32_t>::max()) {
            placement.ingressPhysicalId =
                placement.replicaPhysicalIds.empty() ? topologyResult.endpointPhysicalIds.front() : placement.replicaPhysicalIds.front();
        }
        ValidatePhysicalHost(topologyResult, placement.ingressPhysicalId, "PBFT primary");
        if (placement.replicaPhysicalIds.empty()) {
            std::set<uint32_t> used(placement.clientPhysicalIds.begin(), placement.clientPhysicalIds.end());
            for (uint32_t physicalId : placement.clientPhysicalIds) {
                ValidatePhysicalHost(topologyResult, physicalId, "client");
            }
            used.insert(placement.ingressPhysicalId);
            placement.replicaPhysicalIds.push_back(placement.ingressPhysicalId);
            AppendAvailableHosts(placement.replicaPhysicalIds, replicaCount, topologyResult, used);
        }
        if (placement.replicaPhysicalIds.size() != replicaCount || placement.replicaPhysicalIds.front() != placement.ingressPhysicalId) {
            throw std::invalid_argument("PBFT replica placement must contain replicaCount hosts with the primary first");
        }
        std::set<uint32_t> used;
        for (uint32_t physicalId : placement.replicaPhysicalIds) {
            ValidatePhysicalHost(topologyResult, physicalId, "PBFT replica");
            if (!used.insert(physicalId).second) {
                throw std::invalid_argument("PBFT replica placement contains duplicates");
            }
        }
        for (uint32_t physicalId : placement.clientPhysicalIds) {
            ValidatePhysicalHost(topologyResult, physicalId, "client");
            if (!used.insert(physicalId).second) {
                throw std::invalid_argument("client physical host overlaps a PBFT replica");
            }
        }
        AppendAvailableHosts(placement.clientPhysicalIds, effectiveClients, topologyResult, used);
        placement.clientPhysicalId = placement.clientPhysicalIds.front();
        return placement;
    }

    ConsensusRolePlacement ConsensusExperimentConfig::ResolveBidlPlacement(const ConsensusTopology& topologyResult) const {
        const uint32_t effectiveClients = GetEffectiveClientCount();
        if (topologyResult.endpointPhysicalIds.size() < 2 * replicaCount + 1 + effectiveClients) {
            throw std::invalid_argument("BIDL placement requires orderers, executors, a sequencer, and clients");
        }
        std::vector<LegacyFlowRecord> flows = GetLegacyBlockchainFlows(flowFile);
        ConsensusRolePlacement placement;
        placement.ordererPhysicalIds = ParseIdList(ordererPhysicalIds);
        placement.executorPhysicalIds = ParseIdList(executorPhysicalIds);
        placement.clientPhysicalIds = ParseIdList(clientPhysicalIds);
        if (placement.clientPhysicalIds.empty() && clientPhysicalId != std::numeric_limits<uint32_t>::max()) {
            placement.clientPhysicalIds.push_back(clientPhysicalId);
        }
        if (placement.clientPhysicalIds.empty() && !flows.empty() && (workload == "auto" || workload == "trace")) {
            std::set<uint32_t> sources;
            for (const auto& flow : flows) {
                if (sources.insert(flow.source).second) {
                    placement.clientPhysicalIds.push_back(flow.source);
                }
            }
        }
        placement.ingressPhysicalId =
            ingressPhysicalId != std::numeric_limits<uint32_t>::max()
                ? ingressPhysicalId
                : (!flows.empty() ? flows.front().destination : topologyResult.endpointPhysicalIds[2 * replicaCount]);
        ValidatePhysicalHost(topologyResult, placement.ingressPhysicalId, "BIDL sequencer");
        std::set<uint32_t> used{placement.ingressPhysicalId};
        for (uint32_t physicalId : placement.clientPhysicalIds) {
            ValidatePhysicalHost(topologyResult, physicalId, "client");
            if (!used.insert(physicalId).second) {
                throw std::invalid_argument("BIDL client and sequencer must use different hosts");
            }
        }
        for (uint32_t physicalId : placement.ordererPhysicalIds) {
            ValidatePhysicalHost(topologyResult, physicalId, "BIDL orderer");
            if (!used.insert(physicalId).second) {
                throw std::invalid_argument("BIDL role placement overlaps");
            }
        }
        AppendAvailableHosts(placement.ordererPhysicalIds, replicaCount, topologyResult, used);
        for (uint32_t physicalId : placement.executorPhysicalIds) {
            ValidatePhysicalHost(topologyResult, physicalId, "BIDL executor");
            if (!used.insert(physicalId).second) {
                throw std::invalid_argument("BIDL role placement overlaps");
            }
        }
        AppendAvailableHosts(placement.executorPhysicalIds, replicaCount, topologyResult, used);
        AppendAvailableHosts(placement.clientPhysicalIds, effectiveClients, topologyResult, used);
        placement.clientPhysicalId = placement.clientPhysicalIds.front();
        return placement;
    }

    uint32_t ConsensusExperimentConfig::ScheduleLegacyBlockchainWorkload(Ptr<ConsensusClientApplication> client,
                                                                         const ConsensusTopology& topologyResult,
                                                                         const ConsensusRolePlacement& placement, Time start,
                                                                         Time stop) const {
        if (!client) {
            throw std::invalid_argument("legacy blockchain workload requires a client application");
        }
        client->SetAttribute("AutoGenerate", BooleanValue(false));
        ValidatePhysicalHost(topologyResult, placement.clientPhysicalId, "client");
        ValidatePhysicalHost(topologyResult, placement.ingressPhysicalId, "ingress");
        uint32_t scheduled = 0;
        for (const LegacyFlowRecord& flow : GetLegacyBlockchainFlows(flowFile)) {
            if (flow.source != placement.clientPhysicalId || flow.destination != placement.ingressPhysicalId) {
                throw std::invalid_argument("app_id=1 flow does not match role placement");
            }
            Time submission = std::max(start, NanoSeconds(flow.startTimeNs));
            if (submission >= stop) {
                continue;
            }
            Simulator::Schedule(submission - Simulator::Now(), &SubmitLegacyRequest, client, flow.bytes);
            ++scheduled;
        }
        if (HasLegacyBlockchainWorkload() && scheduled == 0) {
            throw std::invalid_argument("no app_id=1 transaction falls inside the application time window");
        }
        return scheduled;
    }

    ConsensusTopology ConsensusExperimentConfig::BuildTcpTopology(uint32_t endpointCount) const {
        if (endpointCount < 2) {
            throw std::invalid_argument("a consensus topology needs at least two endpoints");
        }
        if (UsesRdmaNetwork()) {
            throw std::invalid_argument("BuildTcpTopology cannot install an RDMA/Qbb topology");
        }

        ConsensusTopology result;
        result.endpoints.Create(endpointCount);
        for (uint32_t endpoint = 0; endpoint < endpointCount; ++endpoint) {
            result.endpointPhysicalIds.push_back(endpoint);
            result.physicalToEndpoint[endpoint] = endpoint;
        }

        if (topology == "csma") {
            result.nodes.Add(result.endpoints);
            InternetStackHelper internet;
            internet.Install(result.nodes);

            CsmaHelper link;
            link.SetChannelAttribute("DataRate", StringValue(dataRate));
            link.SetChannelAttribute("Delay", StringValue(linkDelay));
            result.devices = link.Install(result.endpoints);
            for (uint32_t index = 0; index < result.devices.GetN(); ++index) {
                result.deviceLabels.push_back("endpoint-" + std::to_string(index));
            }
            if (queueDisc != "none") {
                TrafficControlHelper trafficControl;
                trafficControl.SetRootQueueDisc(queueDisc);
                result.queueDiscs = trafficControl.Install(result.devices);
            }

            Ipv4AddressHelper addresses;
            addresses.SetBase("10.1.0.0", "255.255.0.0");
            result.endpointInterfaces = addresses.Assign(result.devices);
        } else if (topology == "leaf-spine") {
            NodeContainer leaves;
            NodeContainer spine;
            leaves.Create(2);
            spine.Create(1);
            result.nodes.Add(result.endpoints);
            result.nodes.Add(leaves);
            result.nodes.Add(spine);

            InternetStackHelper internet;
            internet.Install(result.nodes);
            PointToPointHelper link;
            link.SetDeviceAttribute("DataRate", StringValue(dataRate));
            link.SetChannelAttribute("Delay", StringValue(linkDelay));

            uint32_t subnet = 1;
            for (uint32_t endpoint = 0; endpoint < endpointCount; ++endpoint) {
                NetDeviceContainer pair = link.Install(result.endpoints.Get(endpoint), leaves.Get(endpoint % 2));
                result.devices.Add(pair);
                result.deviceLabels.push_back("endpoint-" + std::to_string(endpoint));
                result.deviceLabels.push_back("leaf-" + std::to_string(endpoint % 2) + "-down-" + std::to_string(endpoint));
                if (queueDisc != "none") {
                    TrafficControlHelper trafficControl;
                    trafficControl.SetRootQueueDisc(queueDisc);
                    result.queueDiscs.Add(trafficControl.Install(pair));
                }

                std::ostringstream network;
                network << "10.10." << subnet++ << ".0";
                Ipv4AddressHelper addresses;
                addresses.SetBase(network.str().c_str(), "255.255.255.252");
                Ipv4InterfaceContainer assigned = addresses.Assign(pair);
                result.endpointInterfaces.Add(assigned.Get(0));
            }
            for (uint32_t leaf = 0; leaf < leaves.GetN(); ++leaf) {
                NetDeviceContainer pair = link.Install(leaves.Get(leaf), spine.Get(0));
                result.devices.Add(pair);
                result.deviceLabels.push_back("leaf-" + std::to_string(leaf) + "-up");
                result.deviceLabels.push_back("spine-0-down-" + std::to_string(leaf));
                if (queueDisc != "none") {
                    TrafficControlHelper trafficControl;
                    trafficControl.SetRootQueueDisc(queueDisc);
                    result.queueDiscs.Add(trafficControl.Install(pair));
                }

                std::ostringstream network;
                network << "10.10." << subnet++ << ".0";
                Ipv4AddressHelper addresses;
                addresses.SetBase(network.str().c_str(), "255.255.255.252");
                addresses.Assign(pair);
            }
        } else {
            throw std::invalid_argument("the built-in runner supports topology=csma or leaf-spine");
        }

        result.nodeAddresses.resize(result.nodes.GetN());
        for (uint32_t endpoint = 0; endpoint < result.endpointPhysicalIds.size(); ++endpoint) {
            uint32_t physicalId = result.endpointPhysicalIds[endpoint];
            result.nodeAddresses[physicalId] = result.endpointInterfaces.GetAddress(endpoint);
        }

        Ipv4GlobalRoutingHelper::PopulateRoutingTables();
        return result;
    }

    ConsensusTopology ConsensusExperimentConfig::BuildRdmaTopology(uint32_t endpointCount) const {
        if (endpointCount < 2) {
            throw std::invalid_argument("a consensus topology needs at least two endpoints");
        }
        if (!UsesRdmaNetwork()) {
            throw std::invalid_argument("BuildRdmaTopology requires transport=rdma");
        }
        ConfigureRdmaGlobals(*this);
        if (topology == "qbb-file") {
            LegacyTopologySpec spec = ReadLegacyTopologyFile(topologyFile);
            ConsensusTopology result;
            std::vector<Ptr<Node>> nodes(spec.nodeCount);
            for (uint32_t physical = 0; physical < spec.nodeCount; ++physical) {
                if (spec.switchIds.count(physical) != 0) {
                    Ptr<SwitchNode> node = CreateObject<SwitchNode>();
                    node->SetNodeType(1);
                    nodes[physical] = node;
                    result.switches.Add(node);
                } else {
                    nodes[physical] = CreateObject<Node>();
                    result.physicalToEndpoint[physical] = result.endpoints.GetN();
                    result.endpointPhysicalIds.push_back(physical);
                    result.endpoints.Add(nodes[physical]);
                }
                result.nodes.Add(nodes[physical]);
            }
            if (result.endpoints.GetN() < endpointCount) {
                throw std::invalid_argument("legacy topology has fewer hosts than required endpoints");
            }
            InternetStackHelper internet;
            internet.Install(result.nodes);
            result.nodeAddresses.resize(spec.nodeCount);

            struct Edge {
                uint32_t neighbor;
                uint32_t interface;
                uint64_t delayNs;
                uint64_t rate;
            };
            std::vector<std::vector<Edge>> graph(spec.nodeCount);
            std::vector<bool> addressed(spec.nodeCount, false);
            std::vector<Ptr<Ipv4>> hostIpv4(spec.nodeCount);
            std::vector<uint32_t> hostIpv4Interface(spec.nodeCount, 0);
            for (const auto& link : spec.links) {
                QbbHelper qbb;
                qbb.SetDeviceAttribute("DataRate", StringValue(link.dataRate));
                qbb.SetChannelAttribute("Delay", StringValue(link.delay));
                NetDeviceContainer pair = qbb.Install(nodes[link.source], nodes[link.destination]);
                for (uint32_t side = 0; side < pair.GetN(); ++side) {
                    Ptr<QbbNetDevice> device = DynamicCast<QbbNetDevice>(pair.Get(side));
                    device->SetDynamicThreshold(qbbDynamicThreshold);
                    double effectiveErrorRate = link.errorRate > 0.0 ? link.errorRate : linkErrorRate;
                    if (effectiveErrorRate > 0.0) {
                        Ptr<RateErrorModel> error = CreateObject<RateErrorModel>();
                        error->SetAttribute("ErrorRate", DoubleValue(effectiveErrorRate));
                        error->SetAttribute("ErrorUnit", StringValue("ERROR_UNIT_PACKET"));
                        device->SetReceiveErrorModel(error);
                    }
                }
                result.devices.Add(pair);
                result.deviceLabels.push_back("node-" + std::to_string(link.source) + "-to-" + std::to_string(link.destination));
                result.deviceLabels.push_back("node-" + std::to_string(link.destination) + "-to-" + std::to_string(link.source));
                uint64_t delayNs = Time(link.delay).GetNanoSeconds();
                uint64_t rate = DataRate(link.dataRate).GetBitRate();
                graph[link.source].push_back({link.destination, pair.Get(0)->GetIfIndex(), delayNs, rate});
                graph[link.destination].push_back({link.source, pair.Get(1)->GetIfIndex(), delayNs, rate});
                for (uint32_t side = 0; side < 2; ++side) {
                    uint32_t physical = side == 0 ? link.source : link.destination;
                    if (spec.switchIds.count(physical) == 0 && !addressed[physical]) {
                        Ptr<Ipv4> ipv4 = nodes[physical]->GetObject<Ipv4>();
                        uint32_t interface = ipv4->AddInterface(pair.Get(side));
                        ipv4->AddAddress(interface, Ipv4InterfaceAddress(LegacyNodeAddress(physical), Ipv4Mask("255.0.0.0")));
                        ipv4->SetUp(interface);
                        result.nodeAddresses[physical] = LegacyNodeAddress(physical);
                        hostIpv4[physical] = ipv4;
                        hostIpv4Interface[physical] = interface;
                        addressed[physical] = true;
                    }
                }
            }
            for (uint32_t physical : result.endpointPhysicalIds) {
                if (!addressed[physical]) {
                    throw std::invalid_argument("legacy topology contains an isolated host");
                }
                result.endpointInterfaces.Add(hostIpv4[physical], hostIpv4Interface[physical]);
            }

            using Route = std::pair<Ipv4Address, uint32_t>;
            std::vector<std::vector<Route>> hostRoutes(spec.nodeCount);
            const uint64_t infinity = std::numeric_limits<uint64_t>::max();
            for (uint32_t destinationIndex = 0; destinationIndex < result.endpointPhysicalIds.size(); ++destinationIndex) {
                uint32_t destination = result.endpointPhysicalIds[destinationIndex];
                std::vector<uint64_t> distance(spec.nodeCount, infinity);
                using State = std::pair<uint64_t, uint32_t>;
                std::priority_queue<State, std::vector<State>, std::greater<State>> pending;
                distance[destination] = 0;
                pending.push({0, destination});
                while (!pending.empty()) {
                    auto [currentDistance, current] = pending.top();
                    pending.pop();
                    if (currentDistance != distance[current]) {
                        continue;
                    }
                    for (const auto& edge : graph[current]) {
                        if (distance[edge.neighbor] > currentDistance + edge.delayNs) {
                            distance[edge.neighbor] = currentDistance + edge.delayNs;
                            pending.push({distance[edge.neighbor], edge.neighbor});
                        }
                    }
                }
                Ipv4Address destinationAddress = LegacyNodeAddress(destination);
                for (uint32_t current = 0; current < spec.nodeCount; ++current) {
                    if (current == destination || distance[current] == infinity) {
                        continue;
                    }
                    for (const auto& edge : graph[current]) {
                        if (distance[edge.neighbor] != infinity && distance[current] == edge.delayNs + distance[edge.neighbor]) {
                            if (spec.switchIds.count(current) != 0) {
                                DynamicCast<SwitchNode>(nodes[current])->AddTableEntry(destinationAddress, edge.interface);
                            } else {
                                hostRoutes[current].push_back({destinationAddress, edge.interface});
                            }
                        }
                    }
                }
                for (uint32_t sourceIndex = 0; sourceIndex < result.endpointPhysicalIds.size(); ++sourceIndex) {
                    if (sourceIndex == destinationIndex) {
                        continue;
                    }
                    uint32_t current = result.endpointPhysicalIds[sourceIndex];
                    uint64_t propagation = distance[current];
                    uint64_t serialization = 0;
                    uint64_t bottleneck = infinity;
                    std::set<uint32_t> visited;
                    while (current != destination && visited.insert(current).second) {
                        auto next = std::find_if(graph[current].begin(), graph[current].end(), [&](const Edge& edge) {
                            return distance[current] == edge.delayNs + distance[edge.neighbor];
                        });
                        if (next == graph[current].end()) {
                            break;
                        }
                        serialization += (static_cast<uint64_t>(rdmaMtu) * 8ULL * 1000000000ULL + next->rate - 1) / next->rate;
                        bottleneck = std::min(bottleneck, next->rate);
                        current = next->neighbor;
                    }
                    if (current != destination) {
                        throw std::invalid_argument("legacy topology is disconnected");
                    }
                    uint64_t rtt = 2 * propagation + serialization;
                    uint64_t bdp = (rtt * bottleneck + 8000000000ULL - 1) / 8000000000ULL;
                    result.pairRttNs[{sourceIndex, destinationIndex}] = rtt;
                    result.pairBdpBytes[{sourceIndex, destinationIndex}] = bdp;
                    result.maxRttNs = std::max(result.maxRttNs, rtt);
                    result.maxBdpBytes = std::max(result.maxBdpBytes, bdp);
                }
            }

            for (auto iterator = result.switches.Begin(); iterator != result.switches.End(); ++iterator) {
                Ptr<ConsensusSwitchPolicy> policy = CreateSwitchPolicy(*this);
                policy->Configure(*iterator);
                result.switchPolicies.push_back(policy);
                (*iterator)->SetAttribute("AckHighPrio", UintegerValue(ackHighPriority ? 1 : 0));
                (*iterator)->SetAttribute("MaxRtt", UintegerValue(result.maxRttNs));
            }
            for (uint32_t endpoint = 0; endpoint < result.endpointPhysicalIds.size(); ++endpoint) {
                uint32_t physical = result.endpointPhysicalIds[endpoint];
                Ptr<RdmaHw> hardware = CreateObject<RdmaHw>();
                ConfigureRdmaHardware(hardware, *this, result.maxBdpBytes);
                Ptr<RdmaDriver> driver = CreateObject<RdmaDriver>();
                driver->SetNode(nodes[physical]);
                driver->SetRdmaHw(hardware);
                nodes[physical]->AggregateObject(driver);
                driver->Init();
                for (auto route : hostRoutes[physical]) {
                    hardware->AddTableEntry(route.first, route.second);
                }
            }
            return result;
        }
        if (topology != "qbb-star" && topology != "qbb-leaf-spine") {
            throw std::invalid_argument("the RDMA runner supports topology=qbb-star or qbb-leaf-spine");
        }

        ConsensusTopology result;
        result.endpoints.Create(endpointCount);
        for (uint32_t endpoint = 0; endpoint < endpointCount; ++endpoint) {
            result.endpointPhysicalIds.push_back(endpoint);
            result.physicalToEndpoint[endpoint] = endpoint;
        }
        uint32_t activeLeafCount = topology == "qbb-star" ? 1 : leafCount;
        uint32_t activeSpineCount = topology == "qbb-star" ? 0 : spineCount;
        std::vector<Ptr<SwitchNode>> leaves;
        std::vector<Ptr<SwitchNode>> spines;
        for (uint32_t leaf = 0; leaf < activeLeafCount; ++leaf) {
            Ptr<SwitchNode> node = CreateObject<SwitchNode>();
            node->SetNodeType(1);
            leaves.push_back(node);
            result.switches.Add(node);
        }
        for (uint32_t spine = 0; spine < activeSpineCount; ++spine) {
            Ptr<SwitchNode> node = CreateObject<SwitchNode>();
            node->SetNodeType(2);
            spines.push_back(node);
            result.switches.Add(node);
        }
        result.nodes.Add(result.endpoints);
        result.nodes.Add(result.switches);

        InternetStackHelper internet;
        internet.Install(result.nodes);
        result.nodeAddresses.resize(result.nodes.GetN());

        QbbHelper qbb;
        qbb.SetDeviceAttribute("DataRate", StringValue(dataRate));
        qbb.SetChannelAttribute("Delay", StringValue(linkDelay));
        std::vector<uint32_t> hostInterfaces;
        std::vector<Ipv4Address> hostAddresses;
        std::vector<uint32_t> endpointLeaves;
        std::vector<uint32_t> leafHostInterfaces(endpointCount);
        std::vector<std::vector<uint32_t>> leafSpineInterfaces(activeLeafCount, std::vector<uint32_t>(activeSpineCount));
        std::vector<std::vector<uint32_t>> spineLeafInterfaces(activeSpineCount, std::vector<uint32_t>(activeLeafCount));
        hostInterfaces.reserve(endpointCount);
        hostAddresses.reserve(endpointCount);
        endpointLeaves.reserve(endpointCount);

        for (uint32_t index = 0; index < endpointCount; ++index) {
            uint32_t leaf = index % activeLeafCount;
            NetDeviceContainer pair = qbb.Install(result.endpoints.Get(index), leaves[leaf]);
            DynamicCast<QbbNetDevice>(pair.Get(0))->SetDynamicThreshold(qbbDynamicThreshold);
            DynamicCast<QbbNetDevice>(pair.Get(1))->SetDynamicThreshold(qbbDynamicThreshold);
            if (linkErrorRate > 0.0) {
                for (uint32_t side = 0; side < 2; ++side) {
                    Ptr<RateErrorModel> error = CreateObject<RateErrorModel>();
                    error->SetAttribute("ErrorRate", DoubleValue(linkErrorRate));
                    error->SetAttribute("ErrorUnit", StringValue("ERROR_UNIT_PACKET"));
                    DynamicCast<QbbNetDevice>(pair.Get(side))->SetReceiveErrorModel(error);
                }
            }
            result.devices.Add(pair);
            result.deviceLabels.push_back("endpoint-" + std::to_string(index));
            result.deviceLabels.push_back("leaf-" + std::to_string(leaf) + "-host-" + std::to_string(index));

            Ptr<Ipv4> ipv4 = result.endpoints.Get(index)->GetObject<Ipv4>();
            uint32_t interface = ipv4->AddInterface(pair.Get(0));
            Ipv4Address address(0x0b000001 + index);
            ipv4->AddAddress(interface, Ipv4InterfaceAddress(address, Ipv4Mask("255.0.0.0")));
            ipv4->SetUp(interface);
            result.nodeAddresses[index] = address;
            result.endpointInterfaces.Add(ipv4, interface);
            hostInterfaces.push_back(pair.Get(0)->GetIfIndex());
            hostAddresses.push_back(address);
            endpointLeaves.push_back(leaf);
            leafHostInterfaces[index] = pair.Get(1)->GetIfIndex();
            leaves[leaf]->AddTableEntry(hostAddresses.back(), leafHostInterfaces[index]);
        }

        for (uint32_t leaf = 0; leaf < activeLeafCount; ++leaf) {
            for (uint32_t spine = 0; spine < activeSpineCount; ++spine) {
                NetDeviceContainer pair = qbb.Install(leaves[leaf], spines[spine]);
                DynamicCast<QbbNetDevice>(pair.Get(0))->SetDynamicThreshold(qbbDynamicThreshold);
                DynamicCast<QbbNetDevice>(pair.Get(1))->SetDynamicThreshold(qbbDynamicThreshold);
                if (linkErrorRate > 0.0) {
                    for (uint32_t side = 0; side < 2; ++side) {
                        Ptr<RateErrorModel> error = CreateObject<RateErrorModel>();
                        error->SetAttribute("ErrorRate", DoubleValue(linkErrorRate));
                        error->SetAttribute("ErrorUnit", StringValue("ERROR_UNIT_PACKET"));
                        DynamicCast<QbbNetDevice>(pair.Get(side))->SetReceiveErrorModel(error);
                    }
                }
                result.devices.Add(pair);
                result.deviceLabels.push_back("leaf-" + std::to_string(leaf) + "-spine-" + std::to_string(spine));
                result.deviceLabels.push_back("spine-" + std::to_string(spine) + "-leaf-" + std::to_string(leaf));
                leafSpineInterfaces[leaf][spine] = pair.Get(0)->GetIfIndex();
                spineLeafInterfaces[spine][leaf] = pair.Get(1)->GetIfIndex();
            }
        }

        for (uint32_t destination = 0; destination < endpointCount; ++destination) {
            uint32_t destinationLeaf = endpointLeaves[destination];
            for (uint32_t leaf = 0; leaf < activeLeafCount; ++leaf) {
                if (leaf == destinationLeaf) {
                    continue;
                }
                for (uint32_t spine = 0; spine < activeSpineCount; ++spine) {
                    leaves[leaf]->AddTableEntry(hostAddresses[destination], leafSpineInterfaces[leaf][spine]);
                }
            }
            for (uint32_t spine = 0; spine < activeSpineCount; ++spine) {
                spines[spine]->AddTableEntry(hostAddresses[destination], spineLeafInterfaces[spine][destinationLeaf]);
            }
        }

        for (auto iterator = result.switches.Begin(); iterator != result.switches.End(); ++iterator) {
            Ptr<ConsensusSwitchPolicy> policy = CreateSwitchPolicy(*this);
            NS_ABORT_MSG_IF(!policy, "Configured switch policy is not a ConsensusSwitchPolicy");
            policy->Configure(*iterator);
            result.switchPolicies.push_back(policy);
        }

        uint64_t hops = activeSpineCount == 0 ? 2 : 4;
        result.maxRttNs = 2 * hops * static_cast<uint64_t>(Time(linkDelay).GetNanoSeconds()) +
                          hops * static_cast<uint64_t>(rdmaMtu) * 8ULL * 1000000000ULL / DataRate(dataRate).GetBitRate();
        result.maxBdpBytes = result.maxRttNs * DataRate(dataRate).GetBitRate() / 8000000000ULL;
        for (auto iterator = result.switches.Begin(); iterator != result.switches.End(); ++iterator) {
            (*iterator)->SetAttribute("AckHighPrio", UintegerValue(ackHighPriority ? 1 : 0));
            (*iterator)->SetAttribute("MaxRtt", UintegerValue(result.maxRttNs));
        }
        for (uint32_t source = 0; source < endpointCount; ++source) {
            for (uint32_t destination = 0; destination < endpointCount; ++destination) {
                if (source != destination) {
                    result.pairRttNs[{source, destination}] = result.maxRttNs;
                    result.pairBdpBytes[{source, destination}] = result.maxBdpBytes;
                }
            }
        }

        for (uint32_t index = 0; index < endpointCount; ++index) {
            Ptr<RdmaHw> hardware = CreateObject<RdmaHw>();
            ConfigureRdmaHardware(hardware, *this, result.maxBdpBytes);

            Ptr<RdmaDriver> driver = CreateObject<RdmaDriver>();
            driver->SetNode(result.endpoints.Get(index));
            driver->SetRdmaHw(hardware);
            result.endpoints.Get(index)->AggregateObject(driver);
            driver->Init();
            for (auto destination : hostAddresses) {
                if (destination != hostAddresses[index]) {
                    hardware->AddTableEntry(destination, hostInterfaces[index]);
                }
            }
        }
        return result;
    }

    ApplicationContainer ConsensusExperimentConfig::InstallBackgroundTraffic(const NodeContainer& nodes,
                                                                             const Ipv4InterfaceContainer& interfaces) const {
        ApplicationContainer applications;
        if (backgroundFlows == 0) {
            return applications;
        }
        if (nodes.GetN() < 2 || interfaces.GetN() != nodes.GetN()) {
            throw std::invalid_argument("background traffic requires one IPv4 interface per node");
        }

        for (uint32_t flow = 0; flow < backgroundFlows; ++flow) {
            uint32_t source = flow % nodes.GetN();
            uint32_t destination = (source + 1 + flow / nodes.GetN()) % nodes.GetN();
            if (destination == source) {
                destination = (destination + 1) % nodes.GetN();
            }
            uint16_t port = static_cast<uint16_t>(20000 + flow);
            Address sinkAddress(InetSocketAddress(interfaces.GetAddress(destination), port));

            PacketSinkHelper sink("ns3::TcpSocketFactory", InetSocketAddress(Ipv4Address::GetAny(), port));
            ApplicationContainer sinkApp = sink.Install(nodes.Get(destination));
            applications.Add(sinkApp);

            OnOffHelper sourceHelper("ns3::TcpSocketFactory", sinkAddress);
            sourceHelper.SetConstantRate(DataRate(backgroundRate), backgroundPacketSize);
            ApplicationContainer sourceApp = sourceHelper.Install(nodes.Get(source));
            applications.Add(sourceApp);
        }
        return applications;
    }

    ApplicationContainer ConsensusExperimentConfig::InstallRdmaBackgroundTraffic(const ConsensusTopology& topologyResult,
                                                                                 Time applicationStart) const {
        ApplicationContainer applications;
        const NodeContainer& nodes = topologyResult.endpoints;
        const Ipv4InterfaceContainer& interfaces = topologyResult.endpointInterfaces;
        if (rdmaLongFlows == 0 && rdmaShortFlows == 0 && rdmaIncastSenders == 0 && flowFile.empty()) {
            return applications;
        }
        if (!UsesRdmaNetwork() || nodes.GetN() < 2 || interfaces.GetN() != nodes.GetN()) {
            throw std::invalid_argument("RDMA background traffic requires an RDMA topology and one address per endpoint");
        }

        constexpr uint32_t ID_BASE = 50000;
        constexpr uint16_t PORT = 19000;
        std::vector<Ptr<RdmaWorkloadApplication>> workloads;
        for (uint32_t index = 0; index < nodes.GetN(); ++index) {
            uint32_t localId = ID_BASE + index;
            Ptr<RdmaConsensusTransport> rdma = CreateObject<RdmaConsensusTransport>();
            rdma->SetAttribute("PriorityGroup", UintegerValue(rdmaBackgroundPriorityGroup));
            uint32_t window = rdmaWindow;
            if (window == 0 && rdmaHasWindow && topologyResult.maxBdpBytes > 0) {
                window = static_cast<uint32_t>(std::min<uint64_t>(topologyResult.maxBdpBytes, std::numeric_limits<uint32_t>::max()));
            }
            rdma->SetAttribute("Window", UintegerValue(window));
            rdma->SetAttribute("BaseRttNs",
                               UintegerValue(topologyResult.maxRttNs > 0 ? topologyResult.maxRttNs
                                                                         : static_cast<uint64_t>(Time(linkDelay).GetNanoSeconds()) * 4));
            rdma->SetLocalEndpoint(localId, InetSocketAddress(interfaces.GetAddress(index), PORT));
            for (uint32_t peer = 0; peer < nodes.GetN(); ++peer) {
                if (peer != index) {
                    rdma->SetPeerAddress(ID_BASE + peer, InetSocketAddress(interfaces.GetAddress(peer), PORT));
                }
            }

            Ptr<RdmaWorkloadApplication> application = CreateObject<RdmaWorkloadApplication>();
            application->SetNodeId(localId);
            application->SetTransport(rdma);
            nodes.Get(index)->AddApplication(application);
            workloads.push_back(application);
            applications.Add(application);
        }

        uint64_t flowId = 1;
        for (uint32_t flow = 0; flow < rdmaLongFlows; ++flow) {
            uint32_t source = flow % nodes.GetN();
            uint32_t destination = (source + 1 + flow / nodes.GetN()) % nodes.GetN();
            workloads[source]->AddFlow({ID_BASE + destination, flowId++, rdmaLongFlowBytes, MilliSeconds(rdmaLongStartMs)});
        }
        for (uint32_t flow = 0; flow < rdmaShortFlows; ++flow) {
            uint32_t source = flow % nodes.GetN();
            uint32_t destination = (source + 1 + flow / nodes.GetN()) % nodes.GetN();
            workloads[source]->AddFlow({ID_BASE + destination, flowId++, rdmaShortFlowBytes,
                                        MilliSeconds(rdmaShortStartMs) + MicroSeconds(flow * rdmaShortIntervalUs)});
        }
        uint32_t incastSenders = std::min<uint32_t>(rdmaIncastSenders, nodes.GetN() - 1);
        for (uint32_t burst = 0; burst < rdmaIncastBursts; ++burst) {
            Time offset = MilliSeconds(rdmaIncastStartMs) + MicroSeconds(burst * rdmaIncastIntervalUs);
            for (uint32_t sender = 1; sender <= incastSenders; ++sender) {
                workloads[sender]->AddFlow({ID_BASE, flowId++, rdmaIncastFlowBytes, offset});
            }
        }

        if (!flowFile.empty()) {
            std::vector<LegacyFlowRecord> records = ReadLegacyFlowFile(flowFile);
            std::map<std::pair<uint32_t, uint16_t>, Ptr<RdmaWorkloadApplication>> receivers;
            uint32_t replayIndex = 0;
            for (const LegacyFlowRecord& record : records) {
                if (record.applicationId != 0) {
                    continue;
                }
                auto source = topologyResult.physicalToEndpoint.find(record.source);
                auto destination = topologyResult.physicalToEndpoint.find(record.destination);
                if (source == topologyResult.physicalToEndpoint.end() || destination == topologyResult.physicalToEndpoint.end()) {
                    throw std::invalid_argument("legacy app_id=0 flow endpoint is not a host");
                }
                if (replayIndex >= 30000) {
                    throw std::invalid_argument("too many legacy flows for unique RDMA source ports");
                }
                uint32_t sourceIndex = source->second;
                uint32_t destinationIndex = destination->second;
                uint32_t sourceId = ID_BASE + sourceIndex;
                uint32_t destinationId = ID_BASE + destinationIndex;
                auto receiverKey = std::make_pair(destinationIndex, record.destinationPort);
                if (receivers.count(receiverKey) == 0) {
                    Ptr<RdmaConsensusTransport> receiveTransport = CreateObject<RdmaConsensusTransport>();
                    receiveTransport->SetAttribute("PriorityGroup", UintegerValue(record.priorityGroup));
                    receiveTransport->SetLocalEndpoint(destinationId,
                                                       InetSocketAddress(interfaces.GetAddress(destinationIndex), record.destinationPort));
                    Ptr<RdmaWorkloadApplication> receiver = CreateObject<RdmaWorkloadApplication>();
                    receiver->SetNodeId(destinationId);
                    receiver->SetTransport(receiveTransport);
                    nodes.Get(destinationIndex)->AddApplication(receiver);
                    receivers[receiverKey] = receiver;
                    applications.Add(receiver);
                }

                Ptr<RdmaConsensusTransport> sendTransport = CreateObject<RdmaConsensusTransport>();
                sendTransport->SetAttribute("PriorityGroup", UintegerValue(record.priorityGroup));
                auto pair = std::make_pair(sourceIndex, destinationIndex);
                uint64_t pairRtt = topologyResult.pairRttNs.count(pair) != 0 ? topologyResult.pairRttNs.at(pair) : topologyResult.maxRttNs;
                uint64_t pairBdp =
                    topologyResult.pairBdpBytes.count(pair) != 0 ? topologyResult.pairBdpBytes.at(pair) : topologyResult.maxBdpBytes;
                uint64_t selectedRtt = rdmaGlobalRtt ? topologyResult.maxRttNs : pairRtt;
                uint64_t selectedBdp = rdmaGlobalRtt ? topologyResult.maxBdpBytes : pairBdp;
                uint32_t window = rdmaWindow;
                if (window == 0 && rdmaHasWindow) {
                    window = static_cast<uint32_t>(std::min<uint64_t>(selectedBdp, std::numeric_limits<uint32_t>::max()));
                }
                sendTransport->SetAttribute("Window", UintegerValue(window));
                sendTransport->SetAttribute("BaseRttNs", UintegerValue(selectedRtt));
                uint16_t localPort = static_cast<uint16_t>(30000 + replayIndex++);
                sendTransport->SetLocalEndpoint(sourceId, InetSocketAddress(interfaces.GetAddress(sourceIndex), localPort));
                sendTransport->SetPeerAddress(destinationId,
                                              InetSocketAddress(interfaces.GetAddress(destinationIndex), record.destinationPort));
                Ptr<RdmaWorkloadApplication> sender = CreateObject<RdmaWorkloadApplication>();
                sender->SetNodeId(sourceId);
                sender->SetTransport(sendTransport);
                Time absoluteStart = NanoSeconds(record.startTimeNs);
                sender->AddFlow({destinationId, static_cast<uint64_t>(1000000) + replayIndex, record.bytes,
                                 absoluteStart > applicationStart ? absoluteStart - applicationStart : Seconds(0)});
                nodes.Get(sourceIndex)->AddApplication(sender);
                applications.Add(sender);
            }
        }

        return applications;
    }

    TypeId ConsensusStatistics::GetTypeId() {
        static TypeId tid =
            TypeId("ns3::ConsensusStatistics").SetParent<Object>().SetGroupName("Consensus").AddConstructor<ConsensusStatistics>();
        return tid;
    }

    ConsensusStatistics::ConsensusStatistics()
        : m_submittedRequests(0), m_completedRequests(0), m_retransmissions(0), m_timedOutRequests(0), m_decisions(0), m_viewChanges(0),
          m_bidlBlocks(0), m_bidlTransactions(0), m_firstSubmission(Seconds(0)), m_lastCompletion(Seconds(0)),
          m_measurementStart(Seconds(0)), m_measurementStop(Seconds(0)), m_hasSubmission(false) {}

    void ConsensusStatistics::SetMeasurementWindow(Time start, Time stop) {
        m_measurementStart = start;
        m_measurementStop = stop;
    }

    void ConsensusStatistics::AttachClient(Ptr<ConsensusClientApplication> client) {
        if (!client) {
            return;
        }
        m_clients.push_back(client);
        client->TraceConnectWithoutContext("RequestSubmitted", MakeCallback(&ConsensusStatistics::OnRequestSubmitted, this));
        client->TraceConnectWithoutContext("RequestCompleted", MakeCallback(&ConsensusStatistics::OnRequestCompleted, this));
        client->TraceConnectWithoutContext("RequestRetransmitted", MakeCallback(&ConsensusStatistics::OnRequestRetransmitted, this));
        client->TraceConnectWithoutContext("RequestTimedOut", MakeCallback(&ConsensusStatistics::OnRequestTimedOut, this));
    }

    void ConsensusStatistics::AttachReplicas(const ApplicationContainer& applications) {
        for (uint32_t index = 0; index < applications.GetN(); ++index) {
            Ptr<ConsensusApplication> application = DynamicCast<ConsensusApplication>(applications.Get(index));
            if (!application) {
                continue;
            }
            application->TraceConnectWithoutContext("MessageTx", MakeCallback(&ConsensusStatistics::OnMessage, this));
            application->TraceConnectWithoutContext("Decision", MakeCallback(&ConsensusStatistics::OnDecision, this));

            Ptr<ConsensusEngine> engine = application->GetEngine();
            if (engine) {
                engine->TraceConnectWithoutContext("LeaderChanged", MakeCallback(&ConsensusStatistics::OnViewChanged, this));
                engine->TraceConnectWithoutContext("ProtocolPhase", MakeCallback(&ConsensusStatistics::OnEnginePhase, this));
            }
        }
    }

    void ConsensusStatistics::AttachBidlApplications(const ApplicationContainer& applications) {
        for (uint32_t index = 0; index < applications.GetN(); ++index) {
            Ptr<BidlApplication> application = DynamicCast<BidlApplication>(applications.Get(index));
            if (!application) {
                continue;
            }
            m_bidlApplications.push_back(application);
            application->TraceConnectWithoutContext("MessageTx", MakeCallback(&ConsensusStatistics::OnMessage, this));
            application->TraceConnectWithoutContext("PhaseChanged", MakeCallback(&ConsensusStatistics::OnBidlPhase, this));
            application->TraceConnectWithoutContext("BatchCreated", MakeCallback(&ConsensusStatistics::OnBidlBatch, this));
            application->TraceConnectWithoutContext("BlockCommitted", MakeCallback(&ConsensusStatistics::OnBidlCommit, this));
            Ptr<ConsensusEngine> engine = application->GetEngine();
            if (engine) {
                engine->TraceConnectWithoutContext("LeaderChanged", MakeCallback(&ConsensusStatistics::OnViewChanged, this));
                engine->TraceConnectWithoutContext("ProtocolPhase", MakeCallback(&ConsensusStatistics::OnEnginePhase, this));
            }
        }
    }

    void ConsensusStatistics::AttachRdmaWorkloads(const ApplicationContainer& applications) {
        for (uint32_t index = 0; index < applications.GetN(); ++index) {
            Ptr<RdmaWorkloadApplication> application = DynamicCast<RdmaWorkloadApplication>(applications.Get(index));
            if (application) {
                application->TraceConnectWithoutContext("FlowCompleted",
                                                        MakeCallback(&ConsensusStatistics::OnBackgroundFlowCompleted, this));
            }
        }
    }

    void ConsensusStatistics::AttachNetDevices(const NetDeviceContainer& devices, const DataRate& linkRate,
                                               const std::vector<std::string>& labels) {
        m_devices.resize(devices.GetN());
        for (uint32_t index = 0; index < devices.GetN(); ++index) {
            m_devices[index].rate = linkRate;
            m_devices[index].label = index < labels.size() ? labels[index] : "device-" + std::to_string(index);
            Ptr<NetDevice> device = devices.Get(index);
            Ptr<QbbNetDevice> qbbDevice = DynamicCast<QbbNetDevice>(device);
            if (qbbDevice) {
                m_devices[index].rate = qbbDevice->GetDataRate();
            }
            if (device->IsQbb()) {
                device->TraceConnectWithoutContext("QbbDequeue", MakeCallback(&ConsensusStatistics::OnQbbTx, this).Bind(index));
            } else {
                device->TraceConnectWithoutContext("MacTx", MakeCallback(&ConsensusStatistics::OnDeviceTx, this).Bind(index));
            }
            device->TraceConnectWithoutContext("MacRx", MakeCallback(&ConsensusStatistics::OnDeviceRx, this).Bind(index));
            if (device->IsQbb()) {
                device->TraceConnectWithoutContext("QbbDrop", MakeCallback(&ConsensusStatistics::OnQbbDrop, this).Bind(index));
                device->TraceConnectWithoutContext("QbbPfc", MakeCallback(&ConsensusStatistics::OnPfc, this).Bind(index));
            } else {
                device->TraceConnectWithoutContext("MacTxDrop", MakeCallback(&ConsensusStatistics::OnDeviceDrop, this).Bind(index));
            }
            device->TraceConnectWithoutContext("MacRxDrop", MakeCallback(&ConsensusStatistics::OnDeviceDrop, this).Bind(index));
        }
    }

    void ConsensusStatistics::AttachQueueDiscs(const QueueDiscContainer& queueDiscs) {
        m_queues.resize(queueDiscs.GetN());
        for (uint32_t index = 0; index < queueDiscs.GetN(); ++index) {
            Ptr<QueueDisc> queue = queueDiscs.Get(index);
            queue->TraceConnectWithoutContext("PacketsInQueue", MakeCallback(&ConsensusStatistics::OnQueuePackets, this).Bind(index));
            queue->TraceConnectWithoutContext("BytesInQueue", MakeCallback(&ConsensusStatistics::OnQueueBytes, this).Bind(index));
            queue->TraceConnectWithoutContext("Drop", MakeCallback(&ConsensusStatistics::OnQueueDropStored, this).Bind(index));
            queue->TraceConnectWithoutContext("DropBeforeEnqueue", MakeCallback(&ConsensusStatistics::OnQueueDrop, this).Bind(index));
            queue->TraceConnectWithoutContext("DropAfterDequeue", MakeCallback(&ConsensusStatistics::OnQueueDrop, this).Bind(index));
            queue->TraceConnectWithoutContext("Mark", MakeCallback(&ConsensusStatistics::OnQueueMark, this).Bind(index));
            queue->TraceConnectWithoutContext("SojournTime", MakeCallback(&ConsensusStatistics::OnSojourn, this).Bind(index));
        }
    }

    void ConsensusStatistics::AttachSwitchPolicies(const std::vector<Ptr<ConsensusSwitchPolicy>>& policies) {
        m_switchPolicies = policies;
    }

    uint64_t ConsensusStatistics::GetCompletedRequests() const {
        return m_completedRequests;
    }

    uint64_t ConsensusStatistics::GetMeasuredRequests() const {
        return m_requestSamples.size();
    }

    uint64_t ConsensusStatistics::GetCompletedTransactions() const {
        uint64_t total = 0;
        for (const auto& client : m_clients) {
            total += client->GetCompletedTransactions();
        }
        if (total == 0) {
            for (const auto& application : m_bidlApplications) {
                total = std::max(total, application->GetCommittedTransactions());
            }
        }
        return total;
    }

    uint64_t ConsensusStatistics::GetRetransmissions() const {
        return m_retransmissions;
    }

    uint64_t ConsensusStatistics::GetTimedOutRequests() const {
        return m_timedOutRequests;
    }

    uint64_t ConsensusStatistics::GetViewChanges() const {
        return m_views.size();
    }

    double ConsensusStatistics::GetRequestThroughput() const {
        Time duration = m_measurementStop - m_measurementStart;
        return duration.IsZero() ? 0.0 : static_cast<double>(GetMeasuredRequests()) / duration.GetSeconds();
    }

    double ConsensusStatistics::GetTransactionThroughput() const {
        Time duration = m_measurementStop - m_measurementStart;
        if (!duration.IsZero() && GetCompletedRequests() != 0) {
            double transactionsPerRequest = static_cast<double>(GetCompletedTransactions()) / GetCompletedRequests();
            return transactionsPerRequest * GetMeasuredRequests() / duration.GetSeconds();
        }
        double total = 0.0;
        for (const auto& application : m_bidlApplications) {
            total = std::max(total, application->GetTransactionThroughput());
        }
        return total;
    }

    double ConsensusStatistics::GetLatencyPercentile(double percentile) const {
        if (m_requestSamples.empty()) {
            return 0.0;
        }
        std::vector<double> values;
        values.reserve(m_requestSamples.size());
        for (const auto& sample : m_requestSamples) {
            values.push_back(sample.latency.GetMicroSeconds());
        }
        std::sort(values.begin(), values.end());
        double bounded = std::clamp(percentile, 0.0, 100.0);
        size_t index = static_cast<size_t>(std::ceil((bounded / 100.0) * values.size()) - 1.0);
        return values[std::min(index, values.size() - 1)];
    }

    double ConsensusStatistics::GetPhaseLatencyPercentile(const std::string& protocol, uint16_t phase, double percentile) const {
        std::vector<double> values;
        for (const auto& sample : m_phaseSamples) {
            if (sample.protocol == protocol && sample.phase == phase) {
                values.push_back(sample.elapsed.GetMicroSeconds());
            }
        }
        if (values.empty()) {
            return 0.0;
        }
        std::sort(values.begin(), values.end());
        double bounded = std::clamp(percentile, 0.0, 100.0);
        size_t index = static_cast<size_t>(std::ceil((bounded / 100.0) * values.size()) - 1.0);
        return values[std::min(index, values.size() - 1)];
    }

    void ConsensusStatistics::WriteResults(const std::string& outputPrefix, const ConsensusExperimentConfig& config) const {
        config.WriteJson(outputPrefix + "-config.json");

        std::ofstream summary(outputPrefix + "-summary.csv");
        std::ofstream latency(outputPrefix + "-latency.csv");
        std::ofstream phases(outputPrefix + "-phases.csv");
        std::ofstream messages(outputPrefix + "-messages.csv");
        std::ofstream network(outputPrefix + "-network.csv");
        std::ofstream queues(outputPrefix + "-queues.csv");
        std::ofstream background(outputPrefix + "-background.csv");
        std::ofstream switches(outputPrefix + "-switches.csv");
        if (!summary || !latency || !phases || !messages || !network || !queues || !background || !switches) {
            throw std::runtime_error("cannot write consensus result files");
        }

        std::vector<double> backgroundFct;
        for (const auto& flow : m_backgroundFlows) {
            backgroundFct.push_back(flow.fct.GetMicroSeconds());
        }
        std::sort(backgroundFct.begin(), backgroundFct.end());
        auto backgroundPercentile = [&backgroundFct](double percentile) {
            if (backgroundFct.empty()) {
                return 0.0;
            }
            size_t index = static_cast<size_t>(std::ceil((percentile / 100.0) * backgroundFct.size()) - 1.0);
            return backgroundFct[std::min(index, backgroundFct.size() - 1)];
        };

        summary << "metric,value,unit\n"
                << "submitted_requests," << m_submittedRequests << ",requests\n"
                << "completed_requests," << GetCompletedRequests() << ",requests\n"
                << "measured_requests," << GetMeasuredRequests() << ",requests\n"
                << "measurement_start," << m_measurementStart.GetSeconds() << ",seconds\n"
                << "measurement_stop," << m_measurementStop.GetSeconds() << ",seconds\n"
                << "completed_transactions," << GetCompletedTransactions() << ",transactions\n"
                << "request_throughput," << GetRequestThroughput() << ",requests_per_second\n"
                << "transaction_throughput," << GetTransactionThroughput() << ",transactions_per_second\n"
                << "latency_p50," << GetLatencyPercentile(50) << ",microseconds\n"
                << "latency_p95," << GetLatencyPercentile(95) << ",microseconds\n"
                << "latency_p99," << GetLatencyPercentile(99) << ",microseconds\n"
                << "pbft_prepared_p50," << GetPhaseLatencyPercentile("pbft", PBFT_PREPARED, 50) << ",microseconds\n"
                << "pbft_decided_p50," << GetPhaseLatencyPercentile("pbft", PBFT_DECIDED, 50) << ",microseconds\n"
                << "pbft_decided_p95," << GetPhaseLatencyPercentile("pbft", PBFT_DECIDED, 95) << ",microseconds\n"
                << "bidl_commit_p50," << GetPhaseLatencyPercentile("bidl", BIDL_BLOCK_COMMITTED, 50) << ",microseconds\n"
                << "bidl_commit_p95," << GetPhaseLatencyPercentile("bidl", BIDL_BLOCK_COMMITTED, 95) << ",microseconds\n"
                << "retransmissions," << m_retransmissions << ",messages\n"
                << "timed_out_requests," << m_timedOutRequests << ",requests\n"
                << "replica_decisions," << m_decisions << ",decisions\n"
                << "view_changes," << GetViewChanges() << ",views\n"
                << "replica_view_transitions," << m_viewChanges << ",events\n"
                << "bidl_blocks," << m_bidlBlocks << ",blocks\n"
                << "submitted_transactions," << m_bidlTransactions << ",transactions\n";
        summary << "background_completed_flows," << m_backgroundFlows.size() << ",flows\n"
                << "background_fct_p50," << backgroundPercentile(50) << ",microseconds\n"
                << "background_fct_p95," << backgroundPercentile(95) << ",microseconds\n";

        switches << "policy_index,type,enqueue,dequeue,mark,drop,tagged_events\n";
        for (uint32_t index = 0; index < m_switchPolicies.size(); ++index) {
            const auto& policy = m_switchPolicies[index];
            switches << index << ',' << policy->GetInstanceTypeId().GetName() << ',' << policy->GetEnqueueEvents() << ','
                     << policy->GetDequeueEvents() << ',' << policy->GetMarkEvents() << ',' << policy->GetDropEvents() << ','
                     << policy->GetTaggedEvents() << '\n';
        }

        latency << "client_id,request_id,latency_us\n";
        for (const auto& sample : m_requestSamples) {
            latency << sample.clientId << ',' << sample.requestId << ',' << sample.latency.GetMicroSeconds() << '\n';
        }

        phases << "protocol,node_id,instance_id,phase,elapsed_us\n";
        for (const auto& sample : m_phaseSamples) {
            phases << sample.protocol << ',' << sample.nodeId << ',' << sample.instanceId << ',' << sample.phase << ','
                   << sample.elapsed.GetMicroSeconds() << '\n';
        }

        messages << "message_type,count,bytes\n";
        for (const auto& [type, count] : m_messagesByType) {
            messages << type << ',' << count << ',' << m_messageBytesByType.at(type) << '\n';
        }

        Time window = m_measurementStop - m_measurementStart;
        network << "device,label,tx_packets,tx_bytes,rx_packets,rx_bytes,drops,pfc_pause,pfc_resume,"
                   "utilization\n";
        for (uint32_t index = 0; index < m_devices.size(); ++index) {
            const auto& device = m_devices[index];
            double utilization = window.IsPositive() && device.rate.GetBitRate() > 0
                                     ? static_cast<double>(device.txBytes * 8) / (device.rate.GetBitRate() * window.GetSeconds())
                                     : 0.0;
            network << index << ',' << device.label << ',' << device.txPackets << ',' << device.txBytes << ',' << device.rxPackets << ','
                    << device.rxBytes << ',' << device.drops << ',' << device.pfcPause << ',' << device.pfcResume << ',' << utilization
                    << '\n';
        }

        queues << "queue,max_packets,max_bytes,drops,ecn_marks,average_sojourn_us\n";
        for (uint32_t index = 0; index < m_queues.size(); ++index) {
            const auto& queue = m_queues[index];
            double average = queue.sojournSamples == 0 ? 0.0 : queue.totalSojourn.GetMicroSeconds() / queue.sojournSamples;
            queues << index << ',' << queue.maxPackets << ',' << queue.maxBytes << ',' << queue.drops << ',' << queue.marks << ','
                   << average << '\n';
        }

        background << "receiver_id,sender_id,flow_id,bytes,fct_us\n";
        for (const auto& flow : m_backgroundFlows) {
            background << flow.receiverId << ',' << flow.senderId << ',' << flow.flowId << ',' << flow.bytes << ','
                       << flow.fct.GetMicroSeconds() << '\n';
        }
    }

    void ConsensusStatistics::OnRequestSubmitted(uint32_t clientId, uint64_t requestId, Time time) {
        ++m_submittedRequests;
        m_submissionTimes[{clientId, requestId}] = time;
        if (!m_hasSubmission) {
            m_firstSubmission = time;
            m_hasSubmission = true;
        }
    }

    void ConsensusStatistics::OnRequestCompleted(uint32_t clientId, uint64_t requestId, Time latency) {
        ++m_completedRequests;
        auto submitted = m_submissionTimes.find({clientId, requestId});
        Time completed = Simulator::Now();
        if (submitted != m_submissionTimes.end() && submitted->second >= m_measurementStart && submitted->second < m_measurementStop &&
            completed < m_measurementStop) {
            m_requestSamples.push_back({clientId, requestId, latency});
        }
        m_lastCompletion = completed;
    }

    bool ConsensusStatistics::IsMeasuring() const {
        return Simulator::Now() >= m_measurementStart && Simulator::Now() < m_measurementStop;
    }

    void ConsensusStatistics::OnRequestRetransmitted(uint32_t clientId, uint64_t requestId, uint32_t targetId) {
        (void)clientId;
        (void)requestId;
        (void)targetId;
        if (IsMeasuring()) {
            ++m_retransmissions;
        }
    }

    void ConsensusStatistics::OnRequestTimedOut(uint32_t clientId, uint64_t requestId) {
        (void)clientId;
        (void)requestId;
        if (IsMeasuring()) {
            ++m_timedOutRequests;
        }
    }

    void ConsensusStatistics::OnMessage(uint32_t senderId, uint32_t receiverId, uint16_t type, uint64_t instanceId, uint32_t bytes) {
        if (!IsMeasuring()) {
            return;
        }
        (void)senderId;
        (void)receiverId;
        (void)instanceId;
        ++m_messagesByType[type];
        m_messageBytesByType[type] += bytes;
    }

    void ConsensusStatistics::OnDecision(uint32_t replicaId, uint64_t instanceId, uint64_t digest, Time time) {
        if (!IsMeasuring()) {
            return;
        }
        (void)replicaId;
        (void)instanceId;
        (void)digest;
        (void)time;
        ++m_decisions;
    }

    void ConsensusStatistics::OnViewChanged(uint32_t replicaId, uint64_t view, uint32_t primaryId) {
        if (!IsMeasuring()) {
            return;
        }
        (void)replicaId;
        (void)primaryId;
        ++m_viewChanges;
        m_views.insert(view);
    }

    void ConsensusStatistics::OnPbftPhase(uint32_t replicaId, uint64_t instanceId, uint16_t phase, Time elapsed) {
        m_phaseSamples.push_back({"pbft", replicaId, instanceId, phase, elapsed});
    }

    void ConsensusStatistics::OnEnginePhase(uint32_t replicaId, uint8_t protocol, uint64_t instanceId, uint16_t phase, Time elapsed) {
        if (!IsMeasuring()) {
            return;
        }
        std::string name = "protocol-" + std::to_string(protocol);
        if (protocol == static_cast<uint8_t>(ConsensusProtocol::PBFT)) {
            name = "pbft";
        } else if (protocol == static_cast<uint8_t>(ConsensusProtocol::HOTSTUFF)) {
            name = "hotstuff";
        }
        m_phaseSamples.push_back({name, replicaId, instanceId, phase, elapsed});
    }

    void ConsensusStatistics::OnBidlPhase(uint32_t nodeId, uint64_t blockId, uint16_t phase, Time elapsed) {
        if (!IsMeasuring()) {
            return;
        }
        m_phaseSamples.push_back({"bidl", nodeId, blockId, phase, elapsed});
    }

    void ConsensusStatistics::OnBidlBatch(uint32_t nodeId, uint64_t blockId, uint32_t transactions) {
        if (!IsMeasuring()) {
            return;
        }
        (void)nodeId;
        (void)blockId;
        ++m_bidlBlocks;
        m_bidlTransactions += transactions;
        if (!m_hasSubmission) {
            m_firstSubmission = Simulator::Now();
            m_hasSubmission = true;
        }
    }

    void ConsensusStatistics::OnBidlCommit(uint32_t nodeId, uint64_t blockId, Time latency) {
        if (!IsMeasuring()) {
            return;
        }
        (void)nodeId;
        (void)latency;
        m_committedBidlBlocks.insert(blockId);
        m_lastCompletion = Simulator::Now();
    }

    void ConsensusStatistics::OnBackgroundFlowCompleted(uint32_t receiverId, uint32_t senderId, uint64_t flowId, uint32_t bytes, Time fct) {
        if (!IsMeasuring()) {
            return;
        }
        m_backgroundFlows.push_back({receiverId, senderId, flowId, bytes, fct});
    }

    void ConsensusStatistics::OnDeviceTx(uint32_t device, Ptr<const Packet> packet) {
        if (!IsMeasuring())
            return;
        ++m_devices.at(device).txPackets;
        m_devices.at(device).txBytes += packet->GetSize();
    }

    void ConsensusStatistics::OnDeviceRx(uint32_t device, Ptr<const Packet> packet) {
        if (!IsMeasuring())
            return;
        ++m_devices.at(device).rxPackets;
        m_devices.at(device).rxBytes += packet->GetSize();
    }

    void ConsensusStatistics::OnDeviceDrop(uint32_t device, Ptr<const Packet> packet) {
        if (!IsMeasuring())
            return;
        (void)packet;
        ++m_devices.at(device).drops;
    }

    void ConsensusStatistics::OnQbbTx(uint32_t device, Ptr<const Packet> packet, uint32_t queue) {
        (void)queue;
        OnDeviceTx(device, packet);
    }

    void ConsensusStatistics::OnQbbDrop(uint32_t device, Ptr<const Packet> packet, uint32_t queue) {
        (void)queue;
        OnDeviceDrop(device, packet);
    }

    void ConsensusStatistics::OnPfc(uint32_t device, uint32_t event) {
        if (!IsMeasuring())
            return;
        if (event == 0 || event == 3) {
            ++m_devices.at(device).pfcResume;
        } else {
            ++m_devices.at(device).pfcPause;
        }
    }

    void ConsensusStatistics::OnQueuePackets(uint32_t queue, uint32_t oldValue, uint32_t newValue) {
        if (!IsMeasuring())
            return;
        (void)oldValue;
        m_queues.at(queue).packets = newValue;
        m_queues.at(queue).maxPackets = std::max(m_queues.at(queue).maxPackets, newValue);
    }

    void ConsensusStatistics::OnQueueBytes(uint32_t queue, uint32_t oldValue, uint32_t newValue) {
        if (!IsMeasuring())
            return;
        (void)oldValue;
        m_queues.at(queue).bytes = newValue;
        m_queues.at(queue).maxBytes = std::max(m_queues.at(queue).maxBytes, newValue);
    }

    void ConsensusStatistics::OnQueueDropStored(uint32_t queue, Ptr<const QueueDiscItem> item) {
        if (!IsMeasuring())
            return;
        (void)item;
        ++m_queues.at(queue).drops;
    }

    void ConsensusStatistics::OnQueueDrop(uint32_t queue, Ptr<const QueueDiscItem> item, const char* reason) {
        if (!IsMeasuring())
            return;
        (void)item;
        (void)reason;
        ++m_queues.at(queue).drops;
    }

    void ConsensusStatistics::OnQueueMark(uint32_t queue, Ptr<const QueueDiscItem> item, const char* reason) {
        if (!IsMeasuring())
            return;
        (void)item;
        (void)reason;
        ++m_queues.at(queue).marks;
    }

    void ConsensusStatistics::OnSojourn(uint32_t queue, Time value) {
        if (!IsMeasuring())
            return;
        m_queues.at(queue).totalSojourn += value;
        ++m_queues.at(queue).sojournSamples;
    }

} // namespace ns3
