#include "consensus-engine.h"

#include "ns3/enum.h"
#include "ns3/log.h"
#include "ns3/simulator.h"
#include "ns3/uinteger.h"

#include <algorithm>

namespace ns3 {

    NS_LOG_COMPONENT_DEFINE("ConsensusEngine");
    NS_OBJECT_ENSURE_REGISTERED(ConsensusResourceModel);
    NS_OBJECT_ENSURE_REGISTERED(ConsensusEngine);

    TypeId ConsensusResourceModel::GetTypeId() {
        static TypeId tid =
            TypeId("ns3::ConsensusResourceModel")
                .SetParent<Object>()
                .SetGroupName("Consensus")
                .AddConstructor<ConsensusResourceModel>()
                .AddAttribute("Mode", "Whether resource tasks complete immediately or wait in a finite queue.",
                              EnumValue(RESOURCE_IMMEDIATE), MakeEnumAccessor(&ConsensusResourceModel::m_mode),
                              MakeEnumChecker(RESOURCE_IMMEDIATE, "Immediate", RESOURCE_QUEUED, "Queued"))
                .AddAttribute("CpuCores", "Number of independent CPU service channels.", UintegerValue(1),
                              MakeUintegerAccessor(&ConsensusResourceModel::m_cpuCores), MakeUintegerChecker<uint32_t>(1))
                .AddAttribute("StorageChannels", "Number of independent storage service channels.", UintegerValue(1),
                              MakeUintegerAccessor(&ConsensusResourceModel::m_storageChannels), MakeUintegerChecker<uint32_t>(1));
        return tid;
    }

    ConsensusResourceModel::ConsensusResourceModel() : m_mode(RESOURCE_IMMEDIATE), m_cpuCores(1), m_storageChannels(1) {}

    void ConsensusResourceModel::SubmitCpuTask(Time serviceTime, Callback<void> completed) {
        EnsureCapacity();
        SubmitTask(serviceTime, completed, m_cpuAvailableTimes);
    }

    void ConsensusResourceModel::SubmitStorageTask(Time serviceTime, Callback<void> completed) {
        EnsureCapacity();
        SubmitTask(serviceTime, completed, m_storageAvailableTimes);
    }

    void ConsensusResourceModel::Reset() {
        m_cpuAvailableTimes.clear();
        m_storageAvailableTimes.clear();
    }

    void ConsensusResourceModel::EnsureCapacity() {
        if (m_cpuAvailableTimes.size() != m_cpuCores) {
            m_cpuAvailableTimes.assign(m_cpuCores, Simulator::Now());
        }
        if (m_storageAvailableTimes.size() != m_storageChannels) {
            m_storageAvailableTimes.assign(m_storageChannels, Simulator::Now());
        }
    }

    void ConsensusResourceModel::SubmitTask(Time serviceTime, Callback<void> completed, std::vector<Time>& availableTimes) {
        if (completed.IsNull()) {
            return;
        }
        if (m_mode == RESOURCE_IMMEDIATE) {
            Simulator::ScheduleNow(completed);
            return;
        }
        auto channel = std::min_element(availableTimes.begin(), availableTimes.end());
        Time start = std::max(Simulator::Now(), *channel);
        Time finish = start + serviceTime;
        *channel = finish;
        Simulator::Schedule(finish - Simulator::Now(), completed);
    }

    TypeId ConsensusEngine::GetTypeId() {
        static TypeId tid = TypeId("ns3::ConsensusEngine")
                                .SetParent<Object>()
                                .SetGroupName("Consensus")
                                .AddTraceSource("ProtocolPhase", "A protocol-independent engine phase transition.",
                                                MakeTraceSourceAccessor(&ConsensusEngine::m_phaseTrace),
                                                "ns3::ConsensusEngine::ProtocolPhaseTracedCallback")
                                .AddTraceSource("LeaderChanged", "The engine enters an epoch with a new leader.",
                                                MakeTraceSourceAccessor(&ConsensusEngine::m_leaderTrace),
                                                "ns3::ConsensusEngine::LeaderChangedTracedCallback");
        return tid;
    }

    ConsensusEngine::ConsensusEngine() : m_replicaId(0) {}

    void ConsensusEngine::Configure(uint32_t replicaId, const std::vector<uint32_t>& replicaIds) {
        m_replicaId = replicaId;
        m_replicaIds = replicaIds;
    }

    void ConsensusEngine::SetSendCallback(SendCallback callback) {
        m_sendCallback = callback;
    }

    void ConsensusEngine::SetDecisionCallback(DecisionCallback callback) {
        m_decisionCallback = callback;
    }

    void ConsensusEngine::SetResourceModel(Ptr<ConsensusResourceModel> resourceModel) {
        m_resourceModel = resourceModel;
    }

    uint32_t ConsensusEngine::GetReplicaId() const {
        return m_replicaId;
    }

    const std::vector<uint32_t>& ConsensusEngine::GetReplicaIds() const {
        return m_replicaIds;
    }

    uint32_t ConsensusEngine::GetLeaderId() const {
        return m_replicaIds.empty() ? 0 : m_replicaIds.front();
    }

    void ConsensusEngine::Send(uint32_t peerId, const ConsensusMessage& message) const {
        if (!m_sendCallback.IsNull()) {
            m_sendCallback(peerId, message);
        }
    }

    void ConsensusEngine::Decide(const ConsensusDecision& decision) const {
        if (!m_decisionCallback.IsNull()) {
            m_decisionCallback(decision);
        }
    }

    void ConsensusEngine::ReportPhase(ConsensusProtocol protocol, uint64_t instanceId, uint16_t phase, Time elapsed) const {
        m_phaseTrace(m_replicaId, static_cast<uint8_t>(protocol), instanceId, phase, elapsed);
    }

    void ConsensusEngine::ReportLeader(uint64_t epoch, uint32_t leaderId) const {
        m_leaderTrace(m_replicaId, epoch, leaderId);
    }

} // namespace ns3
