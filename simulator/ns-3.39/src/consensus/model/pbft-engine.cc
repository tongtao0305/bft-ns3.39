#include "pbft-engine.h"

#include "ns3/abort.h"
#include "ns3/log.h"
#include "ns3/simulator.h"
#include "ns3/uinteger.h"

#include <algorithm>

namespace ns3 {

    NS_LOG_COMPONENT_DEFINE("PbftEngine");
    NS_OBJECT_ENSURE_REGISTERED(PbftEngine);

    TypeId PbftEngine::GetTypeId() {
        static TypeId tid =
            TypeId("ns3::PbftEngine")
                .SetParent<ConsensusEngine>()
                .SetGroupName("Consensus")
                .AddConstructor<PbftEngine>()
                .AddAttribute("PrimaryId", "Replica identifier of the fixed primary.", UintegerValue(0),
                              MakeUintegerAccessor(&PbftEngine::m_primaryId), MakeUintegerChecker<uint32_t>())
                .AddAttribute("FaultTolerance", "Maximum number of Byzantine replicas.", UintegerValue(1),
                              MakeUintegerAccessor(&PbftEngine::m_faultTolerance), MakeUintegerChecker<uint32_t>())
                .AddAttribute("ControlPayloadBytes", "Simulated payload bytes in PREPARE and COMMIT messages.", UintegerValue(64),
                              MakeUintegerAccessor(&PbftEngine::m_controlPayloadBytes), MakeUintegerChecker<uint32_t>())
                .AddAttribute("MessageProcessingTime", "CPU service time before processing a replica message.", TimeValue(MicroSeconds(5)),
                              MakeTimeAccessor(&PbftEngine::m_messageProcessingTime), MakeTimeChecker())
                .AddAttribute("PrePrepareProcessingTime", "CPU service time for PRE-PREPARE messages.", TimeValue(MicroSeconds(8)),
                              MakeTimeAccessor(&PbftEngine::m_prePrepareProcessingTime), MakeTimeChecker())
                .AddAttribute("PrepareProcessingTime", "CPU service time for PREPARE messages.", TimeValue(MicroSeconds(6)),
                              MakeTimeAccessor(&PbftEngine::m_prepareProcessingTime), MakeTimeChecker())
                .AddAttribute("CommitProcessingTime", "CPU service time for COMMIT messages.", TimeValue(MicroSeconds(6)),
                              MakeTimeAccessor(&PbftEngine::m_commitProcessingTime), MakeTimeChecker())
                .AddAttribute("ViewChangeProcessingTime", "CPU service time for VIEW_CHANGE and NEW_VIEW messages.", TimeValue(MicroSeconds(10)),
                              MakeTimeAccessor(&PbftEngine::m_viewChangeProcessingTime), MakeTimeChecker())
                .AddAttribute("RetransmissionTimeout", "Time between retransmissions of the current PBFT phase.",
                              TimeValue(MilliSeconds(2)), MakeTimeAccessor(&PbftEngine::m_retransmissionTimeout), MakeTimeChecker())
                .AddAttribute("InstanceTimeout", "Time before an unresolved instance initiates view change.", TimeValue(MilliSeconds(20)),
                              MakeTimeAccessor(&PbftEngine::m_instanceTimeout), MakeTimeChecker())
                .AddAttribute("MaxRetransmissions", "Phase retransmissions before initiating view change.", UintegerValue(3),
                              MakeUintegerAccessor(&PbftEngine::m_maxRetransmissions), MakeUintegerChecker<uint32_t>())
                .AddTraceSource("ViewChanged", "The replica enters a new PBFT view.",
                                MakeTraceSourceAccessor(&PbftEngine::m_viewChangedTrace), "ns3::PbftEngine::ViewChangedTracedCallback")
                .AddTraceSource("PhaseChanged", "A PBFT instance reaches proposed, prepared, or decided.",
                                MakeTraceSourceAccessor(&PbftEngine::m_phaseChangedTrace), "ns3::PbftEngine::PhaseChangedTracedCallback");
        return tid;
    }

    PbftEngine::PbftEngine()
        : m_primaryId(0), m_faultTolerance(1), m_controlPayloadBytes(64), m_messageProcessingTime(MicroSeconds(5)),
          m_prePrepareProcessingTime(MicroSeconds(8)), m_prepareProcessingTime(MicroSeconds(6)),
          m_commitProcessingTime(MicroSeconds(6)), m_viewChangeProcessingTime(MicroSeconds(10)),
          m_retransmissionTimeout(MilliSeconds(2)), m_instanceTimeout(MilliSeconds(20)), m_maxRetransmissions(3), m_view(0),
          m_nextSequence(1), m_nextMessageId(1), m_decidedCount(0), m_running(false) {}

    void PbftEngine::Configure(uint32_t replicaId, const std::vector<uint32_t>& replicaIds) {
        ConsensusEngine::Configure(replicaId, replicaIds);
    }

    void PbftEngine::Start() {
        NS_ABORT_MSG_IF(m_replicaIds.size() < 3 * m_faultTolerance + 1, "PBFT requires at least 3f+1 replicas");
        NS_ABORT_MSG_IF(!IsReplica(m_primaryId), "PBFT primary is not in the replica set");
        NS_ABORT_MSG_IF(!IsReplica(m_replicaId), "Local replica is not in the replica set");
        m_running = true;
        ReportLeader(m_view, GetPrimaryForView(m_view));
    }

    void PbftEngine::Stop() {
        m_running = false;
        for (auto& [key, instance] : m_instances) {
            (void)key;
            if (instance.timeoutEvent.IsRunning()) {
                Simulator::Cancel(instance.timeoutEvent);
            }
        }
        for (auto& [view, state] : m_viewChanges) {
            (void)view;
            if (state.timeoutEvent.IsRunning()) {
                Simulator::Cancel(state.timeoutEvent);
            }
        }
        for (auto& [request, event] : m_requestTimeouts) {
            (void)request;
            if (event.IsRunning()) {
                Simulator::Cancel(event);
            }
        }
    }

    void PbftEngine::Submit(const ClientRequest& request) {
        if (!m_running) {
            return;
        }

        auto requestKey = std::make_pair(request.clientId, request.requestId);
        m_pendingRequests[requestKey] = request;
        if (m_replicaId != GetPrimaryForView(m_view)) {
            EventId& timeout = m_requestTimeouts[requestKey];
            if (!timeout.IsRunning()) {
                timeout = Simulator::Schedule(m_instanceTimeout, &PbftEngine::StartViewChange, this, m_view + 1);
            }
            return;
        }
        ProposeRequest(request);
    }

    void PbftEngine::ProposeRequest(const ClientRequest& request) {
        auto proposalKey = std::make_tuple(m_view, request.clientId, request.requestId);
        if (!m_proposedRequests.insert(proposalKey).second) {
            return;
        }

        uint64_t sequence = m_nextSequence++;
        PbftInstance& instance = GetInstance(m_view, sequence);
        instance.view = m_view;
        instance.sequence = sequence;
        instance.digest = request.digest;
        instance.clientId = request.clientId;
        instance.requestId = request.requestId;
        instance.transactionCount = request.transactionCount;
        instance.prePrepareAccepted = true;
        instance.proposalTime = Simulator::Now();
        m_phaseChangedTrace(m_replicaId, instance.sequence, PBFT_PROPOSED, Seconds(0));
        ReportPhase(ConsensusProtocol::PBFT, instance.sequence, PBFT_PROPOSED, Seconds(0));

        ConsensusMessage message = MakeMessage(ConsensusMessageType::PRE_PREPARE, instance);
        message.payloadSize = request.payloadSize;
        Broadcast(message);
        ScheduleInstanceTimeout(instance);
    }

    void PbftEngine::Receive(const ConsensusMessage& message) {
        if (!m_running || message.protocol != ConsensusProtocol::PBFT || !IsReplica(message.senderId)) {
            return;
        }

        Callback<void> process = MakeCallback(&PbftEngine::ProcessMessage, this).Bind(message);
        if (m_resourceModel) {
            m_resourceModel->SubmitCpuTask(GetMessageProcessingTime(message), process);
        } else {
            Simulator::ScheduleNow(process);
        }
    }

    Time PbftEngine::GetMessageProcessingTime(const ConsensusMessage& message) const {
        switch (message.type) {
        case ConsensusMessageType::PRE_PREPARE: return m_prePrepareProcessingTime;
        case ConsensusMessageType::PREPARE: return m_prepareProcessingTime;
        case ConsensusMessageType::COMMIT: return m_commitProcessingTime;
        case ConsensusMessageType::VIEW_CHANGE:
        case ConsensusMessageType::NEW_VIEW: return m_viewChangeProcessingTime;
        default: return m_messageProcessingTime;
        }
    }

    uint64_t PbftEngine::GetDecidedCount() const {
        return m_decidedCount;
    }

    uint64_t PbftEngine::GetCurrentView() const {
        return m_view;
    }

    uint32_t PbftEngine::GetLeaderId() const {
        return GetPrimaryForView(m_view);
    }

    uint32_t PbftEngine::GetCurrentPrimary() const {
        return GetLeaderId();
    }

    const std::map<std::pair<uint64_t, uint64_t>, PbftInstance>& PbftEngine::GetInstances() const {
        return m_instances;
    }

    void PbftEngine::ProcessMessage(ConsensusMessage message) {
        if (!m_running) {
            return;
        }

        switch (message.type) {
            case ConsensusMessageType::VIEW_CHANGE:
                HandleViewChange(message);
                return;
            case ConsensusMessageType::NEW_VIEW:
                HandleNewView(message);
                return;
            default:
                break;
        }

        if (message.view != m_view || message.instanceId == 0) {
            return;
        }

        switch (message.type) {
            case ConsensusMessageType::PRE_PREPARE:
                HandlePrePrepare(message);
                break;
            case ConsensusMessageType::PREPARE:
                HandlePrepare(message);
                break;
            case ConsensusMessageType::COMMIT:
                HandleCommit(message);
                break;
            default:
                break;
        }
    }

    void PbftEngine::HandlePrePrepare(const ConsensusMessage& message) {
        if (message.senderId != GetPrimaryForView(message.view)) {
            return;
        }
        auto decided = m_decidedBySequence.find(message.instanceId);
        if (decided != m_decidedBySequence.end() && decided->second != message.digest) return;

        PbftInstance& instance = GetInstance(message.view, message.instanceId);
        if (instance.prePrepareAccepted) {
            return;
        }

        instance.view = message.view;
        instance.sequence = message.instanceId;
        instance.digest = message.digest;
        instance.clientId = message.clientId;
        instance.requestId = message.requestId;
        instance.transactionCount = message.transactionCount;
        instance.prePrepareAccepted = true;
        instance.proposalTime = Simulator::Now();
        m_phaseChangedTrace(m_replicaId, instance.sequence, PBFT_PROPOSED, Seconds(0));
        ReportPhase(ConsensusProtocol::PBFT, instance.sequence, PBFT_PROPOSED, Seconds(0));
        ScheduleInstanceTimeout(instance);

        if (m_replicaId != GetPrimaryForView(m_view)) {
            SendPrepare(instance);
        }

        std::vector<ConsensusMessage> pending = std::move(instance.pendingMessages);
        instance.pendingMessages.clear();
        for (const auto& cached : pending) {
            if (cached.type == ConsensusMessageType::PREPARE) {
                HandlePrepare(cached);
            } else if (cached.type == ConsensusMessageType::COMMIT) {
                HandleCommit(cached);
            }
        }
    }

    void PbftEngine::HandlePrepare(const ConsensusMessage& message) {
        PbftInstance& instance = GetInstance(message.view, message.instanceId);
        if (!instance.prePrepareAccepted) {
            instance.pendingMessages.push_back(message);
            return;
        }
        if (message.digest != instance.digest || message.senderId == GetPrimaryForView(message.view)) {
            return;
        }

        instance.prepareSenders.insert(message.senderId);
        TryPrepare(instance);
    }

    void PbftEngine::HandleCommit(const ConsensusMessage& message) {
        PbftInstance& instance = GetInstance(message.view, message.instanceId);
        if (!instance.prePrepareAccepted) {
            instance.pendingMessages.push_back(message);
            return;
        }
        if (message.digest != instance.digest) {
            return;
        }

        instance.commitSenders.insert(message.senderId);
        TryDecide(instance);
    }

    void PbftEngine::SendPrepare(PbftInstance& instance) {
        if (instance.prepareSent) {
            return;
        }

        instance.prepareSent = true;
        instance.prepareSenders.insert(m_replicaId);
        Broadcast(MakeMessage(ConsensusMessageType::PREPARE, instance));
        TryPrepare(instance);
    }

    void PbftEngine::TryPrepare(PbftInstance& instance) {
        if (instance.prepared || !instance.prePrepareAccepted || instance.prepareSenders.size() < 2 * m_faultTolerance) {
            return;
        }

        instance.prepared = true;
        instance.preparedTime = Simulator::Now();
        m_phaseChangedTrace(m_replicaId, instance.sequence, PBFT_PREPARED, instance.preparedTime - instance.proposalTime);
        ReportPhase(ConsensusProtocol::PBFT, instance.sequence, PBFT_PREPARED, instance.preparedTime - instance.proposalTime);
        SendCommit(instance);
    }

    void PbftEngine::SendCommit(PbftInstance& instance) {
        if (instance.commitSent) {
            return;
        }

        instance.commitSent = true;
        instance.commitSenders.insert(m_replicaId);
        Broadcast(MakeMessage(ConsensusMessageType::COMMIT, instance));
        TryDecide(instance);
    }

    void PbftEngine::TryDecide(PbftInstance& instance) {
        if (instance.decided || !instance.prepared || instance.commitSenders.size() < 2 * m_faultTolerance + 1) {
            return;
        }
        auto decided = m_decidedBySequence.find(instance.sequence);
        if (decided != m_decidedBySequence.end()) {
            if (decided->second != instance.digest) StartViewChange(m_view + 1);
            return;
        }
        m_decidedBySequence[instance.sequence] = instance.digest;

        instance.decided = true;
        if (instance.timeoutEvent.IsRunning()) {
            Simulator::Cancel(instance.timeoutEvent);
        }
        instance.decidedTime = Simulator::Now();
        m_phaseChangedTrace(m_replicaId, instance.sequence, PBFT_DECIDED, instance.decidedTime - instance.proposalTime);
        ReportPhase(ConsensusProtocol::PBFT, instance.sequence, PBFT_DECIDED, instance.decidedTime - instance.proposalTime);
        ++m_decidedCount;
        auto requestKey = std::make_pair(instance.clientId, instance.requestId);
        m_pendingRequests.erase(requestKey);
        auto timeout = m_requestTimeouts.find(requestKey);
        if (timeout != m_requestTimeouts.end() && timeout->second.IsRunning()) {
            Simulator::Cancel(timeout->second);
        }

        ConsensusDecision decision;
        decision.protocol = ConsensusProtocol::PBFT;
        decision.view = instance.view;
        decision.instanceId = instance.sequence;
        decision.digest = instance.digest;
        decision.clientId = instance.clientId;
        decision.requestId = instance.requestId;
        decision.transactionCount = instance.transactionCount;
        decision.decisionTime = instance.decidedTime;
        Decide(decision);
    }

    void PbftEngine::ScheduleInstanceTimeout(PbftInstance& instance) {
        if (instance.timeoutEvent.IsRunning()) {
            Simulator::Cancel(instance.timeoutEvent);
        }
        instance.timeoutEvent =
            Simulator::Schedule(m_retransmissionTimeout, &PbftEngine::HandleInstanceTimeout, this, instance.view, instance.sequence);
    }

    void PbftEngine::HandleInstanceTimeout(uint64_t view, uint64_t sequence) {
        if (!m_running || view != m_view) {
            return;
        }
        auto iterator = m_instances.find({view, sequence});
        if (iterator == m_instances.end() || iterator->second.decided) {
            return;
        }

        PbftInstance& instance = iterator->second;
        if (instance.retransmissions >= m_maxRetransmissions) {
            StartViewChange(view + 1);
            return;
        }
        ++instance.retransmissions;

        if (m_replicaId == GetPrimaryForView(view) && instance.prePrepareAccepted && !instance.prepared) {
            ConsensusMessage message = MakeMessage(ConsensusMessageType::PRE_PREPARE, instance);
            Broadcast(message);
        } else if (instance.commitSent) {
            Broadcast(MakeMessage(ConsensusMessageType::COMMIT, instance));
        } else if (instance.prepareSent) {
            Broadcast(MakeMessage(ConsensusMessageType::PREPARE, instance));
        }
        ScheduleInstanceTimeout(instance);
    }

    void PbftEngine::RequestViewChange() {
        StartViewChange(m_view + 1);
    }

    void PbftEngine::Write32(std::vector<uint8_t>& b, uint32_t v) {
        for (int shift = 24; shift >= 0; shift -= 8) b.push_back(static_cast<uint8_t>(v >> shift));
    }
    void PbftEngine::Write64(std::vector<uint8_t>& b, uint64_t v) {
        for (int shift = 56; shift >= 0; shift -= 8) b.push_back(static_cast<uint8_t>(v >> shift));
    }
    bool PbftEngine::Read32(const std::vector<uint8_t>& b, size_t& pos, uint32_t& v) {
        if (b.size() - std::min(pos, b.size()) < 4) return false;
        v = 0;
        for (int i = 0; i < 4; ++i) v = (v << 8) | b[pos++];
        return true;
    }
    bool PbftEngine::Read64(const std::vector<uint8_t>& b, size_t& pos, uint64_t& v) {
        if (b.size() - std::min(pos, b.size()) < 8) return false;
        v = 0;
        for (int i = 0; i < 8; ++i) v = (v << 8) | b[pos++];
        return true;
    }
    void PbftEngine::EncodeRecord(std::vector<uint8_t>& b, const PbftViewChangeState::PreparedRecord& r) {
        Write64(b, r.view); Write64(b, r.sequence); Write64(b, r.digest);
        Write32(b, r.clientId); Write64(b, r.requestId); Write32(b, r.transactionCount);
        Write32(b, r.prepareSenders.size());
        for (uint32_t sender : r.prepareSenders) Write32(b, sender);
    }
    bool PbftEngine::DecodeRecord(const std::vector<uint8_t>& b, size_t& pos, PbftViewChangeState::PreparedRecord& r) {
        uint32_t count;
        if (!Read64(b, pos, r.view) || !Read64(b, pos, r.sequence) || !Read64(b, pos, r.digest) ||
            !Read32(b, pos, r.clientId) || !Read64(b, pos, r.requestId) ||
            !Read32(b, pos, r.transactionCount) || !Read32(b, pos, count) || count > 100000) return false;
        for (uint32_t i = 0; i < count; ++i) {
            uint32_t sender;
            if (!Read32(b, pos, sender) || !r.prepareSenders.insert(sender).second) return false;
        }
        return true;
    }
    void PbftEngine::EncodeReport(std::vector<uint8_t>& b,
                                  const std::vector<PbftViewChangeState::PreparedRecord>& report) {
        Write32(b, report.size());
        for (const auto& record : report) EncodeRecord(b, record);
    }
    bool PbftEngine::DecodeReport(const std::vector<uint8_t>& b, size_t& pos,
                                  std::vector<PbftViewChangeState::PreparedRecord>& report) {
        uint32_t count;
        if (!Read32(b, pos, count) || count > 100000) return false;
        for (uint32_t i = 0; i < count; ++i) {
            PbftViewChangeState::PreparedRecord record;
            if (!DecodeRecord(b, pos, record)) return false;
            report.push_back(record);
        }
        return true;
    }
    std::vector<PbftViewChangeState::PreparedRecord> PbftEngine::LocalPreparedHistory(uint64_t target) const {
        std::map<uint64_t, PbftViewChangeState::PreparedRecord> bySequence;
        for (const auto& pair : m_instances) {
            const PbftInstance& instance = pair.second;
            if ((!instance.prepared && !instance.decided) || instance.view >= target) continue;
            auto found = bySequence.find(instance.sequence);
            if (found != bySequence.end() && found->second.view >= instance.view) continue;
            PbftViewChangeState::PreparedRecord record;
            record.view = instance.view;
            record.sequence = instance.sequence;
            record.digest = instance.digest;
            record.clientId = instance.clientId;
            record.requestId = instance.requestId;
            record.transactionCount = instance.transactionCount;
            record.prepareSenders = instance.prepareSenders;
            bySequence[record.sequence] = record;
        }
        std::vector<PbftViewChangeState::PreparedRecord> result;
        for (const auto& pair : bySequence) result.push_back(pair.second);
        return result;
    }
    bool PbftEngine::ValidatePreparedReport(const std::vector<PbftViewChangeState::PreparedRecord>& report,
                                             uint64_t target) const {
        std::set<uint64_t> sequences;
        for (const auto& record : report) {
            if (record.view >= target || record.sequence == 0 || record.digest == 0 ||
                record.transactionCount == 0 || record.prepareSenders.size() < 2 * m_faultTolerance ||
                !sequences.insert(record.sequence).second) return false;
            for (uint32_t sender : record.prepareSenders) if (!IsReplica(sender)) return false;
        }
        return true;
    }
    bool PbftEngine::SelectPreparedHistory(
        const std::map<uint32_t, std::vector<PbftViewChangeState::PreparedRecord>>& reports,
        std::map<uint64_t, PbftViewChangeState::PreparedRecord>& selected) const {
        for (const auto& report : reports) {
            for (const auto& record : report.second) {
                auto found = selected.find(record.sequence);
                if (found == selected.end() || record.view > found->second.view) selected[record.sequence] = record;
                else if (record.view == found->second.view &&
                    (record.digest != found->second.digest || record.clientId != found->second.clientId ||
                     record.requestId != found->second.requestId)) return false;
            }
        }
        return true;
    }

    void PbftEngine::StartViewChange(uint64_t newView) {
        if (!m_running || newView <= m_view) {
            return;
        }

        PbftViewChangeState& state = m_viewChanges[newView];
        if (!state.localVoteSent) {
            state.localVoteSent = true;
            state.senders.insert(m_replicaId);
            state.reports[m_replicaId] = LocalPreparedHistory(newView);
            ConsensusMessage message;
            message.protocol = ConsensusProtocol::PBFT;
            message.type = ConsensusMessageType::VIEW_CHANGE;
            message.view = newView;
            EncodeReport(message.payloadBytes, state.reports[m_replicaId]);
            Broadcast(message);
        }

        if (state.timeoutEvent.IsRunning()) {
            Simulator::Cancel(state.timeoutEvent);
        }
        state.timeoutEvent = Simulator::Schedule(m_instanceTimeout, &PbftEngine::HandleViewChangeTimeout, this, newView);
        TryEnterView(newView);
    }

    void PbftEngine::HandleViewChange(const ConsensusMessage& message) {
        if (message.view <= m_view || !IsReplica(message.senderId)) {
            return;
        }
        size_t offset = 0;
        std::vector<PbftViewChangeState::PreparedRecord> report;
        if (!DecodeReport(message.payloadBytes, offset, report) || offset != message.payloadBytes.size() ||
            !ValidatePreparedReport(report, message.view)) return;
        PbftViewChangeState& state = m_viewChanges[message.view];
        state.senders.insert(message.senderId);
        state.reports.emplace(message.senderId, report);
        if (!state.localVoteSent) {
            StartViewChange(message.view);
        }

        if (m_replicaId == GetPrimaryForView(message.view) && state.senders.size() >= 2 * m_faultTolerance + 1 && !state.newViewSent) {
            if (!SelectPreparedHistory(state.reports, state.preparedHistory)) return;
            state.newViewSent = true;
            ConsensusMessage newView;
            newView.protocol = ConsensusProtocol::PBFT;
            newView.type = ConsensusMessageType::NEW_VIEW;
            newView.view = message.view;
            Write32(newView.payloadBytes, state.reports.size());
            for (const auto& pair : state.reports) {
                Write32(newView.payloadBytes, pair.first);
                EncodeReport(newView.payloadBytes, pair.second);
            }
            Write32(newView.payloadBytes, state.preparedHistory.size());
            for (const auto& pair : state.preparedHistory) EncodeRecord(newView.payloadBytes, pair.second);
            Broadcast(newView);
            EnterView(message.view);
            return;
        }
        TryEnterView(message.view);
    }

    void PbftEngine::HandleNewView(const ConsensusMessage& message) {
        if (message.view <= m_view || message.senderId != GetPrimaryForView(message.view)) {
            return;
        }
        size_t offset = 0;
        uint32_t count;
        if (!Read32(message.payloadBytes, offset, count) || count < 2 * m_faultTolerance + 1 ||
            count > m_replicaIds.size()) return;
        std::map<uint32_t, std::vector<PbftViewChangeState::PreparedRecord>> reports;
        for (uint32_t i = 0; i < count; ++i) {
            uint32_t sender;
            std::vector<PbftViewChangeState::PreparedRecord> report;
            if (!Read32(message.payloadBytes, offset, sender) || !IsReplica(sender) || reports.count(sender) ||
                !DecodeReport(message.payloadBytes, offset, report) ||
                !ValidatePreparedReport(report, message.view)) return;
            reports.emplace(sender, report);
        }
        if (!Read32(message.payloadBytes, offset, count) || count > 100000) return;
        std::map<uint64_t, PbftViewChangeState::PreparedRecord> selected;
        for (uint32_t i = 0; i < count; ++i) {
            PbftViewChangeState::PreparedRecord record;
            if (!DecodeRecord(message.payloadBytes, offset, record) || selected.count(record.sequence)) return;
            selected.emplace(record.sequence, record);
        }
        if (offset != message.payloadBytes.size()) return;
        std::map<uint64_t, PbftViewChangeState::PreparedRecord> expected;
        if (!SelectPreparedHistory(reports, expected) || expected.size() != selected.size()) return;
        for (const auto& pair : expected) {
            auto found = selected.find(pair.first);
            if (found == selected.end() || found->second.view != pair.second.view ||
                found->second.digest != pair.second.digest || found->second.clientId != pair.second.clientId ||
                found->second.requestId != pair.second.requestId) return;
        }
        PbftViewChangeState& state = m_viewChanges[message.view];
        state.reports = reports;
        state.preparedHistory = selected;
        for (const auto& pair : reports) state.senders.insert(pair.first);
        m_pendingNewViews[message.view] = message;
        TryEnterView(message.view);
    }

    void PbftEngine::HandleViewChangeTimeout(uint64_t view) {
        if (!m_running || m_view >= view) {
            return;
        }
        PbftViewChangeState& state = m_viewChanges[view];
        if (state.retransmissions++ >= m_maxRetransmissions) {
            StartViewChange(view + 1);
            return;
        }
        state.localVoteSent = false;
        StartViewChange(view);
    }

    void PbftEngine::TryEnterView(uint64_t view) {
        auto state = m_viewChanges.find(view);
        if (state == m_viewChanges.end() || state->second.senders.size() < 2 * m_faultTolerance + 1) {
            return;
        }
        if (m_replicaId == GetPrimaryForView(view) || m_pendingNewViews.count(view) != 0) {
            EnterView(view);
        }
    }

    void PbftEngine::EnterView(uint64_t view) {
        if (view <= m_view) {
            return;
        }
        m_view = view;
        PbftViewChangeState& state = m_viewChanges[view];
        if (state.timeoutEvent.IsRunning()) {
            Simulator::Cancel(state.timeoutEvent);
        }
        m_viewChangedTrace(m_replicaId, view, GetPrimaryForView(view));
        ReportLeader(view, GetPrimaryForView(view));
        m_nextSequence = m_decidedBySequence.empty() ? 1 : m_decidedBySequence.rbegin()->first + 1;
        for (const auto& pair : state.preparedHistory) {
            const auto& record = pair.second;
            m_nextSequence = std::max(m_nextSequence, record.sequence + 1);
            if (m_decidedBySequence.count(record.sequence) != 0) continue;
            PbftInstance& instance = GetInstance(view, record.sequence);
            instance.view = view;
            instance.sequence = record.sequence;
            instance.digest = record.digest;
            instance.clientId = record.clientId;
            instance.requestId = record.requestId;
            instance.transactionCount = record.transactionCount;
            instance.prePrepareAccepted = true;
            instance.proposalTime = Simulator::Now();
            m_pendingRequests.erase({record.clientId, record.requestId});
            ScheduleInstanceTimeout(instance);
            if (m_replicaId == GetPrimaryForView(view))
                Broadcast(MakeMessage(ConsensusMessageType::PRE_PREPARE, instance));
            else SendPrepare(instance);
        }
        if (m_replicaId == GetPrimaryForView(view)) {
            ProposePendingRequests();
        }
    }

    void PbftEngine::ProposePendingRequests() {
        std::vector<ClientRequest> requests;
        for (const auto& [key, request] : m_pendingRequests) {
            (void)key;
            requests.push_back(request);
        }
        for (const auto& request : requests) {
            ProposeRequest(request);
        }
    }

    void PbftEngine::Broadcast(ConsensusMessage message) {
        message.senderId = m_replicaId;
        message.messageId = m_nextMessageId++;
        for (uint32_t peerId : m_replicaIds) {
            if (peerId == m_replicaId) {
                continue;
            }
            message.receiverId = peerId;
            Send(peerId, message);
        }
    }

    PbftInstance& PbftEngine::GetInstance(uint64_t view, uint64_t sequence) {
        InstanceKey key{view, sequence};
        auto [iterator, inserted] = m_instances.emplace(key, PbftInstance{});
        if (inserted) {
            iterator->second.view = view;
            iterator->second.sequence = sequence;
        }
        return iterator->second;
    }

    ConsensusMessage PbftEngine::MakeMessage(ConsensusMessageType type, const PbftInstance& instance) {
        ConsensusMessage message;
        message.protocol = ConsensusProtocol::PBFT;
        message.type = type;
        message.senderId = m_replicaId;
        message.view = instance.view;
        message.instanceId = instance.sequence;
        message.payloadSize = m_controlPayloadBytes;
        message.digest = instance.digest;
        message.clientId = instance.clientId;
        message.requestId = instance.requestId;
        message.transactionCount = instance.transactionCount;
        return message;
    }

    bool PbftEngine::IsReplica(uint32_t replicaId) const {
        return std::find(m_replicaIds.begin(), m_replicaIds.end(), replicaId) != m_replicaIds.end();
    }

    uint32_t PbftEngine::GetPrimaryForView(uint64_t view) const {
        auto initial = std::find(m_replicaIds.begin(), m_replicaIds.end(), m_primaryId);
        if (initial == m_replicaIds.end() || m_replicaIds.empty()) {
            return m_primaryId;
        }
        uint64_t index = std::distance(m_replicaIds.begin(), initial);
        return m_replicaIds[(index + view) % m_replicaIds.size()];
    }

} // namespace ns3
