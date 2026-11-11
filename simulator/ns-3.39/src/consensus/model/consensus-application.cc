#include "consensus-application.h"
#include "bidl-application.h"
#include "zyzzyva-auth.h"
#include "ezbft-engine.h"
#include "zyzzyva-engine.h"

#include "ns3/abort.h"
#include "ns3/boolean.h"
#include "ns3/log.h"
#include "ns3/pointer.h"
#include "ns3/simulator.h"
#include "ns3/uinteger.h"

#include <algorithm>

namespace ns3 {

    namespace {
        void WriteEz64(std::vector<uint8_t>& bytes, uint64_t value) {
            for (int shift = 56; shift >= 0; shift -= 8) bytes.push_back(static_cast<uint8_t>(value >> shift));
        }
        void WriteEz32(std::vector<uint8_t>& bytes, uint32_t value) {
            for (int shift = 24; shift >= 0; shift -= 8) bytes.push_back(static_cast<uint8_t>(value >> shift));
        }
        bool ReadEz64(const std::vector<uint8_t>& bytes, size_t& offset, uint64_t& value) {
            if (offset > bytes.size() || bytes.size() - offset < 8) return false;
            value = 0;
            for (int i = 0; i < 8; ++i) value = (value << 8) | bytes[offset++];
            return true;
        }
        bool ReadEz32(const std::vector<uint8_t>& bytes, size_t& offset, uint32_t& value) {
            if (offset > bytes.size() || bytes.size() - offset < 4) return false;
            value = 0;
            for (int i = 0; i < 4; ++i) value = (value << 8) | bytes[offset++];
            return true;
        }
        std::vector<uint8_t> PackSignedMessage(const ConsensusMessage& message) {
            ConsensusMessage copy = message;
            if (copy.authentication.empty() || copy.payloadBytes.size() < copy.authentication.size() ||
                !std::equal(copy.authentication.begin(), copy.authentication.end(),
                            copy.payloadBytes.end() - copy.authentication.size()))
                copy.payloadBytes.insert(copy.payloadBytes.end(), copy.authentication.begin(), copy.authentication.end());
            Ptr<Packet> packet = copy.ToPacket();
            std::vector<uint8_t> bytes(packet->GetSize());
            packet->CopyData(bytes.data(), bytes.size());
            return bytes;
        }
        bool UnpackSignedMessage(const std::vector<uint8_t>& bytes, ConsensusMessage& message) {
            if (bytes.empty() || bytes.size() > 1024 * 1024) return false;
            Ptr<Packet> packet = Create<Packet>(bytes.data(), bytes.size());
            return ConsensusMessage::FromPacket(packet, message) && ZyzzyvaAuth::Verify(message);
        }
        void WriteEzTuple(std::vector<uint8_t>& bytes, uint64_t command,
                          const std::vector<uint64_t>& dependencies, uint64_t sequence) {
            WriteEz64(bytes, command);
            WriteEz32(bytes, static_cast<uint32_t>(dependencies.size()));
            for (uint64_t dependency : dependencies) WriteEz64(bytes, dependency);
            WriteEz64(bytes, sequence);
        }
        bool ReadEzTuple(const std::vector<uint8_t>& bytes, uint64_t& command,
                         std::vector<uint64_t>& dependencies, uint64_t& sequence) {
            size_t offset = 0;
            uint32_t count = 0;
            if (!ReadEz64(bytes, offset, command) || !ReadEz32(bytes, offset, count) || count > 64 ||
                bytes.size() - offset < uint64_t(count) * 8 + 8) return false;
            dependencies.clear();
            for (uint32_t i = 0; i < count; ++i) {
                uint64_t dependency = 0;
                if (!ReadEz64(bytes, offset, dependency) || dependency == 0 ||
                    (!dependencies.empty() && dependencies.back() >= dependency)) return false;
                dependencies.push_back(dependency);
            }
            return ReadEz64(bytes, offset, sequence) && offset == bytes.size() && sequence > 0;
        }
        uint64_t HashEzTuple(uint64_t command, const std::vector<uint64_t>& dependencies, uint64_t sequence) {
            uint64_t h = command ^ 0x9e3779b97f4a7c15ULL;
            for (uint64_t dependency : dependencies)
                h ^= dependency + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
            h ^= sequence * 0xd6e8feb86659fd93ULL;
            return h ? h : 1;
        }
    }

    NS_LOG_COMPONENT_DEFINE("ConsensusApplication");
    NS_OBJECT_ENSURE_REGISTERED(ConsensusApplication);
    NS_OBJECT_ENSURE_REGISTERED(ConsensusClientApplication);

    TypeId ConsensusApplication::GetTypeId() {
        static TypeId tid =
            TypeId("ns3::ConsensusApplication")
                .SetParent<Application>()
                .SetGroupName("Consensus")
                .AddConstructor<ConsensusApplication>()
                .AddAttribute("VerificationTime", "Simulated Zyzzyva message verification CPU service.", TimeValue(MicroSeconds(1)),
                              MakeTimeAccessor(&ConsensusApplication::m_verificationTime), MakeTimeChecker())
                .AddAttribute("ReplicaId", "Logical identifier of this consensus replica.", UintegerValue(0),
                              MakeUintegerAccessor(&ConsensusApplication::m_replicaId), MakeUintegerChecker<uint32_t>())
                .AddAttribute("Engine", "Consensus protocol engine.", PointerValue(), MakePointerAccessor(&ConsensusApplication::m_engine),
                              MakePointerChecker<ConsensusEngine>())
                .AddAttribute("Transport", "Complete-message network transport.", PointerValue(),
                              MakePointerAccessor(&ConsensusApplication::m_transport), MakePointerChecker<ConsensusTransport>())
                .AddAttribute("ResourceModel", "CPU and storage resource model.", PointerValue(),
                              MakePointerAccessor(&ConsensusApplication::m_resourceModel), MakePointerChecker<ConsensusResourceModel>())
                .AddTraceSource("MessageEvent", "Complete message identity for offline timing.",
                                MakeTraceSourceAccessor(&ConsensusApplication::m_messageEventTrace),
                                "ns3::ConsensusApplication::MessageEventCallback")
                .AddTraceSource("MessageTx", "A consensus message is passed to the transport.",
                                MakeTraceSourceAccessor(&ConsensusApplication::m_messageTxTrace),
                                "ns3::ConsensusApplication::MessageTracedCallback")
                .AddTraceSource("MessageRx", "A complete consensus message is received.",
                                MakeTraceSourceAccessor(&ConsensusApplication::m_messageRxTrace),
                                "ns3::ConsensusApplication::MessageTracedCallback")
                .AddTraceSource("CandidateEvent", "Ordering candidate sent or received with its binding fields.",
                                MakeTraceSourceAccessor(&ConsensusApplication::m_candidateTrace),
                                "ns3::ConsensusApplication::CandidateTracedCallback")
                .AddTraceSource("Decision", "The local engine decides a consensus instance.",
                                MakeTraceSourceAccessor(&ConsensusApplication::m_decisionTrace),
                                "ns3::ConsensusApplication::DecisionTracedCallback");
        return tid;
    }

    ConsensusApplication::ConsensusApplication() : m_replicaId(0), m_nextMessageId(1), m_decisionCount(0), m_running(false) {}

    void ConsensusApplication::SetEngine(Ptr<ConsensusEngine> engine) {
        m_engine = engine;
    }

    void ConsensusApplication::SetTransport(Ptr<ConsensusTransport> transport) {
        m_transport = transport;
    }

    void ConsensusApplication::SetResourceModel(Ptr<ConsensusResourceModel> resourceModel) {
        m_resourceModel = resourceModel;
    }

    void ConsensusApplication::SetReplicaId(uint32_t replicaId) {
        m_replicaId = replicaId;
    }

    void ConsensusApplication::SetReplicaIds(const std::vector<uint32_t>& replicaIds) {
        m_replicaIds = replicaIds;
    }

    uint32_t ConsensusApplication::GetReplicaId() const {
        return m_replicaId;
    }

    uint64_t ConsensusApplication::GetDecisionCount() const {
        return m_decisionCount;
    }

    Ptr<ConsensusEngine> ConsensusApplication::GetEngine() const {
        return m_engine;
    }

    void ConsensusApplication::DoDispose() {
        m_engine = nullptr;
        m_transport = nullptr;
        m_resourceModel = nullptr;
        Application::DoDispose();
    }

    void ConsensusApplication::StartApplication() {
        NS_ABORT_MSG_IF(!m_engine, "ConsensusApplication requires an Engine");
        NS_ABORT_MSG_IF(!m_transport, "ConsensusApplication requires a Transport");
        NS_ABORT_MSG_IF(m_replicaIds.empty(), "ConsensusApplication requires a replica set");

        if (!m_resourceModel) {
            m_resourceModel = CreateObject<ConsensusResourceModel>();
        }

        m_engine->Configure(m_replicaId, m_replicaIds);
        m_engine->SetResourceModel(m_resourceModel);
        m_engine->SetSendCallback(MakeCallback(&ConsensusApplication::OnEngineSend, this));
        m_engine->SetDecisionCallback(MakeCallback(&ConsensusApplication::OnEngineDecision, this));
        m_transport->SetReceiveCallback(MakeCallback(&ConsensusApplication::OnTransportReceive, this));

        m_running = true;
        m_transport->Start(GetNode());
        m_engine->Start();
    }

    void ConsensusApplication::StopApplication() {
        m_running = false;
        if (m_engine) {
            m_engine->Stop();
        }
        if (m_transport) {
            m_transport->Stop();
        }
    }

    // 处理网络收到的完整消息
    void ConsensusApplication::OnTransportReceive(uint32_t peerId, Ptr<Packet> packet) {
        ConsensusMessage message;
        if (!m_running || !ConsensusMessage::FromPacket(packet, message) || message.senderId != peerId ||
            message.receiverId != m_replicaId) {
            return;
        }
        if (DynamicCast<ZyzzyvaEngine>(m_engine) &&
            (message.protocol != ConsensusProtocol::ZYZZYVA || !ZyzzyvaAuth::Verify(message))) return;
        if (DynamicCast<EzBftEngine>(m_engine) &&
            (message.protocol != ConsensusProtocol::EZBFT || !ZyzzyvaAuth::Verify(message))) return;

        // 反序列化收到的数据包
        m_messageRxTrace(message.senderId, m_replicaId, static_cast<uint16_t>(message.type), message.instanceId, packet->GetSize());
        m_messageEventTrace(false, message);
        if (message.type == ConsensusMessageType::ZYZZYVA_ORDER_REQ || message.type == ConsensusMessageType::EZBFT_SPEC_ORDER)
            m_candidateTrace(message.senderId, m_replicaId, static_cast<uint16_t>(message.type), message.view,
                             message.instanceId, message.digest, message.clientId, message.requestId, 1, message.iorsMarked ? 1 : 0);

        if (DynamicCast<ZyzzyvaEngine>(m_engine)) {
            ++m_verificationPending;
            m_resourceModel->SubmitCpuTask(m_verificationTime,
                MakeCallback(&ConsensusApplication::CompleteVerifiedMessage, this).Bind(message));
            return;
        }
        CompleteVerifiedMessage(message);
    }

    void ConsensusApplication::CompleteVerifiedMessage(ConsensusMessage message) {
        if (DynamicCast<ZyzzyvaEngine>(m_engine) && m_verificationPending) --m_verificationPending;
        if (!m_running) return;
        // 客户端内容与认证信息在模拟验证结束后交给引擎。
        if (message.type == ConsensusMessageType::CLIENT_REQUEST) {
            if (DynamicCast<ZyzzyvaEngine>(m_engine)) {
                m_engine->Receive(message); // Preserve the portable client signature through forwarding.
                return;
            }
            ClientRequest request;
            request.clientId = message.clientId;
            request.requestId = message.requestId;
            request.payloadSize = message.payloadSize;
            request.transactionCount = message.transactionCount;
            request.digest = message.digest;
            m_engine->Submit(request);
            return;
        }

        // 其它情况直接转发给共识引擎
        m_engine->Receive(message);
    }

    // 发送消息
    void ConsensusApplication::OnEngineSend(uint32_t peerId, const ConsensusMessage& message) {
        SendMessage(peerId, message);
    }

    // 完成共识时触发
    void ConsensusApplication::OnEngineDecision(const ConsensusDecision& decision) {
        ++m_decisionCount;
        m_decisionTrace(m_replicaId, decision.instanceId, decision.digest, Simulator::Now());

        ConsensusMessage reply;
        reply.protocol = decision.protocol;
        reply.type = ConsensusMessageType::CLIENT_REPLY;
        reply.view = decision.view;
        reply.instanceId = decision.instanceId;
        reply.payloadSize = 0;
        reply.digest = decision.digest;
        reply.clientId = decision.clientId;
        reply.requestId = decision.requestId;
        reply.transactionCount = decision.transactionCount;
        SendMessage(decision.clientId, reply);
    }

    void ConsensusApplication::SendMessage(uint32_t peerId, ConsensusMessage message) {
        if (!m_running) {
            return;
        }

        message.senderId = m_replicaId;
        message.receiverId = peerId;
        if (message.messageId == 0) {
            message.messageId = m_nextMessageId++;
        }
        if ((message.protocol == ConsensusProtocol::ZYZZYVA || message.protocol == ConsensusProtocol::EZBFT) &&
            !ZyzzyvaAuth::Sign(message)) return;
        Ptr<Packet> packet = message.ToPacket();
        m_messageTxTrace(m_replicaId, peerId, static_cast<uint16_t>(message.type), message.instanceId, packet->GetSize());
        m_messageEventTrace(true, message);
        if (message.type == ConsensusMessageType::ZYZZYVA_ORDER_REQ || message.type == ConsensusMessageType::EZBFT_SPEC_ORDER)
            m_candidateTrace(m_replicaId, peerId, static_cast<uint16_t>(message.type), message.view,
                             message.instanceId, message.digest, message.clientId, message.requestId, 0, message.iorsMarked ? 1 : 0);
        m_transport->Send(peerId, packet, ConsensusTransportMetadata::FromMessage(message));
    }

    TypeId ConsensusClientApplication::GetTypeId() {
        static TypeId tid =
            TypeId("ns3::ConsensusClientApplication")
                .SetParent<Application>()
                .SetGroupName("Consensus")
                .AddConstructor<ConsensusClientApplication>()
                .AddTraceSource("RequestAborted", "A BIDL request terminates with an authenticated-node abort quorum.",
                                MakeTraceSourceAccessor(&ConsensusClientApplication::m_requestAbortedTrace),
                                "ns3::ConsensusClientApplication::RequestTracedCallback")
                .AddTraceSource("MessageEvent", "Complete message identity for offline timing.",
                                MakeTraceSourceAccessor(&ConsensusClientApplication::m_messageEventTrace),
                                "ns3::ConsensusClientApplication::MessageEventCallback")
                .AddTraceSource("MessageTx", "Client request or certificate submitted to the transport.",
                                MakeTraceSourceAccessor(&ConsensusClientApplication::m_messageTxTrace),
                                "ns3::ConsensusApplication::MessageTracedCallback")
                .AddTraceSource("MessageRx", "Authenticated response received by the client.",
                                MakeTraceSourceAccessor(&ConsensusClientApplication::m_messageRxTrace),
                                "ns3::ConsensusApplication::MessageTracedCallback")
                .AddAttribute("ClientId", "Logical identifier of this client.", UintegerValue(1000),
                              MakeUintegerAccessor(&ConsensusClientApplication::m_clientId), MakeUintegerChecker<uint32_t>())
                .AddAttribute("PrimaryId", "Logical identifier of the fixed primary.", UintegerValue(0),
                              MakeUintegerAccessor(&ConsensusClientApplication::m_primaryId), MakeUintegerChecker<uint32_t>())
                .AddAttribute("FaultTolerance", "Client completes after f+1 matching replies.", UintegerValue(1),
                              MakeUintegerAccessor(&ConsensusClientApplication::m_faultTolerance), MakeUintegerChecker<uint32_t>())
                .AddAttribute("ReplyQuorum", "Matching replies required for completion; zero uses f+1.", UintegerValue(0),
                              MakeUintegerAccessor(&ConsensusClientApplication::m_replyQuorum), MakeUintegerChecker<uint32_t>())
                .AddAttribute("ZyzzyvaClient", "Use Zyzzyva speculative and certificate completion rules.", BooleanValue(false),
                              MakeBooleanAccessor(&ConsensusClientApplication::m_zyzzyva), MakeBooleanChecker())
                .AddAttribute("RequirePrimaryProofs", "Require signed primary ORDER-REQ proofs in speculative responses.", BooleanValue(false),
                              MakeBooleanAccessor(&ConsensusClientApplication::m_requirePrimaryProofs), MakeBooleanChecker())
                .AddAttribute("BidlClient", "Bind BIDL client content using simulated authentication.", BooleanValue(false),
                              MakeBooleanAccessor(&ConsensusClientApplication::m_bidl), MakeBooleanChecker())
                .AddAttribute("EzBftClient", "Use EZBFT fast and slow client paths.", BooleanValue(false),
                              MakeBooleanAccessor(&ConsensusClientApplication::m_ezbft), MakeBooleanChecker())
                .AddAttribute("RequestSize", "Application payload bytes in each request.", UintegerValue(1024),
                              MakeUintegerAccessor(&ConsensusClientApplication::m_requestSize), MakeUintegerChecker<uint32_t>())
                .AddAttribute("TransactionsPerRequest", "Transactions represented by one request.", UintegerValue(1),
                              MakeUintegerAccessor(&ConsensusClientApplication::m_transactionsPerRequest), MakeUintegerChecker<uint32_t>(1))
                .AddAttribute("MaxRequests", "Number of requests generated by this client.", UintegerValue(1),
                              MakeUintegerAccessor(&ConsensusClientApplication::m_maxRequests), MakeUintegerChecker<uint32_t>(1))
                .AddAttribute("AutoGenerate", "Generate requests from Interval/MaxRequests; disable for trace replay.", BooleanValue(true),
                              MakeBooleanAccessor(&ConsensusClientApplication::m_autoGenerate), MakeBooleanChecker())
                .AddAttribute("MaxRetransmissions", "Maximum retransmissions before a request is abandoned.", UintegerValue(8),
                              MakeUintegerAccessor(&ConsensusClientApplication::m_maxRetransmissions), MakeUintegerChecker<uint32_t>())
                .AddAttribute("Interval", "Interval between generated requests.", TimeValue(MilliSeconds(10)),
                              MakeTimeAccessor(&ConsensusClientApplication::m_interval), MakeTimeChecker())
                .AddAttribute("InitialDelay", "Delay from application start to the first request.", TimeValue(MilliSeconds(10)),
                              MakeTimeAccessor(&ConsensusClientApplication::m_initialDelay), MakeTimeChecker())
                .AddAttribute("FastPathTimeout", "Wait for all speculative responses before the Zyzzyva slow path.",
                              TimeValue(MilliSeconds(1)), MakeTimeAccessor(&ConsensusClientApplication::m_fastPathTimeout), MakeTimeChecker())
                .AddAttribute("RequestTimeout", "Time without f+1 replies before retransmitting to another replica.",
                              TimeValue(MilliSeconds(50)), MakeTimeAccessor(&ConsensusClientApplication::m_requestTimeout),
                              MakeTimeChecker())
                .AddAttribute("Transport", "Complete-message network transport.", PointerValue(),
                              MakePointerAccessor(&ConsensusClientApplication::m_transport), MakePointerChecker<ConsensusTransport>())
                .AddTraceSource("RequestSubmitted", "A client request is submitted.",
                                MakeTraceSourceAccessor(&ConsensusClientApplication::m_requestSubmittedTrace),
                                "ns3::ConsensusClientApplication::RequestTracedCallback")
                .AddTraceSource("RequestCompleted", "A client request obtains the configured matching-reply quorum.",
                                MakeTraceSourceAccessor(&ConsensusClientApplication::m_requestCompletedTrace),
                                "ns3::ConsensusClientApplication::RequestTracedCallback")
                .AddTraceSource("RequestRetransmitted", "A client request is sent to another replica after timeout.",
                                MakeTraceSourceAccessor(&ConsensusClientApplication::m_requestRetransmittedTrace),
                                "ns3::ConsensusClientApplication::RetransmitTracedCallback")
                .AddTraceSource("RequestTimedOut", "A client request exhausts its retransmission budget.",
                                MakeTraceSourceAccessor(&ConsensusClientApplication::m_requestTimedOutTrace),
                                "ns3::ConsensusClientApplication::TimeoutTracedCallback");
        return tid;
    }

    ConsensusClientApplication::ConsensusClientApplication()
        : m_clientId(1000), m_primaryId(0), m_faultTolerance(1), m_replyQuorum(0), m_requestSize(1024), m_transactionsPerRequest(1),
          m_maxRequests(1), m_maxRetransmissions(8), m_autoGenerate(true), m_interval(MilliSeconds(10)), m_initialDelay(MilliSeconds(10)),
          m_requestTimeout(MilliSeconds(50)), m_nextRequestId(1), m_completedRequests(0), m_completedTransactions(0),
          m_totalLatency(Seconds(0)), m_firstSubmission(Seconds(0)), m_lastCompletion(Seconds(0)), m_hasSubmission(false),
          m_running(false) {}

    void ConsensusClientApplication::SetTransport(Ptr<ConsensusTransport> transport) {
        m_transport = transport;
    }

    void ConsensusClientApplication::SetReplicaIds(const std::vector<uint32_t>& replicaIds) {
        m_replicaIds = replicaIds;
    }

    uint64_t ConsensusClientApplication::SubmitRequest(uint32_t requestSize, uint32_t transactionCount) {
        if (!m_running || requestSize == 0 || transactionCount == 0) {
            return 0;
        }

        const uint64_t requestId = m_nextRequestId++;
        const uint64_t digest = (static_cast<uint64_t>(m_clientId) << 32) ^ (requestId * 0x9e3779b97f4a7c15ULL) ^ requestSize;
        ReplyState state;
        state.submitted = Simulator::Now();
        state.digest = digest;
        state.payloadSize = requestSize;
        state.transactionCount = transactionCount;
        m_requests.emplace(requestId, state);
        if (!m_hasSubmission) {
            m_firstSubmission = state.submitted;
            m_hasSubmission = true;
        }
        m_requestSubmittedTrace(m_clientId, requestId, state.submitted);
        SendRequest(requestId);
        return requestId;
    }

    uint64_t ConsensusClientApplication::GetCompletedRequests() const {
        return m_completedRequests;
    }

    uint64_t ConsensusClientApplication::GetCompletedTransactions() const {
        return m_completedTransactions;
    }

    uint8_t ConsensusClientApplication::GetRequestCompletionPath(uint64_t requestId) const {
        auto found = m_requests.find(requestId);
        return found == m_requests.end() ? 0 : found->second.completionPath;
    }

    uint32_t ConsensusClientApplication::GetRequestTransactionCount(uint64_t requestId) const {
        auto request = m_requests.find(requestId);
        return request == m_requests.end() ? 0 : request->second.transactionCount;
    }

    Time ConsensusClientApplication::GetAverageLatency() const {
        return m_completedRequests == 0 ? Seconds(0) : m_totalLatency / m_completedRequests;
    }

    double ConsensusClientApplication::GetRequestThroughput() const {
        Time duration = m_lastCompletion - m_firstSubmission;
        return !m_hasSubmission || duration.IsZero() ? 0.0 : static_cast<double>(m_completedRequests) / duration.GetSeconds();
    }

    double ConsensusClientApplication::GetTransactionThroughput() const {
        Time duration = m_lastCompletion - m_firstSubmission;
        return !m_hasSubmission || duration.IsZero() ? 0.0 : static_cast<double>(m_completedTransactions) / duration.GetSeconds();
    }

    void ConsensusClientApplication::DoDispose() {
        m_transport = nullptr;
        Application::DoDispose();
    }

    void ConsensusClientApplication::StartApplication() {
        NS_ABORT_MSG_IF(!m_transport, "ConsensusClientApplication requires a Transport");
        NS_ABORT_MSG_IF(m_replicaIds.empty(), "ConsensusClientApplication requires replica IDs");
        m_transport->SetReceiveCallback(MakeCallback(&ConsensusClientApplication::OnTransportReceive, this));
        m_running = true;
        m_transport->Start(GetNode());
        if (m_autoGenerate) {
            m_generateEvent = Simulator::Schedule(m_initialDelay, &ConsensusClientApplication::GenerateRequest, this);
        }
    }

    void ConsensusClientApplication::StopApplication() {
        m_running = false;
        if (m_generateEvent.IsRunning()) {
            Simulator::Cancel(m_generateEvent);
        }
        for (auto& [requestId, state] : m_requests) {
            (void)requestId;
            Simulator::Cancel(state.fastPathEvent);
            if (state.timeoutEvent.IsRunning()) {
                Simulator::Cancel(state.timeoutEvent);
            }
        }
        if (m_transport) {
            m_transport->Stop();
        }
    }

    void ConsensusClientApplication::GenerateRequest() {
        if (!m_running || m_nextRequestId > m_maxRequests) {
            return;
        }

        SubmitRequest(m_requestSize, m_transactionsPerRequest);

        if (m_nextRequestId <= m_maxRequests) {
            m_generateEvent = Simulator::Schedule(m_interval, &ConsensusClientApplication::GenerateRequest, this);
        }
    }

    void ConsensusClientApplication::SendRequest(uint64_t requestId) {
        auto iterator = m_requests.find(requestId);
        if (!m_running || iterator == m_requests.end() || iterator->second.completed) {
            return;
        }

        ReplyState& state = iterator->second;
        auto primary = std::find(m_replicaIds.begin(), m_replicaIds.end(), m_primaryId);
        uint32_t primaryIndex = primary == m_replicaIds.end() ? 0 : std::distance(m_replicaIds.begin(), primary);
        uint32_t targetIndex = (primaryIndex + state.attempts) % m_replicaIds.size();
        uint32_t targetId = m_replicaIds[targetIndex];
        if (m_bidl && primary == m_replicaIds.end()) {
            // The ingress gateway is not a consensus reply voter.
            targetId = state.attempts == 0 ? m_primaryId : m_replicaIds[(state.attempts - 1) % m_replicaIds.size()];
        }

        ConsensusMessage request;
        request.protocol = m_zyzzyva ? ConsensusProtocol::ZYZZYVA : (m_ezbft ? ConsensusProtocol::EZBFT : ConsensusProtocol::GENERIC);
        request.type = ConsensusMessageType::CLIENT_REQUEST;
        request.senderId = m_clientId;
        request.receiverId = targetId;
        request.messageId = requestId;
        request.payloadSize = state.payloadSize;
        request.digest = state.digest;
        request.clientId = m_clientId;
        request.requestId = requestId;
        request.transactionCount = state.transactionCount;
        if (m_bidl) {
            ClientRequest transaction{m_clientId, requestId, state.payloadSize, state.transactionCount, state.digest};
            BidlApplication::SignRequest(transaction);
            request.payloadBytes = BidlApplication::EncodeTransactions({{1, transaction, static_cast<uint64_t>(state.submitted.GetNanoSeconds())}});
        }
        if (m_zyzzyva) {
            ClientRequest transaction{m_clientId, requestId, state.payloadSize, state.transactionCount, state.digest};
            ZyzzyvaAuth::SignClientRequest(transaction);
            request.payloadBytes = ZyzzyvaAuth::EncodeClientRequest(transaction);
            if (!ZyzzyvaAuth::Sign(request)) return;
        } else if (m_ezbft) {
            request.payloadBytes.resize(state.payloadSize, 0);
            if (!ZyzzyvaAuth::Sign(request)) return;
        }

        if (state.attempts > 0) {
            m_requestRetransmittedTrace(m_clientId, requestId, targetId);
        }
        ++state.attempts;
        if (m_zyzzyva && state.attempts == 1)
            state.fastPathEvent = Simulator::Schedule(m_fastPathTimeout, &ConsensusClientApplication::HandleFastPathTimeout, this, requestId);
        SendClientMessage(targetId, request);
        state.timeoutEvent = Simulator::Schedule(m_requestTimeout, &ConsensusClientApplication::HandleRequestTimeout, this, requestId);
    }

    void ConsensusClientApplication::HandleRequestTimeout(uint64_t requestId) {
        auto iterator = m_requests.find(requestId);
        if (!m_running || iterator == m_requests.end() || iterator->second.completed) {
            return;
        }
        if (m_ezbft) {
            ReplyState& state = iterator->second;
            if (!state.ezSpeculativeReplies.empty()) {
                const auto& firstKey = state.ezSpeculativeReplies.begin()->first;
                const uint32_t owner = std::get<0>(firstKey);
                const uint64_t epoch = std::get<1>(firstKey);
                const uint64_t instance = std::get<2>(firstKey);
                std::map<uint32_t, std::pair<std::vector<uint8_t>, std::vector<uint8_t>>> votes;
                for (const auto& group : state.ezSpeculativeReplies) {
                    if (std::get<0>(group.first) != owner || std::get<1>(group.first) != epoch ||
                        std::get<2>(group.first) != instance) continue;
                    const auto tuple = state.ezTuplePayloads.find(group.first);
                    const auto signatures = state.ezReplySignatures.find(group.first);
                    if (tuple == state.ezTuplePayloads.end() || signatures == state.ezReplySignatures.end()) continue;
                    for (uint32_t voter : group.second) {
                        auto signature = signatures->second.find(voter);
                        if (signature != signatures->second.end()) votes.emplace(voter, std::make_pair(tuple->second, signature->second));
                    }
                }
                if (votes.size() >= 2 * m_faultTolerance + 1) {
                    uint64_t command = 0, sequence = 0;
                    std::set<uint64_t> dependencyUnion;
                    bool valid = true;
                    uint32_t included = 0;
                    for (const auto& vote : votes) {
                        if (included++ >= 2 * m_faultTolerance + 1) break;
                        uint64_t candidateCommand = 0, candidateSequence = 0;
                        std::vector<uint64_t> candidateDependencies;
                        if (!ReadEzTuple(vote.second.first, candidateCommand, candidateDependencies, candidateSequence)) {
                            valid = false; break;
                        }
                        if (command == 0) command = candidateCommand;
                        if (command != candidateCommand) { valid = false; break; }
                        dependencyUnion.insert(candidateDependencies.begin(), candidateDependencies.end());
                        sequence = std::max(sequence, candidateSequence);
                    }
                    const std::vector<uint64_t> dependencies(dependencyUnion.begin(), dependencyUnion.end());
                    ReplyState::EzBftKey slowKey{owner, epoch, instance, HashEzTuple(command, dependencies, sequence)};
                    if (valid && state.ezCommitsSent.insert(slowKey).second) {
                        ConsensusMessage commit;
                        commit.protocol = ConsensusProtocol::EZBFT; commit.type = ConsensusMessageType::EZBFT_COMMIT;
                        commit.senderId = m_clientId; commit.view = epoch; commit.instanceId = instance;
                        commit.messageId = static_cast<uint64_t>(owner) + 1; commit.digest = std::get<3>(slowKey);
                        commit.clientId = m_clientId; commit.requestId = requestId; commit.transactionCount = state.transactionCount;
                        WriteEzTuple(commit.payloadBytes, command, dependencies, sequence);
                        WriteEz32(commit.payloadBytes, static_cast<uint32_t>(2 * m_faultTolerance + 1));
                        included = 0;
                        for (const auto& vote : votes) {
                            if (included++ >= 2 * m_faultTolerance + 1) break;
                            WriteEz32(commit.payloadBytes, vote.first);
                            WriteEz32(commit.payloadBytes, static_cast<uint32_t>(vote.second.first.size()));
                            commit.payloadBytes.insert(commit.payloadBytes.end(), vote.second.first.begin(), vote.second.first.end());
                            commit.payloadBytes.insert(commit.payloadBytes.end(), vote.second.second.begin(), vote.second.second.end());
                        }
                        for (uint32_t replica : m_replicaIds) {
                            commit.receiverId = replica;
                            ConsensusMessage signedCommit = commit;
                            if (!ZyzzyvaAuth::Sign(signedCommit)) continue;
                            SendClientMessage(replica, signedCommit);
                        }
                        state.timeoutEvent = Simulator::Schedule(m_requestTimeout, &ConsensusClientApplication::HandleRequestTimeout,
                                                                 this, requestId);
                        return;
                    }
                }
            }
        }
        if (iterator->second.attempts > m_maxRetransmissions) {
            m_requestTimedOutTrace(m_clientId, requestId);
            return;
        }
        SendRequest(requestId);
    }

    void ConsensusClientApplication::HandleFastPathTimeout(uint64_t requestId) {
        auto request = m_requests.find(requestId);
        if (!m_running || request == m_requests.end() || request->second.completed) return;
        request->second.fastPathExpired = true;
        TryZyzzyvaSlowPath(requestId);
    }

    void ConsensusClientApplication::SendClientMessage(uint32_t peer, const ConsensusMessage& message) {
        Ptr<Packet> packet = message.ToPacket();
        m_messageTxTrace(m_clientId, peer, static_cast<uint16_t>(message.type), message.instanceId, packet->GetSize());
        m_messageEventTrace(true, message);
        m_transport->Send(peer, packet, ConsensusTransportMetadata::FromMessage(message));
    }

    void ConsensusClientApplication::TryZyzzyvaSlowPath(uint64_t requestId) {
        auto request = m_requests.find(requestId);
        if (!m_running || request == m_requests.end()) return;
        ReplyState& state = request->second;
        if (state.completed || !state.fastPathExpired) return;
        for (auto it = state.speculativeReplies.rbegin(); it != state.speculativeReplies.rend(); ++it) {
            const auto& key = it->first;
            const auto& voters = it->second;
            if (voters.size() < 2 * m_faultTolerance + 1 || !state.certificatesSent.insert(key).second) continue;
            ConsensusMessage commit;
            commit.protocol = ConsensusProtocol::ZYZZYVA;
            commit.type = ConsensusMessageType::ZYZZYVA_COMMIT_REQUEST;
            commit.senderId = m_clientId;
            commit.view = std::get<0>(key);
            commit.instanceId = std::get<1>(key);
            commit.digest = state.digest;
            commit.clientId = m_clientId;
            commit.requestId = requestId;
            commit.transactionCount = state.transactionCount;
            WriteEz64(commit.payloadBytes, std::get<2>(key));
            WriteEz32(commit.payloadBytes, voters.size());
            for (uint32_t voter : voters) {
                WriteEz32(commit.payloadBytes, voter);
                const auto& signature = state.speculativeSignatures[key][voter];
                commit.payloadBytes.insert(commit.payloadBytes.end(), signature.begin(), signature.end());
            }
            for (uint32_t replica : m_replicaIds) {
                ConsensusMessage signedCommit = commit;
                signedCommit.receiverId = replica;
                if (!ZyzzyvaAuth::Sign(signedCommit)) continue;
                SendClientMessage(replica, signedCommit);
            }
            break;
        }
    }

    void ConsensusClientApplication::OnTransportReceive(uint32_t peerId, Ptr<Packet> packet) {
        ConsensusMessage message;
        if (!m_running || !ConsensusMessage::FromPacket(packet, message) ||
            message.senderId != peerId || message.receiverId != m_clientId) {
            return;
        }
        if ((m_zyzzyva || m_ezbft) && !ZyzzyvaAuth::Verify(message)) return;
        m_messageRxTrace(peerId, m_clientId, static_cast<uint16_t>(message.type), message.instanceId, packet->GetSize());
        m_messageEventTrace(false, message);

        auto iterator = m_requests.find(message.requestId);
        if (iterator == m_requests.end()) {
            return;
        }

        ReplyState& state = iterator->second;
        if ((state.completed && !(m_zyzzyva && message.type == ConsensusMessageType::ZYZZYVA_SPEC_RESPONSE)) ||
            ((!m_ezbft || message.type == ConsensusMessageType::CLIENT_REPLY) && state.digest != message.digest)) {
            return;
        }
        if (m_bidl && message.type == ConsensusMessageType::BIDL_ABORT) {
            if (std::find(m_replicaIds.begin(), m_replicaIds.end(), message.senderId) == m_replicaIds.end()) return;
            auto& voters = state.abortVoters[{message.view, message.instanceId}];
            voters.insert(message.senderId);
            if (voters.size() < (m_replyQuorum ? m_replyQuorum : m_faultTolerance + 1)) return;
            state.completed = true; // Terminal abort; not a committed request or effective transaction.
            ++m_abortedRequests;
            Simulator::Cancel(state.timeoutEvent);
            Simulator::Cancel(state.fastPathEvent);
            m_requestAbortedTrace(m_clientId, message.requestId, Simulator::Now() - state.submitted);
            return;
        }

        if (m_ezbft && message.type == ConsensusMessageType::EZBFT_SPEC_REPLY) {
            if (message.protocol != ConsensusProtocol::EZBFT || message.type != ConsensusMessageType::EZBFT_SPEC_REPLY ||
                std::find(m_replicaIds.begin(), m_replicaIds.end(), message.senderId) == m_replicaIds.end() ||
                message.payloadBytes.size() < 20) return;
            uint64_t command = 0, sequence = 0;
            std::vector<uint64_t> dependencies;
            if (!ReadEzTuple(message.payloadBytes, command, dependencies, sequence) ||
                HashEzTuple(command, dependencies, sequence) != message.digest) return;
            if (message.messageId == 0) return;
            ReplyState::EzBftKey key{static_cast<uint32_t>(message.messageId - 1), message.view, message.instanceId, message.digest};
            auto& voters = state.ezSpeculativeReplies[key];
            voters.insert(message.senderId); state.ezTuplePayloads[key] = message.payloadBytes;
            state.ezReplySignatures[key][message.senderId] = message.authentication;
            const bool allSame = voters.size() == m_replicaIds.size();
            if (allSame && state.ezCommitsSent.insert(key).second) {
                ConsensusMessage commit = message;
                commit.type = allSame ? ConsensusMessageType::EZBFT_COMMIT_FAST : ConsensusMessageType::EZBFT_COMMIT;
                commit.senderId = m_clientId;
                uint32_t count = voters.size();
                commit.payloadBytes.push_back(static_cast<uint8_t>(count >> 24));
                commit.payloadBytes.push_back(static_cast<uint8_t>(count >> 16));
                commit.payloadBytes.push_back(static_cast<uint8_t>(count >> 8));
                commit.payloadBytes.push_back(static_cast<uint8_t>(count));
                for (uint32_t voter : voters) {
                    for (int shift = 24; shift >= 0; shift -= 8) commit.payloadBytes.push_back(static_cast<uint8_t>(voter >> shift));
                    const auto& tuple = state.ezTuplePayloads[key];
                    WriteEz32(commit.payloadBytes, static_cast<uint32_t>(tuple.size()));
                    commit.payloadBytes.insert(commit.payloadBytes.end(), tuple.begin(), tuple.end());
                    const auto& signature = state.ezReplySignatures[key][voter];
                    commit.payloadBytes.insert(commit.payloadBytes.end(), signature.begin(), signature.end());
                }
                for (uint32_t replica : m_replicaIds) {
                    commit.receiverId = replica;
                    ConsensusMessage signedCommit = commit;
                    if (!ZyzzyvaAuth::Sign(signedCommit)) continue;
                    SendClientMessage(replica, signedCommit);
                }
            }
            return;
        }
        if (m_ezbft && message.type != ConsensusMessageType::CLIENT_REPLY) return;

        if (m_zyzzyva) {
            if (message.protocol != ConsensusProtocol::ZYZZYVA ||
                (message.type != ConsensusMessageType::ZYZZYVA_SPEC_RESPONSE &&
                 message.type != ConsensusMessageType::ZYZZYVA_LOCAL_COMMIT) ||
                std::find(m_replicaIds.begin(), m_replicaIds.end(), message.senderId) == m_replicaIds.end() ||
                message.payloadBytes.size() < 8) return;
            uint64_t history = 0;
            for (size_t i = 0; i < 8; ++i) history = (history << 8) | message.payloadBytes[i];
            if (message.type == ConsensusMessageType::ZYZZYVA_LOCAL_COMMIT && message.payloadBytes.size() != 8) return;
            ReplyState::ZyzzyvaKey key{message.view, message.instanceId, history};
            if (message.type == ConsensusMessageType::ZYZZYVA_SPEC_RESPONSE) {
                size_t proofOffset = 8;
                uint32_t proofLength = 0;
                if (!ReadEz32(message.payloadBytes, proofOffset, proofLength)) {
                    if (m_requirePrimaryProofs) return;
                    proofLength = 0;
                }
                if (proofLength == 0) {
                    if (m_requirePrimaryProofs || message.payloadBytes.size() != 8) return;
                } else if (message.payloadBytes.size() - proofOffset != proofLength) return;
                if (proofLength != 0) {
                std::vector<uint8_t> proofBytes(message.payloadBytes.begin() + proofOffset, message.payloadBytes.end());
                ConsensusMessage proof;
                if (!UnpackSignedMessage(proofBytes, proof) || proof.protocol != ConsensusProtocol::ZYZZYVA ||
                    proof.type != ConsensusMessageType::ZYZZYVA_ORDER_REQ ||
                    proof.senderId != m_replicaIds[message.view % m_replicaIds.size()] || proof.view != message.view ||
                    proof.instanceId != message.instanceId || proof.digest != message.digest ||
                    proof.clientId != message.clientId || proof.requestId != message.requestId || proof.payloadBytes.size() < 8) return;
                if (proof.clientId != m_clientId || proof.requestId != message.requestId) return;
                ClientRequest provedRequest;
                if (!ZyzzyvaAuth::DecodeClientRequest(proof, 8, provedRequest) ||
                    provedRequest.clientId != message.clientId || provedRequest.requestId != message.requestId ||
                    provedRequest.digest != message.digest) return;
                for (const auto& previous : state.primaryOrderProofs) {
                    if (previous.second.view == proof.view && previous.second.clientId == proof.clientId &&
                        previous.second.requestId == proof.requestId &&
                        (previous.second.instanceId != proof.instanceId || previous.second.digest != proof.digest) &&
                        !state.pomSent) {
                        ConsensusMessage pom;
                        pom.protocol = ConsensusProtocol::ZYZZYVA;
                        pom.type = ConsensusMessageType::ZYZZYVA_POM;
                        pom.senderId = m_clientId;
                        pom.view = proof.view;
                        pom.instanceId = proof.instanceId;
                        pom.clientId = m_clientId;
                        pom.requestId = message.requestId;
                        const ConsensusMessage* proofPair[] = {&previous.second, &proof};
                        for (const ConsensusMessage* item : proofPair) {
                            auto encoded = PackSignedMessage(*item);
                            WriteEz32(pom.payloadBytes, static_cast<uint32_t>(encoded.size()));
                            pom.payloadBytes.insert(pom.payloadBytes.end(), encoded.begin(), encoded.end());
                        }
                        for (uint32_t replica : m_replicaIds) {
                            pom.receiverId = replica;
                            ConsensusMessage signedPom = pom;
                            if (ZyzzyvaAuth::Sign(signedPom)) SendClientMessage(replica, signedPom);
                        }
                        state.pomSent = true;
                        break;
                    }
                }
                state.primaryOrderProofs.emplace(proof.instanceId, proof);
                }
                auto& voters = state.speculativeReplies[key];
                voters.insert(message.senderId);
                state.speculativeSignatures[key][message.senderId] = message.authentication;
                if (voters.size() >= 2 * m_faultTolerance + 1 && message.view > m_zyzzyvaClientView) {
                    m_zyzzyvaClientView = message.view;
                    m_primaryId = m_replicaIds[message.view % m_replicaIds.size()];
                }
                if (voters.size() == m_replicaIds.size()) {
                    if (!state.completed) state.completionPath = 1;
                    CompleteRequest(message.requestId, state, message.transactionCount);
                    return;
                }
                TryZyzzyvaSlowPath(message.requestId);
            } else {
                auto& voters = state.localCommits[key];
                voters.insert(message.senderId);
                if (voters.size() >= 2 * m_faultTolerance + 1) {
                    if (!state.completed) state.completionPath = 2;
                    CompleteRequest(message.requestId, state, message.transactionCount);
                }
            }
            return;
        }

        if (message.type != ConsensusMessageType::CLIENT_REPLY) return;
        if (std::find(m_replicaIds.begin(), m_replicaIds.end(), message.senderId) == m_replicaIds.end()) return;

        state.senders.insert(message.senderId);
        uint32_t requiredReplies = m_replyQuorum == 0 ? m_faultTolerance + 1 : m_replyQuorum;
        if (state.senders.size() >= requiredReplies) {
            CompleteRequest(message.requestId, state, message.transactionCount);
        }
    }

    void ConsensusClientApplication::CompleteRequest(uint64_t requestId, ReplyState& state, uint32_t transactionCount) {
        if (state.completed) return;
        state.completed = true;
        Simulator::Cancel(state.timeoutEvent);
        Simulator::Cancel(state.fastPathEvent);
        ++m_completedRequests;
        m_completedTransactions += transactionCount;
        Time latency = Simulator::Now() - state.submitted;
        m_totalLatency += latency;
        m_lastCompletion = Simulator::Now();
        m_requestCompletedTrace(m_clientId, requestId, latency);
    }

} // namespace ns3
