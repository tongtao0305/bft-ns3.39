#include "consensus-application.h"

#include "ns3/abort.h"
#include "ns3/boolean.h"
#include "ns3/log.h"
#include "ns3/pointer.h"
#include "ns3/simulator.h"
#include "ns3/uinteger.h"

#include <algorithm>

namespace ns3 {

    NS_LOG_COMPONENT_DEFINE("ConsensusApplication");
    NS_OBJECT_ENSURE_REGISTERED(ConsensusApplication);
    NS_OBJECT_ENSURE_REGISTERED(ConsensusClientApplication);

    TypeId ConsensusApplication::GetTypeId() {
        static TypeId tid =
            TypeId("ns3::ConsensusApplication")
                .SetParent<Application>()
                .SetGroupName("Consensus")
                .AddConstructor<ConsensusApplication>()
                .AddAttribute("ReplicaId", "Logical identifier of this consensus replica.", UintegerValue(0),
                              MakeUintegerAccessor(&ConsensusApplication::m_replicaId), MakeUintegerChecker<uint32_t>())
                .AddAttribute("Engine", "Consensus protocol engine.", PointerValue(), MakePointerAccessor(&ConsensusApplication::m_engine),
                              MakePointerChecker<ConsensusEngine>())
                .AddAttribute("Transport", "Complete-message network transport.", PointerValue(),
                              MakePointerAccessor(&ConsensusApplication::m_transport), MakePointerChecker<ConsensusTransport>())
                .AddAttribute("ResourceModel", "CPU and storage resource model.", PointerValue(),
                              MakePointerAccessor(&ConsensusApplication::m_resourceModel), MakePointerChecker<ConsensusResourceModel>())
                .AddTraceSource("MessageTx", "A consensus message is passed to the transport.",
                                MakeTraceSourceAccessor(&ConsensusApplication::m_messageTxTrace),
                                "ns3::ConsensusApplication::MessageTracedCallback")
                .AddTraceSource("MessageRx", "A complete consensus message is received.",
                                MakeTraceSourceAccessor(&ConsensusApplication::m_messageRxTrace),
                                "ns3::ConsensusApplication::MessageTracedCallback")
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

        // 反序列化收到的数据包
        m_messageRxTrace(message.senderId, m_replicaId, static_cast<uint16_t>(message.type), message.instanceId, packet->GetSize());

        // 如果是客户端的请求，调用共识引擎的 SUBMIT
        if (message.type == ConsensusMessageType::CLIENT_REQUEST) {
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
        Ptr<Packet> packet = message.ToPacket();
        m_messageTxTrace(m_replicaId, peerId, static_cast<uint16_t>(message.type), message.instanceId, packet->GetSize());
        m_transport->Send(peerId, packet, ConsensusTransportMetadata::FromMessage(message));
    }

    TypeId ConsensusClientApplication::GetTypeId() {
        static TypeId tid =
            TypeId("ns3::ConsensusClientApplication")
                .SetParent<Application>()
                .SetGroupName("Consensus")
                .AddConstructor<ConsensusClientApplication>()
                .AddAttribute("ClientId", "Logical identifier of this client.", UintegerValue(1000),
                              MakeUintegerAccessor(&ConsensusClientApplication::m_clientId), MakeUintegerChecker<uint32_t>())
                .AddAttribute("PrimaryId", "Logical identifier of the fixed primary.", UintegerValue(0),
                              MakeUintegerAccessor(&ConsensusClientApplication::m_primaryId), MakeUintegerChecker<uint32_t>())
                .AddAttribute("FaultTolerance", "Client completes after f+1 matching replies.", UintegerValue(1),
                              MakeUintegerAccessor(&ConsensusClientApplication::m_faultTolerance), MakeUintegerChecker<uint32_t>())
                .AddAttribute("ReplyQuorum", "Matching replies required for completion; zero uses f+1.", UintegerValue(0),
                              MakeUintegerAccessor(&ConsensusClientApplication::m_replyQuorum), MakeUintegerChecker<uint32_t>())
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

        ConsensusMessage request;
        request.protocol = ConsensusProtocol::GENERIC;
        request.type = ConsensusMessageType::CLIENT_REQUEST;
        request.senderId = m_clientId;
        request.receiverId = targetId;
        request.messageId = requestId;
        request.payloadSize = state.payloadSize;
        request.digest = state.digest;
        request.clientId = m_clientId;
        request.requestId = requestId;
        request.transactionCount = state.transactionCount;

        if (state.attempts > 0) {
            m_requestRetransmittedTrace(m_clientId, requestId, targetId);
        }
        ++state.attempts;
        m_transport->Send(targetId, request.ToPacket(), ConsensusTransportMetadata::FromMessage(request));
        state.timeoutEvent = Simulator::Schedule(m_requestTimeout, &ConsensusClientApplication::HandleRequestTimeout, this, requestId);
    }

    void ConsensusClientApplication::HandleRequestTimeout(uint64_t requestId) {
        auto iterator = m_requests.find(requestId);
        if (!m_running || iterator == m_requests.end() || iterator->second.completed) {
            return;
        }
        if (iterator->second.attempts > m_maxRetransmissions) {
            m_requestTimedOutTrace(m_clientId, requestId);
            return;
        }
        SendRequest(requestId);
    }

    void ConsensusClientApplication::OnTransportReceive(uint32_t peerId, Ptr<Packet> packet) {
        ConsensusMessage message;
        if (!m_running || !ConsensusMessage::FromPacket(packet, message) || message.type != ConsensusMessageType::CLIENT_REPLY ||
            message.senderId != peerId || message.receiverId != m_clientId) {
            return;
        }

        auto iterator = m_requests.find(message.requestId);
        if (iterator == m_requests.end()) {
            return;
        }

        ReplyState& state = iterator->second;
        if (state.completed || state.digest != message.digest) {
            return;
        }

        state.senders.insert(message.senderId);
        uint32_t requiredReplies = m_replyQuorum == 0 ? m_faultTolerance + 1 : m_replyQuorum;
        if (state.senders.size() >= requiredReplies) {
            state.completed = true;
            if (state.timeoutEvent.IsRunning()) {
                Simulator::Cancel(state.timeoutEvent);
            }
            ++m_completedRequests;
            m_completedTransactions += message.transactionCount;
            Time latency = Simulator::Now() - state.submitted;
            m_totalLatency += latency;
            m_lastCompletion = Simulator::Now();
            m_requestCompletedTrace(m_clientId, message.requestId, latency);
        }
    }

} // namespace ns3
