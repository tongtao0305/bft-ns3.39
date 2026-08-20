#include "consensus-transport.h"

#include "consensus-message.h"

#include "ns3/abort.h"
#include "ns3/boolean.h"
#include "ns3/double.h"
#include "ns3/inet-socket-address.h"
#include "ns3/ipv4-address.h"
#include "ns3/log.h"
#include "ns3/pointer.h"
#include "ns3/rdma-driver.h"
#include "ns3/simulator.h"
#include "ns3/tcp-socket-factory.h"
#include "ns3/uinteger.h"

#include <algorithm>
#include <limits>

namespace ns3 {

    NS_LOG_COMPONENT_DEFINE("ConsensusTransport");
    NS_OBJECT_ENSURE_REGISTERED(ConsensusFaultModel);
    NS_OBJECT_ENSURE_REGISTERED(ConfigurableConsensusFaultModel);
    NS_OBJECT_ENSURE_REGISTERED(ConsensusTransport);
    NS_OBJECT_ENSURE_REGISTERED(TcpConsensusTransport);
    NS_OBJECT_ENSURE_REGISTERED(RdmaConsensusTransport);

    TypeId ConsensusFaultModel::GetTypeId() {
        static TypeId tid = TypeId("ns3::ConsensusFaultModel").SetParent<Object>().SetGroupName("Consensus");
        return tid;
    }

    TypeId ConfigurableConsensusFaultModel::GetTypeId() {
        static TypeId tid =
            TypeId("ns3::ConfigurableConsensusFaultModel")
                .SetParent<ConsensusFaultModel>()
                .SetGroupName("Consensus")
                .AddConstructor<ConfigurableConsensusFaultModel>()
                .AddAttribute("DropProbability", "Independent loss probability for matching messages.", DoubleValue(0.0),
                              MakeDoubleAccessor(&ConfigurableConsensusFaultModel::m_dropProbability), MakeDoubleChecker<double>(0.0, 1.0))
                .AddAttribute("Delay", "Additional delay for matching messages.", TimeValue(Seconds(0)),
                              MakeTimeAccessor(&ConfigurableConsensusFaultModel::m_delay), MakeTimeChecker())
                .AddAttribute("FailedNodeId", "Logical node to isolate; UINT32_MAX disables node isolation.",
                              UintegerValue(std::numeric_limits<uint32_t>::max()),
                              MakeUintegerAccessor(&ConfigurableConsensusFaultModel::m_failedNodeId), MakeUintegerChecker<uint32_t>())
                .AddAttribute("FailureStart", "Start of the logical-node isolation interval.", TimeValue(Seconds(0)),
                              MakeTimeAccessor(&ConfigurableConsensusFaultModel::m_failureStart), MakeTimeChecker())
                .AddAttribute("FailureStop", "End of isolation; zero means the end of simulation.", TimeValue(Seconds(0)),
                              MakeTimeAccessor(&ConfigurableConsensusFaultModel::m_failureStop), MakeTimeChecker())
                .AddAttribute("MessageType", "ConsensusMessageType to impair; zero matches every type.", UintegerValue(0),
                              MakeUintegerAccessor(&ConfigurableConsensusFaultModel::m_messageType), MakeUintegerChecker<uint16_t>())
                .AddAttribute("AffectSend", "Apply probabilistic loss and delay on egress.", BooleanValue(false),
                              MakeBooleanAccessor(&ConfigurableConsensusFaultModel::m_affectSend), MakeBooleanChecker())
                .AddAttribute("AffectReceive", "Apply probabilistic loss and delay on ingress.", BooleanValue(true),
                              MakeBooleanAccessor(&ConfigurableConsensusFaultModel::m_affectReceive), MakeBooleanChecker());
        return tid;
    }

    ConfigurableConsensusFaultModel::ConfigurableConsensusFaultModel()
        : m_random(CreateObject<UniformRandomVariable>()), m_dropProbability(0.0), m_delay(Seconds(0)),
          m_failedNodeId(std::numeric_limits<uint32_t>::max()), m_failureStart(Seconds(0)), m_failureStop(Seconds(0)), m_messageType(0),
          m_affectSend(false), m_affectReceive(true) {}

    ConsensusFaultDecision ConfigurableConsensusFaultModel::Evaluate(uint32_t localId, uint32_t peerId, Ptr<const Packet> packet,
                                                                     ConsensusFaultDirection direction) {
        ConsensusFaultDecision decision;
        if ((direction == CONSENSUS_FAULT_SEND && !m_affectSend) || (direction == CONSENSUS_FAULT_RECEIVE && !m_affectReceive)) {
            return decision;
        }
        Time now = Simulator::Now();
        bool failureActive = now >= m_failureStart && (m_failureStop.IsZero() || now < m_failureStop);
        if (failureActive && m_failedNodeId != std::numeric_limits<uint32_t>::max() &&
            (localId == m_failedNodeId || peerId == m_failedNodeId)) {
            decision.drop = true;
            return decision;
        }
        if (m_messageType != 0) {
            ConsensusMessage message;
            if (!packet || !ConsensusMessage::FromPacket(packet->Copy(), message) || static_cast<uint16_t>(message.type) != m_messageType) {
                return decision;
            }
        }
        decision.drop = m_random->GetValue() < m_dropProbability;
        decision.delay = m_delay;
        return decision;
    }

    ConsensusTransportMetadata ConsensusTransportMetadata::FromMessage(const ConsensusMessage& message, uint8_t priorityGroup) {
        uint64_t logicalId = message.instanceId;
        if (logicalId == 0) {
            logicalId = message.requestId != 0 ? message.requestId : message.messageId;
        }
        return {static_cast<uint16_t>(message.type), (static_cast<uint64_t>(message.senderId) << 32) ^ logicalId, priorityGroup};
    }

    TypeId ConsensusTransport::GetTypeId() {
        static TypeId tid =
            TypeId("ns3::ConsensusTransport")
                .SetParent<Object>()
                .SetGroupName("Consensus")
                .AddAttribute("ReceiveDropProbability", "Independent probability of dropping a complete received message.",
                              DoubleValue(0.0), MakeDoubleAccessor(&ConsensusTransport::m_receiveDropProbability),
                              MakeDoubleChecker<double>(0.0, 1.0))
                .AddAttribute("ReceiveDelay", "Additional delay applied after complete-message reassembly.", TimeValue(Seconds(0)),
                              MakeTimeAccessor(&ConsensusTransport::m_receiveDelay), MakeTimeChecker())
                .AddAttribute("FaultModel", "Optional protocol-independent message fault model.", PointerValue(),
                              MakePointerAccessor(&ConsensusTransport::m_faultModel), MakePointerChecker<ConsensusFaultModel>());
        return tid;
    }

    ConsensusTransport::ConsensusTransport()
        : m_localId(0), m_localPort(0), m_receiveDropRandom(CreateObject<UniformRandomVariable>()), m_receiveDropProbability(0.0),
          m_receiveDelay(Seconds(0)) {}

    void ConsensusTransport::SetLocalEndpoint(uint32_t localId, uint16_t localPort) {
        m_localId = localId;
        m_localPort = localPort;
    }

    void ConsensusTransport::SetLocalEndpoint(uint32_t localId, const Address& address) {
        m_localId = localId;
        m_localAddress = address;
        if (InetSocketAddress::IsMatchingType(address)) {
            m_localPort = InetSocketAddress::ConvertFrom(address).GetPort();
        }
    }

    void ConsensusTransport::SetPeerAddress(uint32_t peerId, const Address& address) {
        m_peerAddresses[peerId] = address;
    }

    void ConsensusTransport::SetReceiveCallback(ReceiveCallback callback) {
        m_receiveCallback = callback;
    }

    void ConsensusTransport::SetFaultModel(Ptr<ConsensusFaultModel> faultModel) {
        m_faultModel = faultModel;
    }

    ConsensusTransportMetadata ConsensusTransport::AttachMetadataTag(Ptr<Packet> packet, const ConsensusTransportMetadata& metadata,
                                                                     uint8_t defaultPriorityGroup) const {
        ConsensusTransportMetadata normalized = metadata;
        if (normalized.priorityGroup == ConsensusTransportMetadata::USE_TRANSPORT_DEFAULT) {
            normalized.priorityGroup = defaultPriorityGroup;
        }
        if (packet) {
            ConsensusMetadataTag oldTag;
            packet->RemovePacketTag(oldTag);
            ConsensusMetadataTag tag;
            tag.SetMessageType(normalized.messageType);
            tag.SetFlowId(normalized.flowId);
            tag.SetPriorityGroup(normalized.priorityGroup);
            packet->AddPacketTag(tag);
        }
        return normalized;
    }

    ConsensusFaultDecision ConsensusTransport::EvaluateFault(uint32_t peerId, Ptr<const Packet> packet, ConsensusFaultDirection direction) {
        return m_faultModel ? m_faultModel->Evaluate(m_localId, peerId, packet, direction) : ConsensusFaultDecision{};
    }

    void ConsensusTransport::Deliver(uint32_t peerId, Ptr<Packet> packet) {
        ConsensusFaultDecision fault = EvaluateFault(peerId, packet, CONSENSUS_FAULT_RECEIVE);
        if (!packet || fault.drop || m_receiveDropRandom->GetValue() < m_receiveDropProbability) {
            return;
        }
        Time delay = m_receiveDelay + fault.delay;
        if (delay.IsPositive()) {
            Simulator::Schedule(delay, &ConsensusTransport::DoDeliver, this, peerId, packet->Copy());
            return;
        }
        DoDeliver(peerId, packet);
    }

    void ConsensusTransport::DoDeliver(uint32_t peerId, Ptr<Packet> packet) {
        if (!m_receiveCallback.IsNull() && packet) {
            m_receiveCallback(peerId, packet);
        }
    }

    TypeId TcpConsensusTransport::GetTypeId() {
        static TypeId tid = TypeId("ns3::TcpConsensusTransport")
                                .SetParent<ConsensusTransport>()
                                .SetGroupName("Consensus")
                                .AddConstructor<TcpConsensusTransport>()
                                .AddAttribute("ConnectRetry", "Delay before retrying a failed TCP connection.", TimeValue(MilliSeconds(1)),
                                              MakeTimeAccessor(&TcpConsensusTransport::m_connectRetry), MakeTimeChecker());
        return tid;
    }

    TcpConsensusTransport::TcpConsensusTransport() : m_connectRetry(MilliSeconds(1)), m_running(false) {}

    TcpConsensusTransport::~TcpConsensusTransport() {}

    void TcpConsensusTransport::Start(Ptr<Node> node) {
        NS_ABORT_MSG_IF(m_running, "TCP consensus transport is already running");
        NS_ABORT_MSG_IF(!node, "TCP consensus transport requires a node");
        NS_ABORT_MSG_IF(m_localPort == 0, "TCP consensus transport requires a local port");

        m_node = node;
        m_listenSocket = Socket::CreateSocket(node, TcpSocketFactory::GetTypeId());
        InetSocketAddress local(Ipv4Address::GetAny(), m_localPort);
        NS_ABORT_MSG_IF(m_listenSocket->Bind(local) == -1, "Failed to bind consensus TCP socket");
        NS_ABORT_MSG_IF(m_listenSocket->Listen() == -1, "Failed to listen on consensus TCP socket");

        m_listenSocket->SetAcceptCallback(MakeNullCallback<bool, Ptr<Socket>, const Address&>(),
                                          MakeCallback(&TcpConsensusTransport::HandleAccept, this));
        m_running = true;
    }

    void TcpConsensusTransport::Stop() {
        m_running = false;

        for (auto& [peerId, connection] : m_outgoing) {
            if (connection.retryEvent.IsRunning()) {
                Simulator::Cancel(connection.retryEvent);
            }
            if (connection.socket) {
                connection.socket->SetConnectCallback(MakeNullCallback<void, Ptr<Socket>>(), MakeNullCallback<void, Ptr<Socket>>());
                connection.socket->SetSendCallback(MakeNullCallback<void, Ptr<Socket>, uint32_t>());
                connection.socket->Close();
            }
        }
        m_outgoing.clear();

        for (auto& connection : m_incoming) {
            if (connection.socket) {
                connection.socket->SetRecvCallback(MakeNullCallback<void, Ptr<Socket>>());
                connection.socket->Close();
            }
        }
        m_incoming.clear();

        if (m_listenSocket) {
            m_listenSocket->Close();
            m_listenSocket = nullptr;
        }
    }

    void TcpConsensusTransport::Send(uint32_t peerId, Ptr<Packet> packet, const ConsensusTransportMetadata& metadata) {
        if (!m_running || !packet || m_peerAddresses.find(peerId) == m_peerAddresses.end()) {
            return;
        }

        AttachMetadataTag(packet, metadata, 0);
        ConsensusFaultDecision fault = EvaluateFault(peerId, packet, CONSENSUS_FAULT_SEND);
        if (fault.drop) {
            return;
        }
        if (fault.delay.IsPositive()) {
            Simulator::Schedule(fault.delay, &TcpConsensusTransport::SendNow, this, peerId, packet->Copy());
            return;
        }
        SendNow(peerId, packet);
    }

    void TcpConsensusTransport::SendNow(uint32_t peerId, Ptr<Packet> packet) {
        if (!m_running || !packet || m_peerAddresses.find(peerId) == m_peerAddresses.end()) {
            return;
        }

        OutgoingConnection& connection = m_outgoing[peerId];
        connection.pending.push_back(packet->Copy());
        if (!connection.socket) {
            ConnectPeer(peerId);
        }
        if (connection.connected) {
            FlushPeer(peerId);
        }
    }

    void TcpConsensusTransport::DoDispose() {
        Stop();
        m_node = nullptr;
        ConsensusTransport::DoDispose();
    }

    void TcpConsensusTransport::ConnectPeer(uint32_t peerId) {
        if (!m_running) {
            return;
        }

        auto address = m_peerAddresses.find(peerId);
        if (address == m_peerAddresses.end()) {
            return;
        }

        OutgoingConnection& connection = m_outgoing[peerId];
        connection.socket = Socket::CreateSocket(m_node, TcpSocketFactory::GetTypeId());
        connection.connected = false;
        connection.socket->SetConnectCallback(MakeCallback(&TcpConsensusTransport::HandleConnectSucceeded, this),
                                              MakeCallback(&TcpConsensusTransport::HandleConnectFailed, this));
        connection.socket->SetSendCallback(MakeCallback(&TcpConsensusTransport::HandleSendReady, this));
        connection.socket->Connect(address->second);
    }

    void TcpConsensusTransport::RetryPeer(uint32_t peerId) {
        auto iterator = m_outgoing.find(peerId);
        if (!m_running || iterator == m_outgoing.end() || iterator->second.pending.empty()) {
            return;
        }
        ConnectPeer(peerId);
    }

    void TcpConsensusTransport::FlushPeer(uint32_t peerId) {
        auto iterator = m_outgoing.find(peerId);
        if (iterator == m_outgoing.end()) {
            return;
        }

        OutgoingConnection& connection = iterator->second;
        while (connection.connected && connection.socket && !connection.pending.empty()) {
            Ptr<Packet> packet = connection.pending.front();
            int sent = connection.socket->Send(packet);
            if (sent <= 0) {
                break;
            }
            if (static_cast<uint32_t>(sent) == packet->GetSize()) {
                connection.pending.pop_front();
            } else {
                connection.pending.front() = packet->CreateFragment(sent, packet->GetSize() - static_cast<uint32_t>(sent));
                break;
            }
        }
    }

    uint32_t TcpConsensusTransport::FindOutgoingPeer(Ptr<Socket> socket) const {
        for (const auto& [peerId, connection] : m_outgoing) {
            if (connection.socket == socket) {
                return peerId;
            }
        }
        return std::numeric_limits<uint32_t>::max();
    }

    TcpConsensusTransport::IncomingConnection* TcpConsensusTransport::FindIncoming(Ptr<Socket> socket) {
        auto iterator = std::find_if(m_incoming.begin(), m_incoming.end(),
                                     [socket](const IncomingConnection& connection) { return connection.socket == socket; });
        return iterator == m_incoming.end() ? nullptr : &(*iterator);
    }

    void TcpConsensusTransport::HandleConnectSucceeded(Ptr<Socket> socket) {
        uint32_t peerId = FindOutgoingPeer(socket);
        if (peerId == std::numeric_limits<uint32_t>::max()) {
            return;
        }
        m_outgoing[peerId].connected = true;
        FlushPeer(peerId);
    }

    void TcpConsensusTransport::HandleConnectFailed(Ptr<Socket> socket) {
        uint32_t peerId = FindOutgoingPeer(socket);
        if (peerId == std::numeric_limits<uint32_t>::max()) {
            return;
        }

        OutgoingConnection& connection = m_outgoing[peerId];
        connection.connected = false;
        socket->Close();
        connection.socket = nullptr;
        connection.retryEvent = Simulator::Schedule(m_connectRetry, &TcpConsensusTransport::RetryPeer, this, peerId);
    }

    void TcpConsensusTransport::HandleSendReady(Ptr<Socket> socket, uint32_t availableBytes) {
        (void)availableBytes;
        uint32_t peerId = FindOutgoingPeer(socket);
        if (peerId != std::numeric_limits<uint32_t>::max()) {
            FlushPeer(peerId);
        }
    }

    void TcpConsensusTransport::HandleAccept(Ptr<Socket> socket, const Address& from) {
        (void)from;
        IncomingConnection connection;
        connection.socket = socket;
        connection.buffer = Create<Packet>();
        m_incoming.push_back(connection);

        socket->SetRecvCallback(MakeCallback(&TcpConsensusTransport::HandleRead, this));
        socket->SetCloseCallbacks(MakeCallback(&TcpConsensusTransport::HandlePeerClose, this),
                                  MakeCallback(&TcpConsensusTransport::HandlePeerError, this));
    }

    void TcpConsensusTransport::HandleRead(Ptr<Socket> socket) {
        IncomingConnection* connection = FindIncoming(socket);
        if (!connection) {
            return;
        }

        Ptr<Packet> received;
        while ((received = socket->Recv())) {
            if (received->GetSize() == 0) {
                break;
            }
            connection->buffer->AddAtEnd(received);
        }

        ConsensusHeader header;
        const uint32_t headerSize = header.GetSerializedSize();
        while (connection->buffer->GetSize() >= headerSize) {
            connection->buffer->PeekHeader(header);
            if (!header.IsValid()) {
                NS_LOG_WARN("Dropping malformed consensus TCP stream");
                connection->buffer = Create<Packet>();
                return;
            }

            uint64_t frameSize = static_cast<uint64_t>(headerSize) + header.GetPayloadLength();
            if (connection->buffer->GetSize() < frameSize) {
                return;
            }

            uint32_t frameBytes = static_cast<uint32_t>(frameSize);
            Ptr<Packet> frame = connection->buffer->CreateFragment(0, frameBytes);
            connection->buffer->RemoveAtStart(frameBytes);
            Deliver(header.GetSenderId(), frame);
        }
    }

    void TcpConsensusTransport::HandlePeerClose(Ptr<Socket> socket) {
        auto iterator = std::remove_if(m_incoming.begin(), m_incoming.end(),
                                       [socket](const IncomingConnection& connection) { return connection.socket == socket; });
        m_incoming.erase(iterator, m_incoming.end());
    }

    void TcpConsensusTransport::HandlePeerError(Ptr<Socket> socket) {
        HandlePeerClose(socket);
    }

    TypeId RdmaConsensusTransport::GetTypeId() {
        static TypeId tid =
            TypeId("ns3::RdmaConsensusTransport")
                .SetParent<ConsensusTransport>()
                .SetGroupName("Consensus")
                .AddConstructor<RdmaConsensusTransport>()
                .AddAttribute("PriorityGroup", "RDMA priority group used by consensus messages.", UintegerValue(3),
                              MakeUintegerAccessor(&RdmaConsensusTransport::m_priorityGroup), MakeUintegerChecker<uint16_t>())
                .AddAttribute("Window", "Maximum RDMA bytes in flight; zero uses rate control defaults.", UintegerValue(0),
                              MakeUintegerAccessor(&RdmaConsensusTransport::m_window), MakeUintegerChecker<uint32_t>())
                .AddAttribute("BaseRttNs", "Base RTT supplied to the RDMA congestion controller.", UintegerValue(100000),
                              MakeUintegerAccessor(&RdmaConsensusTransport::m_baseRttNs), MakeUintegerChecker<uint64_t>());
        return tid;
    }

    RdmaConsensusTransport::RdmaConsensusTransport()
        : m_priorityGroup(3), m_nextSourcePort(10000), m_window(0), m_baseRttNs(100000), m_running(false) {}

    void RdmaConsensusTransport::Start(Ptr<Node> node) {
        NS_ABORT_MSG_IF(m_running, "RDMA consensus transport is already running");
        NS_ABORT_MSG_IF(!node, "RDMA consensus transport requires a node");
        NS_ABORT_MSG_IF(!InetSocketAddress::IsMatchingType(m_localAddress), "RDMA consensus transport requires an IPv4 local endpoint");

        m_node = node;
        m_driver = node->GetObject<RdmaDriver>();
        NS_ABORT_MSG_IF(!m_driver || !m_driver->m_rdma, "Install and initialize RdmaDriver before starting consensus");

        m_nextSourcePort = m_localPort == 0 ? 10000 : m_localPort;
        m_driver->m_rdma->RegisterAppReceiveCallback(m_localPort, MakeCallback(&RdmaConsensusTransport::HandleFragment, this));
        m_running = true;
    }

    void RdmaConsensusTransport::Stop() {
        if (m_driver && m_driver->m_rdma) {
            m_driver->m_rdma->UnregisterAppReceiveCallback(m_localPort);
        }
        m_running = false;
        m_reassembly.clear();
        m_sourcePortPeers.clear();
    }

    void RdmaConsensusTransport::Send(uint32_t peerId, Ptr<Packet> packet, const ConsensusTransportMetadata& metadata) {
        auto peer = m_peerAddresses.find(peerId);
        if (!m_running || !packet || peer == m_peerAddresses.end() || !InetSocketAddress::IsMatchingType(peer->second)) {
            return;
        }

        ConsensusTransportMetadata normalized = AttachMetadataTag(packet, metadata, static_cast<uint8_t>(m_priorityGroup));
        ConsensusFaultDecision fault = EvaluateFault(peerId, packet, CONSENSUS_FAULT_SEND);
        if (fault.drop) {
            return;
        }
        if (fault.delay.IsPositive()) {
            Simulator::Schedule(fault.delay, &RdmaConsensusTransport::SendNow, this, peerId, packet->Copy(), normalized);
            return;
        }
        SendNow(peerId, packet, normalized);
    }

    void RdmaConsensusTransport::SendNow(uint32_t peerId, Ptr<Packet> packet, ConsensusTransportMetadata metadata) {
        auto peer = m_peerAddresses.find(peerId);
        if (!m_running || !packet || peer == m_peerAddresses.end() || !InetSocketAddress::IsMatchingType(peer->second)) {
            return;
        }

        InetSocketAddress local = InetSocketAddress::ConvertFrom(m_localAddress);
        InetSocketAddress remote = InetSocketAddress::ConvertFrom(peer->second);
        uint16_t sourcePort = AllocateSourcePort();
        m_sourcePortPeers[sourcePort] = peerId;

        m_driver->AddQueuePair(packet->Copy(), metadata.priorityGroup, local.GetIpv4(), remote.GetIpv4(), sourcePort, remote.GetPort(),
                               m_window, m_baseRttNs, MakeCallback(&RdmaConsensusTransport::HandleSendComplete, this).Bind(sourcePort),
                               Simulator::GetMaximumSimulationTime());
    }

    void RdmaConsensusTransport::DoDispose() {
        Stop();
        m_driver = nullptr;
        m_node = nullptr;
        ConsensusTransport::DoDispose();
    }

    void RdmaConsensusTransport::HandleFragment(uint32_t sourceIp, uint16_t sourcePort, uint16_t priorityGroup, uint32_t sequence,
                                                Ptr<Packet> fragment) {
        if (!m_running || !fragment) {
            return;
        }

        FlowKey key{sourceIp, sourcePort, priorityGroup};
        Ptr<Packet>& buffer = m_reassembly[key];
        if (!buffer) {
            buffer = Create<Packet>();
        }
        if (sequence != buffer->GetSize()) {
            return;
        }
        buffer->AddAtEnd(fragment);

        ConsensusHeader header;
        uint32_t headerSize = header.GetSerializedSize();
        if (buffer->GetSize() < headerSize) {
            return;
        }
        buffer->PeekHeader(header);
        if (!header.IsValid()) {
            m_reassembly.erase(key);
            return;
        }

        uint64_t frameSize = static_cast<uint64_t>(headerSize) + header.GetPayloadLength();
        if (buffer->GetSize() < frameSize) {
            return;
        }
        if (buffer->GetSize() != frameSize) {
            m_reassembly.erase(key);
            return;
        }

        uint32_t peerId = header.GetSenderId();
        Deliver(peerId, buffer->Copy());
        m_reassembly.erase(key);
        m_driver->m_rdma->DeleteRxQp(sourceIp, priorityGroup, sourcePort);
    }

    void RdmaConsensusTransport::HandleSendComplete(uint16_t sourcePort) {
        m_sourcePortPeers.erase(sourcePort);
    }

    uint16_t RdmaConsensusTransport::AllocateSourcePort() {
        for (uint32_t attempt = 0; attempt < 65535; ++attempt) {
            uint16_t candidate = m_nextSourcePort++;
            if (m_nextSourcePort < 10000) {
                m_nextSourcePort = 10000;
            }
            if (candidate != 0 && m_sourcePortPeers.find(candidate) == m_sourcePortPeers.end()) {
                return candidate;
            }
        }
        NS_ABORT_MSG("RDMA consensus transport exhausted source ports");
        return 0;
    }

} // namespace ns3
