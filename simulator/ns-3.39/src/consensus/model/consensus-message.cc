#include "consensus-message.h"

#include "ns3/log.h"

namespace ns3 {

    NS_LOG_COMPONENT_DEFINE("ConsensusMessage");
    NS_OBJECT_ENSURE_REGISTERED(ConsensusHeader);
    NS_OBJECT_ENSURE_REGISTERED(ConsensusMetadataTag);

    TypeId ConsensusMetadataTag::GetTypeId() {
        static TypeId tid =
            TypeId("ns3::ConsensusMetadataTag").SetParent<Tag>().SetGroupName("Consensus").AddConstructor<ConsensusMetadataTag>();
        return tid;
    }

    TypeId ConsensusMetadataTag::GetInstanceTypeId() const {
        return GetTypeId();
    }

    uint32_t ConsensusMetadataTag::GetSerializedSize() const {
        return 11;
    }

    void ConsensusMetadataTag::Serialize(TagBuffer buffer) const {
        buffer.WriteU16(m_messageType);
        buffer.WriteU64(m_flowId);
        buffer.WriteU8(m_priorityGroup);
    }

    void ConsensusMetadataTag::Deserialize(TagBuffer buffer) {
        m_messageType = buffer.ReadU16();
        m_flowId = buffer.ReadU64();
        m_priorityGroup = buffer.ReadU8();
    }

    void ConsensusMetadataTag::Print(std::ostream& os) const {
        os << "type=" << m_messageType << " flow=" << m_flowId << " pg=" << static_cast<uint32_t>(m_priorityGroup);
    }

    void ConsensusMetadataTag::SetMessageType(uint16_t type) {
        m_messageType = type;
    }

    uint16_t ConsensusMetadataTag::GetMessageType() const {
        return m_messageType;
    }

    void ConsensusMetadataTag::SetFlowId(uint64_t flowId) {
        m_flowId = flowId;
    }

    uint64_t ConsensusMetadataTag::GetFlowId() const {
        return m_flowId;
    }

    void ConsensusMetadataTag::SetPriorityGroup(uint8_t priorityGroup) {
        m_priorityGroup = priorityGroup;
    }

    uint8_t ConsensusMetadataTag::GetPriorityGroup() const {
        return m_priorityGroup;
    }

    ConsensusHeader::ConsensusHeader()
        : m_magic(MAGIC), m_version(VERSION), m_protocol(ConsensusProtocol::GENERIC), m_messageType(ConsensusMessageType::INVALID),
          m_senderId(0), m_receiverId(0), m_view(0), m_instanceId(0), m_messageId(0), m_payloadLength(0), m_digest(0), m_clientId(0),
          m_requestId(0), m_transactionCount(0) {}

    TypeId ConsensusHeader::GetTypeId() {
        static TypeId tid = TypeId("ns3::ConsensusHeader").SetParent<Header>().SetGroupName("Consensus").AddConstructor<ConsensusHeader>();
        return tid;
    }

    TypeId ConsensusHeader::GetInstanceTypeId() const {
        return GetTypeId();
    }

    uint32_t ConsensusHeader::GetSerializedSize() const {
        return 66;
    }

    void ConsensusHeader::Serialize(Buffer::Iterator start) const {
        start.WriteHtonU16(m_magic);
        start.WriteU8(m_version);
        start.WriteU8(static_cast<uint8_t>(m_protocol));
        start.WriteHtonU16(static_cast<uint16_t>(m_messageType));
        start.WriteHtonU32(m_senderId);
        start.WriteHtonU32(m_receiverId);
        start.WriteHtonU64(m_view);
        start.WriteHtonU64(m_instanceId);
        start.WriteHtonU64(m_messageId);
        start.WriteHtonU32(m_payloadLength);
        start.WriteHtonU64(m_digest);
        start.WriteHtonU32(m_clientId);
        start.WriteHtonU64(m_requestId);
        start.WriteHtonU32(m_transactionCount);
    }

    uint32_t ConsensusHeader::Deserialize(Buffer::Iterator start) {
        m_magic = start.ReadNtohU16();
        m_version = start.ReadU8();
        m_protocol = static_cast<ConsensusProtocol>(start.ReadU8());
        m_messageType = static_cast<ConsensusMessageType>(start.ReadNtohU16());
        m_senderId = start.ReadNtohU32();
        m_receiverId = start.ReadNtohU32();
        m_view = start.ReadNtohU64();
        m_instanceId = start.ReadNtohU64();
        m_messageId = start.ReadNtohU64();
        m_payloadLength = start.ReadNtohU32();
        m_digest = start.ReadNtohU64();
        m_clientId = start.ReadNtohU32();
        m_requestId = start.ReadNtohU64();
        m_transactionCount = start.ReadNtohU32();
        return GetSerializedSize();
    }

    void ConsensusHeader::Print(std::ostream& os) const {
        os << "protocol=" << static_cast<uint32_t>(m_protocol) << " type=" << static_cast<uint32_t>(m_messageType)
           << " sender=" << m_senderId << " receiver=" << m_receiverId << " view=" << m_view << " instance=" << m_instanceId
           << " digest=" << m_digest << " payload=" << m_payloadLength;
    }

    void ConsensusHeader::SetProtocol(ConsensusProtocol protocol) {
        m_protocol = protocol;
    }

    ConsensusProtocol ConsensusHeader::GetProtocol() const {
        return m_protocol;
    }

    void ConsensusHeader::SetMessageType(ConsensusMessageType messageType) {
        m_messageType = messageType;
    }

    ConsensusMessageType ConsensusHeader::GetMessageType() const {
        return m_messageType;
    }

    void ConsensusHeader::SetSenderId(uint32_t senderId) {
        m_senderId = senderId;
    }

    uint32_t ConsensusHeader::GetSenderId() const {
        return m_senderId;
    }

    void ConsensusHeader::SetReceiverId(uint32_t receiverId) {
        m_receiverId = receiverId;
    }

    uint32_t ConsensusHeader::GetReceiverId() const {
        return m_receiverId;
    }

    void ConsensusHeader::SetView(uint64_t view) {
        m_view = view;
    }

    uint64_t ConsensusHeader::GetView() const {
        return m_view;
    }

    void ConsensusHeader::SetInstanceId(uint64_t instanceId) {
        m_instanceId = instanceId;
    }

    uint64_t ConsensusHeader::GetInstanceId() const {
        return m_instanceId;
    }

    void ConsensusHeader::SetMessageId(uint64_t messageId) {
        m_messageId = messageId;
    }

    uint64_t ConsensusHeader::GetMessageId() const {
        return m_messageId;
    }

    void ConsensusHeader::SetPayloadLength(uint32_t payloadLength) {
        m_payloadLength = payloadLength;
    }

    uint32_t ConsensusHeader::GetPayloadLength() const {
        return m_payloadLength;
    }

    void ConsensusHeader::SetDigest(uint64_t digest) {
        m_digest = digest;
    }

    uint64_t ConsensusHeader::GetDigest() const {
        return m_digest;
    }

    void ConsensusHeader::SetClientId(uint32_t clientId) {
        m_clientId = clientId;
    }

    uint32_t ConsensusHeader::GetClientId() const {
        return m_clientId;
    }

    void ConsensusHeader::SetRequestId(uint64_t requestId) {
        m_requestId = requestId;
    }

    uint64_t ConsensusHeader::GetRequestId() const {
        return m_requestId;
    }

    void ConsensusHeader::SetTransactionCount(uint32_t transactionCount) {
        m_transactionCount = transactionCount;
    }

    uint32_t ConsensusHeader::GetTransactionCount() const {
        return m_transactionCount;
    }

    bool ConsensusHeader::IsValid() const {
        return m_magic == MAGIC && m_version == VERSION && m_messageType != ConsensusMessageType::INVALID;
    }

    Ptr<Packet> ConsensusMessage::ToPacket() const {
        Ptr<Packet> packet = Create<Packet>(payloadSize);
        ConsensusHeader header;
        header.SetProtocol(protocol);
        header.SetMessageType(type);
        header.SetSenderId(senderId);
        header.SetReceiverId(receiverId);
        header.SetView(view);
        header.SetInstanceId(instanceId);
        header.SetMessageId(messageId);
        header.SetPayloadLength(payloadSize);
        header.SetDigest(digest);
        header.SetClientId(clientId);
        header.SetRequestId(requestId);
        header.SetTransactionCount(transactionCount);
        packet->AddHeader(header);
        return packet;
    }

    bool ConsensusMessage::FromPacket(Ptr<Packet> packet, ConsensusMessage& message) {
        if (!packet) {
            return false;
        }

        ConsensusHeader header;
        if (packet->GetSize() < header.GetSerializedSize()) {
            return false;
        }

        Ptr<Packet> copy = packet->Copy();
        copy->RemoveHeader(header);
        if (!header.IsValid() || copy->GetSize() != header.GetPayloadLength()) {
            return false;
        }

        message.protocol = header.GetProtocol();
        message.type = header.GetMessageType();
        message.senderId = header.GetSenderId();
        message.receiverId = header.GetReceiverId();
        message.view = header.GetView();
        message.instanceId = header.GetInstanceId();
        message.messageId = header.GetMessageId();
        message.payloadSize = header.GetPayloadLength();
        message.digest = header.GetDigest();
        message.clientId = header.GetClientId();
        message.requestId = header.GetRequestId();
        message.transactionCount = header.GetTransactionCount();
        return true;
    }

    bool ConsensusMessage::operator==(const ConsensusMessage& other) const {
        return protocol == other.protocol && type == other.type && senderId == other.senderId && receiverId == other.receiverId &&
               view == other.view && instanceId == other.instanceId && messageId == other.messageId && payloadSize == other.payloadSize &&
               digest == other.digest && clientId == other.clientId && requestId == other.requestId &&
               transactionCount == other.transactionCount;
    }

    bool ConsensusMessage::operator!=(const ConsensusMessage& other) const {
        return !(*this == other);
    }

} // namespace ns3
