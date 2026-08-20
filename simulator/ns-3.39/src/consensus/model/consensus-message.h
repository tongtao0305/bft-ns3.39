#ifndef CONSENSUS_MESSAGE_H
#define CONSENSUS_MESSAGE_H

#include "ns3/header.h"
#include "ns3/nstime.h"
#include "ns3/packet.h"
#include "ns3/ptr.h"
#include "ns3/tag.h"

#include <cstdint>
#include <ostream>

namespace ns3 {

    /**
     * \ingroup consensus
     * Protocol identifiers carried by the common consensus envelope.
     */
    enum class ConsensusProtocol : uint8_t { GENERIC = 0, PBFT = 1, HOTSTUFF = 2 };

    /**
     * \ingroup consensus
     * Message identifiers shared by clients and consensus engines.
     */
    enum class ConsensusMessageType : uint16_t {
        INVALID = 0,
        CLIENT_REQUEST = 1,
        CLIENT_REPLY = 2,
        PRE_PREPARE = 10,
        PREPARE = 11,
        COMMIT = 12,
        VIEW_CHANGE = 13,
        NEW_VIEW = 14,
        BIDL_TRANSACTION = 30,
        BIDL_ORDERED_BLOCK = 33,
        BIDL_PERSIST = 34,
        BIDL_COMMIT = 35,
        RDMA_BACKGROUND_FLOW = 100
    };

    /** Packet tag visible to transports and switch policies without parsing the wire header. */
    class ConsensusMetadataTag : public Tag {
        public:
        static TypeId GetTypeId();
        TypeId GetInstanceTypeId() const override;
        uint32_t GetSerializedSize() const override;
        void Serialize(TagBuffer buffer) const override;
        void Deserialize(TagBuffer buffer) override;
        void Print(std::ostream& os) const override;

        void SetMessageType(uint16_t type);
        uint16_t GetMessageType() const;
        void SetFlowId(uint64_t flowId);
        uint64_t GetFlowId() const;
        void SetPriorityGroup(uint8_t priorityGroup);
        uint8_t GetPriorityGroup() const;

        private:
        uint16_t m_messageType{0};
        uint64_t m_flowId{0};
        uint8_t m_priorityGroup{0};
    };

    /**
     * \ingroup consensus
     * Fixed-size wire envelope understood by applications and transports.
     *
     * Protocol engines may interpret the fields differently, but transports only
     * use PayloadLength for stream framing.
     */
    class ConsensusHeader : public Header {
        public:
        ConsensusHeader();

        static TypeId GetTypeId();
        TypeId GetInstanceTypeId() const override;

        uint32_t GetSerializedSize() const override;
        void Serialize(Buffer::Iterator start) const override;
        uint32_t Deserialize(Buffer::Iterator start) override;
        void Print(std::ostream& os) const override;

        void SetProtocol(ConsensusProtocol protocol);
        ConsensusProtocol GetProtocol() const;

        void SetMessageType(ConsensusMessageType messageType);
        ConsensusMessageType GetMessageType() const;

        void SetSenderId(uint32_t senderId);
        uint32_t GetSenderId() const;

        void SetReceiverId(uint32_t receiverId);
        uint32_t GetReceiverId() const;

        void SetView(uint64_t view);
        uint64_t GetView() const;

        void SetInstanceId(uint64_t instanceId);
        uint64_t GetInstanceId() const;

        void SetMessageId(uint64_t messageId);
        uint64_t GetMessageId() const;

        void SetPayloadLength(uint32_t payloadLength);
        uint32_t GetPayloadLength() const;

        void SetDigest(uint64_t digest);
        uint64_t GetDigest() const;

        void SetClientId(uint32_t clientId);
        uint32_t GetClientId() const;

        void SetRequestId(uint64_t requestId);
        uint64_t GetRequestId() const;

        void SetTransactionCount(uint32_t transactionCount);
        uint32_t GetTransactionCount() const;

        bool IsValid() const;

        private:
        static constexpr uint16_t MAGIC = 0xc05e;
        static constexpr uint8_t VERSION = 1;

        uint16_t m_magic;
        uint8_t m_version;
        ConsensusProtocol m_protocol;
        ConsensusMessageType m_messageType;
        uint32_t m_senderId;
        uint32_t m_receiverId;
        uint64_t m_view;
        uint64_t m_instanceId;
        uint64_t m_messageId;
        uint32_t m_payloadLength;
        uint64_t m_digest;
        uint32_t m_clientId;
        uint64_t m_requestId;
        uint32_t m_transactionCount;
    };

    /**
     * Value object exchanged between ConsensusApplication and ConsensusEngine.
     */
    struct ConsensusMessage {
        ConsensusProtocol protocol{ConsensusProtocol::GENERIC};
        ConsensusMessageType type{ConsensusMessageType::INVALID};
        uint32_t senderId{0};
        uint32_t receiverId{0};
        uint64_t view{0};
        uint64_t instanceId{0};
        uint64_t messageId{0};
        uint32_t payloadSize{0};
        uint64_t digest{0};
        uint32_t clientId{0};
        uint64_t requestId{0};
        uint32_t transactionCount{0};

        Ptr<Packet> ToPacket() const;
        static bool FromPacket(Ptr<Packet> packet, ConsensusMessage& message);

        bool operator==(const ConsensusMessage& other) const;
        bool operator!=(const ConsensusMessage& other) const;
    };

    /**
     * Request submitted by a client to a consensus engine.
     */
    struct ClientRequest {
        uint32_t clientId{0};
        uint64_t requestId{0};
        uint32_t payloadSize{0};
        uint32_t transactionCount{1};
        uint64_t digest{0};
    };

    /**
     * Decision returned by a consensus engine to its hosting application.
     */
    struct ConsensusDecision {
        ConsensusProtocol protocol{ConsensusProtocol::GENERIC};
        uint64_t view{0};
        uint64_t instanceId{0};
        uint64_t digest{0};
        uint32_t clientId{0};
        uint64_t requestId{0};
        uint32_t transactionCount{0};
        Time decisionTime{Seconds(0)};
    };

} // namespace ns3

#endif // CONSENSUS_MESSAGE_H
