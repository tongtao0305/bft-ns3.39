#ifndef CONSENSUS_MESSAGE_H
#define CONSENSUS_MESSAGE_H

#include "ns3/header.h"
#include "ns3/nstime.h"
#include "ns3/packet.h"
#include "ns3/ptr.h"
#include "ns3/tag.h"

#include <cstdint>
#include <ostream>
#include <vector>

namespace ns3 {

    /**
     * \ingroup consensus
     * Protocol identifiers carried by the common consensus envelope.
     */
    enum class ConsensusProtocol : uint8_t { GENERIC = 0, PBFT = 1, HOTSTUFF = 2, ZYZZYVA = 3, EZBFT = 4 };

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
        BIDL_GAP_REQUEST = 31,
        BIDL_ABORT = 32,
        BIDL_ORDERED_BLOCK = 33,
        BIDL_PERSIST = 34,
        BIDL_COMMIT = 35,
        BIDL_CONFLICT_REPORT = 36,
        BIDL_DENYLIST_VOTE = 37,
        BIDL_EXECUTION_RESULT = 38,
        BIDL_RESULT_PERSIST = 39,
        BIDL_BLOCK_REQUEST = 60,
        BIDL_SEQUENCE_EPOCH = 61,
        ZYZZYVA_ORDER_REQ = 40,
        ZYZZYVA_SPEC_RESPONSE = 41,
        ZYZZYVA_COMMIT_REQUEST = 42,
        ZYZZYVA_LOCAL_COMMIT = 43,
        ZYZZYVA_FILL_HOLE = 44,
        ZYZZYVA_HATE_PRIMARY = 45,
        ZYZZYVA_CHECKPOINT = 46,
        ZYZZYVA_REPAIR = 47,
        ZYZZYVA_IORS_EVIDENCE_REQUEST = 62,
        ZYZZYVA_IORS_EVIDENCE_RESPONSE = 63,
        ZYZZYVA_POM = 64,
        BIDL_TRANSACTION_REPAIR = 65,
        BIDL_BLOCK_SUMMARY_REQUEST = 66,
        EZBFT_SPEC_ORDER = 50,
        EZBFT_SPEC_REPLY = 51,
        EZBFT_COMMIT_FAST = 52,
        EZBFT_COMMIT = 53,
        EZBFT_COMMIT_REPLY = 54,
        EZBFT_OWNER_CHANGE = 55,
        EZBFT_NEW_OWNER = 56,
        EZBFT_PREPARE = 57,
        EZBFT_COMMIT_VOTE = 58,
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
        bool iorsPresent{false};
        bool iorsMarked{false};
        uint8_t iorsEvidence{0};
        bool iorsWaited{false};
        std::vector<uint8_t> payloadBytes;
        std::vector<uint8_t> authentication;

        Ptr<Packet> ToPacket() const;
        static bool FromPacket(Ptr<Packet> packet, ConsensusMessage& message, bool includePayload = true);
        static void StripIorsExtension(Ptr<Packet> packet);

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
        std::vector<uint8_t> authentication;
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
