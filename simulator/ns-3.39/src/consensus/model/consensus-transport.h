#ifndef CONSENSUS_TRANSPORT_H
#define CONSENSUS_TRANSPORT_H

#include "consensus-message.h"

#include "ns3/address.h"
#include "ns3/callback.h"
#include "ns3/event-id.h"
#include "ns3/node.h"
#include "ns3/nstime.h"
#include "ns3/object.h"
#include "ns3/packet.h"
#include "ns3/ptr.h"
#include "ns3/random-variable-stream.h"
#include "ns3/socket.h"

#include <cstdint>
#include <deque>
#include <map>
#include <tuple>
#include <vector>

namespace ns3 {

    class RdmaDriver;

    enum ConsensusFaultDirection : uint8_t { CONSENSUS_FAULT_SEND = 0, CONSENSUS_FAULT_RECEIVE = 1 };

    struct ConsensusFaultDecision {
        bool drop{false};
        Time delay{Seconds(0)};
    };

    class ConsensusFaultModel : public Object {
        public:
        static TypeId GetTypeId();

        virtual ConsensusFaultDecision Evaluate(uint32_t localId, uint32_t peerId, Ptr<const Packet> packet,
                                                ConsensusFaultDirection direction) = 0;
    };

    /** Configurable loss, delay, filtering, and node-isolation model. */
    class ConfigurableConsensusFaultModel : public ConsensusFaultModel {
        public:
        static TypeId GetTypeId();

        ConfigurableConsensusFaultModel();

        ConsensusFaultDecision Evaluate(uint32_t localId, uint32_t peerId, Ptr<const Packet> packet,
                                        ConsensusFaultDirection direction) override;

        private:
        Ptr<UniformRandomVariable> m_random;
        double m_dropProbability;
        Time m_delay;
        uint32_t m_failedNodeId;
        Time m_failureStart;
        Time m_failureStop;
        uint16_t m_messageType;
        bool m_affectSend;
        bool m_affectReceive;
    };

    /** Metadata propagated as a packet tag for transport and switch observability. */
    struct ConsensusTransportMetadata {
        static constexpr uint8_t USE_TRANSPORT_DEFAULT = 255;

        uint16_t messageType{0};
        uint64_t flowId{0};
        uint8_t priorityGroup{USE_TRANSPORT_DEFAULT};

        static ConsensusTransportMetadata FromMessage(const ConsensusMessage& message, uint8_t priorityGroup = USE_TRANSPORT_DEFAULT);
    };

    /**
     * \ingroup consensus
     * Protocol-independent complete-message transport interface.
     */
    class ConsensusTransport : public Object {
        public:
        static TypeId GetTypeId();

        using ReceiveCallback = Callback<void, uint32_t, Ptr<Packet>>;

        ConsensusTransport();

        virtual void Start(Ptr<Node> node) = 0;
        virtual void Stop() = 0;
        virtual void Send(uint32_t peerId, Ptr<Packet> packet, const ConsensusTransportMetadata& metadata) = 0;

        void SetLocalEndpoint(uint32_t localId, uint16_t localPort);
        void SetLocalEndpoint(uint32_t localId, const Address& address);
        void SetPeerAddress(uint32_t peerId, const Address& address);
        void SetReceiveCallback(ReceiveCallback callback);
        void SetFaultModel(Ptr<ConsensusFaultModel> faultModel);

        protected:
        void Deliver(uint32_t peerId, Ptr<Packet> packet);
        ConsensusFaultDecision EvaluateFault(uint32_t peerId, Ptr<const Packet> packet, ConsensusFaultDirection direction);
        ConsensusTransportMetadata AttachMetadataTag(Ptr<Packet> packet, const ConsensusTransportMetadata& metadata,
                                                     uint8_t defaultPriorityGroup) const;

        uint32_t m_localId;
        uint16_t m_localPort;
        Address m_localAddress;
        std::map<uint32_t, Address> m_peerAddresses;

        private:
        void DoDeliver(uint32_t peerId, Ptr<Packet> packet);

        ReceiveCallback m_receiveCallback;
        Ptr<UniformRandomVariable> m_receiveDropRandom;
        double m_receiveDropProbability;
        Time m_receiveDelay;
        Ptr<ConsensusFaultModel> m_faultModel;
    };

    /**
     * \ingroup consensus
     * TCP transport with explicit consensus-message framing.
     */
    class TcpConsensusTransport : public ConsensusTransport {
        public:
        static TypeId GetTypeId();

        TcpConsensusTransport();
        ~TcpConsensusTransport() override;

        void Start(Ptr<Node> node) override;
        void Stop() override;
        void Send(uint32_t peerId, Ptr<Packet> packet, const ConsensusTransportMetadata& metadata) override;

        protected:
        void DoDispose() override;

        private:
        struct OutgoingConnection {
            Ptr<Socket> socket;
            bool connected{false};
            std::deque<Ptr<Packet>> pending;
            EventId retryEvent;
        };

        struct IncomingConnection {
            Ptr<Socket> socket;
            Ptr<Packet> buffer;
        };

        void ConnectPeer(uint32_t peerId);
        void SendNow(uint32_t peerId, Ptr<Packet> packet);
        void RetryPeer(uint32_t peerId);
        void FlushPeer(uint32_t peerId);
        uint32_t FindOutgoingPeer(Ptr<Socket> socket) const;
        IncomingConnection* FindIncoming(Ptr<Socket> socket);

        void HandleConnectSucceeded(Ptr<Socket> socket);
        void HandleConnectFailed(Ptr<Socket> socket);
        void HandleSendReady(Ptr<Socket> socket, uint32_t availableBytes);
        void HandleAccept(Ptr<Socket> socket, const Address& from);
        void HandleRead(Ptr<Socket> socket);
        void HandlePeerClose(Ptr<Socket> socket);
        void HandlePeerError(Ptr<Socket> socket);

        Ptr<Node> m_node;
        Ptr<Socket> m_listenSocket;
        std::map<uint32_t, OutgoingConnection> m_outgoing;
        std::vector<IncomingConnection> m_incoming;
        Time m_connectRetry;
        bool m_running;
    };

    /**
     * \ingroup consensus
     * RDMA transport backed by the repository's RdmaHw queue pairs.
     *
     * Every consensus message is one RDMA transaction. RdmaHw delivers accepted
     * in-order fragments and this adapter invokes the application callback only
     * after the complete ConsensusHeader-delimited message has been reassembled.
     */
    class RdmaConsensusTransport : public ConsensusTransport {
        public:
        static TypeId GetTypeId();

        RdmaConsensusTransport();

        void Start(Ptr<Node> node) override;
        void Stop() override;
        void Send(uint32_t peerId, Ptr<Packet> packet, const ConsensusTransportMetadata& metadata) override;

        protected:
        void DoDispose() override;

        private:
        using FlowKey = std::tuple<uint32_t, uint16_t, uint16_t>;

        void HandleFragment(uint32_t sourceIp, uint16_t sourcePort, uint16_t priorityGroup, uint32_t sequence, Ptr<Packet> fragment);
        void HandleSendComplete(uint16_t sourcePort);
        uint16_t AllocateSourcePort();
        void SendNow(uint32_t peerId, Ptr<Packet> packet, ConsensusTransportMetadata metadata);

        Ptr<Node> m_node;
        Ptr<RdmaDriver> m_driver;
        std::map<FlowKey, Ptr<Packet>> m_reassembly;
        std::map<uint16_t, uint32_t> m_sourcePortPeers;
        uint16_t m_priorityGroup;
        uint16_t m_nextSourcePort;
        uint32_t m_window;
        uint64_t m_baseRttNs;
        bool m_running;
    };

} // namespace ns3

#endif // CONSENSUS_TRANSPORT_H
