#include "ns3/boolean.h"
#include "ns3/bidl-application.h"
#include "ns3/consensus-application.h"
#include "ns3/consensus-experiment.h"
#include "ns3/consensus-message.h"
#include "ns3/consensus-runner.h"
#include "ns3/consensus-transport.h"
#include "ns3/consensus-workload.h"
#include "ns3/ezbft-engine.h"
#include "ns3/double.h"
#include "ns3/enum.h"
#include "ns3/inet-socket-address.h"
#include "ns3/ipv4-address.h"
#include "ns3/pbft-engine.h"
#include "ns3/qbb-net-device.h"
#include "ns3/rdma-driver.h"
#include "ns3/simulator.h"
#include "ns3/switch-node.h"
#include "ns3/zyzzyva-engine.h"
#include "ns3/zyzzyva-auth.h"
#include "ns3/test.h"
#include "ns3/uinteger.h"

#include <cstdio>
#include <fstream>
#include <map>
#include <vector>

using namespace ns3;

class RecordingConsensusTransport : public ConsensusTransport {
    public:
    void Start(Ptr<Node> node) override { (void)node; }

    void Stop() override {}

    void Send(uint32_t peerId, Ptr<Packet> packet, const ConsensusTransportMetadata& metadata) override {
        peers.push_back(peerId);
        packets.push_back(packet->Copy());
        metadataRecords.push_back(metadata);
    }

    void Inject(uint32_t peerId, Ptr<Packet> packet) { Deliver(peerId, packet); }

    std::vector<uint32_t> peers;
    std::vector<Ptr<Packet>> packets;
    std::vector<ConsensusTransportMetadata> metadataRecords;
};

class ClientExternalSubmissionTestCase : public TestCase {
    public:
    ClientExternalSubmissionTestCase() : TestCase("Consensus client accepts external transactions without auto generation") {}

    private:
    void Submit(Ptr<ConsensusClientApplication> client) { m_requestId = client->SubmitRequest(2048, 3); }

    void DoRun() override {
        Ptr<Node> node = CreateObject<Node>();
        Ptr<RecordingConsensusTransport> transport = CreateObject<RecordingConsensusTransport>();
        Ptr<ConsensusClientApplication> client = CreateObject<ConsensusClientApplication>();
        client->SetTransport(transport);
        client->SetReplicaIds({0});
        client->SetAttribute("PrimaryId", UintegerValue(0));
        client->SetAttribute("AutoGenerate", BooleanValue(false));
        client->SetAttribute("RequestTimeout", TimeValue(MilliSeconds(100)));
        node->AddApplication(client);
        client->SetStartTime(Seconds(0));
        client->SetStopTime(MilliSeconds(10));
        Simulator::Schedule(MilliSeconds(1), &ClientExternalSubmissionTestCase::Submit, this, client);
        Simulator::Stop(MilliSeconds(10));
        Simulator::Run();

        NS_TEST_ASSERT_MSG_EQ(m_requestId, 1, "External request was not accepted");
        NS_TEST_ASSERT_MSG_EQ(transport->packets.size(), 1, "Auto-generated or duplicate request was sent");
        NS_TEST_ASSERT_MSG_EQ(transport->metadataRecords.size(), 1, "Transport metadata was not propagated");
        NS_TEST_ASSERT_MSG_EQ(transport->metadataRecords.front().messageType, static_cast<uint16_t>(ConsensusMessageType::CLIENT_REQUEST),
                              "Transport message type is incorrect");
        const uint64_t expectedFlowId = (1000ULL << 32) ^ 1ULL;
        NS_TEST_ASSERT_MSG_EQ(transport->metadataRecords.front().flowId, expectedFlowId,
                              "Transport flow id is not stable for the first client request");
        ConsensusMessage request;
        NS_TEST_ASSERT_MSG_EQ(ConsensusMessage::FromPacket(transport->packets.front(), request), true,
                              "External request packet is invalid");
        NS_TEST_ASSERT_MSG_EQ(request.payloadSize, 2048, "External transaction size was not preserved");
        NS_TEST_ASSERT_MSG_EQ(request.transactionCount, 3, "External transaction count was not preserved");
        Simulator::Destroy();
    }

    uint64_t m_requestId{0};
};

class BidlSequencerDeduplicationTestCase : public TestCase {
    public:
    BidlSequencerDeduplicationTestCase() : TestCase("BIDL sequencer assigns one sequence to a retransmitted client request") {}

    private:
    void InjectRequest(Ptr<RecordingConsensusTransport> transport) {
        ConsensusMessage request;
        request.protocol = ConsensusProtocol::GENERIC;
        request.type = ConsensusMessageType::CLIENT_REQUEST;
        request.senderId = 1000;
        request.receiverId = 200;
        request.payloadSize = 1024;
        request.digest = 0x12345678;
        request.clientId = 1000;
        request.requestId = 1;
        request.transactionCount = 1;
        transport->Inject(1000, request.ToPacket());
    }

    void DoRun() override {
        Ptr<Node> node = CreateObject<Node>();
        Ptr<RecordingConsensusTransport> transport = CreateObject<RecordingConsensusTransport>();
        Ptr<BidlApplication> sequencer = CreateObject<BidlApplication>();
        sequencer->SetNodeId(200);
        sequencer->SetRole(BIDL_SEQUENCER);
        sequencer->SetOrdererIds({0, 1, 2, 3});
        sequencer->SetExecutorIds({100, 101, 102, 103});
        sequencer->SetSequencerIds({200});
        sequencer->SetTransport(transport);
        node->AddApplication(sequencer);
        sequencer->SetStartTime(Seconds(0));
        sequencer->SetStopTime(MilliSeconds(10));

        Simulator::Schedule(MilliSeconds(1), &BidlSequencerDeduplicationTestCase::InjectRequest, this, transport);
        Simulator::Schedule(MilliSeconds(2), &BidlSequencerDeduplicationTestCase::InjectRequest, this, transport);
        Simulator::Stop(MilliSeconds(10));
        Simulator::Run();

        NS_TEST_ASSERT_MSG_EQ(transport->packets.size(), 8, "Retransmitted request was sequenced and broadcast twice");
        for (const Ptr<Packet>& packet : transport->packets) {
            ConsensusMessage transaction;
            NS_TEST_ASSERT_MSG_EQ(ConsensusMessage::FromPacket(packet, transaction), true, "Sequenced transaction is invalid");
            NS_TEST_ASSERT_MSG_EQ(transaction.instanceId, 1, "Retransmission received a new transaction sequence");
        }
        Simulator::Destroy();
    }
};

class BidlMaliciousFirstArrivalTestCase : public TestCase {
    public:
    BidlMaliciousFirstArrivalTestCase()
        : TestCase("BIDL executor keeps the first digest and reexecutes only after consensus mismatch") {}

    private:
    void InjectTransaction(Ptr<RecordingConsensusTransport> transport, uint32_t sender, uint64_t digest) {
        ConsensusMessage message;
        message.protocol = ConsensusProtocol::GENERIC;
        message.type = ConsensusMessageType::BIDL_TRANSACTION;
        message.senderId = sender;
        message.receiverId = 100;
        message.instanceId = 1;
        message.payloadSize = 1024;
        message.digest = digest;
        message.clientId = 1000;
        message.requestId = 1;
        message.transactionCount = 1;
        transport->Inject(sender, message.ToPacket());
    }

    void InjectCertificate(Ptr<RecordingConsensusTransport> transport, ConsensusMessageType type, uint32_t sender,
                           uint64_t digest) {
        ConsensusMessage message;
        message.protocol = ConsensusProtocol::GENERIC;
        message.type = type;
        message.senderId = sender;
        message.receiverId = 100;
        message.instanceId = 1;
        message.digest = digest;
        message.transactionCount = 1;
        if (type == ConsensusMessageType::BIDL_ORDERED_BLOCK) {
            BidlTransactionRecord record;
            record.sequence = 1;
            record.request = ClientRequest{1000, 1, 1024, 1, 0x12345678ULL};
            record.ingressTimeNs = MilliSeconds(2).GetNanoSeconds();
            message.payloadBytes = BidlApplication::EncodeTransactions({record});
        }
        transport->Inject(sender, message.ToPacket());
    }

    void OnConflict(uint32_t, uint64_t, uint32_t) { ++m_conflicts; }
    void OnReexecution(uint32_t, uint64_t, uint32_t, Time) { ++m_reexecutions; }

    void DoRun() override {
        for (bool compact : {false, true}) {
            m_conflicts = m_reexecutions = 0;
            RunCase(compact);
        }
    }

    void RunCase(bool compact) {
        constexpr uint64_t legitimateDigest = 0x12345678ULL;
        constexpr uint64_t forgedDigest = 0x87654321ULL;
        const uint64_t canonicalBlockDigest = legitimateDigest + 0x9e3779b97f4a7c15ULL;
        const uint64_t resultSeed = 0x6a09e667f3bcc909ULL;
        const uint64_t canonicalResult = resultSeed ^
            (legitimateDigest + 0x9e3779b97f4a7c15ULL + (resultSeed << 6) + (resultSeed >> 2));

        Ptr<Node> node = CreateObject<Node>();
        Ptr<RecordingConsensusTransport> transport = CreateObject<RecordingConsensusTransport>();
        Ptr<BidlApplication> executor = CreateObject<BidlApplication>();
        executor->SetNodeId(100);
        executor->SetRole(BIDL_EXECUTOR);
        executor->SetOrdererIds({0, 1, 2, 3});
        executor->SetExecutorIds({100, 101, 102, 103});
        executor->SetSequencerIds({200});
        executor->SetBroadcasterIds({300});
        executor->SetTransport(transport);
        executor->SetAttribute("CompactOrderNotifications", BooleanValue(compact));
        executor->TraceConnectWithoutContext("ConflictRejected", MakeCallback(&BidlMaliciousFirstArrivalTestCase::OnConflict, this));
        executor->TraceConnectWithoutContext("BlockReexecution",
                                             MakeCallback(&BidlMaliciousFirstArrivalTestCase::OnReexecution, this));
        node->AddApplication(executor);
        executor->SetStartTime(Seconds(0));
        executor->SetStopTime(MilliSeconds(10));

        Simulator::Schedule(MilliSeconds(1), &BidlMaliciousFirstArrivalTestCase::InjectTransaction, this, transport, 300,
                            forgedDigest);
        Simulator::Schedule(MilliSeconds(2), &BidlMaliciousFirstArrivalTestCase::InjectTransaction, this, transport, 200,
                            legitimateDigest);
        if (compact) {
            for (uint32_t sender = 0; sender < 3; ++sender) {
                Simulator::Schedule(MicroSeconds(2500), [transport, sender, canonicalBlockDigest]() {
                    ConsensusMessage summary;
                    summary.type = ConsensusMessageType::BIDL_ORDERED_BLOCK;
                    summary.senderId = sender; summary.receiverId = 100;
                    summary.instanceId = 1; summary.digest = canonicalBlockDigest;
                    summary.transactionCount = 1; summary.payloadSize = 32;
                    transport->Inject(sender, summary.ToPacket());
                });
            }
        }
        for (uint32_t sender = 0; sender < 3; ++sender) {
            Simulator::Schedule(MilliSeconds(compact ? 4 : 3), &BidlMaliciousFirstArrivalTestCase::InjectCertificate, this, transport,
                                ConsensusMessageType::BIDL_ORDERED_BLOCK, sender, canonicalBlockDigest);
        }
        auto injectPersist = [transport, canonicalBlockDigest, canonicalResult](uint32_t sender, bool differentResult, Time at) {
            std::vector<uint8_t> resultVector{0, 0, 0, 4};
            uint64_t resultVectorDigest = 0x510e527fade682d1ULL;
            for (uint32_t executorId : {100u, 101u, 102u, 103u}) {
                const uint64_t result = canonicalResult + (differentResult && executorId == 103 ? 1 : 0);
                resultVector.push_back(static_cast<uint8_t>(executorId >> 24));
                resultVector.push_back(static_cast<uint8_t>(executorId >> 16));
                resultVector.push_back(static_cast<uint8_t>(executorId >> 8));
                resultVector.push_back(static_cast<uint8_t>(executorId));
                for (int shift = 56; shift >= 0; shift -= 8) resultVector.push_back(static_cast<uint8_t>(result >> shift));
                resultVectorDigest ^= executorId + 0x9e3779b97f4a7c15ULL + (resultVectorDigest << 6) + (resultVectorDigest >> 2);
                resultVectorDigest ^= result + 0x9e3779b97f4a7c15ULL + (resultVectorDigest << 6) + (resultVectorDigest >> 2);
            }
            if (resultVectorDigest == 0) resultVectorDigest = 1;
            ConsensusMessage persist;
            persist.type = ConsensusMessageType::BIDL_RESULT_PERSIST;
            persist.senderId = sender; persist.receiverId = 100; persist.instanceId = 1;
            persist.digest = canonicalBlockDigest; persist.requestId = resultVectorDigest; persist.transactionCount = 1;
            persist.payloadBytes = resultVector;
            Simulator::Schedule(at, [transport, sender, persist]() {
                transport->Inject(sender, persist.ToPacket());
            });
        };
        injectPersist(0, false, MicroSeconds(3005));
        injectPersist(1, false, MicroSeconds(3005));
        injectPersist(2, true, MicroSeconds(3005));
        Simulator::Schedule(MilliSeconds(5), [this, executor]() {
            NS_TEST_ASSERT_MSG_EQ(executor->GetCommittedBlocks(), 0,
                                  "Two matching and one conflicting Persist must not form a quorum");
        });
        injectPersist(3, false, MilliSeconds(6));
        Simulator::Stop(MilliSeconds(30));
        Simulator::Run();

        NS_TEST_ASSERT_MSG_EQ(m_conflicts, 1, "The second different digest was not rejected");
        NS_TEST_ASSERT_MSG_EQ(m_reexecutions, 1, "Consensus mismatch did not trigger exactly one block reexecution");
        NS_TEST_ASSERT_MSG_EQ(executor->GetCommittedBlocks(), 1, "Buffered persist messages did not commit after reexecution");
        uint32_t resultMessages = 0;
        for (const auto& packet : transport->packets) {
            ConsensusMessage result;
            if (!ConsensusMessage::FromPacket(packet, result) || result.type != ConsensusMessageType::BIDL_EXECUTION_RESULT) continue;
            uint64_t actual = 0;
            for (uint8_t byte : result.payloadBytes) actual = (actual << 8) | byte;
            NS_TEST_ASSERT_MSG_EQ(actual, canonicalResult,
                                  "Re-execution still used the forged transaction rather than decided contents");
            ++resultMessages;
        }
        NS_TEST_ASSERT_MSG_EQ(resultMessages, 4, "Canonical results were not sent to every orderer");
        if (compact) {
            uint32_t canonicalRequests = 0;
            for (const auto& packet : transport->packets) {
                ConsensusMessage request;
                if (ConsensusMessage::FromPacket(packet, request) && request.type == ConsensusMessageType::BIDL_BLOCK_REQUEST)
                    ++canonicalRequests;
            }
            NS_TEST_ASSERT_MSG_GT(canonicalRequests, 0, "A real digest mismatch did not fetch canonical contents");
        }
        Simulator::Destroy();
    }

    uint32_t m_conflicts{0};
    uint32_t m_reexecutions{0};
};

class ConsensusMeasurementWindowTestCase : public TestCase {
    public:
    ConsensusMeasurementWindowTestCase() : TestCase("Consensus throughput excludes requests completed after the measurement window") {}

    private:
    void Submit(Ptr<ConsensusClientApplication> client, uint32_t transactions) { client->SubmitRequest(1024, transactions); }

    void Reply(Ptr<RecordingConsensusTransport> transport, uint32_t index) {
        ConsensusMessage request;
        NS_TEST_ASSERT_MSG_EQ(ConsensusMessage::FromPacket(transport->packets.at(index), request), true, "Client request is invalid");
        ConsensusMessage reply;
        reply.protocol = ConsensusProtocol::GENERIC;
        reply.type = ConsensusMessageType::CLIENT_REPLY;
        reply.senderId = 0;
        reply.receiverId = request.clientId;
        reply.digest = request.digest;
        reply.clientId = request.clientId;
        reply.requestId = request.requestId;
        reply.transactionCount = request.transactionCount;
        transport->Inject(0, reply.ToPacket());
    }

    void DoRun() override {
        Ptr<Node> node = CreateObject<Node>();
        Ptr<RecordingConsensusTransport> transport = CreateObject<RecordingConsensusTransport>();
        Ptr<ConsensusClientApplication> client = CreateObject<ConsensusClientApplication>();
        client->SetTransport(transport);
        client->SetReplicaIds({0});
        client->SetAttribute("ReplyQuorum", UintegerValue(1));
        client->SetAttribute("AutoGenerate", BooleanValue(false));
        client->SetAttribute("RequestTimeout", TimeValue(MilliSeconds(10)));
        node->AddApplication(client);
        client->SetStartTime(Seconds(0));
        client->SetStopTime(MilliSeconds(4));

        Ptr<ConsensusStatistics> statistics = CreateObject<ConsensusStatistics>();
        statistics->SetMeasurementWindow(Seconds(0), MilliSeconds(1));
        statistics->AttachClient(client);
        Simulator::Schedule(MicroSeconds(100), &ConsensusMeasurementWindowTestCase::Submit, this, client, 3);
        Simulator::Schedule(MicroSeconds(200), &ConsensusMeasurementWindowTestCase::Submit, this, client, 5);
        Simulator::Schedule(MicroSeconds(500), &ConsensusMeasurementWindowTestCase::Reply, this, transport, 0);
        Simulator::Schedule(MilliSeconds(2), &ConsensusMeasurementWindowTestCase::Reply, this, transport, 1);
        Simulator::Stop(MilliSeconds(4));
        Simulator::Run();

        NS_TEST_ASSERT_MSG_EQ(statistics->GetCompletedRequests(), 2, "Cooldown completion was not processed");
        NS_TEST_ASSERT_MSG_EQ(statistics->GetCompletedTransactions(), 8, "Multiple transactions per request were lost");
        NS_TEST_ASSERT_MSG_EQ(statistics->GetMeasuredRequests(), 1, "Cooldown completion inflated the window request count");
        NS_TEST_ASSERT_MSG_EQ(statistics->GetRequestThroughput(), 1000.0, "Cooldown completion inflated window request throughput");
        NS_TEST_ASSERT_MSG_EQ(statistics->GetWindowTransactionThroughput(), 3000.0, "Window throughput used cooldown completions or request counts instead of transactions");
        NS_TEST_ASSERT_MSG_EQ(statistics->GetLatencyPercentile(95), 1800.0, "Latency discarded the slow completion from the submission cohort");
        Simulator::Destroy();
    }
};

class ConsensusTransportImpairmentTestCase : public TestCase {
    public:
    ConsensusTransportImpairmentTestCase() : TestCase("Consensus transport applies complete-message delay and loss") {}

    private:
    void Receive(uint32_t peerId, Ptr<Packet> packet) {
        (void)peerId;
        (void)packet;
        ++m_deliveries;
        m_deliveryTime = Simulator::Now();
    }

    void DoRun() override {
        Ptr<RecordingConsensusTransport> delayed = CreateObject<RecordingConsensusTransport>();
        delayed->SetAttribute("ReceiveDelay", TimeValue(MicroSeconds(100)));
        delayed->SetReceiveCallback(MakeCallback(&ConsensusTransportImpairmentTestCase::Receive, this));
        delayed->Inject(1, Create<Packet>(64));
        Simulator::Run();
        NS_TEST_ASSERT_MSG_EQ(m_deliveries, 1, "Delayed message was not delivered");
        NS_TEST_ASSERT_MSG_EQ(m_deliveryTime, MicroSeconds(100), "Configured message delay was not applied");
        Simulator::Destroy();

        Ptr<RecordingConsensusTransport> dropped = CreateObject<RecordingConsensusTransport>();
        dropped->SetAttribute("ReceiveDropProbability", DoubleValue(1.0));
        dropped->SetReceiveCallback(MakeCallback(&ConsensusTransportImpairmentTestCase::Receive, this));
        dropped->Inject(1, Create<Packet>(64));
        Simulator::Run();
        NS_TEST_ASSERT_MSG_EQ(m_deliveries, 1, "A probability-one drop was delivered");
        Simulator::Destroy();

        Ptr<RecordingConsensusTransport> isolated = CreateObject<RecordingConsensusTransport>();
        Ptr<ConfigurableConsensusFaultModel> fault = CreateObject<ConfigurableConsensusFaultModel>();
        fault->SetAttribute("FailedNodeId", UintegerValue(1));
        isolated->SetFaultModel(fault);
        isolated->SetReceiveCallback(MakeCallback(&ConsensusTransportImpairmentTestCase::Receive, this));
        isolated->Inject(1, Create<Packet>(64));
        Simulator::Run();
        NS_TEST_ASSERT_MSG_EQ(m_deliveries, 1, "Isolated node traffic was delivered");
        Simulator::Destroy();
    }

    uint32_t m_deliveries{0};
    Time m_deliveryTime{Seconds(0)};
};

class RdmaConsensusTransportTestCase : public TestCase {
    public:
    RdmaConsensusTransportTestCase() : TestCase("RDMA transport reassembles one fragmented consensus message") {}

    private:
    void Receive(uint32_t peerId, Ptr<Packet> packet) {
        ConsensusMessage message;
        if (peerId == 1 && ConsensusMessage::FromPacket(packet, message)) {
            if (message.type == ConsensusMessageType::PRE_PREPARE) {
                ++m_deliveries;
                m_payloadSize = message.payloadSize;
            } else if (message.type == ConsensusMessageType::BIDL_TRANSACTION) {
                ++m_edgeDeliveries;
                m_edgeExtensionPresent = message.iorsPresent;
            }
        }
    }

    void Send(Ptr<RdmaConsensusTransport> transport) {
        ConsensusMessage message;
        message.protocol = ConsensusProtocol::PBFT;
        message.type = ConsensusMessageType::PRE_PREPARE;
        message.senderId = 1;
        message.receiverId = 2;
        message.instanceId = 1;
        message.messageId = 1;
        message.payloadSize = 4096;
        message.digest = 0x12345678;
        transport->Send(2, message.ToPacket(), ConsensusTransportMetadata::FromMessage(message));
    }

    void SendMarkedToLegacyEdge(Ptr<RdmaConsensusTransport> transport) {
        ConsensusMessage message;
        message.type = ConsensusMessageType::BIDL_TRANSACTION;
        message.senderId = 1;
        message.receiverId = 2;
        message.instanceId = 2;
        message.digest = 99;
        message.payloadSize = 16;
        message.iorsPresent = true;
        message.iorsMarked = true;
        transport->Send(2, message.ToPacket(), ConsensusTransportMetadata::FromMessage(message));
    }

    void DoRun() override {
        ConsensusExperimentConfig config;
        config.ApplyPreset("pbft-rdma");
        config.topology = "qbb-star";
        ConsensusTopology topology = config.BuildTopology(2);

        const Ipv4Address senderAddress = topology.endpointInterfaces.GetAddress(0);
        const Ipv4Address receiverAddress = topology.endpointInterfaces.GetAddress(1);
        Ptr<RdmaConsensusTransport> sender = CreateObject<RdmaConsensusTransport>();
        Ptr<RdmaConsensusTransport> receiver = CreateObject<RdmaConsensusTransport>();
        sender->SetLocalEndpoint(1, InetSocketAddress(senderAddress, 9200));
        receiver->SetLocalEndpoint(2, InetSocketAddress(receiverAddress, 9200));
        sender->SetPeerAddress(2, InetSocketAddress(receiverAddress, 9200));
        receiver->SetPeerAddress(1, InetSocketAddress(senderAddress, 9200));
        receiver->SetReceiveCallback(MakeCallback(&RdmaConsensusTransportTestCase::Receive, this));
        sender->Start(topology.endpoints.Get(0));
        receiver->Start(topology.endpoints.Get(1));

        Simulator::Schedule(MilliSeconds(1), &RdmaConsensusTransportTestCase::Send, this, sender);
        Simulator::Schedule(MilliSeconds(2), &RdmaConsensusTransportTestCase::SendMarkedToLegacyEdge, this, sender);
        Simulator::Stop(MilliSeconds(20));
        Simulator::Run();
        sender->Stop();
        receiver->Stop();

        NS_TEST_ASSERT_MSG_EQ(m_deliveries, 1, "Fragmented RDMA message was not delivered exactly once");
        NS_TEST_ASSERT_MSG_EQ(m_payloadSize, 4096, "RDMA reassembly changed the consensus payload size");
        NS_TEST_ASSERT_MSG_EQ(m_edgeDeliveries, 1, "The marked candidate did not reach the legacy edge");
        NS_TEST_ASSERT_MSG_EQ(m_edgeExtensionPresent, false, "The legacy edge exposed an IORS extension");
        Simulator::Destroy();
    }

    uint32_t m_deliveries{0};
    uint32_t m_payloadSize{0};
    uint32_t m_edgeDeliveries{0};
    bool m_edgeExtensionPresent{false};
};

class ConsensusMessageTestCase : public TestCase {
    public:
    ConsensusMessageTestCase() : TestCase("Consensus message survives header serialization") {}

    private:
    void DoRun() override {
        ConsensusMessage original;
        original.protocol = ConsensusProtocol::PBFT;
        original.type = ConsensusMessageType::PRE_PREPARE;
        original.senderId = 1;
        original.receiverId = 2;
        original.view = 3;
        original.instanceId = 4;
        original.messageId = 5;
        original.payloadSize = 4096;
        original.digest = 6;
        original.clientId = 7;
        original.requestId = 8;
        original.transactionCount = 9;

        ConsensusMessage decoded;
        NS_TEST_ASSERT_MSG_EQ(ConsensusMessage::FromPacket(original.ToPacket(), decoded), true, "Message failed to decode");
        NS_TEST_ASSERT_MSG_EQ(static_cast<uint32_t>(decoded.protocol), static_cast<uint32_t>(original.protocol), "Protocol changed");
        NS_TEST_ASSERT_MSG_EQ(static_cast<uint32_t>(decoded.type), static_cast<uint32_t>(original.type), "Message type changed");
        NS_TEST_ASSERT_MSG_EQ(decoded.senderId, original.senderId, "Sender changed");
        NS_TEST_ASSERT_MSG_EQ(decoded.receiverId, original.receiverId, "Receiver changed");
        NS_TEST_ASSERT_MSG_EQ(decoded.view, original.view, "View changed");
        NS_TEST_ASSERT_MSG_EQ(decoded.instanceId, original.instanceId, "Instance changed");
        NS_TEST_ASSERT_MSG_EQ(decoded.payloadSize, original.payloadSize, "Payload size changed");
        NS_TEST_ASSERT_MSG_EQ(decoded.digest, original.digest, "Digest changed");
        NS_TEST_ASSERT_MSG_EQ(decoded.clientId, original.clientId, "Client changed");
        NS_TEST_ASSERT_MSG_EQ(decoded.requestId, original.requestId, "Request changed");
    }
};

class ConsensusResourceTestCase : public TestCase {
    public:
    ConsensusResourceTestCase() : TestCase("Queued consensus resource serializes tasks on one CPU core") {}

    private:
    void RecordCompletion() { m_completionTimes.push_back(Simulator::Now()); }

    void DoRun() override {
        Ptr<ConsensusResourceModel> resource = CreateObject<ConsensusResourceModel>();
        resource->SetAttribute("Mode", EnumValue(RESOURCE_QUEUED));
        resource->SetAttribute("CpuCores", UintegerValue(1));

        for (uint32_t task = 0; task < 3; ++task) {
            resource->SubmitCpuTask(MicroSeconds(100), MakeCallback(&ConsensusResourceTestCase::RecordCompletion, this));
        }

        Simulator::Run();
        NS_TEST_ASSERT_MSG_EQ(m_completionTimes.size(), 3, "Not all CPU tasks completed");
        NS_TEST_ASSERT_MSG_EQ(m_completionTimes[0], MicroSeconds(100), "First task time is wrong");
        NS_TEST_ASSERT_MSG_EQ(m_completionTimes[1], MicroSeconds(200), "Second task time is wrong");
        NS_TEST_ASSERT_MSG_EQ(m_completionTimes[2], MicroSeconds(300), "Third task time is wrong");
        Simulator::Destroy();
    }

    std::vector<Time> m_completionTimes;
};

class PbftNormalPathTestCase : public TestCase {
    public:
    PbftNormalPathTestCase() : TestCase("N=4 f=1 PBFT normal path decides with 24 replica messages") {}

    private:
    void HandleSend(uint32_t from, uint32_t peerId, const ConsensusMessage& message) {
        (void)from;
        ++m_totalMessages;
        ++m_messagesByType[message.type];
        m_engines.at(peerId)->Receive(message);
    }

    void HandleDecision(uint32_t replicaId, const ConsensusDecision& decision) {
        ++m_decisions;
        m_decidedDigests[replicaId] = decision.digest;
    }

    void DoRun() override {
        const std::vector<uint32_t> replicaIds{0, 1, 2, 3};
        for (uint32_t replicaId : replicaIds) {
            Ptr<PbftEngine> engine = CreateObject<PbftEngine>();
            engine->SetAttribute("PrimaryId", UintegerValue(0));
            engine->SetAttribute("FaultTolerance", UintegerValue(1));
            engine->SetAttribute("MessageProcessingTime", TimeValue(MicroSeconds(0)));
            engine->Configure(replicaId, replicaIds);
            engine->SetResourceModel(CreateObject<ConsensusResourceModel>());
            engine->SetSendCallback(MakeCallback(&PbftNormalPathTestCase::HandleSend, this).Bind(replicaId));
            engine->SetDecisionCallback(MakeCallback(&PbftNormalPathTestCase::HandleDecision, this).Bind(replicaId));
            m_engines.emplace(replicaId, engine);
        }

        for (auto& [replicaId, engine] : m_engines) {
            (void)replicaId;
            engine->Start();
        }

        ClientRequest request;
        request.clientId = 1000;
        request.requestId = 1;
        request.payloadSize = 1024;
        request.transactionCount = 1;
        request.digest = 0x12345678;
        m_engines.at(0)->Submit(request);

        Simulator::Run();

        NS_TEST_ASSERT_MSG_EQ(m_totalMessages, 24, "Unexpected PBFT replica message count");
        NS_TEST_ASSERT_MSG_EQ(m_messagesByType[ConsensusMessageType::PRE_PREPARE], 3, "Expected three PRE-PREPARE messages");
        NS_TEST_ASSERT_MSG_EQ(m_messagesByType[ConsensusMessageType::PREPARE], 9, "Expected nine PREPARE messages");
        NS_TEST_ASSERT_MSG_EQ(m_messagesByType[ConsensusMessageType::COMMIT], 12, "Expected twelve COMMIT messages");
        NS_TEST_ASSERT_MSG_EQ(m_decisions, 4, "All replicas must decide");
        for (uint32_t replicaId : replicaIds) {
            NS_TEST_ASSERT_MSG_EQ(m_decidedDigests[replicaId], request.digest, "Replicas decided different digests");
        }

        for (auto& [replicaId, engine] : m_engines) {
            (void)replicaId;
            engine->Stop();
        }
        Simulator::Destroy();
    }

    std::map<uint32_t, Ptr<PbftEngine>> m_engines;
    std::map<ConsensusMessageType, uint32_t> m_messagesByType;
    std::map<uint32_t, uint64_t> m_decidedDigests;
    uint32_t m_totalMessages{0};
    uint32_t m_decisions{0};
};

class PbftViewChangeTestCase : public TestCase {
    public:
    PbftViewChangeTestCase() : TestCase("PBFT rotates to view one and completes a pending request") {}

    private:
    void HandleSend(uint32_t peerId, const ConsensusMessage& message) { m_engines.at(peerId)->Receive(message); }

    void HandleDecision(const ConsensusDecision& decision) {
        (void)decision;
        ++m_decisions;
    }

    void DoRun() override {
        const std::vector<uint32_t> replicaIds{0, 1, 2, 3};
        for (uint32_t replicaId : replicaIds) {
            Ptr<PbftEngine> engine = CreateObject<PbftEngine>();
            engine->SetAttribute("PrimaryId", UintegerValue(0));
            engine->SetAttribute("FaultTolerance", UintegerValue(1));
            engine->SetAttribute("MessageProcessingTime", TimeValue(MicroSeconds(0)));
            engine->SetAttribute("RetransmissionTimeout", TimeValue(MilliSeconds(1)));
            engine->SetAttribute("InstanceTimeout", TimeValue(MilliSeconds(5)));
            engine->Configure(replicaId, replicaIds);
            engine->SetResourceModel(CreateObject<ConsensusResourceModel>());
            engine->SetSendCallback(MakeCallback(&PbftViewChangeTestCase::HandleSend, this));
            engine->SetDecisionCallback(MakeCallback(&PbftViewChangeTestCase::HandleDecision, this));
            m_engines.emplace(replicaId, engine);
        }
        for (auto& [replicaId, engine] : m_engines) {
            (void)replicaId;
            engine->Start();
        }

        ClientRequest request;
        request.clientId = 1000;
        request.requestId = 9;
        request.payloadSize = 1024;
        request.transactionCount = 1;
        request.digest = 0xabcdef;
        m_engines.at(1)->Submit(request);
        Simulator::Stop(MilliSeconds(100));
        Simulator::Run();

        for (uint32_t replicaId : replicaIds) {
            NS_TEST_ASSERT_MSG_EQ(m_engines.at(replicaId)->GetCurrentView(), 1, "Replica did not enter view one");
            NS_TEST_ASSERT_MSG_EQ(m_engines.at(replicaId)->GetCurrentPrimary(), 1, "Replica selected the wrong new primary");
        }
        NS_TEST_ASSERT_MSG_EQ(m_decisions, 4, "The request was not decided after view change");

        for (auto& [replicaId, engine] : m_engines) {
            (void)replicaId;
            engine->Stop();
        }
        Simulator::Destroy();
    }

    std::map<uint32_t, Ptr<PbftEngine>> m_engines;
    uint32_t m_decisions{0};
};

class ClientRetransmissionTestCase : public TestCase {
    public:
    ClientRetransmissionTestCase() : TestCase("Consensus client retransmits to successive replicas") {}

    private:
    void DoRun() override {
        Ptr<Node> node = CreateObject<Node>();
        Ptr<RecordingConsensusTransport> transport = CreateObject<RecordingConsensusTransport>();
        Ptr<ConsensusClientApplication> client = CreateObject<ConsensusClientApplication>();
        client->SetTransport(transport);
        client->SetReplicaIds({0, 1, 2, 3});
        client->SetAttribute("PrimaryId", UintegerValue(0));
        client->SetAttribute("MaxRequests", UintegerValue(1));
        client->SetAttribute("InitialDelay", TimeValue(MicroSeconds(0)));
        client->SetAttribute("RequestTimeout", TimeValue(MilliSeconds(1)));
        client->SetAttribute("MaxRetransmissions", UintegerValue(2));
        node->AddApplication(client);
        client->SetStartTime(Seconds(0));
        client->SetStopTime(MilliSeconds(10));

        Simulator::Run();
        NS_TEST_ASSERT_MSG_EQ(transport->peers.size(), 3, "Unexpected send count");
        NS_TEST_ASSERT_MSG_EQ(transport->peers[0], 0, "Initial request missed primary");
        NS_TEST_ASSERT_MSG_EQ(transport->peers[1], 1, "First retry used wrong replica");
        NS_TEST_ASSERT_MSG_EQ(transport->peers[2], 2, "Second retry used wrong replica");
        Simulator::Destroy();
    }
};

class ConsensusExperimentConfigTestCase : public TestCase {
    public:
    ConsensusExperimentConfigTestCase() : TestCase("Consensus experiment builds a configurable leaf-spine topology") {}

    private:
    void DoRun() override {
        ConsensusExperimentConfig config;
        config.topology = "leaf-spine";
        config.queueDisc = "none";
        config.Validate();
        ConsensusTopology topology = config.BuildTcpTopology(5);

        NS_TEST_ASSERT_MSG_EQ(topology.endpoints.GetN(), 5, "Endpoint count changed");
        NS_TEST_ASSERT_MSG_EQ(topology.endpointInterfaces.GetN(), 5, "Endpoint address count changed");
        NS_TEST_ASSERT_MSG_EQ(topology.devices.GetN(), 14, "Leaf-spine link device count is wrong");
        Simulator::Destroy();
    }
};

class ConsensusConfigurationPrecedenceTestCase : public TestCase {
    public:
    ConsensusConfigurationPrecedenceTestCase() : TestCase("Consensus preset, config file, and CLI precedence is deterministic") {}

    private:
    void DoRun() override {
        const std::string path = "/tmp/ns3-consensus-experiment.conf";
        {
            std::ofstream output(path);
            output << "preset=pbft-rdma\n"
                   << "topology=qbb-star\n"
                   << "requestCount=7\n";
        }

        std::string argument0 = "consensus-test";
        std::string argument1 = "--configFile=" + path;
        std::string argument2 = "--requestCount=9";
        char* arguments[] = {argument0.data(), argument1.data(), argument2.data()};

        ConsensusExperimentConfig config;
        config.PrepareFromCommandLine(3, arguments);
        CommandLine command;
        config.AddCommandLineOptions(command);
        command.Parse(3, arguments);

        NS_TEST_ASSERT_MSG_EQ(config.preset, "pbft-rdma", "File preset was not applied");
        NS_TEST_ASSERT_MSG_EQ(config.architecture, "replicated", "Preset architecture was not applied");
        NS_TEST_ASSERT_MSG_EQ(config.transport, "rdma", "Preset transport was not applied");
        NS_TEST_ASSERT_MSG_EQ(config.topology, "qbb-star", "Config file did not override preset");
        NS_TEST_ASSERT_MSG_EQ(config.requestCount, 9, "Command line did not override config file");
        std::remove(path.c_str());
    }
};

class ConsensusConfigurationValidationTestCase : public TestCase {
    public:
    ConsensusConfigurationValidationTestCase() : TestCase("Consensus configuration validates built-in components") {}

    private:
    void DoRun() override {
        ConsensusExperimentConfig config;
        NS_TEST_ASSERT_MSG_EQ(config.GetEngineType(), "ns3::PbftEngine", "PBFT engine alias was not resolved");
        NS_TEST_ASSERT_MSG_EQ(config.GetTransportType(), "ns3::TcpConsensusTransport", "TCP transport alias was not resolved");

        ConsensusExperimentConfig incompatible;
        incompatible.transport = "rdma";
        incompatible.topology = "csma";
        bool rejected = false;
        try {
            incompatible.Validate();
        } catch (const std::invalid_argument&) {
            rejected = true;
        }
        NS_TEST_ASSERT_MSG_EQ(rejected, true, "Incompatible transport and topology were accepted");
    }
};

class ConsensusRdmaTopologyTestCase : public TestCase {
    public:
    ConsensusRdmaTopologyTestCase() : TestCase("Consensus experiment builds a multi-host Qbb RDMA topology") {}

    private:
    void DoRun() override {
        ConsensusExperimentConfig config;
        config.transport = "rdma";
        config.topology = "qbb-star";
        config.Validate();
        ConsensusTopology topology = config.BuildTopology(5);

        NS_TEST_ASSERT_MSG_EQ(topology.endpoints.GetN(), 5, "Endpoint count changed");
        NS_TEST_ASSERT_MSG_EQ(topology.switches.GetN(), 1, "Qbb switch was not created");
        NS_TEST_ASSERT_MSG_EQ(topology.devices.GetN(), 10, "Qbb link device count is wrong");
        for (uint32_t index = 0; index < topology.endpoints.GetN(); ++index) {
            NS_TEST_ASSERT_MSG_NE(topology.endpoints.Get(index)->GetObject<RdmaDriver>(), nullptr,
                                  "RDMA driver is missing from an endpoint");
        }
        Simulator::Destroy();
    }
};

class ConsensusRdmaLeafSpineTestCase : public TestCase {
    public:
    ConsensusRdmaLeafSpineTestCase() : TestCase("Consensus experiment builds a multi-level Qbb leaf-spine topology") {}

    private:
    void DoRun() override {
        ConsensusExperimentConfig config;
        config.transport = "rdma";
        config.topology = "qbb-leaf-spine";
        config.leafCount = 2;
        config.spineCount = 2;
        config.Validate();
        ConsensusTopology topology = config.BuildTopology(5);

        NS_TEST_ASSERT_MSG_EQ(topology.endpoints.GetN(), 5, "Endpoint count changed");
        NS_TEST_ASSERT_MSG_EQ(topology.switches.GetN(), 4, "Leaf or spine switch is missing");
        NS_TEST_ASSERT_MSG_EQ(topology.devices.GetN(), 18, "Qbb leaf-spine device count is wrong");
        Simulator::Destroy();
    }
};

class ConsensusLegacyReplayTestCase : public TestCase {
    public:
    ConsensusLegacyReplayTestCase() : TestCase("Consensus experiment parses legacy topology and replays app_id=0 RDMA flow") {}

    private:
    void FlowCompleted(uint32_t receiver, uint32_t sender, uint64_t flowId, uint32_t bytes, Time fct) {
        (void)receiver;
        (void)sender;
        (void)flowId;
        NS_TEST_ASSERT_MSG_EQ(bytes, 4096, "Legacy flow byte count changed");
        NS_TEST_ASSERT_MSG_GT(fct.GetNanoSeconds(), 0, "Legacy flow has no measured FCT");
        ++m_completed;
    }

    void DoRun() override {
        const std::string topologyPath = "/tmp/ns3-consensus-legacy-topology.txt";
        const std::string flowPath = "/tmp/ns3-consensus-legacy-flow.txt";
        {
            std::ofstream topology(topologyPath);
            topology << "3 1 2\n0\n1 0 10Gbps 2us 0\n2 0 25Gbps 3us 0\n";
        }
        {
            std::ofstream flows(flowPath);
            flows << "2\n1 2 4 19001 4096 100000 0\n2 1 4 19002 1024 100000 1\n";
        }

        LegacyTopologySpec parsedTopology = ReadLegacyTopologyFile(topologyPath);
        std::vector<LegacyFlowRecord> parsedFlows = ReadLegacyFlowFile(flowPath);
        NS_TEST_ASSERT_MSG_EQ(parsedTopology.nodeCount, 3, "Legacy node count changed");
        NS_TEST_ASSERT_MSG_EQ(parsedTopology.links.size(), 2, "Legacy link count changed");
        NS_TEST_ASSERT_MSG_EQ(parsedFlows.size(), 2, "Legacy flow count changed");

        ConsensusExperimentConfig config;
        config.transport = "rdma";
        config.topology = "qbb-file";
        config.topologyFile = topologyPath;
        config.flowFile = flowPath;
        config.rdmaHasWindow = true;
        config.rdmaGlobalRtt = false;
        config.rdmaTargetUtilization = 0.87;
        config.rdmaEwmaGain = 0.125;
        config.rdmaMinRate = "777Mb/s";
        config.qbbQcnEnabled = false;
        config.qbbPfcEnabled = false;
        config.qbbDynamicThreshold = false;
        config.qbbPauseTime = 9;
        config.ecnKminMap = "2 10000000000 1000 25000000000 2000";
        config.ecnKmaxMap = "2 10000000000 2000 25000000000 4000";
        config.ecnPmaxMap = "2 10000000000 0.1 25000000000 0.2";
        config.Validate();
        ConsensusTopology topology = config.BuildTopology(2);
        NS_TEST_ASSERT_MSG_EQ(topology.endpoints.GetN(), 2, "Legacy host count changed");
        NS_TEST_ASSERT_MSG_EQ(topology.switches.GetN(), 1, "Legacy switch count changed");
        NS_TEST_ASSERT_MSG_EQ(topology.devices.GetN(), 4, "Legacy device count changed");
        NS_TEST_ASSERT_MSG_GT(topology.maxRttNs, 0, "Legacy RTT was not derived");
        NS_TEST_ASSERT_MSG_GT(topology.maxBdpBytes, 0, "Legacy BDP was not derived");
        ConsensusExperimentConfig pbftPlacementConfig = config;
        pbftPlacementConfig.replicaCount = 1;
        pbftPlacementConfig.faultTolerance = 0;
        ConsensusRolePlacement pbftPlacement = pbftPlacementConfig.ResolvePbftPlacement(topology);
        NS_TEST_ASSERT_MSG_EQ(pbftPlacement.clientPhysicalId, 2, "app_id=1 source was not mapped to the client");
        NS_TEST_ASSERT_MSG_EQ(pbftPlacement.ingressPhysicalId, 1, "app_id=1 destination was not mapped to the primary");

        ConsensusTopology bidlPlacementTopology;
        for (uint32_t physicalId = 1; physicalId <= 10; ++physicalId) {
            bidlPlacementTopology.endpointPhysicalIds.push_back(physicalId);
            bidlPlacementTopology.physicalToEndpoint[physicalId] = physicalId - 1;
        }
        config.bidlIngressMode = "network";
        ConsensusRolePlacement bidlPlacement = config.ResolveBidlPlacement(bidlPlacementTopology);
        NS_TEST_ASSERT_MSG_EQ(bidlPlacement.clientPhysicalId, 2, "app_id=1 source was not mapped to the BIDL client");
        NS_TEST_ASSERT_MSG_EQ(bidlPlacement.ingressPhysicalId, 1, "app_id=1 destination was not mapped to the sequencer");
        NS_TEST_ASSERT_MSG_EQ(bidlPlacement.ordererPhysicalIds.size(), 4, "BIDL orderer placement is incomplete");
        NS_TEST_ASSERT_MSG_EQ(bidlPlacement.executorPhysicalIds.size(), 4, "BIDL executor placement is incomplete");
        Ptr<RdmaDriver> driver = topology.endpoints.Get(0)->GetObject<RdmaDriver>();
        DoubleValue targetUtilization;
        DoubleValue ewmaGain;
        DataRateValue minimumRate;
        driver->m_rdma->GetAttribute("TargetUtil", targetUtilization);
        driver->m_rdma->GetAttribute("EwmaGain", ewmaGain);
        driver->m_rdma->GetAttribute("MinRate", minimumRate);
        NS_TEST_ASSERT_MSG_EQ_TOL(targetUtilization.Get(), 0.87, 1e-12, "HPCC target utilization was not applied");
        NS_TEST_ASSERT_MSG_EQ_TOL(ewmaGain.Get(), 0.125, 1e-12, "DCQCN EWMA gain was not applied");
        NS_TEST_ASSERT_MSG_EQ(minimumRate.Get().GetBitRate(), 777000000, "RDMA minimum rate was not applied");
        Ptr<QbbNetDevice> firstDevice = DynamicCast<QbbNetDevice>(topology.devices.Get(0));
        BooleanValue qcnEnabled;
        BooleanValue pfcEnabled;
        UintegerValue pauseTime;
        firstDevice->GetAttribute("QcnEnabled", qcnEnabled);
        firstDevice->GetAttribute("QbbEnabled", pfcEnabled);
        firstDevice->GetAttribute("PauseTime", pauseTime);
        NS_TEST_ASSERT_MSG_EQ(qcnEnabled.Get(), false, "QCN flag was not applied");
        NS_TEST_ASSERT_MSG_EQ(pfcEnabled.Get(), false, "PFC flag was not applied");
        NS_TEST_ASSERT_MSG_EQ(firstDevice->GetDynamicThreshold(), false, "Dynamic PFC threshold flag was not applied");
        NS_TEST_ASSERT_MSG_EQ(pauseTime.Get(), 9, "PFC pause time was not applied");
        Ptr<SwitchNode> switchNode = DynamicCast<SwitchNode>(topology.switches.Get(0));
        NS_TEST_ASSERT_MSG_EQ(switchNode->m_mmu->kmin[1], 1000000, "10 Gbps ECN threshold was not applied");
        NS_TEST_ASSERT_MSG_EQ(switchNode->m_mmu->kmin[2], 2000000, "25 Gbps ECN threshold was not applied");
        NS_TEST_ASSERT_MSG_GT(switchNode->m_mmu->xoff[1][config.GetConsensusPriorityGroup()], 0, "Per-link PFC headroom was not derived");

        ApplicationContainer applications = config.InstallRdmaBackgroundTraffic(topology, Seconds(0));
        for (uint32_t index = 0; index < applications.GetN(); ++index) {
            Ptr<RdmaWorkloadApplication> workload = DynamicCast<RdmaWorkloadApplication>(applications.Get(index));
            if (workload) {
                workload->TraceConnectWithoutContext("FlowCompleted", MakeCallback(&ConsensusLegacyReplayTestCase::FlowCompleted, this));
            }
        }
        applications.Start(Seconds(0));
        applications.Stop(MilliSeconds(10));
        Simulator::Stop(MilliSeconds(10));
        Simulator::Run();
        NS_TEST_ASSERT_MSG_EQ(m_completed, 1, "app_id=0 flow was not completed exactly once");
        Simulator::Destroy();
        std::remove(topologyPath.c_str());
        std::remove(flowPath.c_str());
    }

    uint32_t m_completed{0};
};

class BidlGapRepairTestCase : public TestCase {
  public:
    BidlGapRepairTestCase() : TestCase("BIDL executor requests and receives a missing sequenced transaction") {}
  private:
    static bool FindPacket(Ptr<RecordingConsensusTransport> transport, uint32_t peer,
                           ConsensusMessageType type, uint64_t sequence, ConsensusMessage& result) {
        for (size_t i = transport->packets.size(); i > 0; --i) {
            if (transport->peers[i - 1] != peer ||
                !ConsensusMessage::FromPacket(transport->packets[i - 1], result)) continue;
            if (result.type == type && result.instanceId == sequence) return true;
        }
        return false;
    }
    void DoRun() override {
        Ptr<RecordingConsensusTransport> sequencerTransport = CreateObject<RecordingConsensusTransport>();
        Ptr<RecordingConsensusTransport> executorTransport = CreateObject<RecordingConsensusTransport>();
        Ptr<BidlApplication> sequencer = CreateObject<BidlApplication>();
        Ptr<BidlApplication> executor = CreateObject<BidlApplication>();
        sequencer->SetNodeId(200); sequencer->SetRole(BIDL_SEQUENCER);
        executor->SetNodeId(100); executor->SetRole(BIDL_EXECUTOR);
        for (Ptr<BidlApplication> node : {sequencer, executor}) {
            node->SetOrdererIds({0, 1, 2, 3});
            node->SetExecutorIds({100, 101, 102, 103});
            node->SetSequencerIds({200});
        }
        sequencer->SetTransport(sequencerTransport);
        executor->SetTransport(executorTransport);
        Ptr<Node> sequencerNode = CreateObject<Node>();
        Ptr<Node> executorNode = CreateObject<Node>();
        sequencerNode->AddApplication(sequencer);
        executorNode->AddApplication(executor);
        sequencer->SetStartTime(Seconds(0)); executor->SetStartTime(Seconds(0));
        sequencer->SetStopTime(MilliSeconds(10)); executor->SetStopTime(MilliSeconds(10));
        Simulator::Schedule(MilliSeconds(1), &BidlApplication::InjectTransaction, sequencer, 256, 1, 1);
        Simulator::Schedule(MilliSeconds(2), &BidlApplication::InjectTransaction, sequencer, 256, 1, 2);
        Simulator::Schedule(MilliSeconds(3), [=]() {
            ConsensusMessage second;
            if (FindPacket(sequencerTransport, 100, ConsensusMessageType::BIDL_TRANSACTION, 2, second))
                executorTransport->Inject(200, second.ToPacket());
        });
        Simulator::Schedule(MicroSeconds(3500), [=]() {
            ConsensusMessage gap;
            if (FindPacket(executorTransport, 200, ConsensusMessageType::BIDL_GAP_REQUEST, 1, gap))
                sequencerTransport->Inject(100, gap.ToPacket());
        });
        Simulator::Schedule(MilliSeconds(4), [=]() {
            ConsensusMessage repaired;
            if (FindPacket(sequencerTransport, 100, ConsensusMessageType::BIDL_TRANSACTION_REPAIR, 1, repaired))
                executorTransport->Inject(200, repaired.ToPacket());
        });
        Simulator::Stop(MilliSeconds(10));
        Simulator::Run();
        uint32_t gaps = 0;
        uint32_t copies = 0;
        for (size_t i = 0; i < executorTransport->packets.size(); ++i) {
            ConsensusMessage message;
            if (ConsensusMessage::FromPacket(executorTransport->packets[i], message) &&
                message.type == ConsensusMessageType::BIDL_GAP_REQUEST && message.instanceId == 1) ++gaps;
        }
        for (size_t i = 0; i < sequencerTransport->packets.size(); ++i) {
            ConsensusMessage message;
            if (sequencerTransport->peers[i] == 100 &&
                ConsensusMessage::FromPacket(sequencerTransport->packets[i], message) &&
                (message.type == ConsensusMessageType::BIDL_TRANSACTION || message.type == ConsensusMessageType::BIDL_TRANSACTION_REPAIR) &&
                message.instanceId == 1) ++copies;
        }
        NS_TEST_ASSERT_MSG_EQ(gaps, 1, "Executor did not stop gap retries after receiving the missing transaction");
        NS_TEST_ASSERT_MSG_EQ(copies, 2, "Sequencer did not retransmit the missing transaction");
        Simulator::Destroy();
    }
};

class BidlVerificationQueueDoesNotTriggerGapRepairTestCase : public TestCase {
  public:
    BidlVerificationQueueDoesNotTriggerGapRepairTestCase()
        : TestCase("BIDL transactions queued for verification are not mistaken for missing data") {}
  private:
    static void InjectCandidate(Ptr<RecordingConsensusTransport> transport, uint64_t sequence) {
        ClientRequest request{1000, sequence, 256, 1, 1000 + sequence};
        ConsensusMessage message;
        message.type = ConsensusMessageType::BIDL_TRANSACTION;
        message.senderId = 200;
        message.receiverId = 100;
        message.instanceId = sequence;
        message.payloadSize = request.payloadSize;
        message.digest = request.digest;
        message.clientId = request.clientId;
        message.requestId = request.requestId;
        message.transactionCount = request.transactionCount;
        message.payloadBytes = BidlApplication::EncodeTransactions({{sequence, request, 0}});
        transport->Inject(200, message.ToPacket());
    }
    void DoRun() override {
        Ptr<Node> node = CreateObject<Node>();
        Ptr<RecordingConsensusTransport> transport = CreateObject<RecordingConsensusTransport>();
        Ptr<BidlApplication> executor = CreateObject<BidlApplication>();
        executor->SetNodeId(100);
        executor->SetRole(BIDL_EXECUTOR);
        executor->SetOrdererIds({0, 1, 2, 3});
        executor->SetExecutorIds({100, 101, 102, 103});
        executor->SetSequencerIds({200});
        executor->SetTransport(transport);
        executor->SetAttribute("TransactionVerificationTime", TimeValue(MilliSeconds(2)));
        node->AddApplication(executor);
        executor->SetStartTime(Seconds(0));
        executor->SetStopTime(MilliSeconds(10));
        Simulator::Schedule(MilliSeconds(1), &BidlVerificationQueueDoesNotTriggerGapRepairTestCase::InjectCandidate,
                            transport, 1);
        Simulator::Schedule(MicroSeconds(1100), &BidlVerificationQueueDoesNotTriggerGapRepairTestCase::InjectCandidate,
                            transport, 2);
        Simulator::Stop(MilliSeconds(5));
        Simulator::Run();
        uint32_t gapRequests = 0;
        for (const auto& packet : transport->packets) {
            ConsensusMessage message;
            if (ConsensusMessage::FromPacket(packet, message) && message.type == ConsensusMessageType::BIDL_GAP_REQUEST)
                ++gapRequests;
        }
        NS_TEST_ASSERT_MSG_EQ(gapRequests, 0, "Verification backlog triggered unnecessary gap repair");
        Simulator::Destroy();
    }
};

class EzBftDependencyCertificateTestCase : public TestCase {
  public:
    EzBftDependencyCertificateTestCase()
        : TestCase("EZBFT accepts legal dependency updates, rejects invalid certificates, and executes dependencies first") {}

  private:
    static void Put32(std::vector<uint8_t>& bytes, uint32_t value) {
        for (int shift = 24; shift >= 0; shift -= 8) bytes.push_back(static_cast<uint8_t>(value >> shift));
    }
    static void Put64(std::vector<uint8_t>& bytes, uint64_t value) {
        for (int shift = 56; shift >= 0; shift -= 8) bytes.push_back(static_cast<uint8_t>(value >> shift));
    }
    static uint64_t Hash(uint64_t command, const std::vector<uint64_t>& deps, uint64_t sequence) {
        uint64_t h = command ^ 0x9e3779b97f4a7c15ULL;
        for (uint64_t dep : deps) h ^= dep + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        h ^= sequence * 0xd6e8feb86659fd93ULL;
        return h ? h : 1;
    }
    static std::vector<uint8_t> TupleBytes(uint64_t command, const std::vector<uint64_t>& deps, uint64_t sequence) {
        std::vector<uint8_t> bytes;
        Put64(bytes, command); Put32(bytes, static_cast<uint32_t>(deps.size()));
        for (uint64_t dep : deps) Put64(bytes, dep);
        Put64(bytes, sequence);
        return bytes;
    }
    static ConsensusMessage CertifiedCommit(uint32_t owner, uint32_t client, uint64_t command,
                                            const std::vector<uint64_t>& finalDeps, uint64_t finalSequence,
                                            const std::vector<std::pair<std::vector<uint64_t>, uint64_t>>& votes) {
        ConsensusMessage commit;
        commit.protocol = ConsensusProtocol::EZBFT;
        commit.type = ConsensusMessageType::EZBFT_COMMIT;
        commit.senderId = client;
        commit.view = 0;
        commit.instanceId = 1;
        commit.messageId = owner + 1;
        commit.digest = Hash(command, finalDeps, finalSequence);
        commit.clientId = client;
        commit.requestId = 1;
        commit.transactionCount = 1;
        commit.payloadBytes = TupleBytes(command, finalDeps, finalSequence);
        Put32(commit.payloadBytes, static_cast<uint32_t>(votes.size()));
        for (uint32_t voter = 0; voter < votes.size(); ++voter) {
            ConsensusMessage reply;
            reply.protocol = ConsensusProtocol::EZBFT;
            reply.type = ConsensusMessageType::EZBFT_SPEC_REPLY;
            reply.senderId = voter;
            reply.receiverId = client;
            reply.view = 0;
            reply.instanceId = 1;
            reply.messageId = owner + 1;
            reply.digest = Hash(command, votes[voter].first, votes[voter].second);
            reply.clientId = client;
            reply.requestId = 1;
            reply.transactionCount = 1;
            reply.payloadBytes = TupleBytes(command, votes[voter].first, votes[voter].second);
            ZyzzyvaAuth::Sign(reply);
            ZyzzyvaAuth::Verify(reply);
            Put32(commit.payloadBytes, voter);
            Put32(commit.payloadBytes, static_cast<uint32_t>(reply.payloadBytes.size()));
            commit.payloadBytes.insert(commit.payloadBytes.end(), reply.payloadBytes.begin(), reply.payloadBytes.end());
            commit.payloadBytes.insert(commit.payloadBytes.end(), reply.authentication.begin(), reply.authentication.end());
        }
        return commit;
    }
    void OnSend0(uint32_t peer, const ConsensusMessage& message) {
        if (peer == 1 && message.type == ConsensusMessageType::EZBFT_SPEC_ORDER) m_ownerOrder = message;
    }
    void OnSend1(uint32_t peer, const ConsensusMessage& message) {
        if (peer == 1000 && message.type == ConsensusMessageType::EZBFT_SPEC_REPLY) m_updatedReply = message;
    }
    void DoRun() override {
        Ptr<EzBftEngine> owner0 = CreateObject<EzBftEngine>();
        Ptr<EzBftEngine> replica1 = CreateObject<EzBftEngine>();
        owner0->Configure(0, {0, 1, 2, 3});
        replica1->Configure(1, {0, 1, 2, 3});
        owner0->SetSendCallback(MakeCallback(&EzBftDependencyCertificateTestCase::OnSend0, this));
        replica1->SetSendCallback(MakeCallback(&EzBftDependencyCertificateTestCase::OnSend1, this));
        owner0->Start(); replica1->Start();
        // Both requests write key zero. Replica 1 sees owner 1's request first.
        replica1->Submit(ClientRequest{1001, 1, 256, 1, 0x200});
        owner0->Submit(ClientRequest{1000, 1, 256, 1, 0x100});
        NS_TEST_ASSERT_MSG_EQ((m_ownerOrder.type == ConsensusMessageType::EZBFT_SPEC_ORDER), true,
                              "Owner proposal was not emitted");
        replica1->Receive(m_ownerOrder);
        replica1->Receive(m_ownerOrder);
        NS_TEST_ASSERT_MSG_EQ(replica1->GetEquivocationCount(), 0, "A legal replica dependency update was called equivocation");
        NS_TEST_ASSERT_MSG_EQ(replica1->GetLegalUpdateCount(), 1,
                              "Replica-local dependency change was not classified as a legal update");
        const uint64_t owner1Instance = (uint64_t{1} << 56) | 1;
        NS_TEST_ASSERT_MSG_EQ((m_updatedReply.payloadBytes == TupleBytes(0x100, {owner1Instance}, 2)), true,
                              "SPEC-REPLY did not name the conflicting owner instance");

        ConsensusMessage a = CertifiedCommit(0, 1000, 0x100, {owner1Instance}, 2,
                                             {{{}, 1}, {{owner1Instance}, 2}, {{}, 1}});
        ConsensusMessage invalid = a;
        invalid.payloadBytes.back() ^= 1;
        replica1->Receive(invalid);
        NS_TEST_ASSERT_MSG_EQ(replica1->GetFinalExecutionCount(), 0, "Invalid certificate finalized a request");
        replica1->Receive(a);
        NS_TEST_ASSERT_MSG_EQ(replica1->GetFinalExecutionCount(), 0, "Dependent request executed before its prerequisite");
        ConsensusMessage b = CertifiedCommit(1, 1001, 0x200, {}, 1, {{{}, 1}, {{}, 1}, {{}, 1}});
        replica1->Receive(b);
        NS_TEST_ASSERT_MSG_EQ(replica1->GetFinalExecutionCount(), 2, "Valid slow certificates did not finalize both instances");
        NS_TEST_ASSERT_MSG_EQ((replica1->GetFinalOrder() == std::vector<uint64_t>{owner1Instance, 1}), true,
                              "Final execution violated the certified dependency order");
        owner0->Stop(); replica1->Stop();
        Simulator::Destroy();
    }
    ConsensusMessage m_ownerOrder;
    ConsensusMessage m_updatedReply;
};

class ConsensusTestSuite : public TestSuite {
    public:
    ConsensusTestSuite() : TestSuite("consensus", Type::UNIT) {
        AddTestCase(new ConsensusMessageTestCase(), TestCase::QUICK);
        AddTestCase(new ConsensusTransportImpairmentTestCase(), TestCase::QUICK);
        AddTestCase(new RdmaConsensusTransportTestCase(), TestCase::QUICK);
        AddTestCase(new ClientExternalSubmissionTestCase(), TestCase::QUICK);
        AddTestCase(new BidlSequencerDeduplicationTestCase(), TestCase::QUICK);
        AddTestCase(new BidlMaliciousFirstArrivalTestCase(), TestCase::QUICK);
        AddTestCase(new BidlGapRepairTestCase(), TestCase::QUICK);
        AddTestCase(new BidlVerificationQueueDoesNotTriggerGapRepairTestCase(), TestCase::QUICK);
        AddTestCase(new EzBftDependencyCertificateTestCase(), TestCase::QUICK);
        AddTestCase(new ConsensusMeasurementWindowTestCase(), TestCase::QUICK);
        AddTestCase(new ConsensusResourceTestCase(), TestCase::QUICK);
        AddTestCase(new PbftNormalPathTestCase(), TestCase::QUICK);
        AddTestCase(new PbftViewChangeTestCase(), TestCase::QUICK);
        AddTestCase(new ClientRetransmissionTestCase(), TestCase::QUICK);
        AddTestCase(new ConsensusExperimentConfigTestCase(), TestCase::QUICK);
        AddTestCase(new ConsensusConfigurationPrecedenceTestCase(), TestCase::QUICK);
        AddTestCase(new ConsensusConfigurationValidationTestCase(), TestCase::QUICK);
        AddTestCase(new ConsensusRdmaTopologyTestCase(), TestCase::QUICK);
        AddTestCase(new ConsensusRdmaLeafSpineTestCase(), TestCase::QUICK);
        AddTestCase(new ConsensusLegacyReplayTestCase(), TestCase::QUICK);
    }
};

static ConsensusTestSuite g_consensusTestSuite;

class ConsensusEndToEndMatrixTestCase : public TestCase {
    public:
    ConsensusEndToEndMatrixTestCase() : TestCase("PBFT and BIDL complete over TCP and RDMA Qbb") {}

    private:
    void RunMultiClientTrace() {
        const std::string topologyPath = "/tmp/ns3-consensus-multi-client-topology.txt";
        const std::string flowPath = "/tmp/ns3-consensus-multi-client-flow.txt";
        {
            std::ofstream topology(topologyPath);
            topology << "12 1 11\n0\n";
            for (uint32_t host = 1; host <= 11; ++host) {
                topology << host << " 0 25Gbps 2us 0\n";
            }
        }
        {
            std::ofstream flows(flowPath);
            flows << "4\n"
                  << "10 9 3 100 1000 1001000000 1\n"
                  << "11 9 3 100 1100 1001200000 1\n"
                  << "10 9 3 100 1200 1001400000 1\n"
                  << "11 9 3 100 1300 1001600000 1\n";
        }

        ConsensusExperimentConfig config;
        config.ApplyPreset("pbft-rdma");
        config.topology = "qbb-file";
        config.topologyFile = topologyPath;
        config.flowFile = flowPath;
        config.simulationTime = 1.5;
        ConsensusRunResult result = ConsensusRunner().Run(config, false);
        NS_TEST_ASSERT_MSG_EQ(config.GetEffectiveClientCount(), 2, "Trace clients were not discovered");
        NS_TEST_ASSERT_MSG_EQ(result.success, true, "Multi-client trace did not complete");
        NS_TEST_ASSERT_MSG_EQ(result.completedRequests, 4, "Multi-client trace completion count changed");
        std::remove(topologyPath.c_str());
        std::remove(flowPath.c_str());
    }

    void RunPreset(const std::string& preset, uint32_t requestCount, uint32_t clientCount = 1, const std::string& workload = "auto") {
        ConsensusExperimentConfig config;
        config.ApplyPreset(preset);
        config.requestCount = requestCount;
        config.clientCount = clientCount;
        config.workload = workload;
        config.requestRate = 1000;
        config.simulationTime = 1.6;
        config.outputPrefix = "/tmp/consensus-e2e-" + preset;
        if (preset == "pbft-rdma") {
            config.rdmaIncastSenders = 2;
            config.rdmaIncastBursts = 1;
            config.rdmaIncastFlowBytes = 64 * 1024;
        } else if (preset == "bidl-rdma") {
            config.rdmaLongFlows = 1;
            config.rdmaLongFlowBytes = 64 * 1024;
            config.rdmaShortFlows = 2;
            config.rdmaShortFlowBytes = 16 * 1024;
        }

        ConsensusRunResult result = ConsensusRunner().Run(config, false);
        NS_TEST_ASSERT_MSG_EQ(result.success, true, preset + " did not complete");
        uint64_t expectedRequests = static_cast<uint64_t>(requestCount) * clientCount;
        NS_TEST_ASSERT_MSG_EQ(result.completedRequests, expectedRequests, preset + " request completion count changed");
        NS_TEST_ASSERT_MSG_EQ(result.completedTransactions, expectedRequests, preset + " transaction completion count changed");
        if (config.architecture == "bidl") {
            uint64_t expectedBlocks = (expectedRequests + config.batchSize - 1) / config.batchSize;
            NS_TEST_ASSERT_MSG_EQ(result.committedBlocks, expectedBlocks, preset + " block count changed");
        }
    }

    void RunBidlPartialBlocks() {
        ConsensusExperimentConfig config;
        config.ApplyPreset("bidl-tcp");
        config.requestCount = 5;
        config.requestRate = 1000;
        config.batchSize = 2;
        config.batchTimeoutMs = 0.1;
        config.applicationStartTime = 0.1;
        config.simulationTime = 0.5;
        ConsensusRunResult result = ConsensusRunner().Run(config, false);
        NS_TEST_ASSERT_MSG_EQ(result.success, true, "Valid partial BIDL blocks were reported as an incomplete run");
        NS_TEST_ASSERT_MSG_EQ(result.completedRequests, 5, "Partial BIDL blocks lost requests");
        NS_TEST_ASSERT_MSG_EQ(result.committedBlocks, 5, "Batch timeout did not create the expected partial blocks");
    }

    void RunBidlViewChange() {
        ConsensusExperimentConfig config;
        config.ApplyPreset("bidl-tcp");
        config.requestCount = 2;
        config.requestRate = 1000;
        config.batchSize = 2;
        config.failedNodeId = 0;
        config.failureStartMs = 0;
        config.failureStopMs = 0;
        config.retransmissionTimeoutMs = 1;
        config.viewChangeTimeoutMs = 5;
        config.applicationStartTime = 0.1;
        config.simulationTime = 0.5;
        ConsensusRunResult result = ConsensusRunner().Run(config, false);
        NS_TEST_ASSERT_MSG_EQ(result.success, true, "BIDL did not complete after the initial PBFT primary failed");
        NS_TEST_ASSERT_MSG_EQ(result.completedRequests, 2, "BIDL view change lost client requests");
        NS_TEST_ASSERT_MSG_EQ(result.committedBlocks, 1, "BIDL view change committed the wrong number of blocks");
        NS_TEST_ASSERT_MSG_EQ((result.viewChanges > 0), true, "The sequencer takeover was not counted");
        config.failedNodeId = 200;
        config.bidlIngressMode = "network";
        config.requestTimeoutMs = 5;
        config.clientCount = 4;
        config.requestCount = 2;
        ConsensusRunResult gateway = ConsensusRunner().Run(config, false);
        NS_TEST_ASSERT_MSG_EQ(gateway.success, true, "Gateway failure blocked retry directly to the sequencing orderer");
        NS_TEST_ASSERT_MSG_EQ(gateway.completedRequests, 8, "Gateway fallback lost a client request");
        NS_TEST_ASSERT_MSG_EQ(gateway.replicaProgressMin, 8, "A correct executor failed to commit gateway fallback requests");
        NS_TEST_ASSERT_MSG_EQ(gateway.replicaStateAgreement, true, "Gateway fallback left inconsistent execution results");
    }

    void RunZyzzyvaIors() {
        ConsensusExperimentConfig config;
        config.ApplyPreset("zyzzyva-rdma");
        config.requestSize = 512; // Primary signature plus portable client proof must fit single-packet IORS.
        config.requestCount = 2;
        config.requestRate = 1000;
        config.applicationStartTime = 0.1;
        config.simulationTime = 0.5;
        config.rdmaMtu = 1200;
        config.zyzzyvaEquivocationEnabled = true;
        ConsensusRunResult arms[4];
        for (uint32_t arm = 0; arm < 4; ++arm) {
            config.iorsMode = arm == 0 ? 0 : 1;
            config.iorsDetectOnly = arm == 1;
            config.iorsTerminalGate = arm == 3;
            config.outputPrefix = "/tmp/zyzzyva-equivocation-arm-" + std::to_string(arm);
            arms[arm] = ConsensusRunner().Run(config, false);
            NS_TEST_ASSERT_MSG_EQ(arms[arm].success, true, "A Zyzzyva IORS comparison arm did not complete");
            NS_TEST_ASSERT_MSG_EQ(arms[arm].completedRequests, 2,
                                  "An IORS comparison arm lost a client request");
            if (arm < 2) {
                // A valid client POM may recover before any individual backup
                // receives both conflicting orders at the same slot.
                NS_TEST_ASSERT_MSG_EQ((arms[arm].zyzzyvaEquivocations <= 1), true,
                                      "The single attack was counted as repeated local equivocation");
            } else {
                NS_TEST_ASSERT_MSG_EQ((arms[arm].iorsEquivocation > 0), true,
                                      "The switch-assisted arm did not report the conflicting candidates");
            }
            NS_TEST_ASSERT_MSG_EQ(arms[arm].viewChanges, 1,
                                  "Correct handling of the malicious primary did not trigger one necessary view change");
            NS_TEST_ASSERT_MSG_EQ(arms[arm].replicaStateAgreement, true,
                                  "Correct replicas did not converge after equivocation recovery");
        }
        NS_TEST_ASSERT_MSG_EQ(arms[0].iorsMarked, 0, "The no-IORS arm unexpectedly marked an ORDER-REQ");
        NS_TEST_ASSERT_MSG_EQ((arms[1].iorsEquivocation > 0), true,
                              "Detection-only IORS did not count the equivocation");
        NS_TEST_ASSERT_MSG_EQ(arms[1].iorsMarked, 0,
                              "Detection-only IORS unexpectedly modified a candidate");
        NS_TEST_ASSERT_MSG_EQ((arms[2].iorsMarked > 0), true, "The marking arm missed the equivocation");
        NS_TEST_ASSERT_MSG_EQ((arms[3].iorsAlreadyMarked > 0), true,
                              "A downstream switch did not recognize an already marked candidate");
        NS_TEST_ASSERT_MSG_EQ((arms[0].zyzzyvaRollbacks > 0), true,
                              "The no-IORS arm did not expose speculative rollback from equivocation");
        NS_TEST_ASSERT_MSG_EQ((arms[3].zyzzyvaRollbacks < arms[0].zyzzyvaRollbacks), true,
                              "Bounded waiting did not reduce this scenario's rollback count");
        config.iorsMode = 0;
        config.zyzzyvaViewChangeEnabled = false;
        config.outputPrefix = "/tmp/zyzzyva-equivocation-no-view-change";
        ConsensusRunResult noViewChange = ConsensusRunner().Run(config, false);
        NS_TEST_ASSERT_MSG_EQ(noViewChange.viewChanges, 0,
                              "The no-view-change ablation entered a new view");
        NS_TEST_ASSERT_MSG_EQ(noViewChange.completedRequests, 2,
                              "The no-view-change ablation lost the finite workload's client completions");
        // Longer branch separation exercises a client POM after fast/slow replies,
        // rather than a backup directly receiving both candidates at one slot.
        config.zyzzyvaViewChangeEnabled = true;
        config.zyzzyvaEquivocationDelayUs = 5000;
        config.clientCount = 4;
        config.requestCount = 100;
        config.requestSize = 1024;
        config.rdmaMtu = 1400;
        config.outputPrefix = "/tmp/zyzzyva-client-pom-recovery";
        ConsensusRunResult clientPom = ConsensusRunner().Run(config, false);
        NS_TEST_ASSERT_MSG_EQ(clientPom.success, true, "The delayed-branch client POM did not recover the workload");
        NS_TEST_ASSERT_MSG_EQ(clientPom.completedRequests, 400, "POM recovery lost client requests");
        NS_TEST_ASSERT_MSG_EQ(clientPom.viewChanges, 1, "A valid client POM failed to trigger the required view change");
        NS_TEST_ASSERT_MSG_EQ(clientPom.replicaStateAgreement, true, "Client-POM recovery left divergent replicas");
    }

    void RunZyzzyvaViewChange() {
        ConsensusExperimentConfig config;
        config.ApplyPreset("zyzzyva-rdma");
        config.topology = "qbb-star";
        config.requestCount = 1;
        config.requestTimeoutMs = 5;
        config.viewChangeTimeoutMs = 5;
        config.failedNodeId = 0;
        config.failureStartMs = 0;
        config.failureStopMs = 0;
        config.applicationStartTime = 0.1;
        config.simulationTime = 0.5;
        config.rdmaMtu = 1200;
        ConsensusRunResult result = ConsensusRunner().Run(config, false);
        NS_TEST_ASSERT_MSG_EQ(result.success, true, "Zyzzyva did not recover from a failed first primary");
    }

    void RunEzBftVariants() {
        ConsensusExperimentConfig config;
        config.ApplyPreset("ezbft-rdma");
        config.requestCount = 2;
        config.requestRate = 1000;
        config.applicationStartTime = 0.1;
        config.simulationTime = 0.5;
        config.rdmaMtu = 1200;
        config.ezbftEquivocationEnabled = false;
        for (uint32_t variant = 0; variant <= 1; ++variant) {
            config.ezbftVariant = variant;
            ConsensusRunResult clean = ConsensusRunner().Run(config, false);
            NS_TEST_ASSERT_MSG_EQ(clean.success, true, "EZBFT benign fast path did not complete");
            NS_TEST_ASSERT_MSG_EQ((clean.ezbftFinalExecutions > 0), true, "EZBFT did not finalize any tuple");
            config.ezbftEquivocationEnabled = true;
            ConsensusRunResult attacked = ConsensusRunner().Run(config, false);
            NS_TEST_ASSERT_MSG_EQ((attacked.ezbftEquivocations > 0), true, "EZBFT equivocation was not observed");
            config.ezbftEquivocationEnabled = false;
        }
    }

    void RunIorsFailOpen() {
        ConsensusExperimentConfig zyz;
        zyz.ApplyPreset("zyzzyva-rdma");
        zyz.requestSize = 512;
        zyz.requestCount = 3;
        zyz.rdmaMtu = 1200;
        zyz.iorsMode = 1;
        zyz.zyzzyvaEquivocationEnabled = true;
        zyz.iorsCapacity = 1;
        ConsensusRunResult capacity = ConsensusRunner().Run(zyz, false);
        NS_TEST_ASSERT_MSG_EQ(capacity.success, true, "A full IORS table blocked Zyzzyva");
        NS_TEST_ASSERT_MSG_EQ((capacity.iorsCapacityBypass > 0), true, "The capacity fallback was not exercised");
        zyz.iorsCapacity = 4096;
        zyz.iorsFingerprintBits = 1;
        ConsensusRunResult collision = ConsensusRunner().Run(zyz, false);
        NS_TEST_ASSERT_MSG_EQ(collision.success, true, "An IORS fingerprint collision blocked Zyzzyva");
        NS_TEST_ASSERT_MSG_EQ((collision.iorsCollisionBypass > 0), true, "The collision fallback was not exercised");
        zyz.iorsFingerprintBits = 64;
        zyz.iorsTtlUs = 0.001;
        ConsensusRunResult expiry = ConsensusRunner().Run(zyz, false);
        NS_TEST_ASSERT_MSG_EQ(expiry.success, true, "IORS expiry blocked Zyzzyva");
        NS_TEST_ASSERT_MSG_EQ((expiry.iorsExpired > 0), true, "The expiry path was not exercised");
        zyz.requestSize = 1600;
        zyz.zyzzyvaEquivocationEnabled = false;
        ConsensusRunResult fragmented = ConsensusRunner().Run(zyz, false);
        NS_TEST_ASSERT_MSG_EQ(fragmented.success, true, "Fragmented Zyzzyva requests failed");
        NS_TEST_ASSERT_MSG_EQ(fragmented.iorsObserved, 0, "A fragmented ORDER-REQ entered single-packet IORS");

        ConsensusExperimentConfig bidl;
        bidl.ApplyPreset("bidl-rdma");
        bidl.topology = "qbb-star";
        bidl.requestCount = 3;
        bidl.requestSize = 512; // Include authenticated transaction envelope within the RDMA MTU.
        bidl.bidlAttackEnabled = true;
        bidl.rdmaMtu = 1200;
        bidl.iorsMode = 2;
        bidl.iorsHoldCapacity = 0;
        ConsensusRunResult hold = ConsensusRunner().Run(bidl, false);
        NS_TEST_ASSERT_MSG_EQ(hold.success, true, "Exhausted IORS waiting capacity blocked BIDL");
        NS_TEST_ASSERT_MSG_EQ((hold.iorsHoldBypass > 0), true, "The waiting fallback was not exercised");
    }

    void RunBidlDenylist() {
        ConsensusExperimentConfig config;
        config.ApplyPreset("bidl-rdma");
        config.requestCount = 50;
        config.bidlAttackEnabled = true;
        config.bidlAttackTargetExecutors = 0;
        config.iorsMode = 1;
        config.rdmaMtu = 1200;
        config.bidlDenylistEnabled = true;
        ConsensusRunResult enabled = ConsensusRunner().Run(config, false);
        NS_TEST_ASSERT_MSG_EQ(enabled.success, true, "BIDL denylist blocked ordered requests");
        NS_TEST_ASSERT_MSG_EQ(enabled.bidlDenylistSize, 1, "BIDL did not aggregate cross-view client evidence");
        config.bidlDenylistEnabled = false;
        ConsensusRunResult disabled = ConsensusRunner().Run(config, false);
        NS_TEST_ASSERT_MSG_EQ(disabled.success, true, "Disabling BIDL denylist changed request completion");
        NS_TEST_ASSERT_MSG_EQ(disabled.bidlDenylistSize, 0, "Disabled BIDL denylist still accused a client");
    }

    void RunBidlIorsRepairSources() {
        ConsensusExperimentConfig config;
        config.ApplyPreset("bidl-rdma");
        config.bidlIngressMode = "network";
        config.requestCount = 100;
        config.requestRate = 4000;
        config.requestSize = 1024;
        config.batchSize = 50;
        config.batchTimeoutMs = 5;
        config.rdmaMtu = 1400;
        config.applicationStartTime = 0.1;
        config.measurementTime = 0.4;
        config.simulationTime = 0.7;
        for (bool compact : {false, true}) {
            config.bidlCompactOrderNotifications = compact;
            for (bool assisted : {false, true}) {
                config.iorsMode = assisted ? 2 : 0;
                config.iorsTerminalGate = assisted;
                config.bidlAttackEnabled = false;
                ConsensusRunResult normal = ConsensusRunner().Run(config, false);
                NS_TEST_ASSERT_MSG_EQ(normal.success, true, "Legal BIDL cache repair failed");
                NS_TEST_ASSERT_MSG_EQ(normal.iorsUnauthorized, 0, "IORS marked a legal repair as unauthorized");
                NS_TEST_ASSERT_MSG_EQ(normal.iorsEquivocation, 0, "Legal cache repair created an IORS conflict");
                config.bidlAttackEnabled = true;
                config.bidlAttackTargetExecutors = 0;
                ConsensusRunResult attacked = ConsensusRunner().Run(config, false);
                NS_TEST_ASSERT_MSG_EQ(attacked.success, true, "BIDL notification mode broke attack recovery");
                if (assisted) {
                    NS_TEST_ASSERT_MSG_EQ(attacked.iorsUnauthorized, 0, "IORS relied on sender authorization");
                    NS_TEST_ASSERT_MSG_EQ((attacked.iorsEquivocation > 0), true,
                                          "IORS missed the conflicting sequence/digest binding");
                }
            }
        }
        config.bidlCompactOrderNotifications = true;
        config.bidlAttackMode = 1;
        config.bidlAttackEnabled = true;
        config.bidlAttackTargetExecutors = 0;
        config.iorsMode = 1;
        config.iorsTerminalGate = false;
        ConsensusRunResult sequenceTampering = ConsensusRunner().Run(config, false);
        NS_TEST_ASSERT_MSG_EQ(sequenceTampering.success, true, "Sequence-tampering workload did not complete");
        NS_TEST_ASSERT_MSG_EQ(sequenceTampering.iorsUnauthorized, 0, "IORS classified the attacker by sender identity");
        NS_TEST_ASSERT_MSG_EQ((sequenceTampering.iorsEquivocation > 0), true,
                              "IORS missed one digest mapped to multiple sequences");
        NS_TEST_ASSERT_MSG_EQ((sequenceTampering.iorsMarked > 0), true,
                              "IORS did not mark the conflicting sequence mapping");
    }

    void DoRun() override {
        RunPreset("pbft-tcp", 2);
        RunPreset("bidl-tcp", 2);
        RunPreset("pbft-rdma", 2);
        RunPreset("bidl-rdma", 2);
        RunPreset("pbft-rdma", 2, 2, "closed-loop");
        RunPreset("pbft-tcp", 3, 2, "burst");
        RunPreset("bidl-rdma", 2, 2, "open-loop");
        RunMultiClientTrace();
        RunBidlPartialBlocks();
        RunBidlViewChange();
        RunZyzzyvaIors();
        RunZyzzyvaViewChange();
        RunEzBftVariants();
        RunIorsFailOpen();
        RunBidlDenylist();
        RunBidlIorsRepairSources();
    }
};

class ConsensusEndToEndTestSuite : public TestSuite {
    public:
    ConsensusEndToEndTestSuite() : TestSuite("consensus-e2e", Type::SYSTEM) {
        AddTestCase(new ConsensusEndToEndMatrixTestCase(), TestCase::QUICK);
    }
};

static ConsensusEndToEndTestSuite g_consensusEndToEndTestSuite;

namespace ns3 {
class BidlClientAbortTestCase : public TestCase {
  public:
    BidlClientAbortTestCase() : TestCase("BIDL client requires consensus-node abort replies and never counts abort as commit") {}
  private:
    void Submit(Ptr<ConsensusClientApplication> client) { client->SubmitRequest(512); }
    void Inject(Ptr<RecordingConsensusTransport> transport, uint32_t sender) {
        ConsensusMessage request;
        if (!ConsensusMessage::FromPacket(transport->packets.front(), request)) return;
        ConsensusMessage abort;
        abort.type = ConsensusMessageType::BIDL_ABORT;
        abort.senderId = sender;
        abort.receiverId = 1000;
        abort.clientId = 1000;
        abort.requestId = request.requestId;
        abort.digest = request.digest;
        abort.instanceId = 1;
        abort.transactionCount = 1;
        transport->Inject(sender, abort.ToPacket());
    }
    void CheckOneVote(Ptr<ConsensusClientApplication> client) {
        NS_TEST_ASSERT_MSG_EQ(client->GetAbortedRequests(), 0, "One replica or an outsider formed an abort quorum");
    }
    void DoRun() override {
        Ptr<Node> node = CreateObject<Node>();
        auto transport = CreateObject<RecordingConsensusTransport>();
        auto client = CreateObject<ConsensusClientApplication>();
        client->SetTransport(transport);
        client->SetReplicaIds({0, 1, 2, 3});
        client->SetAttribute("BidlClient", BooleanValue(true));
        client->SetAttribute("AutoGenerate", BooleanValue(false));
        client->SetAttribute("RequestTimeout", TimeValue(MilliSeconds(5)));
        node->AddApplication(client);
        client->SetStartTime(Seconds(0));
        client->SetStopTime(MilliSeconds(10));
        Simulator::Schedule(MilliSeconds(1), &BidlClientAbortTestCase::Submit, this, client);
        Simulator::Schedule(MilliSeconds(2), &BidlClientAbortTestCase::Inject, this, transport, 0);
        Simulator::Schedule(MilliSeconds(2), &BidlClientAbortTestCase::Inject, this, transport, 300);
        Simulator::Schedule(MicroSeconds(2500), &BidlClientAbortTestCase::CheckOneVote, this, client);
        Simulator::Schedule(MilliSeconds(3), &BidlClientAbortTestCase::Inject, this, transport, 1);
        Simulator::Schedule(MilliSeconds(4), &BidlClientAbortTestCase::Inject, this, transport, 1);
        Simulator::Stop(MilliSeconds(10));
        Simulator::Run();
        NS_TEST_ASSERT_MSG_EQ(client->GetAbortedRequests(), 1, "Matching CN replies did not terminate the aborted request exactly once");
        NS_TEST_ASSERT_MSG_EQ(client->GetCompletedRequests(), 0, "An aborted request was counted as committed");
        NS_TEST_ASSERT_MSG_EQ(client->GetCompletedTransactions(), 0, "An abort inflated effective transaction throughput");
        NS_TEST_ASSERT_MSG_EQ(transport->packets.size(), 1, "Abort did not cancel the request retransmission timer");
        Simulator::Destroy();
    }
};

class BidlRepairSourceTestCase : public TestCase {
  public:
    BidlRepairSourceTestCase() : TestCase("BIDL attributes every repair packet and coalesces queued ingress retries") {}
  private:
    void OnRepair(uint32_t sender, uint32_t receiver, uint64_t sequence, uint8_t reason, uint32_t bytes) {
        (void)sender; (void)receiver; (void)sequence;
        ++m_reasons[reason];
        m_bytes += bytes;
    }
    void DoRun() override {
        Ptr<BidlApplication> app = CreateObject<BidlApplication>();
        Ptr<RecordingConsensusTransport> transport = CreateObject<RecordingConsensusTransport>();
        Ptr<PbftEngine> engine = CreateObject<PbftEngine>();
        engine->Configure(0, {0, 1, 2, 3});
        app->SetNodeId(0); app->SetRole(BIDL_ORDERER);
        app->SetOrdererIds({0, 1, 2, 3}); app->SetExecutorIds({100, 101, 102, 103});
        app->SetSequencerIds({200, 0, 1, 2, 3}); app->SetEngine(engine); app->SetTransport(transport);
        app->SetResourceModel(CreateObject<ConsensusResourceModel>());
        app->m_nativeSequencing = true; app->m_running = true;
        app->TraceConnectWithoutContext("RepairTx", MakeCallback(&BidlRepairSourceTestCase::OnRepair, this));
        ClientRequest request{1000, 1, 256, 1, 123};
        app->m_transactions[1] = request;
        app->m_clientRequestSequences[{1000, 1}] = 1;
        app->SequenceTransaction(request);
        ConsensusMessage gap;
        gap.type = ConsensusMessageType::BIDL_GAP_REQUEST; gap.senderId = 100; gap.instanceId = 1;
        app->HandleOrdererMessage(gap);
        NS_TEST_ASSERT_MSG_EQ(m_reasons[static_cast<uint8_t>(BidlRepairReason::DUPLICATE_CLIENT_REQUEST)], 8,
                              "Duplicate request fanout was not classified per transmitted packet");
        NS_TEST_ASSERT_MSG_EQ(m_reasons[static_cast<uint8_t>(BidlRepairReason::GAP_REQUEST)], 1,
                              "Missing-sequence response was not classified");
        uint64_t packetBytes = 0;
        for (auto packet : transport->packets) packetBytes += packet->GetSize();
        NS_TEST_ASSERT_MSG_EQ(m_bytes, packetBytes, "Attributed repair bytes disagree with actual packets");
        request.requestId = 2; request.digest = 124;
        app->EnqueueTransaction(request); app->EnqueueTransaction(request);
        NS_TEST_ASSERT_MSG_EQ(app->m_queuedIngressRequests.size(), 1,
                              "Duplicate ingress queued redundant sequencing CPU work");
        app->m_transactions[2] = request;
        app->m_ordererPersistedResultDigests[1] = 456;
        app->CompleteOrdererPersist(1, 2, app->ComputeBlockDigest(1, 2), 456, {});
        uint32_t persistPackets = 0;
        for (const auto& packet : transport->packets) {
            ConsensusMessage message;
            if (!ConsensusMessage::FromPacket(packet, message) || message.type != ConsensusMessageType::BIDL_RESULT_PERSIST)
                continue;
            NS_TEST_ASSERT_MSG_EQ(message.transactionCount, 2, "Persist lost the block transaction count");
            ++persistPackets;
        }
        NS_TEST_ASSERT_MSG_EQ(persistPackets, 4, "Persist did not reach every executor");
        app->m_running = false;
        Simulator::Destroy();
    }
    std::map<uint8_t, uint64_t> m_reasons;
    uint64_t m_bytes{0};
};

class BidlCanonicalRecoveryTestCase : public TestCase {
  public:
    BidlCanonicalRecoveryTestCase()
        : TestCase("BIDL certified blocks recover missing and sequence-poisoned transactions") {}
  private:
    void CheckRecovery(bool missing, bool lateVerification) {
        auto app = CreateObject<BidlApplication>();
        auto transport = CreateObject<RecordingConsensusTransport>();
        app->SetNodeId(100);
        app->SetRole(BIDL_EXECUTOR);
        app->SetOrdererIds({0, 1, 2, 3});
        app->SetExecutorIds({100});
        app->SetSequencerIds({200});
        app->SetTransport(transport);
        app->SetResourceModel(CreateObject<ConsensusResourceModel>());
        app->m_running = true;
        app->m_compactOrderNotifications = true;
        app->m_faultTolerance = 1;
        ClientRequest first{1000, 1, 256, 1, 101};
        ClientRequest second{1000, 2, 256, 1, 102};
        BidlApplication::SignRequest(first);
        BidlApplication::SignRequest(second);
        const std::vector<BidlTransactionRecord> canonical{{1, first, 1}, {2, second, 1}};
        uint64_t digest = 0;
        for (const auto& record : canonical)
            digest ^= record.request.digest + 0x9e3779b97f4a7c15ULL + (digest << 6) + (digest >> 2);
        if (!missing) {
            app->m_transactions[2] = first;
            app->m_digestSequences[first.digest] = 2;
        }
        if (lateVerification) {
            app->m_transactions[1] = first;
            app->m_digestSequences[first.digest] = 1;
            app->m_executedTransactions.insert(1);
            app->m_executedTransactions.insert(2);
            app->m_transactionExecutionTimes[1] = Seconds(0);
            app->m_transactionExecutionTimes[2] = Seconds(0);
            app->m_nextExecutionSequence = 3;
        }
        ConsensusMessage block;
        block.type = ConsensusMessageType::BIDL_ORDERED_BLOCK;
        block.instanceId = 1;
        block.transactionCount = 2;
        block.digest = digest;
        for (uint32_t sender : {0U, 1U, 2U}) {
            block.senderId = sender;
            app->HandleExecutorMessage(block);
        }
        if (lateVerification) {
            app->m_blocks.at(1).requiresReexecution = false;
            ConsensusMessage repair;
            repair.type = ConsensusMessageType::BIDL_TRANSACTION_REPAIR;
            repair.senderId = 200;
            repair.instanceId = 2;
            repair.clientId = second.clientId;
            repair.requestId = second.requestId;
            repair.digest = second.digest;
            repair.payloadSize = second.payloadSize;
            repair.transactionCount = 1;
            repair.payloadBytes = BidlApplication::EncodeTransactions({canonical[1]});
            app->CompleteTransactionVerification(repair);
            NS_TEST_ASSERT_MSG_EQ(app->m_blocks.at(1).approvalScheduled, true,
                                  "Late verified input did not resume an already executed ordered block");
        } else {
            app->RequestCanonicalBlock(1);
            uint32_t canonicalRequests = 0;
            for (const auto& packet : transport->packets) {
                ConsensusMessage request;
                if (ConsensusMessage::FromPacket(packet, request) &&
                    request.type == ConsensusMessageType::BIDL_BLOCK_REQUEST) ++canonicalRequests;
            }
            NS_TEST_ASSERT_MSG_EQ((canonicalRequests > 0), true,
                                  "An incomplete certified block never requested canonical transactions");
            block.payloadBytes = BidlApplication::EncodeTransactions(canonical);
            for (uint32_t sender : {0U, 1U}) {
                block.senderId = sender;
                app->HandleExecutorMessage(block);
            }
            NS_TEST_ASSERT_MSG_EQ(app->m_blocks.at(1).canonicalTransactions.empty(), true,
                                  "A minority canonical payload bypassed the ordering quorum");
            block.senderId = 2;
            app->HandleExecutorMessage(block);
        }
        Simulator::Stop(MilliSeconds(5));
        Simulator::Run();
        NS_TEST_ASSERT_MSG_EQ(app->ComputeBlockDigest(1, 2), digest,
                              "Certified contents did not replace missing or poisoned local input");
        NS_TEST_ASSERT_MSG_EQ(app->m_blocks.at(1).resultSubmitted, true,
                              "Recovered execution did not report its result to orderers");
        if (!lateVerification)
            NS_TEST_ASSERT_MSG_EQ(app->m_blocks.at(1).reexecutionComplete, true,
                                  "Canonical recovery did not finish corrective execution");
        app->m_running = false;
        Simulator::Destroy();
    }
    void DoRun() override {
        CheckRecovery(true, false);
        CheckRecovery(false, false);
        CheckRecovery(true, true);
    }
};

class BidlAuthenticationTestCase : public TestCase {
  public:
    BidlAuthenticationTestCase() : TestCase("BIDL keeps ordering fields unsigned but binds canonical quorum to authenticated client content") {}
  private:
    void CheckRepairedInput(Ptr<BidlApplication> app) {
        NS_TEST_ASSERT_MSG_EQ(app->m_transactions.count(1), 1, "Legal cache content was not stored after verification");
        NS_TEST_ASSERT_MSG_EQ(app->m_transactions.at(1).clientId, 1000, "Repair installed the wrong client input");
    }
    void CheckQuorum(Ptr<BidlApplication> app) {
        ClientRequest correct{1000, 1, 512, 1, 101};
        BidlApplication::SignRequest(correct);
        NS_TEST_ASSERT_MSG_EQ(BidlApplication::VerifyRequest(correct), true, "Valid client authentication was rejected");
        auto bytes = BidlApplication::EncodeTransactions({{9, correct, 10}});
        std::vector<BidlTransactionRecord> records;
        NS_TEST_ASSERT_MSG_EQ(BidlApplication::DecodeTransactions(bytes, records), true, "Canonical transaction decoding failed");
        records[0].sequence = 99;
        NS_TEST_ASSERT_MSG_EQ(BidlApplication::VerifyRequest(records[0].request), true, "Unsigned sequence unexpectedly changed the client signature");
        records[0].request.digest ^= 1;
        NS_TEST_ASSERT_MSG_EQ(BidlApplication::VerifyRequest(records[0].request), false, "Changed transaction content retained valid authentication");
        ConsensusMessage repair;
        repair.type = ConsensusMessageType::BIDL_TRANSACTION_REPAIR;
        repair.instanceId = 1;
        repair.payloadSize = correct.payloadSize;
        repair.digest = correct.digest;
        repair.clientId = correct.clientId;
        repair.requestId = correct.requestId;
        repair.transactionCount = 1;
        repair.payloadBytes = BidlApplication::EncodeTransactions({{1, correct, 10}});
        app->SetBroadcasterIds({300});
        repair.senderId = 300;
        app->HandleExecutorMessage(repair);
        NS_TEST_ASSERT_MSG_EQ(app->m_transactions.count(1), 0, "A malicious broadcaster bypassed source checks by claiming cache repair");
        NS_TEST_ASSERT_MSG_EQ(app->m_verificationPending.count(1), 0, "An unauthorized repair reached verification");
        repair.senderId = 200;
        app->HandleExecutorMessage(repair);
        NS_TEST_ASSERT_MSG_EQ(app->m_verificationPending.count(1), 1, "Authenticated client content from a legal cache was rejected");
        Simulator::Schedule(MicroSeconds(100), &BidlAuthenticationTestCase::CheckRepairedInput, this, app);
        ConsensusMessage block;
        block.type = ConsensusMessageType::BIDL_ORDERED_BLOCK;
        block.instanceId = 1;
        block.transactionCount = 1;
        block.digest = 101 + 0x9e3779b97f4a7c15ULL;
        ClientRequest conflicting = correct;
        conflicting.clientId = 300;
        BidlApplication::SignRequest(conflicting);
        block.senderId = 0;
        block.payloadBytes = BidlApplication::EncodeTransactions({{1, conflicting, 1}});
        app->HandleExecutorMessage(block);
        block.payloadBytes = BidlApplication::EncodeTransactions({{1, correct, 1}});
        for (uint32_t sender : {1U, 2U}) { block.senderId = sender; app->HandleExecutorMessage(block); }
        NS_TEST_ASSERT_MSG_EQ(app->m_blocks.count(1), 0, "Different canonical payloads were mixed into one ordering quorum");
        block.senderId = 3;
        app->HandleExecutorMessage(block);
        NS_TEST_ASSERT_MSG_EQ(app->m_blocks.at(1).ordered, true, "Three matching authenticated canonical payloads failed to order");
        NS_TEST_ASSERT_MSG_EQ(app->m_blocks.at(1).canonicalTransactions[0].request.clientId, 1000,
                              "One Byzantine canonical payload contaminated the correct quorum");
    }
    void DoRun() override {
        Ptr<Node> node = CreateObject<Node>();
        Ptr<BidlApplication> app = CreateObject<BidlApplication>();
        app->SetNodeId(100);
        app->SetRole(BIDL_EXECUTOR);
        app->SetOrdererIds({0, 1, 2, 3});
        app->SetExecutorIds({100});
        app->SetSequencerIds({200});
        app->SetTransport(CreateObject<RecordingConsensusTransport>());
        app->SetAttribute("RequireClientAuthentication", BooleanValue(true));
        node->AddApplication(app);
        app->SetStartTime(Seconds(0));
        app->SetStopTime(MilliSeconds(3));
        Simulator::Schedule(MilliSeconds(1), &BidlAuthenticationTestCase::CheckQuorum, this, app);
        Simulator::Stop(MilliSeconds(3));
        Simulator::Run();
        Simulator::Destroy();
    }
};

class IorsWireAuthenticationTestCase : public TestCase {
  public:
    IorsWireAuthenticationTestCase() : TestCase("IORS mark leaves a signed Zyzzyva candidate verifiable") {}
  private:
    void DoRun() override {
        ConsensusMessage message;
        message.protocol = ConsensusProtocol::ZYZZYVA;
        message.type = ConsensusMessageType::ZYZZYVA_ORDER_REQ;
        message.senderId = 0;
        message.receiverId = 1;
        message.view = 0;
        message.instanceId = 1;
        message.digest = 42;
        message.clientId = 1000;
        message.requestId = 1;
        message.transactionCount = 1;
        message.payloadBytes.resize(1032, 0);
        NS_TEST_ASSERT_MSG_EQ(ZyzzyvaAuth::Sign(message), true, "Candidate signing failed");
        message.iorsPresent = true;
        message.iorsMarked = true;
        message.iorsEvidence = 2;
        ConsensusMessage decoded;
        NS_TEST_ASSERT_MSG_EQ(ConsensusMessage::FromPacket(message.ToPacket(), decoded), true, "IORS wire frame is invalid");
        NS_TEST_ASSERT_MSG_EQ(decoded.iorsMarked, true, "IORS mark was lost");
        NS_TEST_ASSERT_MSG_EQ(ZyzzyvaAuth::Verify(decoded), true, "Mutable mark invalidated the protocol signature");
    }
};

class ZyzzyvaClientCertificateTestCase : public TestCase {
  public:
    ZyzzyvaClientCertificateTestCase()
        : TestCase("Zyzzyva client rejects an invalid response signature before building a slow certificate") {}

  private:
    static void Write64(std::vector<uint8_t>& bytes, uint64_t value) {
        for (int shift = 56; shift >= 0; shift -= 8) bytes.push_back(static_cast<uint8_t>(value >> shift));
    }

    static ConsensusMessage MakeReply(const ConsensusMessage& request, uint32_t sender,
                                      ConsensusMessageType type, uint64_t history) {
        ConsensusMessage reply;
        reply.protocol = ConsensusProtocol::ZYZZYVA;
        reply.type = type;
        reply.senderId = sender;
        reply.receiverId = request.clientId;
        reply.view = 0;
        reply.instanceId = 1;
        reply.digest = request.digest;
        reply.clientId = request.clientId;
        reply.requestId = request.requestId;
        reply.transactionCount = 1;
        Write64(reply.payloadBytes, history);
        ZyzzyvaAuth::Sign(reply);
        return reply;
    }

    void Submit(Ptr<ConsensusClientApplication> client) { m_requestId = client->SubmitRequest(1024, 1); }

    void InjectInitialReplies(Ptr<RecordingConsensusTransport> transport) {
        ConsensusMessage request;
        if (transport->packets.empty() || !ConsensusMessage::FromPacket(transport->packets.front(), request)) return;
        m_digest = request.digest;
        m_history = request.digest + 0x9e3779b97f4a7c15ULL;
        m_history ^= 0xd6e8feb86659fd93ULL;
        if (m_history == 0) m_history = 1;
        for (uint32_t sender : {0U, 1U}) {
            ConsensusMessage reply = MakeReply(request, sender, ConsensusMessageType::ZYZZYVA_SPEC_RESPONSE, m_history);
            transport->Inject(sender, reply.ToPacket());
        }
        ConsensusMessage invalid = MakeReply(request, 2, ConsensusMessageType::ZYZZYVA_SPEC_RESPONSE, m_history);
        invalid.payloadBytes.back() ^= 1;
        transport->Inject(2, invalid.ToPacket());
    }

    void AssertStillPending(Ptr<ConsensusClientApplication> client) {
        NS_TEST_ASSERT_MSG_EQ(client->GetCompletedRequests(), 0,
                              "An invalid Zyzzyva response signature was counted toward fast completion");
    }

    void InjectThirdValidReply(Ptr<RecordingConsensusTransport> transport) {
        ConsensusMessage request;
        if (transport->packets.empty() || !ConsensusMessage::FromPacket(transport->packets.front(), request)) return;
        ConsensusMessage reply = MakeReply(request, 2, ConsensusMessageType::ZYZZYVA_SPEC_RESPONSE, m_history);
        transport->Inject(2, reply.ToPacket());
    }

    void InjectLocalCommits(Ptr<RecordingConsensusTransport> transport) {
        ConsensusMessage request;
        if (transport->packets.empty() || !ConsensusMessage::FromPacket(transport->packets.front(), request)) return;
        for (uint32_t sender : {0U, 1U, 2U}) {
            ConsensusMessage reply = MakeReply(request, sender, ConsensusMessageType::ZYZZYVA_LOCAL_COMMIT, m_history);
            transport->Inject(sender, reply.ToPacket());
        }
    }

    void AssertFastPathWait(Ptr<RecordingConsensusTransport> transport) {
        for (const auto& packet : transport->packets) {
            ConsensusMessage message;
            if (ConsensusMessage::FromPacket(packet, message))
                NS_TEST_ASSERT_MSG_NE(static_cast<uint16_t>(message.type), static_cast<uint16_t>(ConsensusMessageType::ZYZZYVA_COMMIT_REQUEST),
                                      "The client sent a slow certificate before the fast-path timeout");
        }
    }

    void DoRun() override {
        Ptr<Node> node = CreateObject<Node>();
        Ptr<RecordingConsensusTransport> transport = CreateObject<RecordingConsensusTransport>();
        Ptr<ConsensusClientApplication> client = CreateObject<ConsensusClientApplication>();
        client->SetTransport(transport);
        client->SetReplicaIds({0, 1, 2, 3});
        client->SetAttribute("ZyzzyvaClient", BooleanValue(true));
        client->SetAttribute("AutoGenerate", BooleanValue(false));
        client->SetAttribute("RequestTimeout", TimeValue(Seconds(5)));
        client->SetAttribute("FastPathTimeout", TimeValue(MilliSeconds(3)));
        node->AddApplication(client);
        client->SetStartTime(Seconds(0));
        client->SetStopTime(Seconds(6));
        Simulator::Schedule(MilliSeconds(1), &ZyzzyvaClientCertificateTestCase::Submit, this, client);
        Simulator::Schedule(MilliSeconds(2), &ZyzzyvaClientCertificateTestCase::InjectInitialReplies, this, transport);
        Simulator::Schedule(MicroSeconds(2500), &ZyzzyvaClientCertificateTestCase::AssertStillPending, this, client);
        Simulator::Schedule(MilliSeconds(3), &ZyzzyvaClientCertificateTestCase::InjectThirdValidReply, this, transport);
        Simulator::Schedule(MicroSeconds(3500), &ZyzzyvaClientCertificateTestCase::AssertFastPathWait, this, transport);
        Simulator::Schedule(MicroSeconds(4500), &ZyzzyvaClientCertificateTestCase::InjectLocalCommits, this, transport);
        Simulator::Stop(MilliSeconds(5));
        Simulator::Run();

        uint32_t commitRequests = 0;
        for (const Ptr<Packet>& packet : transport->packets) {
            ConsensusMessage message;
            if (!ConsensusMessage::FromPacket(packet, message) ||
                message.type != ConsensusMessageType::ZYZZYVA_COMMIT_REQUEST) continue;
            ++commitRequests;
            NS_TEST_ASSERT_MSG_EQ((message.payloadBytes.size() >= 12), true,
                                  "The client emitted a truncated slow-path certificate");
            NS_TEST_ASSERT_MSG_EQ(message.payloadBytes[8], 0, "The client certificate has an invalid signer count");
            NS_TEST_ASSERT_MSG_EQ(message.payloadBytes[9], 0, "The client certificate has an invalid signer count");
            NS_TEST_ASSERT_MSG_EQ(message.payloadBytes[10], 0, "The client certificate has an invalid signer count");
            NS_TEST_ASSERT_MSG_EQ(message.payloadBytes[11], 3, "The invalid reply was included in the certificate");
        }
        NS_TEST_ASSERT_MSG_EQ(m_requestId, 1, "The test client did not submit its request");
        NS_TEST_ASSERT_MSG_EQ((m_digest != 0), true, "The request digest was not captured");
        NS_TEST_ASSERT_MSG_EQ(commitRequests, 4, "A valid 2f+1 slow-path certificate was not sent to every replica");
        NS_TEST_ASSERT_MSG_EQ(client->GetCompletedRequests(), 1,
                              "Matching local commits did not finish the valid slow path");
        Simulator::Destroy();
    }

    uint64_t m_requestId{0};
    uint64_t m_digest{0};
    uint64_t m_history{0};
};

class ZyzzyvaRecoveryTestCase : public TestCase {
  public:
    ZyzzyvaRecoveryTestCase() : TestCase("Original and Velma recovery diverge on the known multi-view certificate pattern") {}
  private:
    std::vector<ZyzzyvaEngine::Entry> ReferenceVelma(
        const std::vector<ZyzzyvaEngine::ViewProof>& proofs, uint32_t f) {
        struct C { const std::vector<ZyzzyvaEngine::Entry>* log; size_t length; int64_t fast{-1}; int64_t slow{-1}; };
        auto prefixEqual = [](const std::vector<ZyzzyvaEngine::Entry>& a,
                              const std::vector<ZyzzyvaEngine::Entry>& b, size_t n) {
            if (n > a.size() || n > b.size()) return false;
            for (size_t i=0;i<n;++i)
                if (a[i].sequence!=b[i].sequence || a[i].digest!=b[i].digest || a[i].history!=b[i].history) return false;
            return true;
        };
        std::vector<C> cs{{nullptr,0,-1,-1}};
        for (const auto& p:proofs) for (size_t n=1;n<=p.log.size();++n) {
            bool found=false;
            for (const auto& c:cs) if (c.length==n && prefixEqual(*c.log,p.log,n)) { found=true; break; }
            if (!found) cs.push_back({&p.log,n,-1,-1});
        }
        for (auto& c:cs) {
            std::map<uint64_t,uint32_t> support;
            for (const auto& p:proofs) if (p.log.size()>=c.length &&
                (c.length==0 || prefixEqual(p.log,*c.log,c.length)))
                ++support[c.length ? p.log[c.length-1].view : 0];
            uint32_t votes=0;
            for (auto it=support.rbegin();it!=support.rend();++it) {
                votes+=it->second; if (votes>=f+1) { c.fast=it->first; break; }
            }
            for (const auto& p:proofs) {
                const auto& cert=p.certificate;
                if (cert.voters.size()<2*f+1 || !cert.sequence || cert.sequence>c.length || cert.sequence>p.log.size()) continue;
                if (prefixEqual(*c.log,p.log,cert.sequence) && (*c.log)[cert.sequence-1].history==cert.history)
                    c.slow=std::max(c.slow,static_cast<int64_t>(cert.view));
            }
        }
        int64_t mf=-1,ms=-1; for (const auto& c:cs) { mf=std::max(mf,c.fast); ms=std::max(ms,c.slow); }
        const C* selected=&cs.front();
        for (const auto& c:cs) {
            bool safe=false;
            auto extends=[&](const C& x,const C& y){return x.length>=y.length && (y.length==0 || prefixEqual(*x.log,*y.log,y.length));};
            if (c.slow==ms && c.slow>mf) safe=true;
            if (c.fast==mf && c.fast>ms) safe=true;
            if (c.slow==ms && c.slow==mf) { safe=true; for (const auto& o:cs) if(o.fast==mf&&!extends(c,o)) safe=false; }
            if (c.fast==mf && c.fast==ms) { bool okay=true; for(const auto& o:cs) if(o.slow==ms&&!extends(c,o)) okay=false; safe=safe||okay; }
            if (mf<0 && ms<0) safe=true;
            if(safe&&c.length>selected->length) selected=&c;
        }
        if (!selected->length) return {};
        return {selected->log->begin(),selected->log->begin()+selected->length};
    }
    void CheckSelectorEquivalence() {
        ZyzzyvaEngine engine;
        engine.m_faultTolerance=1;
        std::vector<ZyzzyvaEngine::ViewProof> proofs;
        for (uint32_t i=0;i<8;++i) {
            ZyzzyvaEngine::ViewProof p;
            p.sender=i;
            uint64_t previous=0;
            const uint32_t length=(i%4)+1;
            for(uint32_t n=1;n<=length;++n) {
                ZyzzyvaEngine::Entry e; e.sequence=n; e.view=(i+n)%4; e.digest=(n==1?100+i/3:200+n);
                e.history=ZyzzyvaEngine::Extend(previous,e.digest,n); previous=e.history; p.log.push_back(e);
            }
            if(i%3==0&&!p.log.empty()) {
                p.certificate.view=i%4; p.certificate.sequence=1;
                p.certificate.history=p.log.front().history; p.certificate.voters={0,1,2};
            }
            proofs.push_back(p);
        }
        for (size_t n=1;n<=proofs.size();++n) {
            std::vector<ZyzzyvaEngine::ViewProof> subset(proofs.begin(),proofs.begin()+n);
            auto optimized=engine.SelectVelma(subset), reference=ReferenceVelma(subset,1);
            NS_TEST_ASSERT_MSG_EQ(optimized.size(),reference.size(),"Indexed Velma selector changed selected history length");
            for(size_t i=0;i<optimized.size()&&i<reference.size();++i)
                NS_TEST_ASSERT_MSG_EQ(optimized[i].history,reference[i].history,"Indexed Velma selector changed selected prefix");
        }
    }
    void CompleteFastHistory(Ptr<ConsensusClientApplication> client,
                             Ptr<RecordingConsensusTransport> transport) {
        uint64_t requestId = client->SubmitRequest(1024, 1);
        ConsensusMessage request;
        if (transport->packets.empty() ||
            !ConsensusMessage::FromPacket(transport->packets.front(), request)) return;
        m_fastDigest = request.digest;
        uint64_t history = ZyzzyvaEngine::Extend(0, request.digest, 1);
        for (uint32_t replica = 0; replica < 4; ++replica) {
            ConsensusMessage response;
            response.protocol = ConsensusProtocol::ZYZZYVA;
            response.type = ConsensusMessageType::ZYZZYVA_SPEC_RESPONSE;
            response.senderId = replica;
            response.receiverId = 1000;
            response.view = 2;
            response.instanceId = 1;
            response.digest = request.digest;
            response.clientId = 1000;
            response.requestId = requestId;
            response.transactionCount = 1;
            ZyzzyvaEngine::Write64(response.payloadBytes, history);
            if (!ZyzzyvaAuth::Sign(response)) return;
            transport->Inject(replica, response.ToPacket());
            if (replica == 0)
                Simulator::ScheduleNow(&ZyzzyvaRecoveryTestCase::CheckRouting, this, client, transport, 0);
        }
        Simulator::ScheduleNow(&ZyzzyvaRecoveryTestCase::CheckRouting, this, client, transport, 2);
    }
    void CheckRouting(Ptr<ConsensusClientApplication> client, Ptr<RecordingConsensusTransport> transport, uint32_t expected) {
        client->SubmitRequest(512, 1);
        ConsensusMessage probe;
        NS_TEST_ASSERT_MSG_EQ(ConsensusMessage::FromPacket(transport->packets.back(), probe), true, "Routing probe was not sent");
        NS_TEST_ASSERT_MSG_EQ(probe.receiverId, expected, "Client routing changed without a quorum or failed to follow the confirmed primary");
    }
    void OnRecoveryPhase(uint32_t, uint8_t, uint64_t view, uint16_t phase, Time) {
        if (view == 3 && phase >= ZYZZYVA_NEW_VIEW_INSTALLED && phase <= ZYZZYVA_RECOVERY_COMPLETED)
            m_recoveryPhases[phase] = Simulator::Now();
    }
    void DoRun() override {
        CheckSelectorEquivalence();
        Ptr<Node> node = CreateObject<Node>();
        Ptr<RecordingConsensusTransport> transport = CreateObject<RecordingConsensusTransport>();
        Ptr<ConsensusClientApplication> client = CreateObject<ConsensusClientApplication>();
        transport->SetAttribute("ReceiveDelay", TimeValue(Seconds(0)));
        client->SetTransport(transport);
        client->SetReplicaIds({0, 1, 2, 3});
        client->SetAttribute("ZyzzyvaClient", BooleanValue(true));
        client->SetAttribute("AutoGenerate", BooleanValue(false));
        node->AddApplication(client);
        client->SetStartTime(Seconds(0));
        client->SetStopTime(MilliSeconds(3));
        Simulator::Schedule(MilliSeconds(1), &ZyzzyvaRecoveryTestCase::CompleteFastHistory,
                            this, client, transport);
        Simulator::Stop(MilliSeconds(3));
        Simulator::Run();
        m_fastCompleted = client->GetCompletedRequests() == 1;
        NS_TEST_ASSERT_MSG_EQ((m_fastDigest != 0), true, "The signed Zyzzyva request was not generated");
        NS_TEST_ASSERT_MSG_EQ(m_fastCompleted, true, "A correct client failed to complete the view-two fast history");
        Simulator::Destroy();

        Ptr<ZyzzyvaEngine> engine = CreateObject<ZyzzyvaEngine>();
        engine->Configure(0, {0, 1, 2, 3});
        ZyzzyvaEngine::Entry a;
        a.sequence = 1;
        a.view = 1;
        a.digest = 11;
        a.history = ZyzzyvaEngine::Extend(0, a.digest, 1);
        a.client = 1000;
        a.request = 1;
        a.transactions = 1;
        ZyzzyvaEngine::Entry b;
        b.sequence = 1;
        b.view = 2;
        b.digest = m_fastDigest;
        b.history = ZyzzyvaEngine::Extend(0, b.digest, 1);
        ZyzzyvaEngine::ViewProof first;
        first.sender = 0;
        first.targetView = 3;
        first.log = {a};
        first.certificate.view = 1;
        first.certificate.sequence = 1;
        first.certificate.digest = a.digest;
        first.certificate.history = a.history;
        first.certificate.voters = {0, 1, 2};
        for (uint32_t voter : first.certificate.voters) {
            ConsensusMessage response;
            response.protocol = ConsensusProtocol::ZYZZYVA;
            response.type = ConsensusMessageType::ZYZZYVA_SPEC_RESPONSE;
            response.senderId = voter;
            response.receiverId = a.client;
            response.view = a.view;
            response.instanceId = a.sequence;
            response.digest = a.digest;
            response.clientId = a.client;
            response.requestId = a.request;
            response.transactionCount = a.transactions;
            ZyzzyvaEngine::Write64(response.payloadBytes, a.history);
            NS_TEST_ASSERT_MSG_EQ(ZyzzyvaAuth::Sign(response), true, "Counterexample certificate signing failed");
            first.certificate.signatures[voter] = response.authentication;
        }
        ZyzzyvaEngine::ViewProof second;
        second.sender = 2;
        second.targetView = 3;
        second.log = {b};
        ZyzzyvaEngine::ViewProof third;
        third.sender = 3;
        third.targetView = 3;
        third.log = {b};
        std::vector<uint8_t> hateData;
        ZyzzyvaEngine::Write64(hateData, 2);
        for (auto* proof : {&first, &second, &third}) {
            for (uint32_t voter : {0U, 2U}) {
                std::vector<uint8_t> signature;
                NS_TEST_ASSERT_MSG_EQ(ZyzzyvaAuth::SignDetached(voter, hateData, signature), true,
                                      "Counterexample view-change authorization failed");
                proof->hateProofs[voter] = signature;
            }
            NS_TEST_ASSERT_MSG_EQ(engine->ValidateProof(*proof), true, "Counterexample proof failed verification");
        }
        std::vector<ZyzzyvaEngine::ViewProof> proofs{first, second, third};
        auto original = engine->SelectOriginal(proofs);
        auto corrected = engine->SelectVelma(proofs);
        NS_TEST_ASSERT_MSG_EQ(original.size(), 1, "Original recovery failed to select a history");
        NS_TEST_ASSERT_MSG_EQ(corrected.size(), 1, "Velma recovery failed to select a history");
        NS_TEST_ASSERT_MSG_EQ(original[0].digest, a.digest, "Original recovery did not prefer the stale certificate");
        NS_TEST_ASSERT_MSG_EQ(corrected[0].digest, b.digest, "Velma recovery failed to prefer newer fast evidence");

        Ptr<ConsensusResourceModel> recoveryCpu = CreateObject<ConsensusResourceModel>();
        recoveryCpu->SetAttribute("Mode", EnumValue(RESOURCE_QUEUED));
        engine->SetResourceModel(recoveryCpu);
        engine->SetAttribute("RollbackTimePerTransaction", TimeValue(MicroSeconds(20)));
        engine->SetAttribute("ExecutionTimePerTransaction", TimeValue(MicroSeconds(30)));
        engine->TraceConnectWithoutContext("ProtocolPhase", MakeCallback(&ZyzzyvaRecoveryTestCase::OnRecoveryPhase, this));
        engine->Start();
        engine->m_log = {a};
        engine->m_applicationHistory = {123};
        recoveryCpu->SubmitCpuTask(MicroSeconds(100), Callback<void>([]() {}));
        ConsensusMessage newView;
        newView.senderId = 3;
        newView.view = 3;
        ZyzzyvaEngine::Write32(newView.payloadBytes, proofs.size());
        for (const auto& proof : proofs) ZyzzyvaEngine::EncodeProof(newView.payloadBytes, proof);
        ZyzzyvaEngine::Write32(newView.payloadBytes, corrected.size());
        for (const auto& entry : corrected) ZyzzyvaEngine::EncodeEntry(newView.payloadBytes, entry);
        engine->HandleNewView(newView);
        NS_TEST_ASSERT_MSG_EQ(engine->m_recovering, true, "New-view installation incorrectly completed application recovery");
        NS_TEST_ASSERT_MSG_EQ(m_recoveryPhases.count(ZYZZYVA_RECOVERY_COMPLETED), 0, "Recovery completed before undo and replay ran");
        Simulator::Stop(MicroSeconds(200));
        Simulator::Run();
        NS_TEST_ASSERT_MSG_EQ(m_recoveryPhases.size(), 4, "A recovery lifecycle event was omitted");
        NS_TEST_ASSERT_MSG_EQ(m_recoveryPhases.at(ZYZZYVA_UNDO_COMPLETED), MicroSeconds(120), "Undo event omitted queued CPU service");
        NS_TEST_ASSERT_MSG_EQ(m_recoveryPhases.at(ZYZZYVA_RECOVERY_COMPLETED), MicroSeconds(150), "Recovery completion omitted replay service");
        NS_TEST_ASSERT_MSG_EQ(engine->m_recovering, false, "Application remained in recovery after replay completed");
        engine->Stop();
        Simulator::Destroy();

        Ptr<ZyzzyvaEngine> gap = CreateObject<ZyzzyvaEngine>();
        gap->Configure(1, {0, 1, 2, 3});
        gap->Start();
        ConsensusMessage order1;
        order1.protocol = ConsensusProtocol::ZYZZYVA;
        order1.type = ConsensusMessageType::ZYZZYVA_ORDER_REQ;
        order1.senderId = 0;
        order1.view = 0;
        order1.instanceId = 1;
        order1.digest = 101;
        order1.clientId = 1000;
        order1.requestId = 1;
        order1.transactionCount = 1;
        uint64_t firstHistory = ZyzzyvaEngine::Extend(0, order1.digest, 1);
        ZyzzyvaEngine::Write64(order1.payloadBytes, firstHistory);
        ConsensusMessage order2 = order1;
        order2.instanceId = 2;
        order2.digest = 202;
        order2.requestId = 2;
        order2.payloadBytes.clear();
        ZyzzyvaEngine::Write64(order2.payloadBytes,
                                ZyzzyvaEngine::Extend(firstHistory, order2.digest, 2));
        gap->Receive(order2);
        NS_TEST_ASSERT_MSG_EQ(gap->m_log.size(), 0, "A future ordering request bypassed its missing predecessor");
        gap->Receive(order1);
        NS_TEST_ASSERT_MSG_EQ(gap->m_log.size(), 2, "A repaired gap did not release the buffered ordering request");
        gap->Stop();
    }
    uint64_t m_fastDigest{0};
    bool m_fastCompleted{false};
    std::map<uint16_t, Time> m_recoveryPhases;
};

class ZyzzyvaProgressTestCase : public TestCase {
  public:
    ZyzzyvaProgressTestCase() : TestCase("Zyzzyva continues after one accusation, repairs authenticated gaps, and escapes a missing NEW-VIEW") {}
  private:
    void Deliver(uint32_t peer, ConsensusMessage message) {
        if (message.type == ConsensusMessageType::NEW_VIEW && message.view == 1) return;
        if (message.type == ConsensusMessageType::NEW_VIEW && message.view >= 2) m_recovered = true;
        m_engines.at(peer)->Receive(message);
    }
    void Send(uint32_t sender, uint32_t peer, const ConsensusMessage& input) {
        ConsensusMessage message = input;
        message.senderId = sender;
        message.receiverId = peer;
        if (!ZyzzyvaAuth::Sign(message)) return;
        ConsensusMessage verified;
        if (!ConsensusMessage::FromPacket(message.ToPacket(), verified) || !ZyzzyvaAuth::Verify(verified)) return;
        Simulator::Schedule(MicroSeconds(10), &ZyzzyvaProgressTestCase::Deliver, this, peer, verified);
    }
    void Accuse() {
        m_engines[0]->TriggerHate();
        m_engines[2]->TriggerHate();
    }
    void DoRun() override {
        Ptr<ZyzzyvaEngine> one = CreateObject<ZyzzyvaEngine>();
        one->Configure(1, {0, 1, 2, 3});
        one->Start();
        one->TriggerHate();
        ConsensusMessage outsider;
        outsider.protocol = ConsensusProtocol::ZYZZYVA;
        outsider.type = ConsensusMessageType::ZYZZYVA_HATE_PRIMARY;
        outsider.senderId = 1000;
        std::vector<uint8_t> accusation;
        ZyzzyvaEngine::Write64(accusation, 0);
        ZyzzyvaAuth::SignDetached(1000, accusation, outsider.payloadBytes);
        one->Receive(outsider);
        NS_TEST_ASSERT_MSG_EQ(one->m_pendingView, 0, "A client signature was counted as a replica accusation vote");
        ConsensusMessage order;
        order.protocol = ConsensusProtocol::ZYZZYVA;
        order.type = ConsensusMessageType::ZYZZYVA_ORDER_REQ;
        order.senderId = 0;
        order.receiverId = 1;
        order.instanceId = 1;
        order.digest = 101;
        order.clientId = 1000;
        order.requestId = 1;
        order.transactionCount = 1;
        ZyzzyvaEngine::Write64(order.payloadBytes, ZyzzyvaEngine::Extend(0, 101, 1));
        one->Receive(order);
        NS_TEST_ASSERT_MSG_EQ(one->GetLogSize(), 1, "One HATE vote stopped normal speculative processing");
        one->Stop();

        Ptr<ZyzzyvaEngine> authenticated = CreateObject<ZyzzyvaEngine>();
        authenticated->Configure(1, {0, 1, 2, 3});
        authenticated->SetAttribute("RequireClientAuthentication", BooleanValue(true));
        authenticated->Start();
        ClientRequest clientRequest{1000, 1, 512, 1, 101};
        ZyzzyvaAuth::SignClientRequest(clientRequest);
        auto body = ZyzzyvaAuth::EncodeClientRequest(clientRequest);
        order.payloadBytes.insert(order.payloadBytes.end(), body.begin(), body.end());
        ConsensusMessage forged = order;
        forged.digest = 202;
        ZyzzyvaAuth::Sign(forged);
        ConsensusMessage verified;
        ConsensusMessage::FromPacket(forged.ToPacket(), verified);
        NS_TEST_ASSERT_MSG_EQ(ZyzzyvaAuth::Verify(verified), true, "The modeled malicious Primary signature was invalid");
        authenticated->Receive(verified);
        NS_TEST_ASSERT_MSG_EQ(authenticated->GetLogSize(), 0, "The Primary forged an honest client's content signature");
        ConsensusMessage valid = order;
        ZyzzyvaAuth::Sign(valid);
        ConsensusMessage::FromPacket(valid.ToPacket(), verified);
        NS_TEST_ASSERT_MSG_EQ(ZyzzyvaAuth::Verify(verified), true, "Valid Primary proposal signature was rejected");
        authenticated->Receive(verified);
        NS_TEST_ASSERT_MSG_EQ(authenticated->GetLogSize(), 1, "An authenticated client/Primary proposal pair was rejected");
        authenticated->Stop();
        order.payloadBytes.resize(8); // Retain the isolated legacy repair fixture below.

        Ptr<ZyzzyvaEngine> repaired = CreateObject<ZyzzyvaEngine>();
        repaired->Configure(2, {0, 1, 2, 3});
        repaired->Start();
        NS_TEST_ASSERT_MSG_EQ(ZyzzyvaAuth::Sign(order), true, "Primary proposal signing failed");
        ConsensusMessage wrapper;
        wrapper.protocol = ConsensusProtocol::ZYZZYVA;
        wrapper.type = ConsensusMessageType::ZYZZYVA_REPAIR;
        wrapper.senderId = 3;
        wrapper.instanceId = 1;
        Ptr<Packet> packet = order.ToPacket();
        wrapper.payloadBytes.resize(packet->GetSize());
        packet->CopyData(wrapper.payloadBytes.data(), wrapper.payloadBytes.size());
        ConsensusMessage invalid = wrapper;
        invalid.payloadBytes.back() ^= 1;
        repaired->Receive(invalid);
        NS_TEST_ASSERT_MSG_EQ(repaired->GetLogSize(), 0, "Invalid inner primary signature was accepted");
        repaired->Receive(wrapper);
        NS_TEST_ASSERT_MSG_EQ(repaired->GetLogSize(), 1, "Backup repair lost the primary's original content binding");
        repaired->Stop();
        Simulator::Destroy();

        for (uint32_t id = 0; id < 4; ++id) {
            Ptr<ZyzzyvaEngine> engine = CreateObject<ZyzzyvaEngine>();
            engine->Configure(id, {0, 1, 2, 3});
            engine->SetAttribute("InstanceTimeout", TimeValue(MilliSeconds(2)));
            engine->SetSendCallback(MakeCallback(&ZyzzyvaProgressTestCase::Send, this).Bind(id));
            engine->Start();
            m_engines.push_back(engine);
        }
        Simulator::Schedule(MilliSeconds(1), &ZyzzyvaProgressTestCase::Accuse, this);
        Simulator::Stop(MilliSeconds(30));
        Simulator::Run();
        NS_TEST_ASSERT_MSG_EQ(m_recovered, true, "A missing NEW-VIEW left the replicas stuck indefinitely");
        for (const auto& engine : m_engines) {
            NS_TEST_ASSERT_MSG_EQ((engine->m_view >= 2), true, "Replica did not install the next available view");
            engine->Stop();
        }
        Simulator::Destroy();
        m_engines.clear();
    }
    std::vector<Ptr<ZyzzyvaEngine>> m_engines;
    bool m_recovered{false};
};

class ZyzzyvaRepairBackpressureTestCase : public TestCase {
  public:
    ZyzzyvaRepairBackpressureTestCase()
        : TestCase("Repeated future orders merge repair requests and retry only on timeout") {}
  private:
    void OnSend(uint32_t peer, const ConsensusMessage& message) {
        if (message.type == ConsensusMessageType::ZYZZYVA_FILL_HOLE) ++m_fillMessages;
    }
    void DoRun() override {
        auto engine = CreateObject<ZyzzyvaEngine>();
        engine->Configure(1, {0, 1, 2, 3});
        engine->SetAttribute("InstanceTimeout", TimeValue(MilliSeconds(1)));
        engine->SetAttribute("ViewChangeEnabled", BooleanValue(false));
        engine->SetSendCallback(MakeCallback(&ZyzzyvaRepairBackpressureTestCase::OnSend, this));
        engine->Start();
        ConsensusMessage order;
        order.protocol = ConsensusProtocol::ZYZZYVA;
        order.type = ConsensusMessageType::ZYZZYVA_ORDER_REQ;
        order.senderId = 0;
        order.payloadBytes.resize(8);
        for (uint64_t slot = 3; slot <= 100; ++slot) {
            order.instanceId = slot;
            order.digest = slot;
            engine->Receive(order);
            engine->Receive(order); // Duplicate traffic must not amplify a gap.
        }
        NS_TEST_ASSERT_MSG_EQ(m_fillMessages, 3, "One missing prefix generated repeated repair broadcasts");
        Simulator::Stop(MicroSeconds(2500));
        Simulator::Run();
        NS_TEST_ASSERT_MSG_EQ(m_fillMessages, 9, "Missing-prefix repairs were not retried once per timeout");
        NS_TEST_ASSERT_MSG_EQ(engine->GetLogSize(), 0, "A gap was incorrectly treated as an executed request");
        NS_TEST_ASSERT_MSG_EQ(engine->GetLeaderId(), 0, "The ablation changed views while retrying a repair");
        engine->Stop();
        Simulator::Destroy();
    }
    uint32_t m_fillMessages{0};
};

class ZyzzyvaRecoveryTestSuite : public TestSuite {
  public:
    ZyzzyvaRecoveryTestSuite() : TestSuite("zyzzyva-recovery", Type::UNIT) {
        AddTestCase(new IorsWireAuthenticationTestCase(), TestCase::QUICK);
        AddTestCase(new BidlAuthenticationTestCase(), TestCase::QUICK);
        AddTestCase(new BidlRepairSourceTestCase(), TestCase::QUICK);
        AddTestCase(new BidlCanonicalRecoveryTestCase(), TestCase::QUICK);
        AddTestCase(new BidlClientAbortTestCase(), TestCase::QUICK);
        AddTestCase(new ZyzzyvaClientCertificateTestCase(), TestCase::QUICK);
        AddTestCase(new ZyzzyvaRecoveryTestCase(), TestCase::QUICK);
        AddTestCase(new ZyzzyvaProgressTestCase(), TestCase::QUICK);
        AddTestCase(new ZyzzyvaRepairBackpressureTestCase(), TestCase::QUICK);
    }
};
static ZyzzyvaRecoveryTestSuite g_zyzzyvaRecoveryTestSuite;
} // namespace ns3
