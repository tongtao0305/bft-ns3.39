#include "ns3/bidl-application.h"
#include "ns3/broadcom-egress-queue.h"
#include "ns3/consensus-experiment.h"
#include "ns3/consensus-transport.h"
#include "ns3/double.h"
#include "ns3/enum.h"
#include "ns3/inet-socket-address.h"
#include "ns3/qbb-header.h"
#include "ns3/rdma-driver.h"
#include "ns3/simulator.h"
#include "ns3/test.h"

namespace ns3 {

class RuntimeRecordingTransport : public ConsensusTransport {
  public:
    void Start(Ptr<Node> node) override {}
    void Stop() override {}
    void Send(uint32_t peer, Ptr<Packet> packet, const ConsensusTransportMetadata& metadata) override {}
};

class BidlReplayAdmissionTestCase : public TestCase {
  public:
    BidlReplayAdmissionTestCase() : TestCase("BIDL rejects pending and verified hash replays before occupying another sequence") {}
  private:
    void DoRun() override {
        auto application = CreateObject<BidlApplication>();
        application->SetNodeId(100);
        application->SetRole(BIDL_EXECUTOR);
        application->SetSequencerIds({200});
        application->SetOrdererIds({0, 1, 2, 3});
        application->SetExecutorIds({100});
        application->SetTransport(CreateObject<RuntimeRecordingTransport>());
        auto resources = CreateObject<ConsensusResourceModel>();
        resources->SetAttribute("Mode", EnumValue(RESOURCE_QUEUED));
        application->SetResourceModel(resources);
        application->m_running = true;
        ClientRequest first{1000, 1, 256, 1, 101};
        ClientRequest second{1000, 2, 256, 1, 102};
        BidlApplication::SignRequest(first);
        BidlApplication::SignRequest(second);
        auto makeMessage = [](uint64_t sequence, const ClientRequest& request) {
            ConsensusMessage message;
            message.type = ConsensusMessageType::BIDL_TRANSACTION;
            message.senderId = 200;
            message.instanceId = sequence;
            message.payloadSize = request.payloadSize;
            message.clientId = request.clientId;
            message.requestId = request.requestId;
            message.digest = request.digest;
            message.transactionCount = 1;
            message.payloadBytes = BidlApplication::EncodeTransactions({{sequence, request, 1}});
            return message;
        };
        application->VerifyTransaction(makeMessage(1, first));
        application->VerifyTransaction(makeMessage(2, first));
        NS_TEST_ASSERT_MSG_EQ(application->m_verificationPending.count(2), 0, "Pending replay occupied the next sequence");
        application->VerifyTransaction(makeMessage(2, second));
        NS_TEST_ASSERT_MSG_EQ(application->m_verificationDigests.at(2), second.digest, "Replay displaced a genuine transaction");
        Simulator::Stop(MicroSeconds(100));
        Simulator::Run();
        NS_TEST_ASSERT_MSG_EQ(application->m_transactions.at(1).digest, first.digest, "First transaction was lost");
        NS_TEST_ASSERT_MSG_EQ(application->m_transactions.at(2).digest, second.digest, "Genuine next transaction was lost");
        application->VerifyTransaction(makeMessage(3, first));
        NS_TEST_ASSERT_MSG_EQ(application->m_verificationPending.count(3), 0, "Verified replay entered the CPU queue");
        application->StopApplication();
        Simulator::Destroy();
    }
};

class RdmaCompletedAckTestCase : public TestCase {
  public:
    RdmaCompletedAckTestCase() : TestCase("Final marked ACK never restarts timers or completes a QP twice") {}
  private:
    void Completed(Ptr<RdmaQueuePair> queuePair) { ++m_completed; }
    void AppCompleted() { ++m_appCompleted; }
    void DoRun() override {
        for (bool marked : {false, true}) {
            m_completed = m_appCompleted = 0;
            auto node = CreateObject<Node>();
            node->SetNodeType(0);
            auto device = CreateObject<QbbNetDevice>();
            device->SetNode(node);
            device->SetQueue(CreateObject<BEgressQueue>());
            auto hardware = CreateObject<RdmaHw>();
            hardware->SetNode(node);
            hardware->m_cc_mode = 1;
            hardware->m_ack_interval = 1;
            hardware->SetAttribute("AlphaResumInterval", DoubleValue(1.0));
            hardware->m_nic.emplace_back(device);
            hardware->m_nic.back().qpGrp = CreateObject<RdmaQueuePairGroup>();
            hardware->Setup(MakeCallback(&RdmaCompletedAckTestCase::Completed, this));
            auto queuePair = CreateObject<RdmaQueuePair>(3, Ipv4Address("10.0.0.1"), Ipv4Address("10.0.0.2"), 100, 200);
            queuePair->SetSize(100);
            queuePair->m_rate = DataRate("10Gbps");
            queuePair->SetAppNotifyCallback(MakeCallback(&RdmaCompletedAckTestCase::AppCompleted, this));
            hardware->m_qpMap[RdmaHw::GetQpKey(queuePair->dip.Get(), queuePair->sport, 3)] = queuePair;
            hardware->m_rtTable[queuePair->dip.Get()] = {0};
            CustomHeader header;
            header.l3Prot = 0xFC;
            header.sip = queuePair->dip.Get();
            header.ack.pg = 3;
            header.ack.dport = 100;
            header.ack.seq = 100;
            header.ack.flags = marked ? (1 << qbbHeader::FLAG_CNP) : 0;
            hardware->ReceiveAck(Create<Packet>(), header);
            hardware->QpComplete(queuePair);
            hardware->cnp_received_mlx(queuePair);
            Simulator::Stop(MicroSeconds(100));
            Simulator::Run();
            NS_TEST_ASSERT_MSG_EQ(m_completed, 1, "QP completed more than once");
            NS_TEST_ASSERT_MSG_EQ(m_appCompleted, 1, "Application completion was duplicated");
            NS_TEST_ASSERT_MSG_EQ(hardware->m_qpMap.empty(), true, "Completed QP remains registered");
            NS_TEST_ASSERT_MSG_EQ(queuePair->mlx.m_eventUpdateAlpha.IsExpired(), true, "Alpha timer survived completion");
            NS_TEST_ASSERT_MSG_EQ(queuePair->mlx.m_eventDecreaseRate.IsExpired(), true, "Decrease timer survived completion");
            NS_TEST_ASSERT_MSG_EQ(queuePair->mlx.m_rpTimer.IsExpired(), true, "Rate timer survived completion");
            Simulator::Destroy();
        }
    }
    uint32_t m_completed{0};
    uint32_t m_appCompleted{0};
};

class RuntimeMessageTestCase : public TestCase {
  public:
    RuntimeMessageTestCase() : TestCase("Header-only decoding avoids payload allocation and preserves extension stripping") {}
  private:
    void DoRun() override {
        ConsensusMessage message;
        message.type = ConsensusMessageType::RDMA_BACKGROUND_FLOW;
        message.payloadBytes.assign(8192, 37);
        message.iorsPresent = true;
        message.iorsMarked = true;
        auto packet = message.ToPacket();
        ConsensusMessage decoded;
        decoded.payloadBytes.assign(100, 1);
        NS_TEST_ASSERT_MSG_EQ(ConsensusMessage::FromPacket(packet, decoded, false), true, "Header-only decode failed");
        NS_TEST_ASSERT_MSG_EQ(decoded.payloadBytes.empty(), true, "Header-only decode retained payload");
        NS_TEST_ASSERT_MSG_EQ(decoded.payloadSize, 8192, "Logical payload length changed");
        ConsensusMessage::StripIorsExtension(packet);
        NS_TEST_ASSERT_MSG_EQ(ConsensusMessage::FromPacket(packet, decoded), true, "Stripped frame is invalid");
        NS_TEST_ASSERT_MSG_EQ(decoded.iorsPresent, false, "Extension survived stripping");
        NS_TEST_ASSERT_MSG_EQ((decoded.payloadBytes == message.payloadBytes), true, "Stripping changed payload bytes");
    }
};

class RuntimeTransportTestCase : public TestCase {
  public:
    RuntimeTransportTestCase() : TestCase("Shared RDMA hardware keeps transport ports distinct and cancels owned QPs") {}
  private:
    void Receive(uint32_t peer, Ptr<Packet> packet) { ++m_delivered; }
    void Send(Ptr<RdmaConsensusTransport> transport, uint32_t sender, uint32_t bytes) {
        ConsensusMessage message;
        message.senderId = sender;
        message.receiverId = 2;
        message.type = ConsensusMessageType::PRE_PREPARE;
        message.payloadSize = bytes;
        transport->Send(2, message.ToPacket(), ConsensusTransportMetadata::FromMessage(message));
    }
    void SendConcurrent(Ptr<RdmaConsensusTransport> first, Ptr<RdmaConsensusTransport> second, Ptr<RdmaHw> hardware) {
        Send(first, 1, 4096);
        Send(first, 1, 4096);
        Send(second, 3, 4096);
        Simulator::ScheduleNow(&RuntimeTransportTestCase::CheckConcurrent, this, hardware);
    }
    void CheckConcurrent(Ptr<RdmaHw> hardware) {
        NS_TEST_ASSERT_MSG_EQ(hardware->m_qpMap.size(), 3, "One transport overwrote another transport's QP");
    }
    void Cancel(Ptr<RdmaConsensusTransport> transport, Ptr<RdmaHw> hardware) {
        Send(transport, 1, 10000000);
        Simulator::ScheduleNow(&RuntimeTransportTestCase::StopAndCheck, this, transport, hardware);
    }
    void StopAndCheck(Ptr<RdmaConsensusTransport> transport, Ptr<RdmaHw> hardware) {
        NS_TEST_ASSERT_MSG_EQ(hardware->m_qpMap.size(), 1, "Cancellation did not exercise a live QP");
        auto queuePair = hardware->m_qpMap.begin()->second;
        transport->Stop();
        NS_TEST_ASSERT_MSG_EQ(hardware->m_qpMap.empty(), true, "Stopped transport retained a live QP");
        NS_TEST_ASSERT_MSG_EQ(hardware->IsQpPortAvailable(queuePair->dip.Get(), queuePair->sport, queuePair->m_pg),
                              false, "Cancelled port can be reused before stale feedback drains");
    }
    void DoRun() override {
        ConsensusExperimentConfig config;
        config.ApplyPreset("pbft-rdma");
        config.topology = "qbb-star";
        auto topology = config.BuildTopology(2);
        auto first = CreateObject<RdmaConsensusTransport>();
        auto second = CreateObject<RdmaConsensusTransport>();
        auto receiver = CreateObject<RdmaConsensusTransport>();
        auto source = topology.endpointInterfaces.GetAddress(0);
        auto destination = topology.endpointInterfaces.GetAddress(1);
        first->SetLocalEndpoint(1, InetSocketAddress(source, 9200));
        second->SetLocalEndpoint(3, InetSocketAddress(source, 9201));
        receiver->SetLocalEndpoint(2, InetSocketAddress(destination, 9200));
        first->SetPeerAddress(2, InetSocketAddress(destination, 9200));
        second->SetPeerAddress(2, InetSocketAddress(destination, 9200));
        receiver->SetReceiveCallback(MakeCallback(&RuntimeTransportTestCase::Receive, this));
        first->Start(topology.endpoints.Get(0));
        second->Start(topology.endpoints.Get(0));
        receiver->Start(topology.endpoints.Get(1));
        auto hardware = topology.endpoints.Get(0)->GetObject<RdmaDriver>()->m_rdma;
        Simulator::Schedule(MilliSeconds(1), &RuntimeTransportTestCase::SendConcurrent, this, first, second, hardware);
        Simulator::Schedule(MilliSeconds(2), &RuntimeTransportTestCase::Cancel, this, first, hardware);
        Simulator::Stop(MilliSeconds(3));
        Simulator::Run();
        NS_TEST_ASSERT_MSG_EQ(m_delivered, 3, "Concurrent messages were lost or cancelled payload was delivered");
        first->Stop();
        second->Stop();
        receiver->Stop();
        Simulator::Destroy();
    }
    uint32_t m_delivered{0};
};

class RuntimeRegressionTestSuite : public TestSuite {
  public:
    RuntimeRegressionTestSuite() : TestSuite("consensus-runtime", Type::UNIT) {
        AddTestCase(new RdmaCompletedAckTestCase(), TestCase::QUICK);
        AddTestCase(new BidlReplayAdmissionTestCase(), TestCase::QUICK);
        AddTestCase(new RuntimeMessageTestCase(), TestCase::QUICK);
        AddTestCase(new RuntimeTransportTestCase(), TestCase::QUICK);
    }
};

static RuntimeRegressionTestSuite runtimeRegressionTestSuite;

}
