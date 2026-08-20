#include "ns3/boolean.h"
#include "ns3/bidl-application.h"
#include "ns3/consensus-application.h"
#include "ns3/consensus-experiment.h"
#include "ns3/consensus-message.h"
#include "ns3/consensus-runner.h"
#include "ns3/consensus-transport.h"
#include "ns3/consensus-workload.h"
#include "ns3/double.h"
#include "ns3/enum.h"
#include "ns3/inet-socket-address.h"
#include "ns3/ipv4-address.h"
#include "ns3/pbft-engine.h"
#include "ns3/qbb-net-device.h"
#include "ns3/rdma-driver.h"
#include "ns3/simulator.h"
#include "ns3/switch-node.h"
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

class ConsensusMeasurementWindowTestCase : public TestCase {
    public:
    ConsensusMeasurementWindowTestCase() : TestCase("Consensus throughput excludes requests completed after the measurement window") {}

    private:
    void Submit(Ptr<ConsensusClientApplication> client) { client->SubmitRequest(1024, 1); }

    void Reply(Ptr<RecordingConsensusTransport> transport) {
        ConsensusMessage request;
        NS_TEST_ASSERT_MSG_EQ(ConsensusMessage::FromPacket(transport->packets.front(), request), true, "Client request is invalid");
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
        Simulator::Schedule(MicroSeconds(100), &ConsensusMeasurementWindowTestCase::Submit, this, client);
        Simulator::Schedule(MilliSeconds(2), &ConsensusMeasurementWindowTestCase::Reply, this, transport);
        Simulator::Stop(MilliSeconds(4));
        Simulator::Run();

        NS_TEST_ASSERT_MSG_EQ(statistics->GetCompletedRequests(), 1, "Late request reply was not processed");
        NS_TEST_ASSERT_MSG_EQ(statistics->GetMeasuredRequests(), 0, "Late completion was included in measured throughput");
        NS_TEST_ASSERT_MSG_EQ(statistics->GetRequestThroughput(), 0.0, "Late completion inflated measured throughput");
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
            ++m_deliveries;
            m_payloadSize = message.payloadSize;
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
        Simulator::Stop(MilliSeconds(20));
        Simulator::Run();
        sender->Stop();
        receiver->Stop();

        NS_TEST_ASSERT_MSG_EQ(m_deliveries, 1, "Fragmented RDMA message was not delivered exactly once");
        NS_TEST_ASSERT_MSG_EQ(m_payloadSize, 4096, "RDMA reassembly changed the consensus payload size");
        Simulator::Destroy();
    }

    uint32_t m_deliveries{0};
    uint32_t m_payloadSize{0};
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

class ConsensusTestSuite : public TestSuite {
    public:
    ConsensusTestSuite() : TestSuite("consensus", Type::UNIT) {
        AddTestCase(new ConsensusMessageTestCase(), TestCase::QUICK);
        AddTestCase(new ConsensusTransportImpairmentTestCase(), TestCase::QUICK);
        AddTestCase(new RdmaConsensusTransportTestCase(), TestCase::QUICK);
        AddTestCase(new ClientExternalSubmissionTestCase(), TestCase::QUICK);
        AddTestCase(new BidlSequencerDeduplicationTestCase(), TestCase::QUICK);
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
    }
};

class ConsensusEndToEndTestSuite : public TestSuite {
    public:
    ConsensusEndToEndTestSuite() : TestSuite("consensus-e2e", Type::SYSTEM) {
        AddTestCase(new ConsensusEndToEndMatrixTestCase(), TestCase::QUICK);
    }
};

static ConsensusEndToEndTestSuite g_consensusEndToEndTestSuite;
