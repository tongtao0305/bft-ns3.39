#include "consensus-helper.h"

#include "ns3/abort.h"
#include "ns3/consensus-application.h"
#include "ns3/consensus-engine.h"
#include "ns3/consensus-transport.h"
#include "ns3/inet-socket-address.h"
#include "ns3/pbft-engine.h"
#include "ns3/uinteger.h"

namespace ns3 {

    ConsensusHelper::ConsensusHelper() : m_hasFaultModel(false) {
        m_applicationFactory.SetTypeId(ConsensusApplication::GetTypeId());
        m_clientFactory.SetTypeId(ConsensusClientApplication::GetTypeId());
        m_bidlFactory.SetTypeId(BidlApplication::GetTypeId());
        m_engineFactory.SetTypeId(PbftEngine::GetTypeId());
        m_transportFactory.SetTypeId(TcpConsensusTransport::GetTypeId());
        m_resourceFactory.SetTypeId(ConsensusResourceModel::GetTypeId());
    }

    void ConsensusHelper::SetApplicationAttribute(const std::string& name, const AttributeValue& value) {
        m_applicationFactory.Set(name, value);
    }

    void ConsensusHelper::SetClientAttribute(const std::string& name, const AttributeValue& value) {
        m_clientFactory.Set(name, value);
    }

    void ConsensusHelper::SetBidlAttribute(const std::string& name, const AttributeValue& value) {
        m_bidlFactory.Set(name, value);
    }

    void ConsensusHelper::SetEngine(const std::string& typeName) {
        m_engineFactory.SetTypeId(typeName);
    }

    void ConsensusHelper::SetEngineAttribute(const std::string& name, const AttributeValue& value) {
        m_engineFactory.Set(name, value);
    }

    void ConsensusHelper::SetTransport(const std::string& typeName) {
        m_transportFactory.SetTypeId(typeName);
    }

    void ConsensusHelper::SetTransportAttribute(const std::string& name, const AttributeValue& value) {
        m_transportFactory.Set(name, value);
    }

    void ConsensusHelper::SetFaultModel(const std::string& typeName) {
        m_faultFactory.SetTypeId(typeName);
        m_hasFaultModel = true;
    }

    void ConsensusHelper::SetFaultAttribute(const std::string& name, const AttributeValue& value) {
        NS_ABORT_MSG_IF(!m_hasFaultModel, "Configure a fault model before setting attributes");
        m_faultFactory.Set(name, value);
    }

    void ConsensusHelper::SetResourceAttribute(const std::string& name, const AttributeValue& value) {
        m_resourceFactory.Set(name, value);
    }

    ApplicationContainer ConsensusHelper::InstallReplicas(const NodeContainer& nodes, const std::vector<uint32_t>& replicaIds,
                                                          const ConsensusEndpointMap& endpoints) const {
        NS_ABORT_MSG_IF(nodes.GetN() != replicaIds.size(), "One replica identifier is required for every node");

        ApplicationContainer applications;
        for (uint32_t index = 0; index < nodes.GetN(); ++index) {
            uint32_t replicaId = replicaIds[index];
            NS_ABORT_MSG_IF(endpoints.find(replicaId) == endpoints.end(), "Replica endpoint is missing");

            Ptr<ConsensusApplication> application = m_applicationFactory.Create<ConsensusApplication>();
            Ptr<ConsensusEngine> engine = m_engineFactory.Create<ConsensusEngine>();
            Ptr<ConsensusResourceModel> resource = m_resourceFactory.Create<ConsensusResourceModel>();
            Ptr<ConsensusTransport> transport = DynamicCast<ConsensusTransport>(CreateTransport(replicaId, endpoints));

            NS_ABORT_MSG_IF(!engine, "Configured engine is not a ConsensusEngine");
            NS_ABORT_MSG_IF(!transport, "Configured transport is not a ConsensusTransport");

            application->SetReplicaId(replicaId);
            application->SetReplicaIds(replicaIds);
            application->SetEngine(engine);
            application->SetTransport(transport);
            application->SetResourceModel(resource);
            nodes.Get(index)->AddApplication(application);
            applications.Add(application);
        }
        return applications;
    }

    ApplicationContainer ConsensusHelper::InstallClient(Ptr<Node> node, uint32_t clientId, uint32_t primaryId,
                                                        const ConsensusEndpointMap& endpoints) const {
        NS_ABORT_MSG_IF(!node, "Client node is null");
        NS_ABORT_MSG_IF(endpoints.find(clientId) == endpoints.end(), "Client endpoint is missing");
        NS_ABORT_MSG_IF(endpoints.find(primaryId) == endpoints.end(), "Primary endpoint is missing");

        std::vector<uint32_t> replicaIds;
        for (const auto& [peerId, address] : endpoints) {
            (void)address;
            if (peerId != clientId) {
                replicaIds.push_back(peerId);
            }
        }
        return InstallClient(node, clientId, primaryId, replicaIds, endpoints);
    }

    ApplicationContainer ConsensusHelper::InstallClient(Ptr<Node> node, uint32_t clientId, uint32_t primaryId,
                                                        const std::vector<uint32_t>& targetIds,
                                                        const ConsensusEndpointMap& endpoints) const {
        NS_ABORT_MSG_IF(!node, "Client node is null");
        NS_ABORT_MSG_IF(endpoints.find(clientId) == endpoints.end(), "Client endpoint is missing");
        NS_ABORT_MSG_IF(endpoints.find(primaryId) == endpoints.end(), "Primary endpoint is missing");
        NS_ABORT_MSG_IF(targetIds.empty(), "Client target set is empty");
        for (uint32_t targetId : targetIds) {
            NS_ABORT_MSG_IF(endpoints.find(targetId) == endpoints.end(), "Client target endpoint is missing");
        }

        Ptr<ConsensusClientApplication> application = m_clientFactory.Create<ConsensusClientApplication>();
        Ptr<ConsensusTransport> transport = DynamicCast<ConsensusTransport>(CreateTransport(clientId, endpoints));
        NS_ABORT_MSG_IF(!transport, "Configured transport is not a ConsensusTransport");

        application->SetAttribute("ClientId", UintegerValue(clientId));
        application->SetAttribute("PrimaryId", UintegerValue(primaryId));
        application->SetReplicaIds(targetIds);
        application->SetTransport(transport);
        node->AddApplication(application);

        ApplicationContainer applications;
        applications.Add(application);
        return applications;
    }

    ApplicationContainer ConsensusHelper::InstallBidlNode(Ptr<Node> node, uint32_t nodeId, BidlRole role,
                                                          const std::vector<uint32_t>& ordererIds, const std::vector<uint32_t>& executorIds,
                                                          const std::vector<uint32_t>& sequencerIds,
                                                          const ConsensusEndpointMap& endpoints) const {
        NS_ABORT_MSG_IF(!node, "BIDL node is null");
        NS_ABORT_MSG_IF(endpoints.find(nodeId) == endpoints.end(), "BIDL endpoint is missing");

        Ptr<BidlApplication> application = m_bidlFactory.Create<BidlApplication>();
        Ptr<ConsensusResourceModel> resource = m_resourceFactory.Create<ConsensusResourceModel>();
        Ptr<ConsensusTransport> transport = DynamicCast<ConsensusTransport>(CreateTransport(nodeId, endpoints));
        NS_ABORT_MSG_IF(!transport, "Configured transport is not a ConsensusTransport");

        application->SetNodeId(nodeId);
        application->SetRole(role);
        application->SetOrdererIds(ordererIds);
        application->SetExecutorIds(executorIds);
        application->SetSequencerIds(sequencerIds);
        application->SetTransport(transport);
        application->SetResourceModel(resource);
        if (role == BIDL_ORDERER) {
            Ptr<ConsensusEngine> engine = m_engineFactory.Create<ConsensusEngine>();
            NS_ABORT_MSG_IF(!engine, "Configured BIDL engine is not a ConsensusEngine");
            application->SetEngine(engine);
        }
        node->AddApplication(application);

        ApplicationContainer applications;
        applications.Add(application);
        return applications;
    }

    Ptr<Object> ConsensusHelper::CreateTransport(uint32_t localId, const ConsensusEndpointMap& endpoints) const {
        auto local = endpoints.find(localId);
        NS_ABORT_MSG_IF(local == endpoints.end(), "Local endpoint is missing");
        NS_ABORT_MSG_IF(!InetSocketAddress::IsMatchingType(local->second),
                        "Only IPv4 consensus endpoints are supported in the first version");

        Ptr<ConsensusTransport> transport = m_transportFactory.Create<ConsensusTransport>();
        if (m_hasFaultModel) {
            Ptr<ConsensusFaultModel> fault = m_faultFactory.Create<ConsensusFaultModel>();
            NS_ABORT_MSG_IF(!fault, "Configured fault model is not a ConsensusFaultModel");
            transport->SetFaultModel(fault);
        }
        transport->SetLocalEndpoint(localId, local->second);
        for (const auto& [peerId, address] : endpoints) {
            if (peerId != localId) {
                transport->SetPeerAddress(peerId, address);
            }
        }
        return transport;
    }

} // namespace ns3
