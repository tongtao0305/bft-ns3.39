#ifndef CONSENSUS_HELPER_H
#define CONSENSUS_HELPER_H

#include "ns3/address.h"
#include "ns3/application-container.h"
#include "ns3/attribute.h"
#include "ns3/bidl-application.h"
#include "ns3/node-container.h"
#include "ns3/object-factory.h"
#include "ns3/ptr.h"

#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace ns3 {

    using ConsensusEndpointMap = std::map<uint32_t, Address>;

    /**
     * \ingroup consensus
     * Installs protocol-independent consensus applications and concrete engines.
     */
    class ConsensusHelper {
        public:
        ConsensusHelper();

        void SetApplicationAttribute(const std::string& name, const AttributeValue& value);
        void SetClientAttribute(const std::string& name, const AttributeValue& value);
        void SetBidlAttribute(const std::string& name, const AttributeValue& value);

        void SetEngine(const std::string& typeName);
        void SetEngineAttribute(const std::string& name, const AttributeValue& value);

        void SetTransport(const std::string& typeName);
        void SetTransportAttribute(const std::string& name, const AttributeValue& value);
        void SetFaultModel(const std::string& typeName);
        void SetFaultAttribute(const std::string& name, const AttributeValue& value);

        void SetResourceAttribute(const std::string& name, const AttributeValue& value);

        ApplicationContainer InstallReplicas(const NodeContainer& nodes, const std::vector<uint32_t>& replicaIds,
                                             const ConsensusEndpointMap& endpoints) const;

        ApplicationContainer InstallClient(Ptr<Node> node, uint32_t clientId, uint32_t primaryId,
                                           const ConsensusEndpointMap& endpoints) const;
        ApplicationContainer InstallClient(Ptr<Node> node, uint32_t clientId, uint32_t primaryId, const std::vector<uint32_t>& targetIds,
                                           const ConsensusEndpointMap& endpoints) const;

        ApplicationContainer InstallBidlNode(Ptr<Node> node, uint32_t nodeId, BidlRole role, const std::vector<uint32_t>& ordererIds,
                                             const std::vector<uint32_t>& executorIds, const std::vector<uint32_t>& sequencerIds,
                                             const ConsensusEndpointMap& endpoints) const;

        private:
        Ptr<Object> CreateTransport(uint32_t localId, const ConsensusEndpointMap& endpoints) const;

        ObjectFactory m_applicationFactory;
        ObjectFactory m_clientFactory;
        ObjectFactory m_bidlFactory;
        ObjectFactory m_engineFactory;
        ObjectFactory m_transportFactory;
        ObjectFactory m_faultFactory;
        ObjectFactory m_resourceFactory;
        bool m_hasFaultModel;
    };

} // namespace ns3

#endif // CONSENSUS_HELPER_H
