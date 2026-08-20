#ifndef CONSENSUS_ENGINE_H
#define CONSENSUS_ENGINE_H

#include "consensus-message.h"

#include "ns3/callback.h"
#include "ns3/nstime.h"
#include "ns3/object.h"
#include "ns3/ptr.h"
#include "ns3/traced-callback.h"

#include <cstdint>
#include <vector>

namespace ns3 {

    enum ResourceMode { RESOURCE_IMMEDIATE = 0, RESOURCE_QUEUED = 1 };

    /** Models finite CPU and storage resources used by consensus processing. */
    class ConsensusResourceModel : public Object {
        public:
        static TypeId GetTypeId();

        ConsensusResourceModel();

        void SubmitCpuTask(Time serviceTime, Callback<void> completed);
        void SubmitStorageTask(Time serviceTime, Callback<void> completed);
        void Reset();

        private:
        void EnsureCapacity();
        void SubmitTask(Time serviceTime, Callback<void> completed, std::vector<Time>& availableTimes);

        ResourceMode m_mode;
        uint32_t m_cpuCores;
        uint32_t m_storageChannels;
        std::vector<Time> m_cpuAvailableTimes;
        std::vector<Time> m_storageAvailableTimes;
    };

    /**
     * \ingroup consensus
     * Protocol-independent interface implemented by concrete consensus engines.
     */
    class ConsensusEngine : public Object {
        public:
        static TypeId GetTypeId();

        using SendCallback = Callback<void, uint32_t, const ConsensusMessage&>;
        using DecisionCallback = Callback<void, const ConsensusDecision&>;

        ConsensusEngine();

        virtual void Configure(uint32_t replicaId, const std::vector<uint32_t>& replicaIds);
        virtual void Start() = 0;
        virtual void Stop() = 0;
        virtual void Submit(const ClientRequest& request) = 0;
        virtual void Receive(const ConsensusMessage& message) = 0;

        void SetSendCallback(SendCallback callback);
        void SetDecisionCallback(DecisionCallback callback);
        void SetResourceModel(Ptr<ConsensusResourceModel> resourceModel);

        uint32_t GetReplicaId() const;
        const std::vector<uint32_t>& GetReplicaIds() const;
        virtual uint32_t GetLeaderId() const;

        protected:
        void Send(uint32_t peerId, const ConsensusMessage& message) const;
        void Decide(const ConsensusDecision& decision) const;
        void ReportPhase(ConsensusProtocol protocol, uint64_t instanceId, uint16_t phase, Time elapsed) const;
        void ReportLeader(uint64_t epoch, uint32_t leaderId) const;

        uint32_t m_replicaId;
        std::vector<uint32_t> m_replicaIds;
        Ptr<ConsensusResourceModel> m_resourceModel;

        private:
        SendCallback m_sendCallback;
        DecisionCallback m_decisionCallback;
        TracedCallback<uint32_t, uint8_t, uint64_t, uint16_t, Time> m_phaseTrace;
        TracedCallback<uint32_t, uint64_t, uint32_t> m_leaderTrace;
    };

} // namespace ns3

#endif // CONSENSUS_ENGINE_H
