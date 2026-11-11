#ifndef EZBFT_ENGINE_H
#define EZBFT_ENGINE_H

#include "consensus-engine.h"
#include "ns3/event-id.h"

#include <map>
#include <set>
#include <tuple>
#include <vector>

namespace ns3 {

    /** Packet-level EZBFT experiment model. Variant 0 is the paper protocol;
     * variant 1 adds PBFT prepare/commit confirmation to the final tuple. */
    class EzBftEngine : public ConsensusEngine {
      public:
        static TypeId GetTypeId();
        void Configure(uint32_t replicaId, const std::vector<uint32_t>& replicaIds) override;
        void Start() override;
        void Stop() override;
        void Submit(const ClientRequest& request) override;
        void Receive(const ConsensusMessage& message) override;
        uint64_t GetEquivocationCount() const { return m_equivocations; }
        uint64_t GetSpeculativeCount() const { return m_speculative; }
        uint64_t GetFinalExecutionCount() const { return m_finalExecutions; }
        uint64_t GetOwnerChanges() const { return m_ownerChanges; }
        uint64_t GetReexecutionCount() const { return m_reexecutions; }
        uint64_t GetLegalUpdateCount() const { return m_legalUpdates; }
        const std::vector<uint64_t>& GetFinalOrder() const { return m_finalOrder; }
        std::map<uint64_t, uint64_t> GetFinalTupleDigests() const;

      private:
        using Key = std::tuple<uint32_t, uint64_t, uint64_t>; // owner, epoch, instance
        struct Tuple {
            uint64_t command{0};
            std::vector<uint64_t> dependencies;
            uint64_t sequence{0};
            uint64_t digest{0};
            uint32_t client{0};
            uint64_t request{0};
            uint32_t transactions{0};
        };
        struct Instance {
            Tuple candidate;
            bool present{false};
            uint64_t ownerProposalDigest{0};
            bool prepareSent{false};
            bool commitSent{false};
            bool decided{false};
            bool finalized{false};
            std::set<uint32_t> prepares;
            std::set<uint32_t> commits;
        };

        static uint64_t HashTuple(uint64_t command, const std::vector<uint64_t>& dependencies, uint64_t sequence);
        static uint64_t EncodeInstanceId(const Key& key);
        static void Put64(std::vector<uint8_t>& bytes, uint64_t value);
        static void Put32(std::vector<uint8_t>& bytes, uint32_t value);
        static bool Get64(const std::vector<uint8_t>& bytes, size_t& offset, uint64_t& value);
        static bool Get32(const std::vector<uint8_t>& bytes, size_t& offset, uint32_t& value);
        static void PutTuple(std::vector<uint8_t>& bytes, const Tuple& tuple);
        static bool GetTuple(const std::vector<uint8_t>& bytes, size_t& offset, Tuple& tuple);
        void AddObservedDependencies(uint64_t command, std::vector<uint64_t>& dependencies, uint64_t& sequence) const;
        void RememberAccess(const Key& key, const Tuple& tuple);
        void TryExecuteReady();
        void SendProposal(const Tuple& value, uint32_t owner, uint64_t epoch, uint64_t instance, bool skipTarget);
        void HandleOrder(const ConsensusMessage& message);
        void OnGateTimeout(Key key);
        void HandleCommit(const ConsensusMessage& message);
        void HandlePrepare(const ConsensusMessage& message);
        void HandleCommitVote(const ConsensusMessage& message);
        void HandleOwnerChange(const ConsensusMessage& message);
        void HandleNewOwner(const ConsensusMessage& message);
        void BeginOwnerChange(const Key& key);
        void DecideInstance(const Key& key, Instance& state);
        void SendToAll(ConsensusMessage message);

        uint32_t m_faultTolerance{1};
        uint32_t m_variant{0};
        bool m_equivocationEnabled{false};
        bool m_equivocationInjected{false};
        uint32_t m_equivocationTarget{1};
        Time m_equivocationDelay{MicroSeconds(20)};
        Time m_ownerTimeout{MilliSeconds(50)};
        bool m_iorsTerminalGate{false};
        Time m_gateTimeout{MilliSeconds(1)};
        uint64_t m_nextInstance{0};
        uint64_t m_epoch{0};
        bool m_running{false};
        uint64_t m_equivocations{0};
        uint64_t m_speculative{0};
        uint64_t m_finalExecutions{0};
        uint64_t m_ownerChanges{0};
        uint64_t m_reexecutions{0};
        uint64_t m_legalUpdates{0};
        std::vector<uint64_t> m_finalOrder;
        std::map<Key, Instance> m_instances;
        std::map<uint64_t, Key> m_instanceIdToKey;
        std::set<Key> m_waitingFinal;
        std::map<uint32_t, std::pair<uint64_t, uint64_t>> m_lastWriterByKey;
        std::map<uint32_t, std::pair<uint64_t, uint64_t>> m_lastReaderByKey;
        std::map<uint64_t, uint64_t> m_ownerEpochs;
        std::map<Key, std::set<uint32_t>> m_ownerChangeVotes;
        std::set<Key> m_ownerChangeFinalized;
        std::map<Key, EventId> m_ownerTimers;
        std::map<Key, ConsensusMessage> m_markedPending;
        std::map<Key, EventId> m_gateEvents;
    };

} // namespace ns3

#endif
