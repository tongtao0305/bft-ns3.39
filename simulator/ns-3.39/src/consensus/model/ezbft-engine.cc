#include "ezbft-engine.h"
#include "zyzzyva-auth.h"

#include "ns3/boolean.h"
#include "ns3/simulator.h"
#include "ns3/uinteger.h"

#include <algorithm>

namespace ns3 {

    std::map<uint64_t, uint64_t> EzBftEngine::GetFinalTupleDigests() const {
        std::map<uint64_t, uint64_t> digests;
        for (const auto& entry : m_instances) {
            if (entry.second.decided) digests.emplace(EncodeInstanceId(entry.first), entry.second.candidate.digest);
        }
        return digests;
    }

    NS_OBJECT_ENSURE_REGISTERED(EzBftEngine);

    TypeId EzBftEngine::GetTypeId() {
        static TypeId tid = TypeId("ns3::EzBftEngine").SetParent<ConsensusEngine>().SetGroupName("Consensus")
            .AddConstructor<EzBftEngine>()
            .AddAttribute("FaultTolerance", "Byzantine fault bound.", UintegerValue(1),
                          MakeUintegerAccessor(&EzBftEngine::m_faultTolerance), MakeUintegerChecker<uint32_t>())
            .AddAttribute("Variant", "0=original EZBFT, 1=research variant with PBFT final tuple confirmation.",
                          UintegerValue(0), MakeUintegerAccessor(&EzBftEngine::m_variant), MakeUintegerChecker<uint32_t>(0, 1))
            .AddAttribute("EquivocationEnabled", "Inject a conflicting owner tuple to one replica.", BooleanValue(false),
                          MakeBooleanAccessor(&EzBftEngine::m_equivocationEnabled), MakeBooleanChecker())
            .AddAttribute("EquivocationTarget", "Replica receiving the first conflicting tuple.", UintegerValue(1),
                          MakeUintegerAccessor(&EzBftEngine::m_equivocationTarget), MakeUintegerChecker<uint32_t>())
            .AddAttribute("EquivocationDelay", "Delay before canonical tuple reaches attacked replica.", TimeValue(MicroSeconds(20)),
                          MakeTimeAccessor(&EzBftEngine::m_equivocationDelay), MakeTimeChecker())
            .AddAttribute("OwnerTimeout", "Timeout before an owner change is started.", TimeValue(MilliSeconds(50)),
                          MakeTimeAccessor(&EzBftEngine::m_ownerTimeout), MakeTimeChecker())
            .AddAttribute("IorsTerminalGate", "Bounded wait before speculating on a marked Owner proposal.", BooleanValue(false),
                          MakeBooleanAccessor(&EzBftEngine::m_iorsTerminalGate), MakeBooleanChecker())
            .AddAttribute("IorsGateTimeout", "Fallback timeout for a marked Owner proposal.", TimeValue(MilliSeconds(1)),
                          MakeTimeAccessor(&EzBftEngine::m_gateTimeout), MakeTimeChecker());
        return tid;
    }

    void EzBftEngine::Configure(uint32_t replicaId, const std::vector<uint32_t>& replicaIds) {
        ConsensusEngine::Configure(replicaId, replicaIds);
    }
    void EzBftEngine::Start() {
        NS_ABORT_MSG_IF(m_replicaIds.size() != 3 * m_faultTolerance + 1, "EZBFT needs n=3f+1");
        m_running = true;
    }
    void EzBftEngine::Stop() {
        m_running = false;
        for (auto& timer : m_ownerTimers) Simulator::Cancel(timer.second);
        m_ownerTimers.clear();
        for (auto& timer : m_gateEvents) Simulator::Cancel(timer.second);
        m_gateEvents.clear();
        m_markedPending.clear();
    }
    uint64_t EzBftEngine::HashTuple(uint64_t command, const std::vector<uint64_t>& dependencies, uint64_t sequence) {
        uint64_t h = command ^ 0x9e3779b97f4a7c15ULL;
        for (uint64_t dependency : dependencies) h ^= dependency + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        h ^= sequence * 0xd6e8feb86659fd93ULL;
        return h ? h : 1;
    }
    uint64_t EzBftEngine::EncodeInstanceId(const Key& key) {
        return (uint64_t(std::get<0>(key)) << 56) | ((std::get<1>(key) & 0xffffULL) << 40) |
            (std::get<2>(key) & 0xffffffffffULL);
    }
    void EzBftEngine::Put64(std::vector<uint8_t>& bytes, uint64_t value) {
        for (int shift = 56; shift >= 0; shift -= 8) bytes.push_back(static_cast<uint8_t>(value >> shift));
    }
    void EzBftEngine::Put32(std::vector<uint8_t>& bytes, uint32_t value) {
        for (int shift = 24; shift >= 0; shift -= 8) bytes.push_back(static_cast<uint8_t>(value >> shift));
    }
    bool EzBftEngine::Get64(const std::vector<uint8_t>& bytes, size_t& offset, uint64_t& value) {
        if (offset > bytes.size() || bytes.size() - offset < 8) return false;
        value = 0;
        for (int i = 0; i < 8; ++i) value = (value << 8) | bytes[offset++];
        return true;
    }
    bool EzBftEngine::Get32(const std::vector<uint8_t>& bytes, size_t& offset, uint32_t& value) {
        if (offset > bytes.size() || bytes.size() - offset < 4) return false;
        value = 0;
        for (int i = 0; i < 4; ++i) value = (value << 8) | bytes[offset++];
        return true;
    }
    void EzBftEngine::PutTuple(std::vector<uint8_t>& bytes, const Tuple& tuple) {
        Put64(bytes, tuple.command);
        Put32(bytes, static_cast<uint32_t>(tuple.dependencies.size()));
        for (uint64_t dependency : tuple.dependencies) Put64(bytes, dependency);
        Put64(bytes, tuple.sequence);
    }
    bool EzBftEngine::GetTuple(const std::vector<uint8_t>& bytes, size_t& offset, Tuple& tuple) {
        uint32_t count = 0;
        if (!Get64(bytes, offset, tuple.command) || !Get32(bytes, offset, count) || count > 64 ||
            offset > bytes.size() || bytes.size() - offset < uint64_t(count) * 8 + 8) return false;
        tuple.dependencies.clear();
        for (uint32_t i = 0; i < count; ++i) {
            uint64_t dependency = 0;
            if (!Get64(bytes, offset, dependency) || dependency == 0 ||
                (!tuple.dependencies.empty() && tuple.dependencies.back() >= dependency)) return false;
            tuple.dependencies.push_back(dependency);
        }
        return Get64(bytes, offset, tuple.sequence) && tuple.sequence > 0;
    }
    void EzBftEngine::AddObservedDependencies(uint64_t command, std::vector<uint64_t>& dependencies,
                                               uint64_t& sequence) const {
        std::set<uint64_t> merged(dependencies.begin(), dependencies.end());
        const uint32_t readKey = (command >> 8) % 64;
        const uint32_t writeKey = command % 64;
        auto add = [&](const std::map<uint32_t, std::pair<uint64_t, uint64_t>>& source, uint32_t key) {
            auto found = source.find(key);
            if (found != source.end()) {
                merged.insert(found->second.first);
                sequence = std::max(sequence, found->second.second + 1);
            }
        };
        add(m_lastWriterByKey, readKey);
        add(m_lastWriterByKey, writeKey);
        add(m_lastReaderByKey, writeKey);
        dependencies.assign(merged.begin(), merged.end());
    }
    void EzBftEngine::RememberAccess(const Key& key, const Tuple& tuple) {
        const std::pair<uint64_t, uint64_t> latest{EncodeInstanceId(key), tuple.sequence};
        auto remember = [&](std::map<uint32_t, std::pair<uint64_t, uint64_t>>& table, uint32_t index) {
            auto found = table.find(index);
            if (found == table.end() || found->second.first == latest.first || found->second.second <= latest.second)
                table[index] = latest;
        };
        remember(m_lastReaderByKey, (tuple.command >> 8) % 64);
        remember(m_lastWriterByKey, tuple.command % 64);
    }
    void EzBftEngine::SendToAll(ConsensusMessage message) {
        for (uint32_t peer : m_replicaIds) if (peer != m_replicaId) Send(peer, message);
    }
    void EzBftEngine::Submit(const ClientRequest& request) {
        if (!m_running) return;
        const uint32_t owner = m_replicaId;
        const uint64_t epoch = m_ownerEpochs[owner];
        const uint64_t instance = ++m_nextInstance;
        std::vector<uint64_t> dependencies;
        uint64_t sequence = 1;
        AddObservedDependencies(request.digest, dependencies, sequence);
        Tuple tuple{request.digest, dependencies, sequence, HashTuple(request.digest, dependencies, sequence),
                    request.clientId, request.requestId, request.transactionCount};
        const bool attack = m_equivocationEnabled && !m_equivocationInjected && m_replicaId == m_replicaIds.front() &&
            m_equivocationTarget != m_replicaId &&
            std::find(m_replicaIds.begin(), m_replicaIds.end(), m_equivocationTarget) != m_replicaIds.end();
        if (attack) m_equivocationInjected = true;
        SendProposal(tuple, owner, epoch, instance, attack);
        if (attack) {
            ConsensusMessage canonical;
            canonical.protocol = ConsensusProtocol::EZBFT; canonical.type = ConsensusMessageType::EZBFT_SPEC_ORDER;
            canonical.view = epoch; canonical.instanceId = instance; canonical.messageId = static_cast<uint64_t>(owner) + 1;
            canonical.digest = tuple.digest; canonical.clientId = tuple.client; canonical.requestId = tuple.request;
            canonical.transactionCount = tuple.transactions;
            PutTuple(canonical.payloadBytes, tuple);
            Simulator::Schedule(m_equivocationDelay, [this, canonical]() {
                if (m_running) Send(m_equivocationTarget, canonical);
            });
            Tuple conflicting = tuple;
            conflicting.sequence += 1;
            conflicting.digest = HashTuple(conflicting.command, conflicting.dependencies, conflicting.sequence);
            ConsensusMessage equivocation;
            equivocation.protocol = ConsensusProtocol::EZBFT;
            equivocation.type = ConsensusMessageType::EZBFT_SPEC_ORDER;
            equivocation.messageId = static_cast<uint64_t>(owner) + 1;
            equivocation.view = epoch; equivocation.instanceId = instance; equivocation.digest = conflicting.digest;
            equivocation.clientId = tuple.client; equivocation.requestId = tuple.request;
            equivocation.transactionCount = tuple.transactions;
            PutTuple(equivocation.payloadBytes, conflicting);
            Send(m_equivocationTarget, equivocation);
        }
        Key key{owner, epoch, instance};
        m_ownerTimers[key] = Simulator::Schedule(m_ownerTimeout, &EzBftEngine::BeginOwnerChange, this, key);
    }
    void EzBftEngine::SendProposal(const Tuple& value, uint32_t owner, uint64_t epoch, uint64_t instance, bool skipTarget) {
        ConsensusMessage message;
        message.protocol = ConsensusProtocol::EZBFT; message.type = ConsensusMessageType::EZBFT_SPEC_ORDER;
        message.view = epoch; message.instanceId = instance; message.digest = value.digest;
        message.messageId = static_cast<uint64_t>(owner) + 1;
        message.clientId = value.client; message.requestId = value.request; message.transactionCount = value.transactions;
        PutTuple(message.payloadBytes, value);
        if (owner != m_replicaId) Send(owner, message);
        else { message.senderId = m_replicaId; HandleOrder(message); }
        if (skipTarget) {
            for (uint32_t peer : m_replicaIds) if (peer != m_replicaId && peer != m_equivocationTarget) Send(peer, message);
        } else SendToAll(message);
    }
    void EzBftEngine::HandleOrder(const ConsensusMessage& message) {
        if (message.protocol != ConsensusProtocol::EZBFT || message.senderId >= m_replicaIds.size() ||
            message.messageId != static_cast<uint64_t>(message.senderId) + 1) return;
        size_t offset = 0; Tuple tuple; tuple.client = message.clientId; tuple.request = message.requestId;
        tuple.transactions = message.transactionCount; tuple.digest = message.digest;
        if (!GetTuple(message.payloadBytes, offset, tuple) || offset != message.payloadBytes.size()) return;
        if (tuple.digest != HashTuple(tuple.command, tuple.dependencies, tuple.sequence)) return;
        Key key{message.senderId, message.view, message.instanceId};
        m_instanceIdToKey.emplace(EncodeInstanceId(key), key);
        Instance& state = m_instances[key];
        if (state.ownerProposalDigest == 0) state.ownerProposalDigest = tuple.digest;
        if (state.present && state.ownerProposalDigest != 0 && state.ownerProposalDigest != tuple.digest) {
            ++m_equivocations;
            ReportPhase(ConsensusProtocol::EZBFT, EncodeInstanceId(key), 2, Seconds(0));
            return;
        }
        if (!state.present) {
            // Replica-local observations may legally extend an Owner proposal.
            const uint64_t ownerDigest = tuple.digest;
            AddObservedDependencies(tuple.command, tuple.dependencies, tuple.sequence);
            tuple.digest = HashTuple(tuple.command, tuple.dependencies, tuple.sequence);
            if (tuple.digest != ownerDigest) ++m_legalUpdates;
            state.candidate = tuple; state.present = true; ++m_speculative;
            ReportPhase(ConsensusProtocol::EZBFT, EncodeInstanceId(key), 1, Seconds(0));
            RememberAccess(key, tuple);
        }
        ConsensusMessage response;
        response.protocol = ConsensusProtocol::EZBFT; response.type = ConsensusMessageType::EZBFT_SPEC_REPLY;
        response.view = message.view; response.instanceId = message.instanceId; response.digest = state.candidate.digest;
        response.messageId = message.messageId;
        response.clientId = state.candidate.client; response.requestId = state.candidate.request;
        response.transactionCount = state.candidate.transactions;
        PutTuple(response.payloadBytes, state.candidate);
        Send(state.candidate.client, response);
    }
    void EzBftEngine::OnGateTimeout(Key key) {
        auto pending = m_markedPending.find(key);
        if (pending == m_markedPending.end()) return;
        ConsensusMessage message = pending->second;
        m_markedPending.erase(pending);
        m_gateEvents.erase(key);
        HandleOrder(message);
    }
    void EzBftEngine::HandleCommit(const ConsensusMessage& message) {
        if (message.type == ConsensusMessageType::EZBFT_COMMIT_FAST || message.type == ConsensusMessageType::EZBFT_COMMIT) {
            if (message.messageId == 0) return;
            Key key{static_cast<uint32_t>(message.messageId - 1), message.view, message.instanceId};
            m_instanceIdToKey.emplace(EncodeInstanceId(key), key);
            Instance& state = m_instances[key];
            size_t offset = 0; Tuple tuple;
            tuple.client = message.clientId; tuple.request = message.requestId; tuple.transactions = message.transactionCount;
            if (!GetTuple(message.payloadBytes, offset, tuple)) return;
            tuple.digest = HashTuple(tuple.command, tuple.dependencies, tuple.sequence);
            uint32_t certificateSize = 0;
            if (tuple.digest != message.digest || !Get32(message.payloadBytes, offset, certificateSize) ||
                certificateSize < 2 * m_faultTolerance + 1 || certificateSize > m_replicaIds.size() ||
                (message.type == ConsensusMessageType::EZBFT_COMMIT_FAST && certificateSize != m_replicaIds.size()) ||
                (state.present && state.candidate.command != tuple.command)) return;
            std::set<uint32_t> certificateSenders;
            std::set<uint64_t> certifiedDependencies;
            uint64_t certifiedSequence = 0;
            for (uint32_t i = 0; i < certificateSize; ++i) {
                uint32_t voter = 0;
                Tuple certified;
                uint32_t tupleSize = 0;
                if (!Get32(message.payloadBytes, offset, voter) || !Get32(message.payloadBytes, offset, tupleSize) ||
                    offset > message.payloadBytes.size() ||
                    tupleSize < 20 || tupleSize > 540 ||
                    message.payloadBytes.size() - offset < tupleSize + ZyzzyvaAuth::SIGNATURE_SIZE ||
                    !certificateSenders.insert(voter).second ||
                    std::find(m_replicaIds.begin(), m_replicaIds.end(), voter) == m_replicaIds.end()) return;
                std::vector<uint8_t> tupleBytes(message.payloadBytes.begin() + offset,
                                                message.payloadBytes.begin() + offset + tupleSize);
                size_t tupleOffset = 0;
                if (!GetTuple(tupleBytes, tupleOffset, certified) || tupleOffset != tupleBytes.size()) return;
                offset += tupleSize;
                certifiedDependencies.insert(certified.dependencies.begin(), certified.dependencies.end());
                certifiedSequence = std::max(certifiedSequence, certified.sequence);
                std::vector<uint8_t> signature(message.payloadBytes.begin() + offset,
                                               message.payloadBytes.begin() + offset + ZyzzyvaAuth::SIGNATURE_SIZE);
                offset += ZyzzyvaAuth::SIGNATURE_SIZE;
                ConsensusMessage proof;
                proof.protocol = ConsensusProtocol::EZBFT; proof.type = ConsensusMessageType::EZBFT_SPEC_REPLY;
                proof.senderId = voter; proof.receiverId = message.senderId; proof.view = message.view;
                proof.instanceId = message.instanceId; proof.messageId = message.messageId;
                proof.digest = HashTuple(certified.command, certified.dependencies, certified.sequence);
                proof.clientId = tuple.client; proof.requestId = tuple.request; proof.transactionCount = tuple.transactions;
                PutTuple(proof.payloadBytes, certified);
                if (!ZyzzyvaAuth::VerifyContent(proof, signature)) return;
                if (certified.command != tuple.command) return;
                if (message.type == ConsensusMessageType::EZBFT_COMMIT_FAST &&
                    (certified.dependencies != tuple.dependencies || certified.sequence != tuple.sequence)) return;
            }
            if (message.type == ConsensusMessageType::EZBFT_COMMIT &&
                (tuple.dependencies != std::vector<uint64_t>(certifiedDependencies.begin(), certifiedDependencies.end()) ||
                 tuple.sequence != certifiedSequence)) return;
            if (offset != message.payloadBytes.size()) return;
            if (!state.present) { state.candidate = tuple; state.present = true; }
            if (state.candidate.digest != tuple.digest) {
                ++m_reexecutions;
                ReportPhase(ConsensusProtocol::EZBFT, EncodeInstanceId(key), 4, Seconds(0));
            }
            state.candidate = tuple;
            RememberAccess(key, tuple);
            if (m_variant == 0) {
                DecideInstance(key, state); return;
            }
            ConsensusMessage prepare = message; prepare.type = ConsensusMessageType::EZBFT_PREPARE;
            prepare.senderId = m_replicaId; state.prepares.insert(m_replicaId); state.prepareSent = true;
            SendToAll(prepare); HandlePrepare(prepare);
            return;
        }
    }
    void EzBftEngine::HandlePrepare(const ConsensusMessage& message) {
        if (message.messageId == 0) return;
        Key key{static_cast<uint32_t>(message.messageId - 1), message.view, message.instanceId};
        auto it = m_instances.find(key); if (it == m_instances.end() || it->second.candidate.digest != message.digest) return;
        Instance& state = it->second; state.prepares.insert(message.senderId);
        if (state.prepares.size() >= 2 * m_faultTolerance + 1 && !state.commitSent) {
            state.commitSent = true; state.commits.insert(m_replicaId);
            ConsensusMessage vote = message; vote.type = ConsensusMessageType::EZBFT_COMMIT_VOTE; vote.senderId = m_replicaId;
            SendToAll(vote); HandleCommitVote(vote);
        }
    }
    void EzBftEngine::HandleCommitVote(const ConsensusMessage& message) {
        if (message.messageId == 0) return;
        Key key{static_cast<uint32_t>(message.messageId - 1), message.view, message.instanceId};
        auto it = m_instances.find(key); if (it == m_instances.end() || it->second.candidate.digest != message.digest) return;
        Instance& state = it->second; state.commits.insert(message.senderId);
        if (state.commits.size() >= 2 * m_faultTolerance + 1) DecideInstance(key, state);
    }
    void EzBftEngine::DecideInstance(const Key& key, Instance& state) {
        if (state.finalized) return;
        state.finalized = true;
        m_waitingFinal.insert(key);
        auto timer = m_ownerTimers.find(key); if (timer != m_ownerTimers.end()) { Simulator::Cancel(timer->second); m_ownerTimers.erase(timer); }
        TryExecuteReady();
    }
    void EzBftEngine::TryExecuteReady() {
        while (true) {
            auto selected = m_instances.end();
            bool allPendingDependenciesKnown = true;
            for (const Key& waitingKey : m_waitingFinal) {
                auto it = m_instances.find(waitingKey);
                Instance& candidate = it->second;
                bool ready = true;
                for (uint64_t dependency : candidate.candidate.dependencies) {
                    auto knownKey = m_instanceIdToKey.find(dependency);
                    auto prerequisite = knownKey == m_instanceIdToKey.end() ? m_instances.end() : m_instances.find(knownKey->second);
                    if (prerequisite == m_instances.end() || !prerequisite->second.finalized)
                        allPendingDependenciesKnown = false;
                    if (prerequisite == m_instances.end() || !prerequisite->second.decided) ready = false;
                }
                if (ready && (selected == m_instances.end() ||
                    std::tie(candidate.candidate.sequence, it->first) <
                    std::tie(selected->second.candidate.sequence, selected->first))) selected = it;
            }
            if (selected == m_instances.end() && allPendingDependenciesKnown) {
                // A closed dependency cycle is executed in a deterministic sequence/instance order.
                for (const Key& waitingKey : m_waitingFinal) {
                    auto it = m_instances.find(waitingKey);
                    if (selected == m_instances.end() ||
                        std::tie(it->second.candidate.sequence, it->first) <
                        std::tie(selected->second.candidate.sequence, selected->first)) selected = it;
                }
            }
            if (selected == m_instances.end()) return;
            const Key key = selected->first;
            Instance& state = selected->second;
            state.decided = true;
            m_waitingFinal.erase(key);
            ++m_finalExecutions;
            ReportPhase(ConsensusProtocol::EZBFT, EncodeInstanceId(key), 3, Seconds(0));
            m_finalOrder.push_back(EncodeInstanceId(key));
            ConsensusDecision decision;
            decision.protocol = ConsensusProtocol::EZBFT;
            decision.view = std::get<1>(key);
            decision.instanceId = std::get<2>(key);
            decision.digest = state.candidate.command;
            decision.clientId = state.candidate.client;
            decision.requestId = state.candidate.request;
            decision.transactionCount = state.candidate.transactions;
            decision.decisionTime = Simulator::Now();
            Decide(decision);
        }
    }
    void EzBftEngine::BeginOwnerChange(const Key& key) {
        if (!m_running) return; auto it = m_instances.find(key); if (it != m_instances.end() && it->second.decided) return;
        ++m_ownerChanges;
        ConsensusMessage change; change.protocol = ConsensusProtocol::EZBFT; change.type = ConsensusMessageType::EZBFT_OWNER_CHANGE;
        change.view = std::get<1>(key) + 1; change.instanceId = std::get<2>(key); change.digest = std::get<0>(key);
        change.clientId = m_replicaId; change.senderId = m_replicaId;
        SendToAll(change); HandleOwnerChange(change);
    }
    void EzBftEngine::HandleOwnerChange(const ConsensusMessage& message) {
        Key key{static_cast<uint32_t>(message.digest), message.view - 1, message.instanceId};
        auto& votes = m_ownerChangeVotes[key]; votes.insert(message.senderId);
        // EZBFT advances an instance's owner only after an N-f quorum of
        // authenticated OWNER-CHANGE reports. A single local timeout (or
        // merely f+1 reports) is not enough to authorize the transition.
        const size_t ownerChangeQuorum = m_replicaIds.size() - m_faultTolerance;
        if (votes.size() >= ownerChangeQuorum && m_ownerChangeFinalized.insert(key).second) {
            ConsensusMessage owner = message; owner.type = ConsensusMessageType::EZBFT_NEW_OWNER;
            owner.digest = (static_cast<uint32_t>(message.digest) + 1) % m_replicaIds.size();
            SendToAll(owner); HandleNewOwner(owner);
        }
    }
    void EzBftEngine::HandleNewOwner(const ConsensusMessage& message) {
        if (message.digest >= m_replicaIds.size()) return;
        m_ownerEpochs[static_cast<uint32_t>(message.digest)] = message.view;
    }
    void EzBftEngine::Receive(const ConsensusMessage& message) {
        if (!m_running || message.protocol != ConsensusProtocol::EZBFT) return;
        switch (message.type) {
          case ConsensusMessageType::EZBFT_SPEC_ORDER: {
              Key key{message.senderId, message.view, message.instanceId};
              if (m_iorsTerminalGate && message.iorsMarked && !m_instances[key].present) {
                  if (m_markedPending.emplace(key, message).second)
                      m_gateEvents[key] = Simulator::Schedule(m_gateTimeout, &EzBftEngine::OnGateTimeout, this, key);
                  break;
              }
              auto pending = m_markedPending.find(key);
              if (!message.iorsMarked && pending != m_markedPending.end()) {
                  ConsensusMessage delayed = pending->second;
                  Simulator::Cancel(m_gateEvents[key]);
                  m_gateEvents.erase(key);
                  m_markedPending.erase(pending);
                  HandleOrder(message);
                  HandleOrder(delayed);
                  break;
              }
              HandleOrder(message);
              break;
          }
          case ConsensusMessageType::EZBFT_COMMIT_FAST:
          case ConsensusMessageType::EZBFT_COMMIT: HandleCommit(message); break;
          case ConsensusMessageType::EZBFT_PREPARE: HandlePrepare(message); break;
          case ConsensusMessageType::EZBFT_COMMIT_VOTE: HandleCommitVote(message); break;
          case ConsensusMessageType::EZBFT_OWNER_CHANGE: HandleOwnerChange(message); break;
          case ConsensusMessageType::EZBFT_NEW_OWNER: HandleNewOwner(message); break;
          default: break;
        }
    }

} // namespace ns3
