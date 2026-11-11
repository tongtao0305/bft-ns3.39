#include "zyzzyva-engine.h"
#include "zyzzyva-auth.h"

#include "ns3/boolean.h"
#include "ns3/simulator.h"
#include "ns3/string.h"
#include "ns3/uinteger.h"

#include <algorithm>
#include <map>
#include <limits>
#include <cstring>
#include <sstream>

namespace ns3 {
    namespace {
        std::vector<uint8_t> EncodeAuthenticated(const ConsensusMessage& message) {
            ConsensusMessage copy = message;
            if (copy.authentication.empty() || copy.payloadBytes.size() < copy.authentication.size() ||
                !std::equal(copy.authentication.begin(), copy.authentication.end(),
                            copy.payloadBytes.end() - copy.authentication.size()))
                copy.payloadBytes.insert(copy.payloadBytes.end(), copy.authentication.begin(), copy.authentication.end());
            Ptr<Packet> packet = copy.ToPacket();
            std::vector<uint8_t> bytes(packet->GetSize());
            packet->CopyData(bytes.data(), bytes.size());
            return bytes;
        }
        bool DecodeAuthenticated(const std::vector<uint8_t>& bytes, ConsensusMessage& message) {
            if (bytes.empty() || bytes.size() > 1024 * 1024) return false;
            Ptr<Packet> packet = Create<Packet>(bytes.data(), bytes.size());
            return ConsensusMessage::FromPacket(packet, message) && ZyzzyvaAuth::Verify(message);
        }
    }

    uint64_t ZyzzyvaEngine::GetLogFingerprint() const {
        return m_log.empty() ? 0 : m_log.back().history;
    }

    NS_OBJECT_ENSURE_REGISTERED(ZyzzyvaEngine);

    TypeId ZyzzyvaEngine::GetTypeId() {
        static TypeId tid = TypeId("ns3::ZyzzyvaEngine")
            .SetParent<ConsensusEngine>().SetGroupName("Consensus").AddConstructor<ZyzzyvaEngine>()
            .AddTraceSource("CandidateExecution", "Candidate actually executed, undone or confirmed by this replica.",
                            MakeTraceSourceAccessor(&ZyzzyvaEngine::m_executionCandidateTrace),
                            "ns3::ConsensusApplication::CandidateTracedCallback")
            .AddAttribute("FaultTolerance", "Byzantine fault bound.", UintegerValue(1),
                          MakeUintegerAccessor(&ZyzzyvaEngine::m_faultTolerance), MakeUintegerChecker<uint32_t>())
            .AddAttribute("RequireClientAuthentication", "Validate portable client content signatures in requests, histories and proposals.", BooleanValue(false),
                          MakeBooleanAccessor(&ZyzzyvaEngine::m_requireClientAuthentication), MakeBooleanChecker())
            .AddAttribute("RecoveryRule", "0=original Zyzzyva, 1=Velma.", UintegerValue(1),
                          MakeUintegerAccessor(&ZyzzyvaEngine::m_recoveryRule), MakeUintegerChecker<uint32_t>(0, 1))
            .AddAttribute("ViewChangeEnabled", "Enable normal Zyzzyva view-change processing.", BooleanValue(true),
                          MakeBooleanAccessor(&ZyzzyvaEngine::m_viewChangeEnabled), MakeBooleanChecker())
            .AddAttribute("IorsTerminalGate", "Wait briefly for an unmarked ORDER-REQ.", BooleanValue(false),
                          MakeBooleanAccessor(&ZyzzyvaEngine::m_iorsTerminalGate), MakeBooleanChecker())
            .AddAttribute("IorsGateTimeout", "Marked ORDER-REQ fallback timeout.", TimeValue(MilliSeconds(1)),
                          MakeTimeAccessor(&ZyzzyvaEngine::m_gateTimeout), MakeTimeChecker())
            .AddAttribute("InstanceTimeout", "Timeout before accusing the current primary.", TimeValue(MilliSeconds(50)),
                          MakeTimeAccessor(&ZyzzyvaEngine::m_viewTimeout), MakeTimeChecker())
            .AddAttribute("CheckpointInterval", "Committed requests between checkpoints.", UintegerValue(10),
                          MakeUintegerAccessor(&ZyzzyvaEngine::m_checkpointInterval), MakeUintegerChecker<uint32_t>(1))
            .AddAttribute("ExecutionTimePerTransaction", "Simulated application execution service per transaction.", TimeValue(MicroSeconds(15)),
                          MakeTimeAccessor(&ZyzzyvaEngine::m_executionTime), MakeTimeChecker())
            .AddAttribute("RollbackTimePerTransaction", "Simulated application undo service per transaction.", TimeValue(MicroSeconds(15)),
                          MakeTimeAccessor(&ZyzzyvaEngine::m_rollbackTime), MakeTimeChecker())
            .AddAttribute("EquivocationEnabled", "Primary sends a conflicting ORDER-REQ to one backup.", BooleanValue(false),
                          MakeBooleanAccessor(&ZyzzyvaEngine::m_equivocationEnabled), MakeBooleanChecker())
            .AddAttribute("MaliciousReplicaIds", "Comma-separated replica IDs permitted to inject equivocation.", StringValue(""),
                          MakeStringAccessor(&ZyzzyvaEngine::m_maliciousReplicaIds), MakeStringChecker())
            .AddAttribute("EquivocationTarget", "Target backup ID for equivocation.", UintegerValue(1),
                          MakeUintegerAccessor(&ZyzzyvaEngine::m_equivocationTarget), MakeUintegerChecker<uint32_t>())
            .AddAttribute("EquivocationDelay", "Delay before the canonical candidate reaches the target.",
                          TimeValue(MicroSeconds(20)), MakeTimeAccessor(&ZyzzyvaEngine::m_equivocationDelay), MakeTimeChecker())
            .AddAttribute("EquivocationStartSequence", "First slot at which to inject the controlled equivocation.", UintegerValue(1),
                          MakeUintegerAccessor(&ZyzzyvaEngine::m_equivocationStartSequence), MakeUintegerChecker<uint64_t>(1))
            .AddAttribute("EquivocationPlan", "Semicolon-separated view,primary,absolute-trigger-seconds,target attack plan.", StringValue(""),
                          MakeStringAccessor(&ZyzzyvaEngine::m_equivocationPlan), MakeStringChecker());
        return tid;
    }

    ZyzzyvaEngine::ZyzzyvaEngine() = default;

    void ZyzzyvaEngine::Configure(uint32_t replicaId, const std::vector<uint32_t>& replicaIds) {
        ConsensusEngine::Configure(replicaId, replicaIds);
    }

    void ZyzzyvaEngine::Start() {
        NS_ABORT_MSG_IF(m_replicaIds.size() != 3 * m_faultTolerance + 1, "Zyzzyva needs n=3f+1");
        m_running = true;
        if (!m_resourceModel) m_resourceModel = CreateObject<ConsensusResourceModel>();
        ReportLeader(m_view, GetLeaderId());
    }

    void ZyzzyvaEngine::Stop() {
        m_running = false;
        Simulator::Cancel(m_progressEvent);
        Simulator::Cancel(m_newViewEvent);
        for (auto& pending : m_gateEvents) Simulator::Cancel(pending.second);
        m_gateEvents.clear();
        for (auto& retry : m_iorsEvidenceRetryEvents) Simulator::Cancel(retry.second);
        m_iorsEvidenceRetryEvents.clear();
        m_iorsEvidenceRequests.clear();
        m_markedPending.clear();
        m_futureOrders.clear();
        m_fillHoleSentAt.clear();
    }

    uint64_t ZyzzyvaEngine::GetExecutionBacklog() const {
        uint64_t total = 0;
        for (size_t i = m_applicationHistory.size(); i < m_log.size(); ++i)
            total += std::max<uint32_t>(1, m_log[i].transactions);
        return total;
    }

    uint32_t ZyzzyvaEngine::GetLeaderId() const {
        return m_replicaIds.empty() ? 0 : m_replicaIds[m_view % m_replicaIds.size()];
    }

    uint64_t ZyzzyvaEngine::Extend(uint64_t previous, uint64_t digest, uint64_t sequence) {
        uint64_t value = previous ^ (digest + 0x9e3779b97f4a7c15ULL + (previous << 6) + (previous >> 2));
        value ^= sequence * 0xd6e8feb86659fd93ULL;
        return value ? value : 1;
    }

    void ZyzzyvaEngine::Write32(std::vector<uint8_t>& b, uint32_t v) {
        for (int shift = 24; shift >= 0; shift -= 8) b.push_back(static_cast<uint8_t>(v >> shift));
    }
    void ZyzzyvaEngine::Write64(std::vector<uint8_t>& b, uint64_t v) {
        for (int shift = 56; shift >= 0; shift -= 8) b.push_back(static_cast<uint8_t>(v >> shift));
    }
    bool ZyzzyvaEngine::Read32(const std::vector<uint8_t>& b, size_t& pos, uint32_t& v) {
        if (b.size() - std::min(pos, b.size()) < 4) return false;
        v = 0;
        for (int i = 0; i < 4; ++i) v = (v << 8) | b[pos++];
        return true;
    }
    bool ZyzzyvaEngine::Read64(const std::vector<uint8_t>& b, size_t& pos, uint64_t& v) {
        if (b.size() - std::min(pos, b.size()) < 8) return false;
        v = 0;
        for (int i = 0; i < 8; ++i) v = (v << 8) | b[pos++];
        return true;
    }
    void ZyzzyvaEngine::EncodeEntry(std::vector<uint8_t>& b, const Entry& e) {
        Write64(b, e.sequence); Write64(b, e.view); Write64(b, e.digest); Write64(b, e.history);
        Write32(b, e.client); Write64(b, e.request); Write32(b, e.transactions); Write32(b, e.payloadSize);
        Write32(b, e.clientAuthentication.size());
        b.insert(b.end(), e.clientAuthentication.begin(), e.clientAuthentication.end());
    }
    bool ZyzzyvaEngine::DecodeEntry(const std::vector<uint8_t>& b, size_t& p, Entry& e) {
        uint32_t length = 0;
        if (!(Read64(b, p, e.sequence) && Read64(b, p, e.view) && Read64(b, p, e.digest) &&
               Read64(b, p, e.history) && Read32(b, p, e.client) && Read64(b, p, e.request) &&
               Read32(b, p, e.transactions) && Read32(b, p, e.payloadSize) && Read32(b, p, length)) ||
               length > ZyzzyvaAuth::SIGNATURE_SIZE || p > b.size() || b.size() - p < length) return false;
        e.clientAuthentication.assign(b.begin() + p, b.begin() + p + length);
        p += length;
        return true;
    }
    void ZyzzyvaEngine::EncodeProof(std::vector<uint8_t>& b, const ViewProof& p) {
        const size_t start = b.size();
        Write32(b, p.sender);
        Write64(b, p.targetView);
        Write32(b, p.log.size());
        for (const Entry& e : p.log) EncodeEntry(b, e);
        Write64(b, p.certificate.view); Write64(b, p.certificate.sequence);
        Write64(b, p.certificate.digest); Write64(b, p.certificate.history);
        Write32(b, p.certificate.voters.size());
        for (uint32_t voter : p.certificate.voters) {
            Write32(b, voter);
            auto signature = p.certificate.signatures.find(voter);
            if (signature != p.certificate.signatures.end())
                b.insert(b.end(), signature->second.begin(), signature->second.end());
            else b.resize(b.size() + ZyzzyvaAuth::SIGNATURE_SIZE, 0);
        }
        Write32(b, p.hateProofs.size());
        for (const auto& hate : p.hateProofs) {
            Write32(b, hate.first);
            b.insert(b.end(), hate.second.begin(), hate.second.end());
        }
        if (p.signature.size() == ZyzzyvaAuth::SIGNATURE_SIZE)
            b.insert(b.end(), p.signature.begin(), p.signature.end());
        else {
            std::vector<uint8_t> signature;
            std::vector<uint8_t> body(b.begin() + start, b.end());
            ZyzzyvaAuth::SignDetached(p.sender, body, signature);
            b.insert(b.end(), signature.begin(), signature.end());
        }
    }
    bool ZyzzyvaEngine::DecodeProof(const std::vector<uint8_t>& b, size_t& pos, ViewProof& p) {
        const size_t start = pos;
        uint32_t count = 0;
        if (!Read32(b, pos, p.sender) || !Read64(b, pos, p.targetView) ||
            !Read32(b, pos, count)) return false;
        for (uint32_t i = 0; i < count; ++i) {
            Entry e;
            if (!DecodeEntry(b, pos, e)) return false;
            p.log.push_back(e);
        }
        if (!Read64(b, pos, p.certificate.view) || !Read64(b, pos, p.certificate.sequence) ||
            !Read64(b, pos, p.certificate.digest) || !Read64(b, pos, p.certificate.history) ||
            !Read32(b, pos, count)) return false;
        for (uint32_t i = 0; i < count; ++i) {
            uint32_t voter;
            if (!Read32(b, pos, voter) || !p.certificate.voters.insert(voter).second) return false;
            if (b.size() - std::min(pos, b.size()) < ZyzzyvaAuth::SIGNATURE_SIZE) return false;
            p.certificate.signatures[voter] = std::vector<uint8_t>(b.begin() + pos,
                b.begin() + pos + ZyzzyvaAuth::SIGNATURE_SIZE);
            pos += ZyzzyvaAuth::SIGNATURE_SIZE;
        }
        if (!Read32(b, pos, count)) return false;
        for (uint32_t i = 0; i < count; ++i) {
            uint32_t voter;
            if (!Read32(b, pos, voter) || p.hateProofs.count(voter) ||
                b.size() - std::min(pos, b.size()) < ZyzzyvaAuth::SIGNATURE_SIZE) return false;
            p.hateProofs[voter] = std::vector<uint8_t>(b.begin() + pos,
                b.begin() + pos + ZyzzyvaAuth::SIGNATURE_SIZE);
            pos += ZyzzyvaAuth::SIGNATURE_SIZE;
        }
        if (b.size() - std::min(pos, b.size()) < ZyzzyvaAuth::SIGNATURE_SIZE) return false;
        std::vector<uint8_t> body(b.begin() + start, b.begin() + pos);
        p.signature.assign(b.begin() + pos, b.begin() + pos + ZyzzyvaAuth::SIGNATURE_SIZE);
        pos += ZyzzyvaAuth::SIGNATURE_SIZE;
        return ZyzzyvaAuth::VerifyDetached(p.sender, body, p.signature);
    }

    bool ZyzzyvaEngine::ValidateLog(const std::vector<Entry>& log) const {
        uint64_t previous = 0;
        for (size_t i = 0; i < log.size(); ++i) {
            const Entry& e = log[i];
            if (m_requireClientAuthentication && !ZyzzyvaAuth::VerifyClientRequest(
                ClientRequest{e.client, e.request, e.payloadSize, e.transactions, e.digest, e.clientAuthentication})) return false;
            if (e.sequence != i + 1 || e.history != Extend(previous, e.digest, e.sequence)) return false;
            previous = e.history;
        }
        return true;
    }

    bool ZyzzyvaEngine::ValidateProof(const ViewProof& proof) const {
        if (!ValidateLog(proof.log)) return false;
        if (proof.hateProofs.size() < m_faultTolerance + 1 || proof.targetView == 0) return false;
        std::vector<uint8_t> hateData;
        Write64(hateData, proof.targetView - 1);
        for (const auto& hate : proof.hateProofs) {
            if (std::find(m_replicaIds.begin(), m_replicaIds.end(), hate.first) == m_replicaIds.end() ||
                !ZyzzyvaAuth::VerifyDetached(hate.first, hateData, hate.second)) return false;
        }
        const Certificate& cert = proof.certificate;
        if (cert.sequence == 0) return cert.voters.empty();
        if (cert.sequence > proof.log.size() || cert.voters.size() < 2 * m_faultTolerance + 1 ||
            proof.log[cert.sequence - 1].history != cert.history ||
            proof.log[cert.sequence - 1].digest != cert.digest) return false;
        const Entry& e = proof.log[cert.sequence - 1];
        for (uint32_t voter : cert.voters) {
            auto signature = cert.signatures.find(voter);
            if (std::find(m_replicaIds.begin(), m_replicaIds.end(), voter) == m_replicaIds.end() ||
                signature == cert.signatures.end()) return false;
            ConsensusMessage response;
            response.protocol = ConsensusProtocol::ZYZZYVA;
            response.type = ConsensusMessageType::ZYZZYVA_SPEC_RESPONSE;
            response.senderId = voter;
            response.receiverId = e.client;
            response.view = cert.view;
            response.instanceId = e.sequence;
            response.digest = e.digest;
            response.clientId = e.client;
            response.requestId = e.request;
            response.transactionCount = e.transactions;
            Write64(response.payloadBytes, e.history);
            if (m_requireClientAuthentication) {
                auto orderProof = MakeOrderProof(e);
                auto proofBytes = EncodeAuthenticated(orderProof);
                Write32(response.payloadBytes, static_cast<uint32_t>(proofBytes.size()));
                response.payloadBytes.insert(response.payloadBytes.end(), proofBytes.begin(), proofBytes.end());
            }
            if (!ZyzzyvaAuth::VerifyContent(response, signature->second)) return false;
        }
        return true;
    }

    bool ZyzzyvaEngine::Extends(const std::vector<Entry>& longer, const std::vector<Entry>& prefix) const {
        if (longer.size() < prefix.size()) return false;
        for (size_t i = 0; i < prefix.size(); ++i)
            if (longer[i].history != prefix[i].history || longer[i].digest != prefix[i].digest) return false;
        return true;
    }

    void ZyzzyvaEngine::BroadcastMessage(ConsensusMessage message) {
        message.protocol = ConsensusProtocol::ZYZZYVA;
        message.senderId = m_replicaId;
        for (uint32_t peer : m_replicaIds) {
            if (peer == m_replicaId) Receive(message);
            else Send(peer, message);
        }
    }

    void ZyzzyvaEngine::Submit(const ClientRequest& request) {
        if (!m_running || request.digest == 0) return;
        if (m_requireClientAuthentication && !ZyzzyvaAuth::VerifyClientRequest(request)) return;
        if (m_pendingView > m_view || m_recovering) {
            m_recoveryRequests[{request.clientId, request.requestId}] = request;
            return;
        }
        auto cached = m_requestSequences.find({request.clientId, request.requestId});
        if (cached != m_requestSequences.end()) {
            const Entry& entry = m_log[cached->second - 1];
            if (entry.digest == request.digest && cached->second <= m_applicationHistory.size()) SendSpecResponse(entry);
            return;
        }
        if (m_replicaId != GetLeaderId()) {
            m_pendingForwards.insert({request.clientId, request.requestId});
            ConsensusMessage forwarded;
            forwarded.protocol = ConsensusProtocol::ZYZZYVA;
            forwarded.type = ConsensusMessageType::CLIENT_REQUEST;
            forwarded.digest = request.digest;
            forwarded.clientId = request.clientId;
            forwarded.requestId = request.requestId;
            forwarded.transactionCount = request.transactionCount;
            forwarded.payloadSize = request.payloadSize;
            forwarded.payloadBytes = request.authentication.empty() ? std::vector<uint8_t>(request.payloadSize, 0)
                : ZyzzyvaAuth::EncodeClientRequest(request);
            Send(GetLeaderId(), forwarded);
            ArmProgressTimer();
            return;
        }
        if (m_pendingView > m_view) return;
        Propose(request);
    }

    void ZyzzyvaEngine::Propose(const ClientRequest& request) {
        if (!m_seenRequests.insert({request.clientId, request.requestId}).second) return;
        Entry e;
        e.sequence = m_nextSequence++;
        e.view = m_view;
        e.digest = request.digest;
        e.client = request.clientId;
        e.request = request.requestId;
        e.transactions = request.transactionCount;
        e.payloadSize = request.payloadSize;
        e.clientAuthentication = request.authentication;
        e.history = Extend(m_log.empty() ? 0 : m_log.back().history, e.digest, e.sequence);
        m_log.push_back(e);
        m_requestSequences[{e.client, e.request}] = e.sequence;
        ConsensusMessage order;
        order.protocol = ConsensusProtocol::ZYZZYVA;
        order.type = ConsensusMessageType::ZYZZYVA_ORDER_REQ;
        order.view = m_view;
        order.instanceId = e.sequence;
        order.digest = e.digest;
        order.clientId = e.client;
        order.requestId = e.request;
        order.transactionCount = e.transactions;
        Write64(order.payloadBytes, e.history);
        if (!request.authentication.empty()) {
            auto body = ZyzzyvaAuth::EncodeClientRequest(request);
            order.payloadBytes.insert(order.payloadBytes.end(), body.begin(), body.end());
        } else order.payloadBytes.resize(order.payloadBytes.size() + e.payloadSize, 0);
        uint32_t attackTarget = m_equivocationTarget;
        bool plannedAttack = false;
        if (!m_equivocationPlan.empty()) {
            std::stringstream plans(m_equivocationPlan);
            std::string item;
            while (std::getline(plans, item, ';')) {
                std::replace(item.begin(), item.end(), ',', ' ');
                std::istringstream fields(item);
                uint64_t plannedView = 0;
                uint32_t plannedPrimary = 0, plannedTarget = 0;
                double triggerSeconds = 0;
                if (fields >> plannedView >> plannedPrimary >> triggerSeconds >> plannedTarget &&
                    plannedView == m_view && plannedPrimary == m_replicaId &&
                    Simulator::Now().GetSeconds() >= triggerSeconds &&
                    !m_equivocationInjectedViews.count(plannedView)) {
                    attackTarget = plannedTarget;
                    plannedAttack = true;
                    break;
                }
            }
        }
        bool configuredMalicious = m_maliciousReplicaIds.empty() && !m_replicaIds.empty() &&
                                   m_replicaId == m_replicaIds.front();
        if (!m_maliciousReplicaIds.empty()) {
            std::stringstream ids(m_maliciousReplicaIds);
            std::string token;
            while (std::getline(ids, token, ',')) {
                try { if (static_cast<uint32_t>(std::stoul(token)) == m_replicaId) configuredMalicious = true; }
                catch (...) { }
            }
        }
        const bool attack = m_equivocationEnabled && configuredMalicious &&
            (plannedAttack || (m_equivocationPlan.empty() && !m_equivocationInjectedViews.count(m_view) &&
             e.sequence >= m_equivocationStartSequence && m_replicaId == GetLeaderId())) &&
            attackTarget != m_replicaId &&
            std::find(m_replicaIds.begin(), m_replicaIds.end(), attackTarget) != m_replicaIds.end();
        if (attack) {
            m_equivocationInjectedViews.insert(m_view);
            m_equivocationTarget = attackTarget;
            m_equivocationSlot = e.sequence;
            m_equivocationHoldUntil = Simulator::Now() + m_equivocationDelay;
            ReportPhase(ConsensusProtocol::ZYZZYVA, e.sequence, 19, Seconds(0));
        }
        for (uint32_t peer : m_replicaIds) {
            if (peer == m_replicaId) continue;
            if (attack && peer == attackTarget) continue;
            if (m_equivocationInjectedViews.count(m_view) && peer == m_equivocationTarget && e.sequence > m_equivocationSlot &&
                Simulator::Now() < m_equivocationHoldUntil) continue;
            Send(peer, order);
        }
        if (attack) {
            ConsensusMessage conflicting = order;
            conflicting.digest ^= 0xd6e8feb86659fd93ULL;
            // The primary can sign its own colluding client request, never another client's request.
            ClientRequest alternate{m_replicaId, e.request, e.payloadSize, e.transactions, conflicting.digest};
            ZyzzyvaAuth::SignClientRequest(alternate);
            conflicting.clientId = alternate.clientId;
            conflicting.payloadBytes.clear();
            const uint64_t alternateHistory = Extend(m_log.size() == 1 ? 0 : m_log[m_log.size() - 2].history,
                                                     conflicting.digest, e.sequence);
            Write64(conflicting.payloadBytes, alternateHistory);
            auto alternateBody = ZyzzyvaAuth::EncodeClientRequest(alternate);
            conflicting.payloadBytes.insert(conflicting.payloadBytes.end(), alternateBody.begin(), alternateBody.end());
            Send(attackTarget, conflicting);
            // Later, the targeted replica receives the same authentic client request
            // at the next slot on its divergent prefix. Its SPEC-RESPONSE forms a
            // client-verifiable ordering proof against the earlier slot.
            ConsensusMessage lateRequest = order;
            lateRequest.instanceId = e.sequence + 1;
            lateRequest.payloadBytes.clear();
            Write64(lateRequest.payloadBytes, Extend(alternateHistory, e.digest, lateRequest.instanceId));
            auto originalBody = ZyzzyvaAuth::EncodeClientRequest(request);
            lateRequest.payloadBytes.insert(lateRequest.payloadBytes.end(), originalBody.begin(), originalBody.end());
            Simulator::Schedule(m_equivocationDelay, &ZyzzyvaEngine::SendDelayedOrder, this,
                                attackTarget, lateRequest);
        }
        QueueExecution(e, false);
    }

    void ZyzzyvaEngine::SendDelayedOrder(uint32_t peer, ConsensusMessage message) {
        if (m_running && message.view == m_view) Send(peer, message);
    }

    void ZyzzyvaEngine::StartIorsEvidenceRequest(const ConsensusMessage& markedOrder) {
        const auto key = std::make_pair(markedOrder.view, markedOrder.instanceId);
        if (!m_iorsEvidenceRequests.insert(key).second) return;
        ReportPhase(ConsensusProtocol::ZYZZYVA, markedOrder.instanceId, 20, Seconds(0));
        ConsensusMessage request;
        request.protocol = ConsensusProtocol::ZYZZYVA;
        request.type = ConsensusMessageType::ZYZZYVA_IORS_EVIDENCE_REQUEST;
        request.view = markedOrder.view;
        request.instanceId = markedOrder.instanceId;
        auto encoded = EncodeAuthenticated(markedOrder);
        Write32(request.payloadBytes, static_cast<uint32_t>(encoded.size()));
        request.payloadBytes.insert(request.payloadBytes.end(), encoded.begin(), encoded.end());
        for (uint32_t peer : m_replicaIds) if (peer != m_replicaId) Send(peer, request);
        ReportPhase(ConsensusProtocol::ZYZZYVA, markedOrder.instanceId, 21, Seconds(0));
        m_iorsEvidenceRetryEvents[key] = Simulator::Schedule(m_viewTimeout,
            &ZyzzyvaEngine::RetryIorsEvidenceRequest, this, markedOrder, 1);
    }

    void ZyzzyvaEngine::RetryIorsEvidenceRequest(ConsensusMessage markedOrder, uint32_t attempt) {
        const auto key = std::make_pair(markedOrder.view, markedOrder.instanceId);
        if (!m_running || markedOrder.view != m_view || !m_iorsEvidenceRequests.count(key)) return;
        if (attempt > 2) {
            ReportPhase(ConsensusProtocol::ZYZZYVA, markedOrder.instanceId, 26, Seconds(0));
            m_iorsEvidenceRequests.erase(key);
            m_iorsEvidenceRetryEvents.erase(key);
            return;
        }
        ConsensusMessage request;
        request.protocol = ConsensusProtocol::ZYZZYVA;
        request.type = ConsensusMessageType::ZYZZYVA_IORS_EVIDENCE_REQUEST;
        request.view = markedOrder.view;
        request.instanceId = markedOrder.instanceId;
        auto encoded = EncodeAuthenticated(markedOrder);
        Write32(request.payloadBytes, static_cast<uint32_t>(encoded.size()));
        request.payloadBytes.insert(request.payloadBytes.end(), encoded.begin(), encoded.end());
        for (uint32_t peer : m_replicaIds) if (peer != m_replicaId) Send(peer, request);
        ReportPhase(ConsensusProtocol::ZYZZYVA, markedOrder.instanceId, 22, Seconds(0));
        m_iorsEvidenceRetryEvents[key] = Simulator::Schedule(m_viewTimeout,
            &ZyzzyvaEngine::RetryIorsEvidenceRequest, this, markedOrder, attempt + 1);
    }

    void ZyzzyvaEngine::HandleIorsEvidenceRequest(const ConsensusMessage& message) {
        if (!m_running || message.view != m_view || message.instanceId == 0) return;
        size_t offset = 0;
        uint32_t length = 0;
        if (!Read32(message.payloadBytes, offset, length) || length == 0 ||
            message.payloadBytes.size() - offset != length) return;
        std::vector<uint8_t> bytes(message.payloadBytes.begin() + offset, message.payloadBytes.end());
        ConsensusMessage candidate;
        if (!DecodeAuthenticated(bytes, candidate) || candidate.type != ConsensusMessageType::ZYZZYVA_ORDER_REQ ||
            candidate.senderId != GetLeaderId() || candidate.view != message.view ||
            candidate.instanceId != message.instanceId) return;
        auto known = m_seenOrders.find({message.view, message.instanceId});
        if (known == m_seenOrders.end() || known->second.digest == candidate.digest) return;
        ConsensusMessage response;
        response.protocol = ConsensusProtocol::ZYZZYVA;
        response.type = ConsensusMessageType::ZYZZYVA_IORS_EVIDENCE_RESPONSE;
        response.view = message.view;
        response.instanceId = message.instanceId;
        for (const auto* proof : {&candidate, &known->second}) {
            auto proofBytes = EncodeAuthenticated(*proof);
            Write32(response.payloadBytes, static_cast<uint32_t>(proofBytes.size()));
            response.payloadBytes.insert(response.payloadBytes.end(), proofBytes.begin(), proofBytes.end());
        }
        Send(message.senderId, response);
    }

    void ZyzzyvaEngine::HandleIorsEvidenceResponse(const ConsensusMessage& message) {
        if (!m_running || message.view != m_view || message.instanceId == 0 ||
            std::find(m_replicaIds.begin(), m_replicaIds.end(), message.senderId) == m_replicaIds.end()) return;
        size_t offset = 0;
        ConsensusMessage evidence[2];
        for (auto& proof : evidence) {
            uint32_t length = 0;
            if (!Read32(message.payloadBytes, offset, length) || length == 0 ||
                message.payloadBytes.size() - offset < length) return;
            std::vector<uint8_t> bytes(message.payloadBytes.begin() + offset,
                                       message.payloadBytes.begin() + offset + length);
            if (!DecodeAuthenticated(bytes, proof)) return;
            offset += length;
        }
        if (offset != message.payloadBytes.size()) return;
        if (evidence[0].type != ConsensusMessageType::ZYZZYVA_ORDER_REQ ||
            evidence[1].type != ConsensusMessageType::ZYZZYVA_ORDER_REQ ||
            evidence[0].senderId != GetLeaderId() || evidence[1].senderId != GetLeaderId() ||
            evidence[0].view != message.view || evidence[1].view != message.view ||
            evidence[0].instanceId != message.instanceId || evidence[1].instanceId != message.instanceId ||
            evidence[0].digest == evidence[1].digest ||
            !ZyzzyvaAuth::VerifyContent(evidence[0], evidence[0].authentication) ||
            !ZyzzyvaAuth::VerifyContent(evidence[1], evidence[1].authentication)) {
            ReportPhase(ConsensusProtocol::ZYZZYVA, message.instanceId, 24, Seconds(0));
            return;
        }
        ReportPhase(ConsensusProtocol::ZYZZYVA, message.instanceId, 23, Seconds(0));
        auto key = std::make_pair(message.view, message.instanceId);
        auto retry = m_iorsEvidenceRetryEvents.find(key);
        if (retry != m_iorsEvidenceRetryEvents.end()) {
            Simulator::Cancel(retry->second);
            m_iorsEvidenceRetryEvents.erase(retry);
        }
        m_iorsEvidenceRequests.erase(key);
        TriggerHate(&evidence[0], &evidence[1]);
    }

    void ZyzzyvaEngine::SendSpecResponse(const Entry& e) {
        ConsensusMessage response;
        response.protocol = ConsensusProtocol::ZYZZYVA;
        response.type = ConsensusMessageType::ZYZZYVA_SPEC_RESPONSE;
        response.view = e.view;
        response.instanceId = e.sequence;
        response.digest = e.digest;
        response.clientId = e.client;
        response.requestId = e.request;
        response.transactionCount = e.transactions;
        Write64(response.payloadBytes, e.history);
        ConsensusMessage proof = MakeOrderProof(e);
        auto proofBytes = EncodeAuthenticated(proof);
        Write32(response.payloadBytes, static_cast<uint32_t>(proofBytes.size()));
        response.payloadBytes.insert(response.payloadBytes.end(), proofBytes.begin(), proofBytes.end());
        Send(e.client, response);
    }

    ConsensusMessage ZyzzyvaEngine::MakeOrderProof(const Entry& e) const {
        ConsensusMessage proof;
        proof.protocol = ConsensusProtocol::ZYZZYVA;
        proof.type = ConsensusMessageType::ZYZZYVA_ORDER_REQ;
        proof.senderId = m_replicaIds[e.view % m_replicaIds.size()];
        proof.receiverId = e.client;
        proof.view = e.view;
        proof.instanceId = e.sequence;
        proof.digest = e.digest;
        proof.clientId = e.client;
        proof.requestId = e.request;
        proof.transactionCount = e.transactions;
        Write64(proof.payloadBytes, e.history);
        ClientRequest request{e.client, e.request, e.payloadSize, e.transactions, e.digest, e.clientAuthentication};
        auto body = ZyzzyvaAuth::EncodeClientRequest(request);
        proof.payloadBytes.insert(proof.payloadBytes.end(), body.begin(), body.end());
        ZyzzyvaAuth::Sign(proof);
        return proof;
    }

    void ZyzzyvaEngine::AcceptOrder(const ConsensusMessage& message) {
        if (message.view != m_view || message.senderId != GetLeaderId()) return;
        if (message.instanceId == 0 || m_pendingView > m_view) return;
        if (m_recovering) {
            m_futureOrders.emplace(std::make_pair(message.view, message.instanceId), message);
            return;
        }
        size_t offset = 0;
        uint64_t history;
        if (!Read64(message.payloadBytes, offset, history)) return;
        if (message.instanceId <= m_log.size()) {
            const Entry& e = m_log[message.instanceId - 1];
            if (e.digest != message.digest || e.history != history) {
                auto original = m_seenOrders.find(std::make_pair(message.view, message.instanceId));
                if (original != m_seenOrders.end()) TriggerHate(&original->second, &message);
                else TriggerHate();
            }
            return;
        }
        if (message.instanceId != m_log.size() + 1) {
            m_futureOrders.emplace(std::make_pair(message.view, message.instanceId), message);
            RequestFillHole(m_view, m_log.size() + 1, message.instanceId - 1);
            ArmProgressTimer();
            return;
        }
        Entry e;
        e.sequence = message.instanceId;
        e.view = message.view;
        e.digest = message.digest;
        e.history = history;
        e.client = message.clientId;
        e.request = message.requestId;
        e.transactions = message.transactionCount;
        e.payloadSize = message.payloadBytes.size() - offset;
        ClientRequest clientRequest;
        if (ZyzzyvaAuth::DecodeClientRequest(message, offset, clientRequest)) {
            e.payloadSize = clientRequest.payloadSize;
            e.clientAuthentication = clientRequest.authentication;
        } else if (m_requireClientAuthentication) return;
        if (history != Extend(m_log.empty() ? 0 : m_log.back().history, e.digest, e.sequence)) {
            ReportPhase(ConsensusProtocol::ZYZZYVA, e.sequence, 8, Seconds(0));
            TriggerHate();
            return;
        }
        m_log.push_back(e);
        m_requestSequences[{e.client, e.request}] = e.sequence;
        m_pendingForwards.erase({e.client, e.request});
        m_seenOrders.emplace(std::make_pair(message.view, message.instanceId), message);
        m_nextSequence = m_log.size() + 1;
        QueueExecution(e, false);
        auto next = m_futureOrders.find(std::make_pair(m_view, m_nextSequence));
        if (next != m_futureOrders.end()) {
            ConsensusMessage pending = next->second;
            m_futureOrders.erase(next);
            AcceptOrder(pending);
        }
        if (m_futureOrders.empty() && m_pendingForwards.empty()) Simulator::Cancel(m_progressEvent);
    }

    void ZyzzyvaEngine::OnGateTimeout(uint64_t view, uint64_t sequence) {
        auto key = std::make_pair(view, sequence);
        auto pending = m_markedPending.find(key);
        if (pending == m_markedPending.end()) return;
        ConsensusMessage message = pending->second;
        m_markedPending.erase(pending);
        m_gateEvents.erase(key);
        ReportPhase(ConsensusProtocol::ZYZZYVA, sequence, 25, Seconds(0));
        if (m_running) AcceptOrder(message);
    }

    void ZyzzyvaEngine::HandleCommitRequest(const ConsensusMessage& message) {
        if (message.instanceId == 0 || message.instanceId > m_log.size()) return;
        if (message.instanceId > m_applicationHistory.size() || m_recovering) {
            m_pendingCommits[message.instanceId] = message;
            return;
        }
        const Entry& e = m_log[message.instanceId - 1];
        if (e.digest != message.digest || e.client != message.clientId || e.request != message.requestId ||
            message.view > m_view) return;
        size_t offset = 0;
        uint64_t history;
        uint32_t count;
        if (!Read64(message.payloadBytes, offset, history) || history != e.history ||
            !Read32(message.payloadBytes, offset, count) || count < 2 * m_faultTolerance + 1 ||
            count > m_replicaIds.size()) return;
        Certificate certificate;
        certificate.view = message.view;
        certificate.sequence = message.instanceId;
        certificate.digest = message.digest;
        certificate.history = history;
        for (uint32_t i = 0; i < count; ++i) {
            uint32_t sender;
            if (!Read32(message.payloadBytes, offset, sender) ||
                std::find(m_replicaIds.begin(), m_replicaIds.end(), sender) == m_replicaIds.end() ||
                !certificate.voters.insert(sender).second) return;
            if (message.payloadBytes.size() - std::min(offset, message.payloadBytes.size()) <
                ZyzzyvaAuth::SIGNATURE_SIZE) return;
            std::vector<uint8_t> signature(message.payloadBytes.begin() + offset,
                                           message.payloadBytes.begin() + offset + ZyzzyvaAuth::SIGNATURE_SIZE);
            offset += ZyzzyvaAuth::SIGNATURE_SIZE;
            ConsensusMessage response;
            response.protocol = ConsensusProtocol::ZYZZYVA;
            response.type = ConsensusMessageType::ZYZZYVA_SPEC_RESPONSE;
            response.senderId = sender;
            response.receiverId = message.clientId;
            response.view = message.view;
            response.instanceId = e.sequence;
            response.digest = e.digest;
            response.clientId = e.client;
            response.requestId = e.request;
            response.transactionCount = e.transactions;
            Write64(response.payloadBytes, e.history);
            if (m_requireClientAuthentication) {
                auto orderProof = MakeOrderProof(e);
                auto proofBytes = EncodeAuthenticated(orderProof);
                Write32(response.payloadBytes, static_cast<uint32_t>(proofBytes.size()));
                response.payloadBytes.insert(response.payloadBytes.end(), proofBytes.begin(), proofBytes.end());
            }
            if (!ZyzzyvaAuth::VerifyContent(response, signature)) return;
            certificate.signatures[sender] = signature;
        }
        if (offset != message.payloadBytes.size()) return;
        auto storedCertificate = m_certificates.find(e.sequence);
        if (storedCertificate == m_certificates.end() || storedCertificate->second.view <= certificate.view)
            m_certificates[e.sequence] = certificate;
        m_committedThrough = std::max(m_committedThrough, e.sequence);
        ReportPhase(ConsensusProtocol::ZYZZYVA, e.sequence, 3, Seconds(0));
        m_executionCandidateTrace(m_replicaIds[e.view % m_replicaIds.size()], m_replicaId,
            static_cast<uint16_t>(ConsensusMessageType::ZYZZYVA_ORDER_REQ), message.view, e.sequence, e.digest,
            e.client, e.request, 5, 0);
        ConsensusMessage reply;
        reply.protocol = ConsensusProtocol::ZYZZYVA;
        reply.type = ConsensusMessageType::ZYZZYVA_LOCAL_COMMIT;
        reply.view = message.view;
        reply.instanceId = e.sequence;
        reply.digest = e.digest;
        reply.clientId = e.client;
        reply.requestId = e.request;
        reply.transactionCount = e.transactions;
        Write64(reply.payloadBytes, e.history);
        Send(e.client, reply);
        if (e.sequence % m_checkpointInterval == 0) {
            ConsensusMessage checkpoint;
            checkpoint.type = ConsensusMessageType::ZYZZYVA_CHECKPOINT;
            checkpoint.view = m_view;
            checkpoint.instanceId = e.sequence;
            checkpoint.digest = e.history;
            BroadcastMessage(checkpoint);
        }
        ConsensusDecision decision;
        decision.protocol = ConsensusProtocol::ZYZZYVA;
        decision.view = message.view;
        decision.instanceId = e.sequence;
        decision.digest = e.digest;
        decision.clientId = e.client;
        decision.requestId = e.request;
        decision.transactionCount = e.transactions;
        decision.decisionTime = Simulator::Now();
        Decide(decision);
    }

    void ZyzzyvaEngine::RequestFillHole(uint64_t view, uint64_t first, uint64_t last) {
        const auto key = std::make_pair(view, first);
        auto sent = m_fillHoleSentAt.find(key);
        // Repeated future orders and repair replies must not recursively fan
        // out the same missing range. Retry only on the progress timer.
        if (sent != m_fillHoleSentAt.end() && Simulator::Now() - sent->second < m_viewTimeout) return;
        m_fillHoleSentAt[key] = Simulator::Now();
        ConsensusMessage fill;
        fill.protocol = ConsensusProtocol::ZYZZYVA;
        fill.type = ConsensusMessageType::ZYZZYVA_FILL_HOLE;
        fill.view = view;
        fill.instanceId = first;
        fill.requestId = last;
        BroadcastMessage(fill);
    }

    void ZyzzyvaEngine::HandleFillHole(const ConsensusMessage& message) {
        if (message.view != m_view || message.senderId == m_replicaId) return;
        if (m_replicaId == GetLeaderId() && message.senderId == m_equivocationTarget &&
            Simulator::Now() < m_equivocationHoldUntil) return;
        if (message.instanceId == 0) {
            auto cached = m_newViews.find(message.view);
            if (cached == m_newViews.end()) return;
            if (m_replicaId == GetLeaderId()) { Send(message.senderId, cached->second); return; }
            if (cached->second.authentication.empty()) return;
            ConsensusMessage inner = cached->second;
            inner.payloadBytes.insert(inner.payloadBytes.end(), inner.authentication.begin(), inner.authentication.end());
            Ptr<Packet> packet = inner.ToPacket();
            ConsensusMessage repair;
            repair.protocol = ConsensusProtocol::ZYZZYVA;
            repair.type = ConsensusMessageType::ZYZZYVA_REPAIR;
            repair.view = m_view;
            repair.payloadBytes.resize(packet->GetSize());
            packet->CopyData(repair.payloadBytes.data(), repair.payloadBytes.size());
            Send(message.senderId, repair);
            return;
        }
        for (uint64_t seq = message.instanceId; seq <= message.requestId && seq <= m_log.size(); ++seq) {
            auto original = m_seenOrders.find({m_view, seq});
            if (m_replicaId != GetLeaderId()) {
                if (original == m_seenOrders.end() || original->second.authentication.empty()) continue;
                ConsensusMessage inner = original->second;
                inner.payloadBytes.insert(inner.payloadBytes.end(), inner.authentication.begin(), inner.authentication.end());
                Ptr<Packet> packet = inner.ToPacket();
                ConsensusMessage repair;
                repair.protocol = ConsensusProtocol::ZYZZYVA;
                repair.type = ConsensusMessageType::ZYZZYVA_REPAIR;
                repair.view = m_view;
                repair.instanceId = seq;
                repair.payloadBytes.resize(packet->GetSize());
                packet->CopyData(repair.payloadBytes.data(), repair.payloadBytes.size());
                Send(message.senderId, repair);
                continue;
            }
            const Entry& e = m_log[seq - 1];
            ConsensusMessage order;
            order.protocol = ConsensusProtocol::ZYZZYVA;
            order.type = ConsensusMessageType::ZYZZYVA_ORDER_REQ;
            order.view = e.view;
            order.instanceId = e.sequence;
            order.digest = e.digest;
            order.clientId = e.client;
            order.requestId = e.request;
            order.transactionCount = e.transactions;
            Write64(order.payloadBytes, e.history);
            if (!e.clientAuthentication.empty()) {
                ClientRequest request{e.client, e.request, e.payloadSize, e.transactions, e.digest, e.clientAuthentication};
                auto body = ZyzzyvaAuth::EncodeClientRequest(request);
                order.payloadBytes.insert(order.payloadBytes.end(), body.begin(), body.end());
            } else order.payloadBytes.resize(order.payloadBytes.size() + e.payloadSize, 0);
            Send(message.senderId, order);
        }
    }

    void ZyzzyvaEngine::HandleRepair(const ConsensusMessage& message) {
        if (message.view < m_view || message.payloadBytes.empty() ||
            std::find(m_replicaIds.begin(), m_replicaIds.end(), message.senderId) == m_replicaIds.end()) return;
        ConsensusMessage order;
        if (!ConsensusMessage::FromPacket(Create<Packet>(message.payloadBytes.data(), message.payloadBytes.size()), order) ||
            !ZyzzyvaAuth::Verify(order) || order.protocol != ConsensusProtocol::ZYZZYVA ||
            order.senderId != m_replicaIds[message.view % m_replicaIds.size()] ||
            order.view != message.view || order.instanceId != message.instanceId) return;
        if (order.type == ConsensusMessageType::NEW_VIEW) { HandleNewView(order); return; }
        if (order.type != ConsensusMessageType::ZYZZYVA_ORDER_REQ) return;
        Receive(order);
    }

    void ZyzzyvaEngine::HandleCheckpoint(const ConsensusMessage& message) {
        if (std::find(m_replicaIds.begin(), m_replicaIds.end(), message.senderId) == m_replicaIds.end() ||
            message.instanceId == 0 || message.instanceId > m_applicationHistory.size() ||
            m_log[message.instanceId - 1].history != message.digest) return;
        auto& voters = m_checkpointVotes[{message.instanceId, message.digest}];
        voters.insert(message.senderId);
        if (voters.size() >= 2 * m_faultTolerance + 1)
            m_stableCheckpoint = std::max(m_stableCheckpoint, message.instanceId);
    }

    void ZyzzyvaEngine::ArmProgressTimer() {
        if (m_progressEvent.IsRunning()) return;
        m_progressEvent = Simulator::Schedule(m_viewTimeout, &ZyzzyvaEngine::OnProgressTimeout, this, m_view);
    }
    void ZyzzyvaEngine::OnProgressTimeout(uint64_t view) {
        if (m_running && view == m_view && (!m_futureOrders.empty() || !m_pendingForwards.empty())) {
            ReportPhase(ConsensusProtocol::ZYZZYVA, m_log.size(), 9, Seconds(0));
            TriggerHate();
            auto highest = m_futureOrders.upper_bound({view, UINT64_MAX});
            if (highest != m_futureOrders.begin()) {
                --highest;
                if (highest->first.first == view && highest->first.second > m_log.size() + 1)
                    RequestFillHole(view, m_log.size() + 1, highest->first.second - 1);
            }
            ArmProgressTimer();
        }
    }
    void ZyzzyvaEngine::TriggerHate(const ConsensusMessage* first, const ConsensusMessage* second) {
        const bool sameSlotEquivocation = first && second && first->instanceId == second->instanceId &&
            first->digest != second->digest;
        const bool clientOrderEquivocation = first && second && first->clientId == second->clientId &&
            first->requestId == second->requestId &&
            (first->instanceId != second->instanceId || first->digest != second->digest);
        const bool authenticatedEvidence = first && second &&
            first->type == ConsensusMessageType::ZYZZYVA_ORDER_REQ && second->type == first->type &&
            first->senderId == GetLeaderId() && second->senderId == first->senderId &&
            first->view == m_view && second->view == first->view &&
            (sameSlotEquivocation || clientOrderEquivocation) &&
            ZyzzyvaAuth::VerifyContent(*first, first->authentication) &&
            ZyzzyvaAuth::VerifyContent(*second, second->authentication);
        if (!m_running || (m_hateVotes[m_view].count(m_replicaId) &&
            (!authenticatedEvidence || m_evidenceBroadcastViews.count(m_view)))) return;
        if (authenticatedEvidence) {
            m_evidenceBroadcastViews.insert(m_view);
            if (m_equivocationEvidenceViews.insert(m_view).second)
                ReportPhase(ConsensusProtocol::ZYZZYVA, first->instanceId, 5, Seconds(0));
        }
        m_suspectedViews.insert(m_view); // An accusation alone does not stop normal ordering.
        ConsensusMessage hate;
        hate.type = ConsensusMessageType::ZYZZYVA_HATE_PRIMARY;
        hate.view = m_view;
        std::vector<uint8_t> hateData;
        Write64(hateData, m_view);
        if (!ZyzzyvaAuth::SignDetached(m_replicaId, hateData, hate.payloadBytes)) return;
        m_hateVotes[m_view].insert(m_replicaId);
        m_hateProofs[m_view][m_replicaId] = hate.payloadBytes;
        if (authenticatedEvidence) {
            for (const ConsensusMessage* proof : {first, second}) {
                ConsensusMessage signedProof = *proof;
                signedProof.payloadBytes.insert(signedProof.payloadBytes.end(), proof->authentication.begin(),
                                                proof->authentication.end());
                Ptr<Packet> packet = signedProof.ToPacket();
                std::vector<uint8_t> bytes(packet->GetSize());
                packet->CopyData(bytes.data(), bytes.size());
                Write32(hate.payloadBytes, static_cast<uint32_t>(bytes.size()));
                hate.payloadBytes.insert(hate.payloadBytes.end(), bytes.begin(), bytes.end());
            }
        }
        BroadcastMessage(hate);
    }
    void ZyzzyvaEngine::HandleHate(const ConsensusMessage& message) {
        if (std::find(m_replicaIds.begin(), m_replicaIds.end(), message.senderId) == m_replicaIds.end()) return;
        if (message.view != m_view && message.view != m_pendingView) return;
        std::vector<uint8_t> hateData;
        Write64(hateData, message.view);
        if (message.payloadBytes.size() < ZyzzyvaAuth::SIGNATURE_SIZE) return;
        std::vector<uint8_t> signature(message.payloadBytes.begin(),
                                       message.payloadBytes.begin() + ZyzzyvaAuth::SIGNATURE_SIZE);
        if (!ZyzzyvaAuth::VerifyDetached(message.senderId, hateData, signature)) return;
        if (message.payloadBytes.size() > ZyzzyvaAuth::SIGNATURE_SIZE) {
            if (message.view != m_view) return;
            size_t offset = ZyzzyvaAuth::SIGNATURE_SIZE;
            ConsensusMessage evidence[2];
            for (ConsensusMessage& order : evidence) {
                uint32_t length = 0;
                if (!Read32(message.payloadBytes, offset, length) || length == 0 || length > 16 * 1024 * 1024 ||
                    offset > message.payloadBytes.size() || message.payloadBytes.size() - offset < length) return;
                Ptr<Packet> packet = Create<Packet>(message.payloadBytes.data() + offset, length);
                if (!ConsensusMessage::FromPacket(packet, order) || !ZyzzyvaAuth::Verify(order)) return;
                offset += length;
            }
            if (offset != message.payloadBytes.size() || evidence[0].protocol != ConsensusProtocol::ZYZZYVA ||
                evidence[1].protocol != ConsensusProtocol::ZYZZYVA ||
                evidence[0].type != ConsensusMessageType::ZYZZYVA_ORDER_REQ ||
                evidence[1].type != ConsensusMessageType::ZYZZYVA_ORDER_REQ ||
                evidence[0].senderId != GetLeaderId() || evidence[1].senderId != GetLeaderId() ||
                evidence[0].view != m_view || evidence[1].view != m_view ||
                !((evidence[0].instanceId == evidence[1].instanceId && evidence[0].digest != evidence[1].digest) ||
                  (evidence[0].clientId == evidence[1].clientId && evidence[0].requestId == evidence[1].requestId &&
                   (evidence[0].instanceId != evidence[1].instanceId || evidence[0].digest != evidence[1].digest)))) return;
            if (m_requireClientAuthentication) {
                ClientRequest first, second;
                if (!ZyzzyvaAuth::DecodeClientRequest(evidence[0], 8, first) ||
                    !ZyzzyvaAuth::DecodeClientRequest(evidence[1], 8, second)) return;
            }
            if (m_equivocationEvidenceViews.insert(m_view).second)
                ReportPhase(ConsensusProtocol::ZYZZYVA, evidence[0].instanceId, 5, Seconds(0));
            if (m_hateVotes[m_view].count(m_replicaId) == 0) TriggerHate();
        }
        m_hateProofs[message.view][message.senderId] = signature;
        auto& votes = m_hateVotes[message.view];
        votes.insert(message.senderId);
        if (m_viewChangeEnabled && votes.size() >= m_faultTolerance + 1 &&
            !m_viewProofs[message.view + 1].count(m_replicaId)) {
            ReportPhase(ConsensusProtocol::ZYZZYVA, m_log.size(),
                        m_equivocationEvidenceViews.count(m_view) ? 6 : 7, Seconds(0));
            StartViewChange(message.view + 1);
        }
    }

    void ZyzzyvaEngine::AccuseView(uint64_t view) {
        if (!m_viewChangeEnabled || !m_running || m_hateVotes[view].count(m_replicaId)) return;
        ConsensusMessage hate;
        hate.type = ConsensusMessageType::ZYZZYVA_HATE_PRIMARY;
        hate.view = view;
        std::vector<uint8_t> body;
        Write64(body, view);
        if (!ZyzzyvaAuth::SignDetached(m_replicaId, body, hate.payloadBytes)) return;
        m_hateVotes[view].insert(m_replicaId);
        m_hateProofs[view][m_replicaId] = hate.payloadBytes;
        BroadcastMessage(hate);
    }

    void ZyzzyvaEngine::OnNewViewTimeout(uint64_t target) {
        if (!m_viewChangeEnabled || !m_running || m_view >= target || m_pendingView != target) return;
        ReportPhase(ConsensusProtocol::ZYZZYVA, target, 10, Seconds(0));
        ++m_viewTimeoutBackoff;
        AccuseView(target);
    }

    void ZyzzyvaEngine::StartViewChange(uint64_t target) {
        if (!m_viewChangeEnabled || target <= m_view || m_viewProofs[target].count(m_replicaId)) return;
        if (target != std::max(m_view, m_pendingView) + 1 || m_hateProofs[target - 1].size() < m_faultTolerance + 1) return;
        m_pendingView = target;
        ReportPhase(ConsensusProtocol::ZYZZYVA, target, ZYZZYVA_VIEW_CHANGE_STARTED, Seconds(0));
        Simulator::Cancel(m_progressEvent);
        Simulator::Cancel(m_newViewEvent);
        ViewProof proof;
        proof.sender = m_replicaId;
        proof.targetView = target;
        proof.hateProofs = m_hateProofs[target - 1];
        proof.log = m_log;
        for (const auto& item : m_certificates) {
            const Certificate& candidate = item.second;
            if (candidate.view > proof.certificate.view ||
                (candidate.view == proof.certificate.view && candidate.sequence > proof.certificate.sequence))
                proof.certificate = candidate;
        }
        ConsensusMessage change;
        change.type = ConsensusMessageType::VIEW_CHANGE;
        change.view = target;
        EncodeProof(change.payloadBytes, proof);
        BroadcastMessage(change);
    }

    std::vector<ZyzzyvaEngine::Entry> ZyzzyvaEngine::SelectOriginal(const std::vector<ViewProof>& proofs) const {
        std::vector<Entry> chosen;
        uint64_t committed = 0;
        for (const ViewProof& p : proofs) {
            const Certificate& c = p.certificate;
            if (c.sequence > committed && c.voters.size() >= 2 * m_faultTolerance + 1 &&
                c.sequence <= p.log.size() && p.log[c.sequence - 1].history == c.history) {
                committed = c.sequence;
                chosen.assign(p.log.begin(), p.log.begin() + c.sequence);
            }
        }
        size_t longest = chosen.size();
        for (const ViewProof& p : proofs) longest = std::max(longest, p.log.size());
        for (size_t size = chosen.size() + 1; size <= longest; ++size) {
            for (const ViewProof& p : proofs) {
                if (p.log.size() < size || !Extends(p.log, chosen)) continue;
                std::vector<Entry> candidate(p.log.begin(), p.log.begin() + size);
                uint32_t supporters = 0;
                for (const ViewProof& q : proofs) if (Extends(q.log, candidate)) ++supporters;
                if (supporters >= m_faultTolerance + 1) {
                    chosen = candidate;
                    break;
                }
            }
            if (chosen.size() < size) break;
        }
        return chosen;
    }

    std::vector<ZyzzyvaEngine::Entry> ZyzzyvaEngine::SelectVelma(const std::vector<ViewProof>& proofs) const {
        // Velma ranks each prefix by its strongest fast and slow certificate view.
        struct Candidate {
            const std::vector<Entry>* source{nullptr};
            size_t length{0};
            size_t parent{0};
            int64_t fast{-1};
            int64_t slow{-1};
            std::map<uint64_t, uint32_t> support;
        };
        auto equalPrefix = [](const std::vector<Entry>& a, const std::vector<Entry>& b, size_t length) {
            if (length > a.size() || length > b.size()) return false;
            for (size_t i = 0; i < length; ++i)
                if (a[i].sequence != b[i].sequence || a[i].digest != b[i].digest || a[i].history != b[i].history)
                    return false;
            return true;
        };
        using PrefixKey = std::pair<size_t, uint64_t>;
        std::map<PrefixKey, std::vector<size_t>> index;
        std::vector<Candidate> candidates(1); // Empty history.
        auto findCandidate = [&](const std::vector<Entry>& log, size_t length) -> size_t {
            if (length == 0) return 0;
            if (length > log.size()) return candidates.size();
            auto bucket = index.find({length, log[length - 1].history});
            if (bucket == index.end()) return candidates.size();
            for (size_t id : bucket->second)
                if (equalPrefix(*candidates[id].source, log, length)) return id;
            return candidates.size();
        };
        // Construct each unique prefix once. The history hash narrows lookups;
        // full prefix equality preserves behavior even in the event of a collision.
        for (const ViewProof& proof : proofs) {
            ++candidates[0].support[0];
            for (size_t length = 1; length <= proof.log.size(); ++length) {
                size_t id = findCandidate(proof.log, length);
                if (id == candidates.size()) {
                    id = candidates.size();
                    size_t parent = findCandidate(proof.log, length - 1);
                    candidates.push_back(Candidate{&proof.log, length, parent, -1, -1, {}});
                    index[{length, proof.log[length - 1].history}].push_back(id);
                }
                ++candidates[id].support[proof.log[length - 1].view];
            }
        }
        // A proof contributes its slow certificate to every candidate extending
        // the certified prefix. Iterate the proof once instead of rescanning all
        // proofs for every candidate.
        for (const ViewProof& proof : proofs) {
            const Certificate& cert = proof.certificate;
            if (cert.voters.size() < 2 * m_faultTolerance + 1 || cert.sequence == 0 ||
                cert.sequence > proof.log.size() || proof.log[cert.sequence - 1].history != cert.history) continue;
            size_t id = findCandidate(proof.log, cert.sequence);
            if (id != candidates.size())
                candidates[id].slow = std::max(candidates[id].slow, static_cast<int64_t>(cert.view));
        }
        // Prefix candidates are appended only after their parent exists, so a
        // single forward pass propagates certificate support through the trie.
        for (size_t i = 1; i < candidates.size(); ++i) {
            candidates[i].slow = std::max(candidates[i].slow, candidates[candidates[i].parent].slow);
        }
        for (Candidate& c : candidates) {
            uint32_t cumulative = 0;
            for (auto it = c.support.rbegin(); it != c.support.rend(); ++it) {
                cumulative += it->second;
                if (cumulative >= m_faultTolerance + 1) { c.fast = it->first; break; }
            }
        }
        int64_t maxFast = -1, maxSlow = -1;
        for (const Candidate& c : candidates) { maxFast = std::max(maxFast, c.fast); maxSlow = std::max(maxSlow, c.slow); }
        auto compatibleWinner = [&](int64_t rank, bool fastRank) -> size_t {
            size_t deepest = candidates.size();
            for (size_t i = 0; i < candidates.size(); ++i) {
                if ((fastRank ? candidates[i].fast : candidates[i].slow) != rank) continue;
                if (deepest == candidates.size() || candidates[i].length > candidates[deepest].length) deepest = i;
            }
            if (deepest == candidates.size()) return deepest;
            for (size_t i = 0; i < candidates.size(); ++i) {
                if ((fastRank ? candidates[i].fast : candidates[i].slow) != rank) continue;
                if (candidates[i].length > candidates[deepest].length ||
                    (candidates[i].length > 0 &&
                     findCandidate(*candidates[deepest].source, candidates[i].length) != i))
                    return candidates.size();
            }
            return deepest;
        };
        const size_t fastWinner = compatibleWinner(maxFast, true);
        const size_t slowWinner = compatibleWinner(maxSlow, false);
        auto extendsWinner = [&](const Candidate& c, size_t winner) {
            return winner != candidates.size() && c.length >= candidates[winner].length &&
                   (candidates[winner].length == 0 ||
                    findCandidate(*c.source, candidates[winner].length) == winner);
        };
        const Candidate* selected = &candidates.front();
        for (const Candidate& c : candidates) {
            bool safe = false;
            if (c.slow == maxSlow && c.slow > maxFast) safe = true;
            if (c.fast == maxFast && c.fast > maxSlow) safe = true;
            if (c.slow == maxSlow && c.slow == maxFast) {
                safe = extendsWinner(c, fastWinner);
            }
            if (c.fast == maxFast && c.fast == maxSlow) {
                bool compatible = extendsWinner(c, slowWinner);
                safe = safe || compatible;
            }
            if (maxFast < 0 && maxSlow < 0) safe = true;
            if (safe && c.length > selected->length) selected = &c;
        }
        if (selected->length == 0) return {};
        return std::vector<Entry>(selected->source->begin(), selected->source->begin() + selected->length);
    }

    std::vector<ZyzzyvaEngine::Entry> ZyzzyvaEngine::SelectHistory(const std::vector<ViewProof>& proofs) const {
        return m_recoveryRule == 0 ? SelectOriginal(proofs) : SelectVelma(proofs);
    }

    void ZyzzyvaEngine::HandleViewChange(const ConsensusMessage& message) {
        if (!m_viewChangeEnabled) return;
        if (message.view <= m_view) return;
        size_t offset = 0;
        ViewProof proof;
        if (!DecodeProof(message.payloadBytes, offset, proof) || offset != message.payloadBytes.size() ||
            proof.sender != message.senderId || proof.targetView != message.view || !ValidateProof(proof) ||
            std::find(m_replicaIds.begin(), m_replicaIds.end(), proof.sender) == m_replicaIds.end()) return;
        auto& collected = m_viewProofs[message.view];
        collected.emplace(proof.sender, proof);
        if (collected.count(m_replicaId) == 0 && message.view == std::max(m_view, m_pendingView) + 1) {
            for (const auto& vote : proof.hateProofs) m_hateProofs[message.view - 1].emplace(vote);
            StartViewChange(message.view);
        }
        if (collected.size() >= 2 * m_faultTolerance + 1 && m_pendingView == message.view && !m_newViewEvent.IsRunning()) {
            const uint64_t multiplier = uint64_t{1} << std::min<uint32_t>(m_viewTimeoutBackoff, 16);
            m_newViewEvent = Simulator::Schedule(m_viewTimeout * multiplier, &ZyzzyvaEngine::OnNewViewTimeout, this, message.view);
        }
        if (m_replicaId != m_replicaIds[message.view % m_replicaIds.size()] ||
            collected.size() < 2 * m_faultTolerance + 1) return;
        std::vector<ViewProof> proofs;
        for (const auto& pair : collected) proofs.push_back(pair.second);
        std::vector<Entry> selected = SelectHistory(proofs);
        ConsensusMessage newView;
        newView.type = ConsensusMessageType::NEW_VIEW;
        newView.view = message.view;
        Write32(newView.payloadBytes, proofs.size());
        for (const ViewProof& p : proofs) EncodeProof(newView.payloadBytes, p);
        Write32(newView.payloadBytes, selected.size());
        for (const Entry& e : selected) EncodeEntry(newView.payloadBytes, e);
        BroadcastMessage(newView);
    }

    void ZyzzyvaEngine::HandleNewView(const ConsensusMessage& message) {
        if (!m_viewChangeEnabled) return;
        if (message.view <= m_view || message.senderId != m_replicaIds[message.view % m_replicaIds.size()]) return;
        size_t offset = 0;
        uint32_t count;
        if (!Read32(message.payloadBytes, offset, count) || count < 2 * m_faultTolerance + 1 ||
            count > m_replicaIds.size()) return;
        std::vector<ViewProof> proofs;
        std::set<uint32_t> senders;
        for (uint32_t i = 0; i < count; ++i) {
            ViewProof p;
            if (!DecodeProof(message.payloadBytes, offset, p) || p.targetView != message.view || !ValidateProof(p) ||
                !senders.insert(p.sender).second ||
                std::find(m_replicaIds.begin(), m_replicaIds.end(), p.sender) == m_replicaIds.end()) return;
            proofs.push_back(p);
        }
        if (!Read32(message.payloadBytes, offset, count)) return;
        std::vector<Entry> selected;
        for (uint32_t i = 0; i < count; ++i) {
            Entry e;
            if (!DecodeEntry(message.payloadBytes, offset, e)) return;
            selected.push_back(e);
        }
        if (offset != message.payloadBytes.size() || !ValidateLog(selected)) return;
        std::vector<Entry> expected = SelectHistory(proofs);
        if (expected.size() != selected.size() || !Extends(expected, selected)) return;
        m_newViews[message.view] = message;
        uint64_t commonPrefix = 0;
        while (commonPrefix < m_applicationHistory.size() && commonPrefix < selected.size() &&
               m_log[commonPrefix].digest == selected[commonPrefix].digest &&
               m_log[commonPrefix].history == selected[commonPrefix].history) ++commonPrefix;
        uint64_t undoTransactions = 0;
        for (size_t i = 0; i < m_applicationHistory.size(); ++i) {
            if (i >= selected.size() || m_log[i].digest != selected[i].digest ||
                m_log[i].history != selected[i].history) {
                ++m_rollbacks;
                const Entry& old = m_log[i];
                m_executionCandidateTrace(m_replicaIds[old.view % m_replicaIds.size()], m_replicaId,
                    static_cast<uint16_t>(ConsensusMessageType::ZYZZYVA_ORDER_REQ), old.view, old.sequence, old.digest,
                    old.client, old.request, 3, 0);
                undoTransactions += std::max<uint32_t>(1, m_log[i].transactions);
                ReportPhase(ConsensusProtocol::ZYZZYVA, m_log[i].sequence, 4, Seconds(0));
            }
        }
        m_log = selected;
        ++m_executionGeneration;
        m_applicationHistory.resize(commonPrefix);
        m_readyExecutions.clear();
        m_pendingCommits.clear();
        for (auto it = m_certificates.begin(); it != m_certificates.end();) {
            if (it->first > m_log.size() || m_log[it->first - 1].digest != it->second.digest ||
                m_log[it->first - 1].history != it->second.history) it = m_certificates.erase(it);
            else ++it;
        }
        m_view = message.view;
        m_fillHoleSentAt.clear();
        ReportPhase(ConsensusProtocol::ZYZZYVA, message.view, ZYZZYVA_NEW_VIEW_INSTALLED, Seconds(0));
        m_pendingView = m_view;
        m_viewTimeoutBackoff = 0;
        Simulator::Cancel(m_newViewEvent);
        for (Entry& entry : m_log) entry.view = m_view;
        m_nextSequence = m_log.size() + 1;
        m_seenRequests.clear();
        m_requestSequences.clear();
        m_pendingForwards.clear();
        for (const Entry& entry : m_log) m_seenRequests.insert({entry.client, entry.request});
        for (const Entry& entry : m_log) m_requestSequences[{entry.client, entry.request}] = entry.sequence;
        m_markedPending.clear();
        for (auto it = m_futureOrders.begin(); it != m_futureOrders.end();) {
            if (it->first.first < m_view) it = m_futureOrders.erase(it);
            else ++it;
        }
        for (auto& pending : m_gateEvents) Simulator::Cancel(pending.second);
        m_gateEvents.clear();
        Simulator::Cancel(m_progressEvent);
        ReportLeader(m_view, GetLeaderId());
        m_recovering = true;
        m_recoveryStartedAt = Simulator::Now();
        ReportPhase(ConsensusProtocol::ZYZZYVA, m_view, ZYZZYVA_RECOVERY_STARTED, Seconds(0));
        m_resourceModel->SubmitCpuTask(m_rollbackTime * undoTransactions,
            MakeCallback(&ZyzzyvaEngine::BeginReplay, this).Bind(m_executionGeneration, commonPrefix));
    }

    void ZyzzyvaEngine::QueueExecution(const Entry& entry, bool replay) {
        Time submitted = Simulator::Now();
        m_resourceModel->SubmitCpuTask(m_executionTime * std::max<uint32_t>(1, entry.transactions),
            Callback<void>([this, generation = m_executionGeneration, entry, replay, submitted]() {
                CompleteExecution(generation, entry, replay, submitted);
            }));
    }

    void ZyzzyvaEngine::CompleteExecution(uint64_t generation, Entry entry, bool replay, Time queuedAt) {
        if (!m_running) return;
        if (generation != m_executionGeneration) {
            ++m_cancelledExecutions;
            ReportPhase(ConsensusProtocol::ZYZZYVA, entry.sequence, 12, Simulator::Now() - queuedAt);
            return;
        }
        m_readyExecutions.emplace(entry.sequence, std::make_tuple(entry, replay, queuedAt));
        while (true) {
            auto ready = m_readyExecutions.find(m_applicationHistory.size() + 1);
            if (ready == m_readyExecutions.end()) break;
            const Entry executed = std::get<0>(ready->second);
            const bool reexecuted = std::get<1>(ready->second);
            const Time submitted = std::get<2>(ready->second);
            m_readyExecutions.erase(ready);
            const uint64_t previous = m_applicationHistory.empty() ? 0 : m_applicationHistory.back();
            m_applicationHistory.push_back(Extend(previous, executed.digest ^ executed.request ^ executed.client, executed.sequence));
            ++m_speculativeExecutions;
            ReportPhase(ConsensusProtocol::ZYZZYVA, executed.sequence, 1, Simulator::Now() - submitted);
            m_executionCandidateTrace(m_replicaIds[executed.view % m_replicaIds.size()], m_replicaId,
                static_cast<uint16_t>(ConsensusMessageType::ZYZZYVA_ORDER_REQ), executed.view, executed.sequence, executed.digest,
                executed.client, executed.request, 2, 0);
            if (m_executedRequests[{executed.client, executed.request}]++ > 0) {
                ++m_reexecutedRequests;
                ReportPhase(ConsensusProtocol::ZYZZYVA, executed.sequence, 11, Simulator::Now() - submitted);
            }
            if (reexecuted) ReportPhase(ConsensusProtocol::ZYZZYVA, executed.sequence, 13, Simulator::Now() - submitted);
            SendSpecResponse(executed);
            auto commit = m_pendingCommits.find(executed.sequence);
            if (commit != m_pendingCommits.end() && !m_recovering) {
                ConsensusMessage pending = commit->second;
                m_pendingCommits.erase(commit);
                HandleCommitRequest(pending);
            }
        }
        if (m_recovering && m_applicationHistory.size() == m_log.size()) FinishRecovery();
    }

    void ZyzzyvaEngine::BeginReplay(uint64_t generation, uint64_t prefix) {
        if (!m_running || generation != m_executionGeneration) return;
        ReportPhase(ConsensusProtocol::ZYZZYVA, m_view, ZYZZYVA_UNDO_COMPLETED, Simulator::Now() - m_recoveryStartedAt);
        for (uint64_t i = 0; i < prefix; ++i) SendSpecResponse(m_log[i]);
        for (uint64_t i = prefix; i < m_log.size(); ++i) QueueExecution(m_log[i], true);
        if (prefix == m_log.size()) FinishRecovery();
    }

    void ZyzzyvaEngine::FinishRecovery() {
        if (!m_recovering) return;
        m_recovering = false;
        ReportPhase(ConsensusProtocol::ZYZZYVA, m_view, ZYZZYVA_RECOVERY_COMPLETED, Simulator::Now() - m_recoveryStartedAt);
        auto commits = std::move(m_pendingCommits);
        m_pendingCommits.clear();
        for (const auto& item : commits) HandleCommitRequest(item.second);
        auto requests = std::move(m_recoveryRequests);
        m_recoveryRequests.clear();
        for (const auto& item : requests) Submit(item.second);
        auto orders = std::move(m_futureOrders);
        m_futureOrders.clear();
        for (const auto& item : orders) AcceptOrder(item.second);
    }

    void ZyzzyvaEngine::Receive(const ConsensusMessage& message) {
        if (!m_running) return;
        if (message.type == ConsensusMessageType::CLIENT_REQUEST) {
            ClientRequest request;
            if (ZyzzyvaAuth::DecodeClientRequest(message, 0, request)) Submit(request);
            else if (!m_requireClientAuthentication)
                Submit(ClientRequest{message.clientId, message.requestId, message.payloadSize,
                                     message.transactionCount, message.digest});
            return;
        }
        if (message.protocol != ConsensusProtocol::ZYZZYVA) return;
        switch (message.type) {
            case ConsensusMessageType::ZYZZYVA_ORDER_REQ: {
                auto key = std::make_pair(message.view, message.instanceId);
                if (message.view > m_view && message.senderId == m_replicaIds[message.view % m_replicaIds.size()]) {
                    RequestFillHole(message.view, 0, 0);
                    m_futureOrders.emplace(key, message);
                    break;
                }
                // Evidence processing precedes both view-transition and speculative gates.
                if (message.view != m_view || message.senderId != GetLeaderId() ||
                    message.instanceId == 0 || message.payloadBytes.size() < 8) break;
                if (m_requireClientAuthentication) {
                    ClientRequest request;
                    if (!ZyzzyvaAuth::DecodeClientRequest(message, 8, request)) break;
                }
                if (m_replicaId == GetLeaderId()) m_seenOrders.emplace(key, message);
                auto first = m_seenOrders.emplace(key, message);
                if (!first.second && (first.first->second.digest != message.digest ||
                    !std::equal(message.payloadBytes.begin(), message.payloadBytes.begin() + 8,
                                first.first->second.payloadBytes.begin()))) {
                    if (m_conflictedSlots.insert(key).second) {
                        ++m_equivocations;
                        ReportPhase(ConsensusProtocol::ZYZZYVA, message.instanceId, 2, Seconds(0));
                    }
                    TriggerHate(&first.first->second, &message);
                }
                if (message.iorsMarked) StartIorsEvidenceRequest(message);
                if (m_iorsTerminalGate && message.iorsMarked && message.instanceId > m_log.size()) {
                    if (m_markedPending.emplace(key, message).second)
                        m_gateEvents[key] = Simulator::Schedule(m_gateTimeout, &ZyzzyvaEngine::OnGateTimeout,
                                                               this, message.view, message.instanceId);
                    return;
                }
                auto pending = m_markedPending.find(key);
                if (!message.iorsMarked && pending != m_markedPending.end()) {
                    ConsensusMessage delayed = pending->second;
                    Simulator::Cancel(m_gateEvents[key]);
                    m_gateEvents.erase(key);
                    m_markedPending.erase(pending);
                    AcceptOrder(message);
                    AcceptOrder(delayed); // The marked proposal still reaches protocol evidence handling.
                    break;
                }
                AcceptOrder(message);
                break;
            }
            case ConsensusMessageType::ZYZZYVA_COMMIT_REQUEST: HandleCommitRequest(message); break;
            case ConsensusMessageType::ZYZZYVA_FILL_HOLE: HandleFillHole(message); break;
            case ConsensusMessageType::ZYZZYVA_REPAIR: HandleRepair(message); break;
            case ConsensusMessageType::ZYZZYVA_IORS_EVIDENCE_REQUEST: HandleIorsEvidenceRequest(message); break;
            case ConsensusMessageType::ZYZZYVA_IORS_EVIDENCE_RESPONSE: HandleIorsEvidenceResponse(message); break;
            case ConsensusMessageType::ZYZZYVA_POM: HandlePom(message); break;
            case ConsensusMessageType::ZYZZYVA_CHECKPOINT: HandleCheckpoint(message); break;
            case ConsensusMessageType::ZYZZYVA_HATE_PRIMARY: HandleHate(message); break;
            case ConsensusMessageType::VIEW_CHANGE: HandleViewChange(message); break;
            case ConsensusMessageType::NEW_VIEW: HandleNewView(message); break;
            default: break;
        }
    }

    void ZyzzyvaEngine::HandlePom(const ConsensusMessage& message) {
        if (message.view != m_view || message.clientId != message.senderId ||
            std::find(m_replicaIds.begin(), m_replicaIds.end(), message.senderId) != m_replicaIds.end()) return;
        size_t offset = 0;
        ConsensusMessage evidence[2];
        for (auto& proof : evidence) {
            uint32_t length = 0;
            if (!Read32(message.payloadBytes, offset, length) || length == 0 ||
                message.payloadBytes.size() - offset < length) return;
            std::vector<uint8_t> bytes(message.payloadBytes.begin() + offset,
                                       message.payloadBytes.begin() + offset + length);
            if (!DecodeAuthenticated(bytes, proof)) return;
            offset += length;
        }
        if (offset != message.payloadBytes.size()) return;
        const auto& a = evidence[0];
        const auto& b = evidence[1];
        if (a.type != ConsensusMessageType::ZYZZYVA_ORDER_REQ || b.type != a.type ||
            a.senderId != GetLeaderId() || b.senderId != a.senderId || a.view != m_view || b.view != a.view ||
            a.clientId != message.senderId || b.clientId != a.clientId ||
            a.requestId != message.requestId || b.requestId != a.requestId || a.digest != b.digest) return;
        ClientRequest requestA, requestB;
        if (!ZyzzyvaAuth::DecodeClientRequest(a, 8, requestA) ||
            !ZyzzyvaAuth::DecodeClientRequest(b, 8, requestB) ||
            requestA.authentication != requestB.authentication ||
            requestA.clientId != message.senderId || requestB.clientId != message.senderId ||
            requestA.requestId != requestB.requestId) return;
        uint64_t historyA = 0, historyB = 0;
        for (size_t i = 0; i < 8; ++i) {
            historyA = (historyA << 8) | a.payloadBytes[i];
            historyB = (historyB << 8) | b.payloadBytes[i];
        }
        if (a.instanceId == b.instanceId && historyA == historyB) return;
        TriggerHate(&a, &b);
    }

} // namespace ns3
