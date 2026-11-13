#include "bidl-application.h"
#include "pbft-engine.h"
#include "zyzzyva-auth.h"

#include "ns3/abort.h"
#include "ns3/boolean.h"
#include "ns3/double.h"
#include "ns3/enum.h"
#include "ns3/log.h"
#include "ns3/pointer.h"
#include "ns3/simulator.h"
#include "ns3/uinteger.h"

#include <algorithm>

namespace ns3 {

    namespace {
        void BidlWrite32(std::vector<uint8_t>& bytes, uint32_t value) {
            bytes.push_back(static_cast<uint8_t>(value >> 24));
            bytes.push_back(static_cast<uint8_t>(value >> 16));
            bytes.push_back(static_cast<uint8_t>(value >> 8));
            bytes.push_back(static_cast<uint8_t>(value));
        }

        void BidlWrite64(std::vector<uint8_t>& bytes, uint64_t value) {
            for (int shift = 56; shift >= 0; shift -= 8) bytes.push_back(static_cast<uint8_t>(value >> shift));
        }

        bool BidlRead32(const std::vector<uint8_t>& bytes, size_t& offset, uint32_t& value) {
            if (offset > bytes.size() || bytes.size() - offset < 4) return false;
            value = 0;
            for (int i = 0; i < 4; ++i) value = (value << 8) | bytes[offset++];
            return true;
        }

        bool BidlRead64(const std::vector<uint8_t>& bytes, size_t& offset, uint64_t& value) {
            if (offset > bytes.size() || bytes.size() - offset < 8) return false;
            value = 0;
            for (int i = 0; i < 8; ++i) value = (value << 8) | bytes[offset++];
            return true;
        }

        uint64_t BidlMix(uint64_t state, uint64_t value) {
            state ^= value + 0x9e3779b97f4a7c15ULL + (state << 6) + (state >> 2);
            return state ? state : 1;
        }
    }

    NS_LOG_COMPONENT_DEFINE("BidlApplication");
    NS_OBJECT_ENSURE_REGISTERED(BidlApplication);

    namespace {
        std::vector<uint8_t> BidlSignedRequest(const ClientRequest& request) {
            std::vector<uint8_t> bytes;
            BidlWrite32(bytes, 0x42545831);
            BidlWrite32(bytes, request.clientId);
            BidlWrite64(bytes, request.requestId);
            BidlWrite32(bytes, request.payloadSize);
            BidlWrite32(bytes, request.transactionCount);
            BidlWrite64(bytes, request.digest);
            return bytes; // View, sequence and ingress timestamp deliberately remain unsigned.
        }
    }

    void BidlApplication::SignRequest(ClientRequest& request) {
        ZyzzyvaAuth::SignDetached(request.clientId, BidlSignedRequest(request), request.authentication);
    }

    bool BidlApplication::VerifyRequest(const ClientRequest& request) {
        return ZyzzyvaAuth::VerifyDetached(request.clientId, BidlSignedRequest(request), request.authentication);
    }

    bool BidlApplication::ReadRequest(const ConsensusMessage& message, ClientRequest& request) const {
        std::vector<BidlTransactionRecord> records;
        if (DecodeTransactions(message.payloadBytes, records)) {
            if (records.size() != 1) return false;
            request = records.front().request;
            if (request.digest != message.digest || request.clientId != message.clientId ||
                request.requestId != message.requestId || request.transactionCount != message.transactionCount) return false;
            return !m_requireClientAuthentication || VerifyRequest(request);
        }
        if (m_requireClientAuthentication) return false;
        request = ClientRequest{message.clientId, message.requestId, message.payloadSize, message.transactionCount, message.digest};
        return true;
    }

    std::vector<uint8_t> BidlApplication::EncodeTransactions(const std::vector<BidlTransactionRecord>& records) {
        std::vector<uint8_t> bytes;
        BidlWrite32(bytes, 0x4249444c); // Canonical transaction envelope; no ordering signature is implied.
        BidlWrite32(bytes, static_cast<uint32_t>(records.size()));
        for (const auto& record : records) {
            const ClientRequest& request = record.request;
            BidlWrite64(bytes, record.sequence);
            BidlWrite64(bytes, record.ingressTimeNs);
            BidlWrite32(bytes, request.clientId);
            BidlWrite64(bytes, request.requestId);
            BidlWrite32(bytes, request.payloadSize);
            BidlWrite32(bytes, request.transactionCount);
            BidlWrite64(bytes, request.digest);
            BidlWrite32(bytes, static_cast<uint32_t>(request.authentication.size()));
            bytes.insert(bytes.end(), request.authentication.begin(), request.authentication.end());
            bytes.resize(bytes.size() + request.payloadSize, 0); // Simulated request bytes still consume network bandwidth.
        }
        return bytes;
    }

    bool BidlApplication::DecodeTransactions(const std::vector<uint8_t>& bytes,
                                             std::vector<BidlTransactionRecord>& records) {
        records.clear();
        size_t offset = 0;
        uint32_t magic = 0, count = 0;
        if (!BidlRead32(bytes, offset, magic) || magic != 0x4249444c ||
            !BidlRead32(bytes, offset, count) || count == 0 || count > 100000) return false;
        for (uint32_t i = 0; i < count; ++i) {
            BidlTransactionRecord record;
            ClientRequest& request = record.request;
            uint32_t authenticationSize = 0;
            if (!BidlRead64(bytes, offset, record.sequence) || !BidlRead64(bytes, offset, record.ingressTimeNs) ||
                !BidlRead32(bytes, offset, request.clientId) || !BidlRead64(bytes, offset, request.requestId) ||
                !BidlRead32(bytes, offset, request.payloadSize) || !BidlRead32(bytes, offset, request.transactionCount) ||
                !BidlRead64(bytes, offset, request.digest) || !BidlRead32(bytes, offset, authenticationSize) ||
                authenticationSize > 64 || record.sequence == 0 || request.payloadSize == 0 ||
                request.transactionCount == 0 || request.digest == 0 ||
                bytes.size() - offset < static_cast<uint64_t>(authenticationSize) + request.payloadSize) return false;
            request.authentication.assign(bytes.begin() + offset, bytes.begin() + offset + authenticationSize);
            offset += authenticationSize + request.payloadSize;
            records.push_back(std::move(record));
        }
        return offset == bytes.size();
    }

    TypeId BidlApplication::GetTypeId() {
        static TypeId tid =
            TypeId("ns3::BidlApplication")
                .SetParent<Application>()
                .SetGroupName("Consensus")
                .AddConstructor<BidlApplication>()
                .AddAttribute("NativeSequencing", "PBFT leader sequences requests; the ingress node is a forwarding gateway.",
                              BooleanValue(false), MakeBooleanAccessor(&BidlApplication::m_nativeSequencing), MakeBooleanChecker())
                .AddAttribute("CompactOrderNotifications", "Send transaction hashes on the normal path; fetch canonical contents on mismatch.",
                              BooleanValue(false), MakeBooleanAccessor(&BidlApplication::m_compactOrderNotifications), MakeBooleanChecker())
                .AddAttribute("AttackMode", "0=colluding client content replacement; 1=unsigned sequence alteration preserving client authentication.",
                              UintegerValue(0), MakeUintegerAccessor(&BidlApplication::m_attackMode), MakeUintegerChecker<uint32_t>(0, 1))
                .AddAttribute("SequencerTimeout", "Unsequenced client request timeout for PBFT leader replacement.",
                              TimeValue(MilliSeconds(50)), MakeTimeAccessor(&BidlApplication::m_sequencerTimeout), MakeTimeChecker())
                .AddAttribute("RequireClientAuthentication", "Verify simulated client content bindings; ordering metadata remains unsigned.",
                              BooleanValue(false), MakeBooleanAccessor(&BidlApplication::m_requireClientAuthentication), MakeBooleanChecker())
                .AddAttribute("NodeId", "Logical BIDL node identifier.", UintegerValue(0), MakeUintegerAccessor(&BidlApplication::m_nodeId),
                              MakeUintegerChecker<uint32_t>())
                .AddAttribute("Role", "BIDL functional role.", EnumValue(BIDL_SEQUENCER), MakeEnumAccessor(&BidlApplication::m_role),
                              MakeEnumChecker(BIDL_SEQUENCER, "Sequencer", BIDL_ORDERER, "Orderer", BIDL_EXECUTOR, "Executor",
                                              BIDL_MALICIOUS, "Malicious"))
                .AddAttribute("FaultTolerance", "Fault threshold used for ordered and persist certificates.", UintegerValue(1),
                              MakeUintegerAccessor(&BidlApplication::m_faultTolerance), MakeUintegerChecker<uint32_t>())
                .AddAttribute("BatchSize", "Number of client transactions in one BIDL block.", UintegerValue(1),
                              MakeUintegerAccessor(&BidlApplication::m_batchSize), MakeUintegerChecker<uint32_t>(1))
                .AddAttribute("BatchTimeout", "Maximum wait before proposing a partial block.", TimeValue(MilliSeconds(1)),
                              MakeTimeAccessor(&BidlApplication::m_batchTimeout), MakeTimeChecker())
                .AddAttribute("SequencerServiceTime", "Queued ingress, parsing, sequencing, and fanout preparation time per transaction.",
                              TimeValue(MicroSeconds(20)), MakeTimeAccessor(&BidlApplication::m_sequencerServiceTime), MakeTimeChecker())
                .AddAttribute("TransactionVerificationTime", "Queued decode, hash, replay, and signature verification time per transaction.",
                              TimeValue(MicroSeconds(10)), MakeTimeAccessor(&BidlApplication::m_transactionVerificationTime),
                              MakeTimeChecker())
                .AddAttribute("ExecutionTimePerTransaction", "Queued CPU service time for one speculative transaction.",
                              TimeValue(MicroSeconds(15)), MakeTimeAccessor(&BidlApplication::m_executionTimePerTransaction), MakeTimeChecker())
                .AddAttribute("ApprovalTimePerBlock", "Queued result hashing, signing, verification, and approval time for one block.",
                              TimeValue(MicroSeconds(100)), MakeTimeAccessor(&BidlApplication::m_approvalTimePerBlock), MakeTimeChecker())
                .AddAttribute("StorageTime", "Queued storage service time for one ordered block.", TimeValue(MicroSeconds(500)),
                              MakeTimeAccessor(&BidlApplication::m_storageTime), MakeTimeChecker())
                .AddAttribute("ConsensusSoftwareTime",
                              "Per-block software overhead after the network PBFT decision (crypto, scheduling, and framework work).",
                              TimeValue(MicroSeconds(10000)), MakeTimeAccessor(&BidlApplication::m_consensusSoftwareTime),
                              MakeTimeChecker())
                .AddAttribute("CommitProcessingTime", "Queued local processing time after the persist quorum for one block.",
                              TimeValue(MicroSeconds(2500)), MakeTimeAccessor(&BidlApplication::m_commitProcessingTime), MakeTimeChecker())
                .AddAttribute("AttackProbability", "Probability that an observed sequenced transaction is forged.", DoubleValue(1.0),
                              MakeDoubleAccessor(&BidlApplication::m_attackProbability), MakeDoubleChecker<double>(0.0, 1.0))
                .AddAttribute("AttackStart", "Simulation time at which malicious forging starts.", TimeValue(Seconds(0)),
                              MakeTimeAccessor(&BidlApplication::m_attackStart), MakeTimeChecker())
                .AddAttribute("AttackStop", "Simulation time at which forging stops; zero means no stop.", TimeValue(Seconds(0)),
                              MakeTimeAccessor(&BidlApplication::m_attackStop), MakeTimeChecker())
                .AddAttribute("AttackProcessingTime", "CPU time used to create one forged transaction.", TimeValue(MicroSeconds(20)),
                              MakeTimeAccessor(&BidlApplication::m_attackProcessingTime), MakeTimeChecker())
                .AddAttribute("AttackTargetExecutors", "Number of executors receiving each forgery; zero means all.", UintegerValue(1),
                              MakeUintegerAccessor(&BidlApplication::m_attackTargetExecutors), MakeUintegerChecker<uint32_t>())
                .AddAttribute("IorsTerminalGate", "Wait briefly for an unmarked transaction before speculative execution.",
                              BooleanValue(false), MakeBooleanAccessor(&BidlApplication::m_iorsTerminalGate), MakeBooleanChecker())
                .AddAttribute("DenylistEnabled", "Enable conflict-based BIDL client denylist model.", BooleanValue(false),
                              MakeBooleanAccessor(&BidlApplication::m_denylistEnabled), MakeBooleanChecker())
                .AddAttribute("IorsGateTimeout", "Bound on waiting for an unmarked candidate.", TimeValue(MilliSeconds(1)),
                              MakeTimeAccessor(&BidlApplication::m_iorsGateTimeout), MakeTimeChecker())
                .AddAttribute("Engine", "BFT ordering engine used on orderer nodes.", PointerValue(),
                              MakePointerAccessor(&BidlApplication::m_engine), MakePointerChecker<ConsensusEngine>())
                .AddAttribute("Transport", "BIDL complete-message transport.", PointerValue(),
                              MakePointerAccessor(&BidlApplication::m_transport), MakePointerChecker<ConsensusTransport>())
                .AddAttribute("ResourceModel", "CPU and storage queue model.", PointerValue(),
                              MakePointerAccessor(&BidlApplication::m_resourceModel), MakePointerChecker<ConsensusResourceModel>())
                .AddTraceSource("BatchCreated", "The ordering leader proposes a block hash list.",
                                MakeTraceSourceAccessor(&BidlApplication::m_batchCreatedTrace), "ns3::BidlApplication::BatchTracedCallback")
                .AddTraceSource("BlockCommitted", "An executor commits a block after 2f+1 persists.",
                                MakeTraceSourceAccessor(&BidlApplication::m_blockCommittedTrace),
                                "ns3::BidlApplication::CommitTracedCallback")
                .AddTraceSource("TransactionFinalized", "Executor commit with canonical client/request identity.",
                                MakeTraceSourceAccessor(&BidlApplication::m_transactionFinalizedTrace),
                                "ns3::BidlApplication::TransactionFinalizedCallback")
                .AddTraceSource("TransactionCommitted", "A sequence-numbered transaction commits with sequencer-to-commit latency.",
                                MakeTraceSourceAccessor(&BidlApplication::m_transactionCommittedTrace),
                                "ns3::BidlApplication::TransactionCommitTracedCallback")
                .AddTraceSource("BlockCommitTime", "An executor commits a block with its absolute simulation time.",
                                MakeTraceSourceAccessor(&BidlApplication::m_blockCommitTimeTrace),
                                "ns3::BidlApplication::BlockCommitTimeTracedCallback")
                .AddTraceSource("MessageEvent", "Complete message identity for offline timing.",
                                MakeTraceSourceAccessor(&BidlApplication::m_messageEventTrace),
                                "ns3::BidlApplication::MessageEventCallback")
                .AddTraceSource("MessageTx", "A BIDL message is passed to the transport.",
                                MakeTraceSourceAccessor(&BidlApplication::m_messageTxTrace), "ns3::BidlApplication::MessageTracedCallback")
                .AddTraceSource("MessageRx", "A complete BIDL message is received.",
                                MakeTraceSourceAccessor(&BidlApplication::m_messageRxTrace), "ns3::BidlApplication::MessageTracedCallback")
                .AddTraceSource("RepairTx", "A transaction repair packet sent with its triggering reason.",
                                MakeTraceSourceAccessor(&BidlApplication::m_repairTxTrace), "ns3::BidlApplication::RepairTracedCallback")
                .AddTraceSource("CandidateEvent", "Sequenced candidate sent or received with its binding fields.",
                                MakeTraceSourceAccessor(&BidlApplication::m_candidateTrace),
                                "ns3::BidlApplication::CandidateTracedCallback")
                .AddTraceSource("PhaseChanged", "A BIDL block reaches a workflow phase.",
                                MakeTraceSourceAccessor(&BidlApplication::m_phaseChangedTrace), "ns3::BidlApplication::PhaseTracedCallback")
                .AddTraceSource("ForgedTransaction", "A malicious node emits forged copies of an observed transaction.",
                                MakeTraceSourceAccessor(&BidlApplication::m_forgedTransactionTrace),
                                "ns3::BidlApplication::ForgedTracedCallback")
                .AddTraceSource("ConflictRejected", "An executor rejects a later transaction with a conflicting digest.",
                                MakeTraceSourceAccessor(&BidlApplication::m_conflictRejectedTrace),
                                "ns3::BidlApplication::ConflictTracedCallback")
                .AddTraceSource("BlockReexecution", "An executor completes whole-block reexecution after a consensus mismatch.",
                                MakeTraceSourceAccessor(&BidlApplication::m_blockReexecutionTrace),
                                "ns3::BidlApplication::ReexecutionTracedCallback")
                .AddTraceSource("ExecutionBacklog", "Periodic executor verification and execution backlog sample.",
                                MakeTraceSourceAccessor(&BidlApplication::m_executionBacklogTrace),
                                "ns3::BidlApplication::ExecutionBacklogTracedCallback");
        return tid;
    }

    BidlApplication::BidlApplication()
        : m_nodeId(0), m_role(BIDL_SEQUENCER), m_faultTolerance(1), m_batchSize(1), m_batchTimeout(MilliSeconds(1)),
          m_sequencerServiceTime(MicroSeconds(20)), m_transactionVerificationTime(MicroSeconds(10)),
          m_executionTimePerTransaction(MicroSeconds(15)), m_approvalTimePerBlock(MicroSeconds(100)),
          m_storageTime(MicroSeconds(500)), m_consensusSoftwareTime(MicroSeconds(10000)),
          m_commitProcessingTime(MicroSeconds(2500)), m_attackProbability(1.0), m_attackStart(Seconds(0)),
          m_attackStop(Seconds(0)), m_attackProcessingTime(MicroSeconds(20)), m_attackTargetExecutors(1),
          m_nextTransactionSequence(1),
          m_nextBlockSequence(1), m_nextExecutionSequence(1), m_nextMessageId(1), m_executionBusy(false), m_committedBlocks(0),
          m_committedTransactions(0), m_firstCommitWindowStart(Seconds(0)), m_lastCommit(Seconds(0)), m_hasCommitWindow(false),
          m_running(false), m_attackRandom(CreateObject<UniformRandomVariable>()) {}

    void BidlApplication::SetNodeId(uint32_t nodeId) {
        m_nodeId = nodeId;
    }

    void BidlApplication::SetRole(BidlRole role) {
        m_role = role;
    }

    void BidlApplication::SetOrdererIds(const std::vector<uint32_t>& ids) {
        m_ordererIds = ids;
    }

    void BidlApplication::SetExecutorIds(const std::vector<uint32_t>& ids) {
        m_executorIds = ids;
    }

    void BidlApplication::SetSequencerIds(const std::vector<uint32_t>& ids) {
        m_sequencerIds = ids;
    }

    void BidlApplication::SetBroadcasterIds(const std::vector<uint32_t>& ids) {
        m_broadcasterIds = ids;
    }

    void BidlApplication::SetEngine(Ptr<ConsensusEngine> engine) {
        m_engine = engine;
    }

    void BidlApplication::SetTransport(Ptr<ConsensusTransport> transport) {
        m_transport = transport;
    }

    void BidlApplication::SetResourceModel(Ptr<ConsensusResourceModel> resourceModel) {
        m_resourceModel = resourceModel;
    }

    uint64_t BidlApplication::GetCommittedBlocks() const {
        return m_committedBlocks;
    }

    uint64_t BidlApplication::GetCommittedTransactions() const {
        return m_committedTransactions;
    }

    uint64_t BidlApplication::GetCommittedDigest() const {
        uint64_t digest = 0x243f6a8885a308d3ULL;
        for (uint64_t blockId : m_committedBlockIds) {
            auto block = m_blocks.find(blockId);
            if (block != m_blocks.end()) {
                digest = BidlMix(digest, blockId);
                digest = BidlMix(digest, block->second.digest);
                digest = BidlMix(digest, block->second.resultVectorDigest);
            }
        }
        return digest;
    }

    std::map<uint64_t, uint64_t> BidlApplication::GetCommittedBlockHistory() const {
        std::map<uint64_t, uint64_t> history;
        for (uint64_t blockId : m_committedBlockIds) {
            auto block = m_blocks.find(blockId);
            if (block != m_blocks.end() && block->second.committed) {
                history.emplace(blockId, block->second.digest ^ block->second.resultVectorDigest);
            }
        }
        return history;
    }

    uint64_t BidlApplication::GetAbortedBlocks() const {
        return m_abortedBlockIds.size();
    }

    uint64_t BidlApplication::GetSequencedTransactions() const {
        return m_role == BIDL_SEQUENCER ? m_transactions.size() : 0;
    }

    uint64_t BidlApplication::GetSequencerCommittedTransactions() const {
        return m_role == BIDL_SEQUENCER ? m_committedTransactionBlocks.size() : 0;
    }

    uint32_t BidlApplication::GetDenylistSize() const {
        return m_denylist.size();
    }

    uint64_t BidlApplication::GetDenylistDeferredTransactions() const {
        return m_deferredTotal;
    }

    bool BidlApplication::IsSequencer() const {
        return m_role == BIDL_SEQUENCER;
    }

    double BidlApplication::GetTransactionThroughput() const {
        Time duration = m_lastCommit - m_firstCommitWindowStart;
        return !m_hasCommitWindow || duration.IsZero() ? 0.0 : static_cast<double>(m_committedTransactions) / duration.GetSeconds();
    }

    Ptr<ConsensusEngine> BidlApplication::GetEngine() const {
        return m_engine;
    }

    void BidlApplication::DoDispose() {
        m_engine = nullptr;
        m_transport = nullptr;
        m_resourceModel = nullptr;
        Application::DoDispose();
    }

    void BidlApplication::StartApplication() {
        NS_ABORT_MSG_IF(!m_transport, "BidlApplication requires a transport");
        NS_ABORT_MSG_IF(m_ordererIds.empty() || m_executorIds.empty() || m_sequencerIds.empty(),
                        "BidlApplication requires sequencer, orderer, and executor sets");
        if (!m_resourceModel) {
            m_resourceModel = CreateObject<ConsensusResourceModel>();
        }

        m_transport->SetReceiveCallback(MakeCallback(&BidlApplication::OnTransportReceive, this));
        m_transport->Start(GetNode());
        m_running = true;

        if (m_role == BIDL_ORDERER) {
            NS_ABORT_MSG_IF(!m_engine, "BIDL orderer requires a ConsensusEngine");
            m_engine->Configure(m_nodeId, m_ordererIds);
            m_engine->SetResourceModel(m_resourceModel);
            m_engine->SetSendCallback(MakeCallback(&BidlApplication::OnEngineSend, this));
            m_engine->SetDecisionCallback(MakeCallback(&BidlApplication::OnEngineDecision, this));
            m_engine->TraceConnectWithoutContext("LeaderChanged", MakeCallback(&BidlApplication::OnOrderingViewChanged, this));
            m_engine->Start();
        }
        if (m_role == BIDL_EXECUTOR) {
            m_backlogEvent = Simulator::Schedule(MilliSeconds(10), &BidlApplication::SampleExecutionBacklog, this);
        }
    }

    void BidlApplication::StopApplication() {
        m_running = false;
        Simulator::Cancel(m_ingressRetryEvent);
        Simulator::Cancel(m_sequencerTimeoutEvent);
        for (auto& retry : m_gapRequestEvents) Simulator::Cancel(retry.second);
        m_gapRequestEvents.clear();
        for (auto& retry : m_blockFetchEvents) Simulator::Cancel(retry.second);
        m_blockFetchEvents.clear();
        if (m_batchEvent.IsRunning()) {
            Simulator::Cancel(m_batchEvent);
        }
        if (m_backlogEvent.IsRunning()) {
            Simulator::Cancel(m_backlogEvent);
        }
        if (m_engine) {
            m_engine->Stop();
        }
        if (m_transport) {
            m_transport->Stop();
        }
    }

    void BidlApplication::SampleExecutionBacklog() {
        if (!m_running || m_role != BIDL_EXECUTOR) {
            return;
        }
        uint32_t executionBacklog = static_cast<uint32_t>(m_transactions.size() - m_executedTransactions.size());
        uint32_t verificationBacklog = static_cast<uint32_t>(m_verificationPending.size());
        m_executionBacklogTrace(m_nodeId, verificationBacklog, executionBacklog, verificationBacklog + executionBacklog);
        m_backlogEvent = Simulator::Schedule(MilliSeconds(10), &BidlApplication::SampleExecutionBacklog, this);
    }

    void BidlApplication::InjectTransaction(uint32_t payloadSize, uint32_t transactionCount, uint64_t requestId) {
        if (!m_running || m_role != BIDL_SEQUENCER || payloadSize == 0 || transactionCount == 0 || requestId == 0) {
            return;
        }
        ClientRequest request;
        request.clientId = m_nodeId; // A direct ingress has no network client endpoint.
        request.requestId = requestId;
        request.payloadSize = payloadSize;
        request.transactionCount = transactionCount;
        request.digest = requestId ^ (static_cast<uint64_t>(payloadSize) << 32) ^ transactionCount ^ 0x9e3779b97f4a7c15ULL;
        if (request.digest == 0 || request.digest == INVALID_TRANSACTION_DIGEST) {
            ++request.digest;
        }
        SignRequest(request);
        EnqueueTransaction(request);
    }

    void BidlApplication::EnqueueTransaction(const ClientRequest& transaction) {
        if (!m_running || (m_role != BIDL_SEQUENCER && !(m_nativeSequencing && m_role == BIDL_ORDERER)) ||
            (m_requireClientAuthentication && !VerifyRequest(transaction))) {
            return;
        }
        const auto key = std::make_pair(transaction.clientId, transaction.requestId);
        auto aborted = m_terminalAborts.find(key);
        if (aborted != m_terminalAborts.end()) {
            if (aborted->second.digest == transaction.digest) Send(transaction.clientId, aborted->second);
            return;
        }
        if (m_nativeSequencing && m_role == BIDL_ORDERER) {
            auto known = m_clientRequestSequences.find(key);
            if (known != m_clientRequestSequences.end() && m_committedTransactionBlocks.count(known->second)) {
                const ClientRequest& stored = m_transactions.at(known->second);
                if (stored.digest == transaction.digest && stored.payloadSize == transaction.payloadSize &&
                    stored.transactionCount == transaction.transactionCount)
                    ReplyToClient(known->second, m_committedTransactionBlocks.at(known->second));
                return;
            }
        }
        if (m_nativeSequencing && m_role == BIDL_SEQUENCER) {
            if (m_pendingIngress.emplace(key, transaction).second) ForwardIngressRequest(transaction);
            if (!m_ingressRetryEvent.IsRunning())
                m_ingressRetryEvent = Simulator::Schedule(m_sequencerTimeout, &BidlApplication::RetryIngress, this);
            return;
        }
        if (m_nativeSequencing && !CanSequence()) {
            m_pendingIngress[key] = transaction;
            if (!m_sequencerTimeoutEvent.IsRunning())
                m_sequencerTimeoutEvent = Simulator::Schedule(m_sequencerTimeout, &BidlApplication::OnSequencerTimeout, this, m_sequenceView);
            return;
        }
        // A retry of an ingress request already queued for sequencing must not
        // create a second CPU task for the same request.
        if (!m_queuedIngressRequests.insert(key).second) return;
        m_ingressArrivalTimes.emplace(key, Simulator::Now());
        m_resourceModel->SubmitCpuTask(
            m_sequencerServiceTime,
            MakeCallback(&BidlApplication::CompleteSequencerService, this)
                .Bind(transaction.clientId, transaction.requestId, transaction.payloadSize, transaction.transactionCount,
                      transaction.digest, transaction.authentication));
    }

    void BidlApplication::CompleteSequencerService(uint32_t clientId, uint64_t requestId, uint32_t payloadSize,
                                                   uint32_t transactionCount, uint64_t digest, std::vector<uint8_t> authentication) {
        m_queuedIngressRequests.erase({clientId, requestId});
        ClientRequest transaction;
        transaction.clientId = clientId;
        transaction.requestId = requestId;
        transaction.payloadSize = payloadSize;
        transaction.transactionCount = transactionCount;
        transaction.digest = digest;
        transaction.authentication = std::move(authentication);
        SequenceTransaction(transaction);
    }

    void BidlApplication::SequenceTransaction(const ClientRequest& transaction) {
        if (!m_running || !CanSequence()) {
            return;
        }

        auto requestKey = std::make_pair(transaction.clientId, transaction.requestId);
        auto knownRequest = m_clientRequestSequences.find(requestKey);
        if (knownRequest != m_clientRequestSequences.end()) {
            uint64_t sequence = knownRequest->second;
            const ClientRequest& knownTransaction = m_transactions.at(sequence);
            if (knownTransaction.digest != transaction.digest || knownTransaction.payloadSize != transaction.payloadSize ||
                knownTransaction.transactionCount != transaction.transactionCount) {
                return;
            }
            auto committed = m_committedTransactionBlocks.find(sequence);
            if (committed != m_committedTransactionBlocks.end()) {
                ReplyToClient(sequence, committed->second);
            } else if (m_nativeSequencing) {
                std::set<uint32_t> peers(m_ordererIds.begin(), m_ordererIds.end());
                peers.insert(m_executorIds.begin(), m_executorIds.end());
                peers.insert(m_sequencerIds.begin(), m_sequencerIds.end());
                for (uint32_t peer : peers)
                    SendSequencedTransaction(peer, sequence, BidlRepairReason::DUPLICATE_CLIENT_REQUEST);
            }
            return;
        }

        uint64_t sequence = m_nextTransactionSequence++;
        m_transactions[sequence] = transaction;
        m_transactionEpoch[sequence] = m_sequenceView;
        m_clientRequestSequences[requestKey] = sequence;
        Time ingress = Simulator::Now();
        auto ingressIterator = m_ingressArrivalTimes.find(requestKey);
        if (ingressIterator != m_ingressArrivalTimes.end()) {
            ingress = ingressIterator->second;
            m_ingressArrivalTimes.erase(ingressIterator);
        }
        m_transactionArrivalTimes[sequence] = ingress;
        if (!m_hasCommitWindow) {
            m_firstCommitWindowStart = ingress;
            m_hasCommitWindow = true;
        }

        ConsensusMessage message;
        message.protocol = ConsensusProtocol::GENERIC;
        message.type = ConsensusMessageType::BIDL_TRANSACTION;
        message.instanceId = sequence;
        message.payloadSize = transaction.payloadSize;
        message.digest = transaction.digest;
        message.clientId = transaction.clientId;
        message.requestId = transaction.requestId;
        message.transactionCount = transaction.transactionCount;
        message.view = m_sequenceView;
        message.payloadBytes = EncodeTransactions({{sequence, transaction, static_cast<uint64_t>(ingress.GetNanoSeconds())}});
        Broadcast(m_ordererIds, message);
        Broadcast(m_executorIds, message);
        Broadcast(m_broadcasterIds, message);
        if (m_nativeSequencing) {
            Send(m_sequencerIds.front(), message);
            m_digestSequences[transaction.digest] = sequence;
            TryProposeBlocks(false);
        }
    }

    bool BidlApplication::CanSequence() const {
        return m_nativeSequencing ? (m_role == BIDL_ORDERER && m_engine && m_engine->GetLeaderId() == m_nodeId)
                                  : m_role == BIDL_SEQUENCER;
    }

    void BidlApplication::RetryIngress() {
        if (!m_running || m_pendingIngress.empty()) return;
        Simulator::Cancel(m_ingressRetryEvent);
        for (const auto& item : m_pendingIngress) {
            ForwardIngressRequest(item.second);
        }
        m_ingressRetryEvent = Simulator::Schedule(m_sequencerTimeout, &BidlApplication::RetryIngress, this);
    }

    void BidlApplication::ForwardIngressRequest(const ClientRequest& request) {
        ConsensusMessage message;
        message.type = ConsensusMessageType::CLIENT_REQUEST;
        message.clientId = request.clientId;
        message.requestId = request.requestId;
        message.digest = request.digest;
        message.transactionCount = request.transactionCount;
        message.payloadBytes = EncodeTransactions({{1, request, static_cast<uint64_t>(Simulator::Now().GetNanoSeconds())}});
        Broadcast(m_ordererIds, message);
    }

    void BidlApplication::OnSequencerTimeout(uint64_t view) {
        if (!m_running || !m_nativeSequencing || m_pendingIngress.empty() || view != m_sequenceView) return;
        Ptr<PbftEngine> engine = DynamicCast<PbftEngine>(m_engine);
        if (engine && engine->GetCurrentView() == view) engine->RequestViewChange();
    }

    void BidlApplication::SendSequencedTransaction(uint32_t receiver, uint64_t sequence, BidlRepairReason reason) {
        if (!m_running || receiver == m_nodeId) return;
        auto transaction = m_transactions.find(sequence);
        if (transaction == m_transactions.end()) return;
        ConsensusMessage reply;
        reply.protocol = ConsensusProtocol::GENERIC;
        reply.type = ConsensusMessageType::BIDL_TRANSACTION_REPAIR;
        reply.instanceId = sequence;
        reply.payloadSize = transaction->second.payloadSize;
        reply.digest = transaction->second.digest;
        reply.clientId = transaction->second.clientId;
        reply.requestId = transaction->second.requestId;
        reply.transactionCount = transaction->second.transactionCount;
        auto arrival = m_transactionArrivalTimes.find(sequence);
        reply.view = m_transactionEpoch.count(sequence) ? m_transactionEpoch.at(sequence) : m_sequenceView;
        reply.payloadBytes = EncodeTransactions({{sequence, transaction->second,
            arrival == m_transactionArrivalTimes.end() ? 0 : static_cast<uint64_t>(arrival->second.GetNanoSeconds())}});
        m_repairTxTrace(m_nodeId, receiver, sequence, static_cast<uint8_t>(reason), reply.ToPacket()->GetSize());
        Send(receiver, reply);
    }

    void BidlApplication::RequestMissingTransactions(uint64_t first, uint64_t last) {
        if (!m_running || m_role == BIDL_SEQUENCER || first == 0 || last < first) return;
        const uint64_t limit = std::min(last, first + 511);
        for (uint64_t sequence = first; sequence <= limit; ++sequence) {
            if (IsTransactionAvailableForGap(sequence)) continue;
            RetryMissingTransaction(sequence);
        }
    }

    bool BidlApplication::IsTransactionAvailableForGap(uint64_t sequence) const {
        return m_transactions.count(sequence) != 0 || m_verificationPending.count(sequence) != 0 ||
               m_iorsPendingMarked.count(sequence) != 0;
    }

    void BidlApplication::ObserveTransactionSequence(uint64_t sequence) {
        m_highestObservedTransactionSequence = std::max(m_highestObservedTransactionSequence, sequence);
        AdvanceGapProgress();
    }

    void BidlApplication::AdvanceGapProgress() {
        while (IsTransactionAvailableForGap(m_nextGapSequence)) ++m_nextGapSequence;
        if (m_role != BIDL_SEQUENCER && m_nextGapSequence <= m_highestObservedTransactionSequence)
            RequestMissingTransactions(m_nextGapSequence, m_highestObservedTransactionSequence);
    }

    void BidlApplication::RetryMissingTransaction(uint64_t sequence, bool timerExpired) {
        auto existing = m_gapRequestEvents.find(sequence);
        if (existing != m_gapRequestEvents.end()) {
            // A live timer already owns this gap. Repeated observations of
            // later transactions must not reset it and emit another request.
            if (!timerExpired) return;
            m_gapRequestEvents.erase(existing);
        }
        if (!m_running) return;
        if (m_transactions.count(sequence) != 0) {
            m_gapRequestAttempts.erase(sequence);
            return;
        }
        if (m_verificationPending.count(sequence) != 0 || m_iorsPendingMarked.count(sequence) != 0) return;
        if (m_gapRequestAttempts[sequence]++ >= 8) return;
        ConsensusMessage request;
        request.type = ConsensusMessageType::BIDL_GAP_REQUEST;
        request.instanceId = sequence;
        Send(m_sequencerIds.front(), request);
        m_gapRequestEvents[sequence] = Simulator::Schedule(MilliSeconds(2),
            &BidlApplication::RetryMissingTransaction, this, sequence, true);
    }

    void BidlApplication::ReplyToClient(uint64_t sequence, uint64_t blockId) {
        auto transaction = m_transactions.find(sequence);
        if (transaction == m_transactions.end()) {
            return;
        }
        const ClientRequest& request = transaction->second;
        ConsensusMessage reply;
        reply.protocol = ConsensusProtocol::GENERIC;
        reply.type = ConsensusMessageType::CLIENT_REPLY;
        reply.instanceId = blockId;
        reply.digest = request.digest;
        reply.clientId = request.clientId;
        reply.requestId = request.requestId;
        reply.transactionCount = request.transactionCount;
        Send(request.clientId, reply);
    }

    bool BidlApplication::StoreTransaction(const ConsensusMessage& message) {
        if (message.instanceId == 0 || message.payloadSize == 0 || message.digest == 0) {
            return false;
        }

        auto existing = m_transactions.find(message.instanceId);
        if (existing != m_transactions.end()) {
            return existing->second.digest == message.digest && existing->second.clientId == message.clientId &&
                   existing->second.requestId == message.requestId;
        }

        if (message.digest != INVALID_TRANSACTION_DIGEST) {
            auto replay = m_digestSequences.find(message.digest);
            if (replay != m_digestSequences.end() && replay->second != message.instanceId) {
                return false;
            }
        }

        ClientRequest transaction;
        if (!ReadRequest(message, transaction)) return false;
        m_transactions[message.instanceId] = transaction;
        m_clientRequestSequences[{transaction.clientId, transaction.requestId}] = message.instanceId;
        m_nextTransactionSequence = std::max(m_nextTransactionSequence, message.instanceId + 1);
        m_pendingIngress.erase({transaction.clientId, transaction.requestId});
        auto retry = m_gapRequestEvents.find(message.instanceId);
        if (retry != m_gapRequestEvents.end()) {
            Simulator::Cancel(retry->second);
            m_gapRequestEvents.erase(retry);
        }
        m_gapRequestAttempts.erase(message.instanceId);
        m_transactionSource[message.instanceId] = message.senderId;
        m_transactionEpoch[message.instanceId] = message.view;
        if (message.digest == INVALID_TRANSACTION_DIGEST) {
            m_invalidTransactions.insert(message.instanceId);
        } else {
            m_digestSequences[message.digest] = message.instanceId;
        }
        std::vector<BidlTransactionRecord> records;
        m_transactionArrivalTimes[message.instanceId] = DecodeTransactions(message.payloadBytes, records) &&
            records.front().ingressTimeNs ? NanoSeconds(records.front().ingressTimeNs) : Simulator::Now();
        ObserveTransactionSequence(message.instanceId);
        return true;
    }

    bool BidlApplication::IsTransactionReplay(const ConsensusMessage& message) const {
        if (message.digest == INVALID_TRANSACTION_DIGEST) return false;
        auto stored = m_digestSequences.find(message.digest);
        if (stored != m_digestSequences.end() && stored->second != message.instanceId) return true;
        auto pending = m_verificationDigestSequences.find(message.digest);
        return pending != m_verificationDigestSequences.end() && pending->second != message.instanceId;
    }

    void BidlApplication::VerifyTransaction(ConsensusMessage message) {
        if (IsTransactionReplay(message)) return;
        ClientRequest verified;
        if (!ReadRequest(message, verified)) return;
        auto stored = m_transactions.find(message.instanceId);
        if (stored != m_transactions.end()) {
            if (stored->second.digest != message.digest) {
                TraceRejectedConflict(message);
            }
            return;
        }
        auto pending = m_verificationDigests.find(message.instanceId);
        if (pending != m_verificationDigests.end()) {
            if (pending->second != message.digest) {
                TraceRejectedConflict(message);
            }
            return;
        }
        m_verificationPending.insert(message.instanceId);
        m_verificationDigests[message.instanceId] = message.digest;
        if (message.digest != INVALID_TRANSACTION_DIGEST)
            m_verificationDigestSequences[message.digest] = message.instanceId;
        ObserveTransactionSequence(message.instanceId);
        auto retry = m_gapRequestEvents.find(message.instanceId);
        if (retry != m_gapRequestEvents.end()) {
            Simulator::Cancel(retry->second);
            m_gapRequestEvents.erase(retry);
        }
        m_transactionReceiveTimes.emplace(message.instanceId, Simulator::Now());
        m_candidateTrace(message.senderId, m_nodeId, static_cast<uint16_t>(message.type), message.view,
                         message.instanceId, message.digest, message.clientId, message.requestId, 6,
                         message.iorsMarked ? 1 : 0);
        m_resourceModel->SubmitCpuTask(m_transactionVerificationTime,
                                       MakeCallback(&BidlApplication::CompleteTransactionVerification, this).Bind(message));
    }

    void BidlApplication::CompleteTransactionVerification(ConsensusMessage message) {
        m_verificationPending.erase(message.instanceId);
        m_verificationDigests.erase(message.instanceId);
        auto digest = m_verificationDigestSequences.find(message.digest);
        if (digest != m_verificationDigestSequences.end() && digest->second == message.instanceId)
            m_verificationDigestSequences.erase(digest);
        m_candidateTrace(message.senderId, m_nodeId, static_cast<uint16_t>(message.type), message.view,
                         message.instanceId, message.digest, message.clientId, message.requestId,
                         BIDL_CANDIDATE_VERIFICATION_COMPLETE,
                         message.iorsMarked ? 1 : 0);
        if (!StoreTransaction(message)) {
            // The candidate occupied this sequence while verification was in
            // flight. Re-open the hole if validation rejected it.
            m_nextGapSequence = std::min(m_nextGapSequence, message.instanceId);
            AdvanceGapProgress();
            return;
        }
        if (m_role == BIDL_ORDERER) {
            TryProposeBlocks(false);
            TryProcessPendingProposals(message.instanceId);
        } else if (m_role == BIDL_EXECUTOR) {
            TryExecuteNextTransaction();
            auto block = m_orderedSequenceBlocks.find(message.instanceId);
            if (block != m_orderedSequenceBlocks.end()) {
                TryPersist(m_blocks.at(block->second));
                TryProcessPendingExecutorMessages(block->second);
            }
        }
    }

    void BidlApplication::TryProcessPendingExecutorMessages(uint64_t blockId) {
        auto block = m_blocks.find(blockId);
        if (block == m_blocks.end()) return;
        const uint64_t digest = ComputeBlockDigest(blockId, block->second.transactionCount);
        if (digest == 0 || (digest != block->second.digest && !block->second.reexecutionComplete)) return;
        std::vector<ConsensusMessage> ready;
        auto range = m_pendingExecutorMessages.equal_range(blockId);
        for (auto iterator = range.first; iterator != range.second; ++iterator) {
            ready.push_back(iterator->second);
        }
        m_pendingExecutorMessages.erase(range.first, range.second);
        for (const ConsensusMessage& message : ready) {
            HandleExecutorMessage(message);
        }
    }

    uint64_t BidlApplication::ComputeBlockDigest(uint64_t firstSequence, uint32_t count) const {
        uint64_t digest = 0;
        for (uint64_t sequence = firstSequence; sequence < firstSequence + count; ++sequence) {
            auto transaction = m_transactions.find(sequence);
            if (transaction == m_transactions.end()) {
                return 0;
            }
            digest ^= transaction->second.digest + 0x9e3779b97f4a7c15ULL + (digest << 6) + (digest >> 2);
        }
        return digest;
    }

    bool BidlApplication::ValidateBlock(uint64_t firstSequence, uint32_t count, uint64_t digest) const {
        return firstSequence > 0 && count > 0 && digest != 0 && ComputeBlockDigest(firstSequence, count) == digest;
    }

    void BidlApplication::TryProposeBlocks(bool flushPartial) {
        if (!m_running || m_role != BIDL_ORDERER || !m_engine || m_engine->GetLeaderId() != m_nodeId) {
            return;
        }

        while (true) {
            uint32_t available = 0;
            while (available < m_batchSize && m_transactions.count(m_nextBlockSequence + available) != 0) {
                ++available;
            }
            if (available == 0) {
                return;
            }
            if (available < m_batchSize && !flushPartial) {
                if (!m_batchEvent.IsRunning()) {
                    m_batchEvent = Simulator::Schedule(m_batchTimeout, &BidlApplication::FlushPartialBlock, this);
                }
                return;
            }

            if (m_batchEvent.IsRunning()) {
                Simulator::Cancel(m_batchEvent);
            }
            ProposeBlock(m_nextBlockSequence, available);
            m_nextBlockSequence += available;
            flushPartial = false;
        }
    }

    void BidlApplication::ProposeBlock(uint64_t firstSequence, uint32_t count) {
        uint64_t digest = ComputeBlockDigest(firstSequence, count);
        if (digest == 0) {
            return;
        }

        ClientRequest blockHashes;
        blockHashes.clientId = m_sequencerIds.front();
        blockHashes.requestId = firstSequence;
        blockHashes.payloadSize = count * 32;
        blockHashes.transactionCount = count;
        blockHashes.digest = digest;
        m_engine->Submit(blockHashes);
        if (m_engine->GetLeaderId() == m_nodeId && m_tracedBlocks.insert(firstSequence).second) {
            m_consensusStartTimes[firstSequence] = Simulator::Now();
            m_batchCreatedTrace(m_nodeId, firstSequence, count);
            m_phaseChangedTrace(m_nodeId, firstSequence, BIDL_BLOCK_PROPOSED, Seconds(0));
        }
    }

    void BidlApplication::FlushPartialBlock() {
        TryProposeBlocks(true);
    }

    void BidlApplication::IndexPendingProposal(uint64_t blockId) {
        auto previous = m_proposalMissingTransaction.find(blockId);
        if (previous != m_proposalMissingTransaction.end()) {
            auto waiting = m_proposalsWaitingForTransaction.find(previous->second);
            if (waiting != m_proposalsWaitingForTransaction.end()) {
                waiting->second.erase(blockId);
                if (waiting->second.empty()) m_proposalsWaitingForTransaction.erase(waiting);
            }
            m_proposalMissingTransaction.erase(previous);
        }
        auto proposal = m_pendingBlockProposals.find(blockId);
        if (proposal == m_pendingBlockProposals.end()) return;
        for (uint64_t seq = blockId; seq < blockId + proposal->second.transactionCount; ++seq) {
            if (m_transactions.count(seq) != 0) continue;
            m_proposalMissingTransaction[blockId] = seq;
            m_proposalsWaitingForTransaction[seq].insert(blockId);
            break;
        }
    }

    void BidlApplication::TryProcessPendingProposals(uint64_t availableSequence) {
        auto waiting = m_proposalsWaitingForTransaction.find(availableSequence);
        if (waiting == m_proposalsWaitingForTransaction.end()) return;
        auto blockIds = std::move(waiting->second);
        m_proposalsWaitingForTransaction.erase(waiting);
        for (uint64_t blockId : blockIds) {
            m_proposalMissingTransaction.erase(blockId);
            auto iterator = m_pendingBlockProposals.find(blockId);
            if (iterator == m_pendingBlockProposals.end()) continue;
            const ConsensusMessage proposal = iterator->second;
            uint64_t digest = ComputeBlockDigest(proposal.requestId, proposal.transactionCount);
            if (digest == 0) {
                RequestMissingTransactions(proposal.requestId,
                    proposal.requestId + proposal.transactionCount - 1);
                IndexPendingProposal(blockId);
            } else if (digest != proposal.digest) {
                m_pendingBlockProposals.erase(iterator);
            } else {
                m_pendingBlockProposals.erase(iterator);
                m_engine->Receive(proposal);
            }
        }
    }

    void BidlApplication::OnTransportReceive(uint32_t peerId, Ptr<Packet> packet) {
        ConsensusMessage message;
        if (!m_running || !ConsensusMessage::FromPacket(packet, message) || message.senderId != peerId || message.receiverId != m_nodeId) {
            return;
        }
        m_messageRxTrace(message.senderId, m_nodeId, static_cast<uint16_t>(message.type), message.instanceId, packet->GetSize());
        m_messageEventTrace(false, message);
        if (message.type == ConsensusMessageType::BIDL_TRANSACTION || message.type == ConsensusMessageType::BIDL_TRANSACTION_REPAIR)
            m_candidateTrace(message.senderId, m_nodeId, static_cast<uint16_t>(message.type), message.view,
                             message.instanceId, message.digest, message.clientId, message.requestId, 1, message.iorsMarked ? 1 : 0);
        if (message.type == ConsensusMessageType::BIDL_SEQUENCE_EPOCH) {
            HandleSequenceEpoch(message);
            return;
        }
        if (m_role == BIDL_SEQUENCER) {
            HandleSequencerMessage(message);
        } else if (m_role == BIDL_ORDERER) {
            HandleOrdererMessage(message);
        } else if (m_role == BIDL_EXECUTOR) {
            HandleExecutorMessage(message);
        } else {
            HandleMaliciousMessage(message);
        }
    }

    void BidlApplication::OnEngineSend(uint32_t peerId, const ConsensusMessage& message) {
        Send(peerId, message);
    }

    void BidlApplication::OnOrderingViewChanged(uint32_t replica, uint64_t view, uint32_t leader) {
        if (!m_running || replica != m_nodeId) return;
        m_sequenceView = view;
        ConsensusMessage update;
        update.type = ConsensusMessageType::BIDL_SEQUENCE_EPOCH;
        update.view = view;
        update.digest = leader;
        std::vector<uint8_t> body;
        BidlWrite64(body, view);
        BidlWrite32(body, leader);
        ZyzzyvaAuth::SignDetached(m_nodeId, body, update.payloadBytes);
        Broadcast(m_sequencerIds, update);
        Broadcast(m_executorIds, update);
        Broadcast(m_broadcasterIds, update);
        Simulator::ScheduleNow(&BidlApplication::ResumeOrdering, this);
        if (m_nativeSequencing) {
            Simulator::Cancel(m_sequencerTimeoutEvent);
            auto pending = std::move(m_pendingIngress);
            m_pendingIngress.clear();
            for (const auto& item : pending) EnqueueTransaction(item.second);
        }
    }

    void BidlApplication::HandleSequenceEpoch(const ConsensusMessage& message) {
        if (std::find(m_ordererIds.begin(), m_ordererIds.end(), message.senderId) == m_ordererIds.end() ||
            std::find(m_ordererIds.begin(), m_ordererIds.end(), message.digest) == m_ordererIds.end()) return;
        std::vector<uint8_t> body;
        BidlWrite64(body, message.view);
        BidlWrite32(body, message.digest);
        if (!ZyzzyvaAuth::VerifyDetached(message.senderId, body, message.payloadBytes)) return;
        auto& voters = m_sequenceEpochVotes[{message.view, static_cast<uint32_t>(message.digest)}];
        voters.insert(message.senderId);
        if (voters.size() >= 2 * m_faultTolerance + 1 && message.view > m_sequenceView)
            m_sequenceView = message.view;
    }

    void BidlApplication::ResumeOrdering() {
        if (!m_running || m_role != BIDL_ORDERER || !m_engine) return;
        Ptr<PbftEngine> pbft = DynamicCast<PbftEngine>(m_engine);
        if (!pbft) return;
        uint64_t next = 1;
        for (const auto& item : pbft->GetInstances()) {
            const PbftInstance& instance = item.second;
            if (instance.decided || (instance.view == pbft->GetCurrentView() && instance.prePrepareAccepted))
                next = std::max(next, instance.requestId + instance.transactionCount);
        }
        m_nextBlockSequence = next;
        TryProposeBlocks(false);
    }

    void BidlApplication::OnEngineDecision(const ConsensusDecision& decision) {
        if (!ValidateBlock(decision.requestId, decision.transactionCount, decision.digest)) {
            return;
        }
        m_pendingConsensusDecisions[decision.requestId] = decision;
        m_resourceModel->SubmitCpuTask(
            m_consensusSoftwareTime,
            MakeCallback(&BidlApplication::CompleteEngineDecision, this).Bind(decision.requestId));
    }

    void BidlApplication::CompleteEngineDecision(uint64_t requestId) {
        auto pending = m_pendingConsensusDecisions.find(requestId);
        if (pending == m_pendingConsensusDecisions.end()) {
            return;
        }
        ConsensusDecision decision = pending->second;
        m_pendingConsensusDecisions.erase(pending);
        if (!m_running || !ValidateBlock(decision.requestId, decision.transactionCount, decision.digest)) {
            return;
        }

        if (m_engine->GetLeaderId() == m_nodeId && m_tracedBlocks.insert(decision.requestId).second) {
            m_batchCreatedTrace(m_nodeId, decision.requestId, decision.transactionCount);
            m_phaseChangedTrace(m_nodeId, decision.requestId, BIDL_BLOCK_PROPOSED, Seconds(0));
        }
        if (m_engine->GetLeaderId() == m_nodeId) {
            auto started = m_consensusStartTimes.find(decision.requestId);
            if (started != m_consensusStartTimes.end()) {
                m_phaseChangedTrace(m_nodeId, decision.requestId, BIDL_P1_CONSENSUS, Simulator::Now() - started->second);
            }
        }

        ConsensusMessage ordered;
        ordered.protocol = ConsensusProtocol::GENERIC;
        ordered.type = ConsensusMessageType::BIDL_ORDERED_BLOCK;
        ordered.view = decision.view;
        ordered.instanceId = decision.requestId;
        ordered.payloadSize = decision.transactionCount * 32;
        ordered.digest = decision.digest;
        ordered.requestId = decision.requestId;
        ordered.transactionCount = decision.transactionCount;
        if (!m_compactOrderNotifications) {
            std::vector<BidlTransactionRecord> records;
            for (uint64_t sequence = decision.requestId;
                 sequence < decision.requestId + decision.transactionCount; ++sequence) {
                records.push_back({sequence, m_transactions.at(sequence),
                                   static_cast<uint64_t>(m_transactionArrivalTimes.at(sequence).GetNanoSeconds())});
            }
            ordered.payloadBytes = EncodeTransactions(records);
        }
        m_decidedBlocks[ordered.instanceId] = ordered;
        m_nextBlockSequence = std::max(m_nextBlockSequence, ordered.instanceId + ordered.transactionCount);
        Broadcast(m_executorIds, ordered);
    }

    void BidlApplication::HandleSequencerMessage(const ConsensusMessage& message) {
        if (m_nativeSequencing && (message.type == ConsensusMessageType::BIDL_TRANSACTION ||
                                 message.type == ConsensusMessageType::BIDL_TRANSACTION_REPAIR) &&
            std::find(m_ordererIds.begin(), m_ordererIds.end(), message.senderId) != m_ordererIds.end()) {
            StoreTransaction(message);
            return;
        }
        if (message.type == ConsensusMessageType::BIDL_GAP_REQUEST) {
            const bool authorized = std::find(m_ordererIds.begin(), m_ordererIds.end(), message.senderId) != m_ordererIds.end() ||
                std::find(m_executorIds.begin(), m_executorIds.end(), message.senderId) != m_executorIds.end();
            if (authorized && message.instanceId > 0)
                SendSequencedTransaction(message.senderId, message.instanceId, BidlRepairReason::GAP_REQUEST);
            return;
        }
        if (message.type == ConsensusMessageType::CLIENT_REQUEST) {
            ClientRequest request;
            if (ReadRequest(message, request)) EnqueueTransaction(request);
            return;
        }

        if (message.type == ConsensusMessageType::BIDL_ABORT) {
            if (std::find(m_executorIds.begin(), m_executorIds.end(), message.senderId) == m_executorIds.end() ||
                !ValidateBlock(message.instanceId, message.transactionCount, message.digest) ||
                m_committedBlockIds.count(message.instanceId) != 0) return;
            auto& voters = m_abortVotes[message.instanceId];
            voters.insert(message.senderId);
            if (voters.size() >= 2 * m_faultTolerance + 1 && m_abortedBlockIds.insert(message.instanceId).second) {
                for (uint64_t sequence = message.instanceId;
                     sequence < message.instanceId + message.transactionCount; ++sequence) {
                    const ClientRequest& transaction = m_transactions.at(sequence);
                    ConsensusMessage aborted = message;
                    aborted.clientId = transaction.clientId;
                    aborted.requestId = transaction.requestId;
                    aborted.digest = transaction.digest;
                    aborted.transactionCount = transaction.transactionCount;
                    aborted.messageId = 0; // Each per-request notification/retry needs fresh transport framing.
                    m_terminalAborts[{transaction.clientId, transaction.requestId}] = aborted;
                    m_pendingIngress.erase({transaction.clientId, transaction.requestId});
                    Send(transaction.clientId, aborted);
                }
            }
            return;
        }

        if (message.type != ConsensusMessageType::BIDL_COMMIT ||
            std::find(m_executorIds.begin(), m_executorIds.end(), message.senderId) == m_executorIds.end() ||
            !ValidateBlock(message.instanceId, message.transactionCount, message.digest)) {
            return;
        }
        auto& confirmations = m_commitVotes[{message.instanceId, message.transactionCount, message.digest}];
        confirmations.insert(message.senderId);
        if (confirmations.size() < 2 * m_faultTolerance + 1 || !m_committedBlockIds.insert(message.instanceId).second) return;

        for (uint64_t sequence = message.instanceId; sequence < message.instanceId + message.transactionCount; ++sequence) {
            m_committedTransactionBlocks[sequence] = message.instanceId;
            const ClientRequest& request = m_transactions.at(sequence);
            m_pendingIngress.erase({request.clientId, request.requestId});
            ReplyToClient(sequence, message.instanceId);
        }
    }

    void BidlApplication::HandleOrdererMessage(const ConsensusMessage& message) {
        if (m_nativeSequencing && message.type == ConsensusMessageType::CLIENT_REQUEST) {
            ClientRequest request;
            if (ReadRequest(message, request)) EnqueueTransaction(request);
            return;
        }
        if (m_nativeSequencing && message.type == ConsensusMessageType::BIDL_GAP_REQUEST) {
            if (std::find(m_ordererIds.begin(), m_ordererIds.end(), message.senderId) != m_ordererIds.end() ||
                std::find(m_executorIds.begin(), m_executorIds.end(), message.senderId) != m_executorIds.end())
                SendSequencedTransaction(message.senderId, message.instanceId, BidlRepairReason::GAP_REQUEST);
            return;
        }
        if (m_nativeSequencing && (message.type == ConsensusMessageType::BIDL_COMMIT || message.type == ConsensusMessageType::BIDL_ABORT)) {
            HandleSequencerMessage(message);
            return;
        }
        if (message.type == ConsensusMessageType::BIDL_BLOCK_REQUEST ||
            message.type == ConsensusMessageType::BIDL_BLOCK_SUMMARY_REQUEST) {
            auto block = m_decidedBlocks.find(message.instanceId);
            if (block != m_decidedBlocks.end() &&
                std::find(m_executorIds.begin(), m_executorIds.end(), message.senderId) != m_executorIds.end()) {
                if (message.type == ConsensusMessageType::BIDL_BLOCK_SUMMARY_REQUEST) {
                    ConsensusMessage summary = block->second;
                    summary.payloadBytes.clear();
                    Send(message.senderId, summary);
                } else {
                    if (block->second.payloadBytes.empty()) {
                        std::vector<BidlTransactionRecord> records;
                        for (uint64_t sequence = block->second.instanceId;
                             sequence < block->second.instanceId + block->second.transactionCount; ++sequence) {
                            auto transaction = m_transactions.find(sequence);
                            if (transaction == m_transactions.end()) return;
                            records.push_back({sequence, transaction->second,
                                static_cast<uint64_t>(m_transactionArrivalTimes.at(sequence).GetNanoSeconds())});
                        }
                        block->second.payloadBytes = EncodeTransactions(records);
                    }
                    Send(message.senderId, block->second);
                }
            }
            return;
        }
        if (message.type == ConsensusMessageType::BIDL_EXECUTION_RESULT) {
            HandleExecutionResult(message);
            return;
        }
        if (message.type == ConsensusMessageType::BIDL_CONFLICT_REPORT) {
            HandleConflictReport(message);
            return;
        }
        if (message.type == ConsensusMessageType::BIDL_DENYLIST_VOTE) {
            HandleDenylistVote(message);
            return;
        }
        if (message.type == ConsensusMessageType::BIDL_TRANSACTION || message.type == ConsensusMessageType::BIDL_TRANSACTION_REPAIR) {
            if (std::find(m_sequencerIds.begin(), m_sequencerIds.end(), message.senderId) != m_sequencerIds.end()) {
                VerifyTransaction(message);
            }
            return;
        }

        if (message.type == ConsensusMessageType::PRE_PREPARE) {
            uint64_t digest = ComputeBlockDigest(message.requestId, message.transactionCount);
            if (digest == 0) {
                RequestMissingTransactions(message.requestId, message.requestId + message.transactionCount - 1);
                m_pendingBlockProposals[message.requestId] = message;
                IndexPendingProposal(message.requestId);
                return;
            }
            if (!ValidateBlock(message.requestId, message.transactionCount, message.digest)) {
                return;
            }
        }
        m_engine->Receive(message);
        if (m_denylistEnabled &&
            (message.type == ConsensusMessageType::VIEW_CHANGE || message.type == ConsensusMessageType::NEW_VIEW)) {
            // Carry locally accumulated client accusations alongside subsequent
            // view changes, so a replica that missed an earlier vote can catch up.
            for (const auto& suspect : m_suspectLeaders) {
                if (suspect.second.size() < m_faultTolerance + 1 ||
                    !m_sentDenylistViewRecords.emplace(message.view, suspect.first).second) continue;
                ConsensusMessage record;
                record.type = ConsensusMessageType::BIDL_DENYLIST_VOTE;
                record.clientId = suspect.first;
                record.view = message.view;
                Broadcast(m_ordererIds, record);
                Broadcast(m_executorIds, record);
            }
        }
    }

    void BidlApplication::HandleExecutorMessage(const ConsensusMessage& message) {
        if (message.type == ConsensusMessageType::BIDL_DENYLIST_VOTE) {
            HandleDenylistVote(message);
            return;
        }
        if (message.type == ConsensusMessageType::BIDL_TRANSACTION || message.type == ConsensusMessageType::BIDL_TRANSACTION_REPAIR) {
            bool fromSequencer = std::find(m_sequencerIds.begin(), m_sequencerIds.end(), message.senderId) != m_sequencerIds.end();
            bool fromBroadcaster =
                std::find(m_broadcasterIds.begin(), m_broadcasterIds.end(), message.senderId) != m_broadcasterIds.end();
            if (fromSequencer || (fromBroadcaster && message.type == ConsensusMessageType::BIDL_TRANSACTION)) {
                if (IsTransactionReplay(message)) return;
                if (m_iorsTerminalGate) {
                    if (message.iorsMarked && m_transactions.count(message.instanceId) == 0) {
                        m_candidateTrace(message.senderId, m_nodeId, static_cast<uint16_t>(message.type), message.view,
                                         message.instanceId, message.digest, message.clientId, message.requestId, 3, 1);
                        if (m_iorsPendingMarked.emplace(message.instanceId, message).second) {
                            m_iorsGateEvents[message.instanceId] = Simulator::Schedule(
                                m_iorsGateTimeout, &BidlApplication::OnIorsGateTimeout, this, message.instanceId);
                        }
                        auto retry = m_gapRequestEvents.find(message.instanceId);
                        if (retry != m_gapRequestEvents.end()) {
                            Simulator::Cancel(retry->second);
                            m_gapRequestEvents.erase(retry);
                        }
                        ObserveTransactionSequence(message.instanceId);
                        return;
                    }
                    auto pending = m_iorsPendingMarked.find(message.instanceId);
                    if (!message.iorsMarked && pending != m_iorsPendingMarked.end()) {
                        const ConsensusMessage markedCandidate = pending->second;
                        m_candidateTrace(markedCandidate.senderId, m_nodeId, static_cast<uint16_t>(markedCandidate.type),
                                         markedCandidate.view, markedCandidate.instanceId, markedCandidate.digest,
                                         markedCandidate.clientId, markedCandidate.requestId,
                                         BIDL_CANDIDATE_MARKED_REPLACED, 1);
                        Simulator::Cancel(m_iorsGateEvents[message.instanceId]);
                        m_iorsGateEvents.erase(message.instanceId);
                        m_iorsPendingMarked.erase(pending);
                    }
                }
                VerifyTransaction(message);
            }
            return;
        }

        if (message.type == ConsensusMessageType::BIDL_RESULT_PERSIST) {
            HandleResultPersist(message);
            return;
        }

        if (message.type != ConsensusMessageType::BIDL_ORDERED_BLOCK && message.type != ConsensusMessageType::BIDL_PERSIST) {
            return;
        }
        if (message.type == ConsensusMessageType::BIDL_ORDERED_BLOCK) {
            if (message.instanceId == 0 || message.transactionCount == 0 || message.digest == 0 ||
                std::find(m_ordererIds.begin(), m_ordererIds.end(), message.senderId) == m_ordererIds.end()) return;
            std::vector<BidlTransactionRecord> records;
            const bool canonical = DecodeTransactions(message.payloadBytes, records);
            const bool envelope = message.payloadBytes.size() >= 4 && message.payloadBytes[0] == 'B' &&
                message.payloadBytes[1] == 'I' && message.payloadBytes[2] == 'D' && message.payloadBytes[3] == 'L';
            if (envelope && !canonical) return;
            if (canonical) {
                if (records.size() != message.transactionCount) return;
                uint64_t digest = 0;
                for (uint32_t i = 0; i < records.size(); ++i) {
                    if (records[i].sequence != message.instanceId + i ||
                        (m_requireClientAuthentication && !VerifyRequest(records[i].request))) return;
                    digest ^= records[i].request.digest + 0x9e3779b97f4a7c15ULL + (digest << 6) + (digest >> 2);
                }
                if (digest != message.digest) return;
            }
            uint64_t contentBinding = 0;
            for (const auto& record : records) {
                contentBinding = BidlMix(contentBinding, record.sequence);
                contentBinding = BidlMix(contentBinding, record.request.clientId);
                contentBinding = BidlMix(contentBinding, record.request.requestId);
                contentBinding = BidlMix(contentBinding, record.request.payloadSize);
                contentBinding = BidlMix(contentBinding, record.request.transactionCount);
                contentBinding = BidlMix(contentBinding, record.request.digest);
                for (uint8_t byte : record.request.authentication) contentBinding = BidlMix(contentBinding, byte);
            }
            auto& candidate = m_orderedCandidates[{message.instanceId, message.transactionCount, message.digest, contentBinding}];
            candidate.orderedSenders.insert(message.senderId);
            if (canonical) candidate.canonicalTransactions = records;
            if (candidate.orderedSenders.size() < 2 * m_faultTolerance + 1) {
                if (!m_blockFetchEvents.count(message.instanceId))
                    m_blockFetchEvents[message.instanceId] = Simulator::Schedule(
                        MilliSeconds(1), &BidlApplication::RequestCanonicalBlock, this, message.instanceId);
                return;
            }
            for (uint64_t sequence = message.instanceId; sequence < message.instanceId + message.transactionCount; ++sequence) {
                auto prior = m_orderedSequenceBlocks.find(sequence);
                if (prior != m_orderedSequenceBlocks.end() && prior->second != message.instanceId) return;
            }
            BidlBlockState& block = m_blocks[message.instanceId];
            if (block.ordered && (block.digest != message.digest || block.transactionCount != message.transactionCount)) return;
            const bool wasOrdered = block.ordered;
            if (!wasOrdered) {
                block.blockId = message.instanceId;
                block.orderView = message.view;
                block.digest = message.digest;
                block.transactionCount = message.transactionCount;
                auto firstSeen = m_transactionArrivalTimes.find(block.blockId);
                block.firstSeen = firstSeen == m_transactionArrivalTimes.end() ? Simulator::Now() : firstSeen->second;
                block.executionStartedAt = block.firstSeen;
                if (!m_hasCommitWindow) { m_firstCommitWindowStart = block.firstSeen; m_hasCommitWindow = true; }
            }
            if (canonical) block.canonicalTransactions = candidate.canonicalTransactions;
            block.orderedSenders = candidate.orderedSenders;
            block.ordered = true;
            for (uint64_t sequence = block.blockId; sequence < block.blockId + block.transactionCount; ++sequence)
                m_orderedSequenceBlocks[sequence] = block.blockId;
            const uint64_t localDigest = ComputeBlockDigest(message.instanceId, message.transactionCount);
            block.requiresReexecution = block.requiresReexecution || localDigest != message.digest;
            for (const auto& record : block.canonicalTransactions) {
                auto local = m_transactions.find(record.sequence);
                if ((!m_compactOrderNotifications && local == m_transactions.end()) ||
                    (local != m_transactions.end() && (local->second.clientId != record.request.clientId ||
                    local->second.requestId != record.request.requestId))) block.requiresReexecution = true;
            }
            m_observedOrderView = message.view;
            if (!wasOrdered) {
                m_phaseChangedTrace(m_nodeId, block.blockId, BIDL_ORDERED, Simulator::Now() - block.firstSeen);
            }
            TryPersist(block);
            TryProcessPendingExecutorMessages(block.blockId);
        } else {
            auto block = m_blocks.find(message.instanceId);
            if (block != m_blocks.end() && block->second.digest == message.digest) {
                block->second.persistSenders.insert(message.senderId);
                TryCommit(block->second);
            }
        }
    }

    void BidlApplication::RequestCanonicalBlock(uint64_t blockId) {
        // PBFT software service is modeled at about 10 ms per block. Polling
        // every 1 ms while orderers are queued behind that work amplifies
        // normal load into a block-fetch packet storm without speeding up the
        // certificate. Keep retries, but pace them to the modeled block path.
        const Time retryInterval = MilliSeconds(10);
        m_blockFetchEvents.erase(blockId);
        if (!m_running) return;
        auto block = m_blocks.find(blockId);
        if (block != m_blocks.end() && block->second.ordered &&
            (!block->second.requiresReexecution || !block->second.canonicalTransactions.empty())) return;
        ConsensusMessage request;
        request.type = m_compactOrderNotifications && (block == m_blocks.end() || !block->second.ordered)
            ? ConsensusMessageType::BIDL_BLOCK_SUMMARY_REQUEST : ConsensusMessageType::BIDL_BLOCK_REQUEST;
        request.instanceId = blockId;
        Broadcast(m_ordererIds, request);
        m_blockFetchEvents[blockId] = Simulator::Schedule(
            retryInterval, &BidlApplication::RequestCanonicalBlock, this, blockId);
    }

    void BidlApplication::OnIorsGateTimeout(uint64_t sequence) {
        auto pending = m_iorsPendingMarked.find(sequence);
        if (pending == m_iorsPendingMarked.end()) return;
        ConsensusMessage candidate = pending->second;
        m_iorsPendingMarked.erase(pending);
        m_iorsGateEvents.erase(sequence);
        if (m_running && m_transactions.count(sequence) == 0) {
            m_candidateTrace(candidate.senderId, m_nodeId, static_cast<uint16_t>(candidate.type), candidate.view,
                             candidate.instanceId, candidate.digest, candidate.clientId, candidate.requestId,
                             BIDL_CANDIDATE_MARKED_TIMEOUT, 1);
            VerifyTransaction(candidate);
        }
    }

    void BidlApplication::HandleMaliciousMessage(const ConsensusMessage& message) {
        if (message.type != ConsensusMessageType::BIDL_TRANSACTION ||
            std::find(m_sequencerIds.begin(), m_sequencerIds.end(), message.senderId) == m_sequencerIds.end()) {
            return;
        }
        Time now = Simulator::Now();
        if (now < m_attackStart || (!m_attackStop.IsZero() && now >= m_attackStop) ||
            m_attackRandom->GetValue() > m_attackProbability) {
            return;
        }
        m_resourceModel->SubmitCpuTask(m_attackProcessingTime,
                                       MakeCallback(&BidlApplication::ForgeTransaction, this).Bind(message));
    }

    void BidlApplication::ForgeTransaction(ConsensusMessage message) {
        if (!m_running) {
            return;
        }
        ClientRequest forgedRequest;
        std::vector<BidlTransactionRecord> records;
        if (!ReadRequest(message, forgedRequest) || !DecodeTransactions(message.payloadBytes, records)) return;
        if (m_attackMode == 1) {
            ++message.instanceId;
            records.front().sequence = message.instanceId;
        } else {
        message.digest ^= 0xd6e8feb86659fd93ULL;
        message.clientId = m_nodeId; // The attacker uses its own colluding client identity.
        if (message.digest == 0 || message.digest == INVALID_TRANSACTION_DIGEST) {
            message.digest ^= 0x94d049bb133111ebULL;
        }
        forgedRequest.digest = message.digest;
        forgedRequest.clientId = message.clientId;
        SignRequest(forgedRequest);
        records.front().request = forgedRequest;
        }
        message.payloadBytes = EncodeTransactions(records);
        uint32_t targets = m_attackTargetExecutors == 0
                               ? static_cast<uint32_t>(m_executorIds.size())
                               : std::min<uint32_t>(m_attackTargetExecutors, m_executorIds.size());
        uint64_t bytes = 0;
        for (uint32_t index = 0; index < targets; ++index) {
            ConsensusMessage forged = message;
            forged.senderId = 0;
            forged.receiverId = 0;
            forged.messageId = 0;
            bytes += forged.ToPacket()->GetSize();
            Send(m_executorIds[index], forged);
        }
        m_forgedTransactionTrace(m_nodeId, message.instanceId, targets, bytes);
    }

    void BidlApplication::TraceRejectedConflict(const ConsensusMessage& message) {
        if (m_role == BIDL_EXECUTOR) {
            m_conflictRejectedTrace(m_nodeId, message.instanceId, message.senderId);
            if (m_denylistEnabled) {
                uint32_t client = message.clientId;
                auto source = m_transactionSource.find(message.instanceId);
                if (source != m_transactionSource.end() &&
                    std::find(m_broadcasterIds.begin(), m_broadcasterIds.end(), source->second) != m_broadcasterIds.end()) {
                    client = m_transactions.at(message.instanceId).clientId;
                }
                ConsensusMessage report;
                report.type = ConsensusMessageType::BIDL_CONFLICT_REPORT;
                report.view = m_observedOrderView;
                report.instanceId = message.instanceId;
                report.clientId = client;
                if (m_attackMode == 1) {
                    // Valid client authentication does not authenticate the altered ordering fields.
                    report.clientId = 0;
                    report.requestId = source != m_transactionSource.end() &&
                        std::find(m_broadcasterIds.begin(), m_broadcasterIds.end(), source->second) != m_broadcasterIds.end()
                            ? source->second : message.senderId;
                }
                Broadcast(m_ordererIds, report);
            }
        }
    }

    void BidlApplication::HandleConflictReport(const ConsensusMessage& message) {
        if (!m_denylistEnabled ||
            std::find(m_executorIds.begin(), m_executorIds.end(), message.senderId) == m_executorIds.end()) return;
        if (message.clientId == 0) {
            auto& reporters = m_metadataConflictReporters[{static_cast<uint32_t>(message.requestId), message.view}];
            reporters.insert(message.senderId);
            Ptr<PbftEngine> pbft = DynamicCast<PbftEngine>(m_engine);
            if (reporters.size() >= m_faultTolerance + 1 && pbft && pbft->GetCurrentView() == message.view)
                pbft->RequestViewChange();
            return;
        }
        auto& reporters = m_conflictReporters[{message.clientId, message.view}];
        reporters.insert(message.senderId);
        if (reporters.size() < m_faultTolerance + 1 || m_ordererIds.empty()) return;
        Ptr<PbftEngine> pbft = DynamicCast<PbftEngine>(m_engine);
        if (pbft && pbft->GetCurrentView() == message.view) pbft->RequestViewChange();
        uint32_t leader = m_ordererIds[message.view % m_ordererIds.size()];
        auto& leaders = m_suspectLeaders[message.clientId];
        leaders.insert(leader);
        if (leaders.size() < m_faultTolerance + 1 || m_denylistVotes[message.clientId].count(m_nodeId)) return;
        ConsensusMessage vote;
        vote.type = ConsensusMessageType::BIDL_DENYLIST_VOTE;
        vote.clientId = message.clientId;
        vote.view = message.view;
        Broadcast(m_ordererIds, vote);
        Broadcast(m_executorIds, vote);
        m_denylistVotes[message.clientId].insert(m_nodeId);
    }

    void BidlApplication::HandleDenylistVote(const ConsensusMessage& message) {
        if (!m_denylistEnabled ||
            std::find(m_ordererIds.begin(), m_ordererIds.end(), message.senderId) == m_ordererIds.end()) return;
        auto& voters = m_denylistVotes[message.clientId];
        voters.insert(message.senderId);
        if (voters.size() >= m_faultTolerance + 1) m_denylist.insert(message.clientId);
    }

    void BidlApplication::TryExecuteNextTransaction() {
        if (!m_running || m_executionBusy) {
            return;
        }
        auto transaction = m_transactions.find(m_nextExecutionSequence);
        while (transaction != m_transactions.end() && m_denylistEnabled &&
               m_denylist.count(transaction->second.clientId)) {
            m_deferredTransactions.insert(m_nextExecutionSequence++);
            ++m_deferredTotal;
            transaction = m_transactions.find(m_nextExecutionSequence);
        }
        if (transaction == m_transactions.end()) {
            return;
        }

        m_executionBusy = true;
        Time service = m_executionTimePerTransaction * std::max<uint32_t>(1, transaction->second.transactionCount);
        TraceTransaction(m_nextExecutionSequence, BIDL_CANDIDATE_EXECUTION_QUEUED);
        m_resourceModel->SubmitCpuTask(service, MakeCallback(&BidlApplication::CompleteTransaction, this)
                                                  .Bind(m_nextExecutionSequence, m_executionGeneration));
    }

    void BidlApplication::CompleteTransaction(uint64_t sequence, uint64_t generation) {
        if (!m_running || sequence != m_nextExecutionSequence || generation != m_executionGeneration) {
            return;
        }
        m_executedTransactions.insert(sequence);
        TraceTransaction(sequence, BIDL_CANDIDATE_SPECULATIVE_EXECUTION);
        m_transactionExecutionTimes[sequence] = Simulator::Now();
        ++m_nextExecutionSequence;
        m_executionBusy = false;

        // Only the block containing this transaction can become fully
        // executable here. Scanning every historical block for every
        // transaction made a long sequential run quadratic in its length.
        auto block = m_blocks.upper_bound(sequence);
        if (block != m_blocks.begin()) {
            --block;
            if (sequence < block->first + block->second.transactionCount) {
                TryPersist(block->second);
            }
        }
        TryExecuteNextTransaction();
    }

    bool BidlApplication::AllTransactionsExecuted(const BidlBlockState& block) const {
        for (uint64_t sequence = block.blockId; sequence < block.blockId + block.transactionCount; ++sequence) {
            if (m_executedTransactions.count(sequence) == 0) {
                return false;
            }
        }
        return true;
    }

    bool BidlApplication::BlockContainsInvalidTransaction(const BidlBlockState& block) const {
        if (block.requiresReexecution) {
            return true;
        }
        for (uint64_t sequence = block.blockId; sequence < block.blockId + block.transactionCount; ++sequence) {
            if (m_invalidTransactions.count(sequence) != 0) {
                return true;
            }
        }
        return false;
    }

    void BidlApplication::CompleteBlockReexecution(uint64_t blockId) {
        auto block = m_blocks.find(blockId);
        if (!m_running || block == m_blocks.end() || block->second.aborted) {
            return;
        }
        if (block->second.requiresReexecution && block->second.canonicalTransactions.empty()) return;
        ++m_executionGeneration;
        m_executionBusy = false;
        for (const auto& record : block->second.canonicalTransactions) {
            auto previous = m_transactions.find(record.sequence);
            if (previous != m_transactions.end()) {
                if (m_executedTransactions.count(record.sequence) &&
                    (previous->second.digest != record.request.digest || previous->second.clientId != record.request.clientId ||
                     previous->second.requestId != record.request.requestId))
                    TraceTransaction(record.sequence, BIDL_CANDIDATE_OLD_BRANCH_REPLACED);
                auto oldDigest = m_digestSequences.find(previous->second.digest);
                if (oldDigest != m_digestSequences.end() && oldDigest->second == record.sequence)
                    m_digestSequences.erase(oldDigest);
            }
            m_transactions[record.sequence] = record.request;
            m_transactionEpoch[record.sequence] = block->second.orderView;
            m_transactionSource[record.sequence] = m_ordererIds[block->second.orderView % m_ordererIds.size()];
            m_digestSequences[record.request.digest] = record.sequence;
            m_invalidTransactions.erase(record.sequence);
            if (record.ingressTimeNs) m_transactionArrivalTimes[record.sequence] = NanoSeconds(record.ingressTimeNs);
            m_executedTransactions.insert(record.sequence);
            m_transactionExecutionTimes[record.sequence] = Simulator::Now();
            TraceTransaction(record.sequence, BIDL_CANDIDATE_CORRECTION_COMPLETE);
        }
        if (ComputeBlockDigest(blockId, block->second.transactionCount) != block->second.digest) return;
        block->second.reexecutionComplete = true;
        for (uint64_t sequence = blockId; sequence < blockId + block->second.transactionCount; ++sequence) {
            if (m_deferredTransactions.erase(sequence) != 0) {
                m_executedTransactions.insert(sequence);
                m_transactionExecutionTimes[sequence] = Simulator::Now();
            }
        }
        // Deferred first execution uses the same queued CPU callback, but is not
        // a correction/re-execution event and must not inflate attack metrics.
        if (block->second.requiresReexecution) {
            m_blockReexecutionTrace(m_nodeId, blockId, block->second.transactionCount,
                                    Simulator::Now() - block->second.reexecutionStartedAt);
        }
        TryPersist(block->second);
        TryProcessPendingExecutorMessages(blockId);
        while (m_executedTransactions.count(m_nextExecutionSequence)) ++m_nextExecutionSequence;
        TryExecuteNextTransaction();
    }

    void BidlApplication::CompleteApproval(uint64_t blockId) {
        auto block = m_blocks.find(blockId);
        if (!m_running || block == m_blocks.end() || block->second.aborted) {
            return;
        }
        block->second.approvalComplete = true;
        m_phaseChangedTrace(m_nodeId, blockId, BIDL_APPROVAL_COMPLETE, Simulator::Now() - block->second.firstSeen);
        SubmitExecutionResult(block->second);
    }

    void BidlApplication::SubmitExecutionResult(BidlBlockState& block) {
        if (block.resultSubmitted || m_role != BIDL_EXECUTOR) return;
        uint64_t resultDigest = 0x6a09e667f3bcc909ULL;
        for (uint64_t sequence = block.blockId; sequence < block.blockId + block.transactionCount; ++sequence) {
            auto transaction = m_transactions.find(sequence);
            if (transaction == m_transactions.end()) return;
            resultDigest = BidlMix(resultDigest, transaction->second.digest);
        }
        block.resultSubmitted = true;
        ConsensusMessage result;
        result.type = ConsensusMessageType::BIDL_EXECUTION_RESULT;
        result.instanceId = block.blockId;
        result.digest = block.digest;
        result.transactionCount = block.transactionCount;
        BidlWrite64(result.payloadBytes, resultDigest);
        Broadcast(m_ordererIds, result);
    }

    void BidlApplication::HandleExecutionResult(const ConsensusMessage& message) {
        if (m_role != BIDL_ORDERER ||
            std::find(m_executorIds.begin(), m_executorIds.end(), message.senderId) == m_executorIds.end() ||
            !ValidateBlock(message.instanceId, message.transactionCount, message.digest)) return;
        size_t offset = 0;
        uint64_t resultDigest = 0;
        if (!BidlRead64(message.payloadBytes, offset, resultDigest) || offset != message.payloadBytes.size()) return;

        auto& reports = m_ordererExecutionResults[message.instanceId];
        reports.emplace(message.senderId, resultDigest);
        if (reports.size() != m_executorIds.size() || m_ordererPersistedResultDigests.count(message.instanceId)) return;

        std::vector<uint8_t> resultVector;
        uint64_t vectorDigest = 0x510e527fade682d1ULL;
        BidlWrite32(resultVector, static_cast<uint32_t>(m_executorIds.size()));
        for (uint32_t executor : m_executorIds) {
            auto report = reports.find(executor);
            if (report == reports.end()) return;
            BidlWrite32(resultVector, executor);
            BidlWrite64(resultVector, report->second);
            vectorDigest = BidlMix(vectorDigest, executor);
            vectorDigest = BidlMix(vectorDigest, report->second);
        }
        m_ordererPersistedResultDigests[message.instanceId] = vectorDigest;
        m_resourceModel->SubmitCpuTask(
            m_approvalTimePerBlock,
            MakeCallback(&BidlApplication::CompleteOrdererPersist, this)
                .Bind(message.instanceId, message.transactionCount, message.digest, vectorDigest, resultVector));
    }

    void BidlApplication::CompleteOrdererPersist(uint64_t blockId, uint32_t transactionCount, uint64_t blockDigest, uint64_t resultDigest,
                                                  const std::vector<uint8_t>& resultVector) {
        if (!m_running || m_role != BIDL_ORDERER || !m_ordererPersistedResultDigests.count(blockId) ||
            !ValidateBlock(blockId, transactionCount, blockDigest)) return;
        ConsensusMessage persist;
        persist.type = ConsensusMessageType::BIDL_RESULT_PERSIST;
        persist.instanceId = blockId;
        persist.transactionCount = transactionCount;
        persist.digest = blockDigest;
        persist.requestId = resultDigest;
        persist.payloadBytes = resultVector;
        Broadcast(m_executorIds, persist);
    }

    void BidlApplication::HandleResultPersist(const ConsensusMessage& message) {
        if (m_role != BIDL_EXECUTOR ||
            std::find(m_ordererIds.begin(), m_ordererIds.end(), message.senderId) == m_ordererIds.end()) return;
        auto blockIt = m_blocks.find(message.instanceId);
        if (blockIt == m_blocks.end()) {
            m_pendingExecutorMessages.emplace(message.instanceId, message);
            return;
        }
        if (blockIt->second.digest != message.digest || blockIt->second.transactionCount == 0) return;
        const uint64_t localDigest = ComputeBlockDigest(message.instanceId, blockIt->second.transactionCount);
        if (localDigest == 0 ||
            (localDigest != message.digest && !blockIt->second.reexecutionComplete)) {
            m_pendingExecutorMessages.emplace(message.instanceId, message);
            return;
        }
        if (localDigest != message.digest && blockIt->second.digest != message.digest) return;
        size_t offset = 0;
        uint32_t count = 0;
        if (!BidlRead32(message.payloadBytes, offset, count) || count != m_executorIds.size()) return;
        std::map<uint32_t, uint64_t> results;
        uint64_t vectorDigest = 0x510e527fade682d1ULL;
        for (uint32_t i = 0; i < count; ++i) {
            uint32_t executor = 0;
            uint64_t result = 0;
            if (!BidlRead32(message.payloadBytes, offset, executor) || !BidlRead64(message.payloadBytes, offset, result) ||
                std::find(m_executorIds.begin(), m_executorIds.end(), executor) == m_executorIds.end() ||
                !results.emplace(executor, result).second) return;
            vectorDigest = BidlMix(vectorDigest, executor);
            vectorDigest = BidlMix(vectorDigest, result);
        }
        if (offset != message.payloadBytes.size() || vectorDigest != message.requestId) return;
        BidlBlockState& block = blockIt->second;
        auto senderVote = block.persistVectorBySender.find(message.senderId);
        if (senderVote != block.persistVectorBySender.end() && senderVote->second != vectorDigest) return;
        block.persistVectorBySender[message.senderId] = vectorDigest;
        auto& matchingVoters = block.persistVectorVoters[vectorDigest];
        matchingVoters.insert(message.senderId);
        if (matchingVoters.size() < 2 * m_faultTolerance + 1) return;
        if (block.resultVectorReceived && block.resultVectorDigest != vectorDigest) return;
        block.resultVectorReceived = true;
        block.resultVectorDigest = vectorDigest;
        block.resultVector = message.payloadBytes;
        block.persistSenders = matchingVoters;
        block.approvalComplete = std::all_of(results.begin(), results.end(), [&](const auto& result) {
            return result.second == results.begin()->second;
        });
        if (!block.approvalComplete && !block.aborted) {
            block.aborted = true;
            ConsensusMessage aborted;
            aborted.type = ConsensusMessageType::BIDL_ABORT;
            aborted.instanceId = block.blockId;
            aborted.digest = block.digest;
            aborted.transactionCount = block.transactionCount;
            aborted.view = block.orderView;
            Broadcast(m_sequencerIds, aborted);
            return;
        }
        if (block.approvalComplete && !block.storageScheduled &&
            block.persistSenders.size() >= 2 * m_faultTolerance + 1) {
            block.storageScheduled = true;
            block.persistenceStartedAt = Simulator::Now();
            m_resourceModel->SubmitStorageTask(m_storageTime,
                MakeCallback(&BidlApplication::CompleteStorage, this).Bind(block.blockId));
        }
    }

    void BidlApplication::TryPersist(BidlBlockState& block) {
        if (!block.ordered || block.aborted || block.storageScheduled || block.resultSubmitted) {
            return;
        }
        if (m_compactOrderNotifications && !block.requiresReexecution) {
            const uint64_t localDigest = ComputeBlockDigest(block.blockId, block.transactionCount);
            block.requiresReexecution = localDigest != block.digest;
        }
        if (block.requiresReexecution && !block.reexecutionComplete) {
            if (block.canonicalTransactions.empty()) {
                if (!m_blockFetchEvents.count(block.blockId)) RequestCanonicalBlock(block.blockId);
                return;
            }
            if (!block.reexecutionScheduled) {
                block.reexecutionScheduled = true;
                block.reexecutionStartedAt = Simulator::Now();
                for (const auto& record : block.canonicalTransactions) TraceTransaction(record.sequence, BIDL_CANDIDATE_CORRECTION_QUEUED);
                m_resourceModel->SubmitCpuTask(m_executionTimePerTransaction * block.transactionCount,
                    MakeCallback(&BidlApplication::CompleteBlockReexecution, this).Bind(block.blockId));
            }
            return;
        }
        if (!AllTransactionsExecuted(block)) {
            bool onlyDeferred = true;
            bool hasDeferred = false;
            for (uint64_t sequence = block.blockId; sequence < block.blockId + block.transactionCount; ++sequence) {
                if (m_transactions.count(sequence) == 0 ||
                    (m_executedTransactions.count(sequence) == 0 && m_deferredTransactions.count(sequence) == 0))
                    onlyDeferred = false;
                hasDeferred = hasDeferred || m_deferredTransactions.count(sequence) != 0;
            }
            if (onlyDeferred && hasDeferred && !block.reexecutionScheduled) {
                block.reexecutionScheduled = true;
                block.reexecutionStartedAt = Simulator::Now();
                for (const auto& record : block.canonicalTransactions) TraceTransaction(record.sequence, BIDL_CANDIDATE_CORRECTION_QUEUED);
                m_resourceModel->SubmitCpuTask(m_executionTimePerTransaction * block.transactionCount,
                    MakeCallback(&BidlApplication::CompleteBlockReexecution, this).Bind(block.blockId));
            }
            return;
        }
        if (!block.executionComplete) {
            block.executionComplete = true;
            block.executionCompletedAt = block.executionStartedAt;
            for (uint64_t sequence = block.blockId; sequence < block.blockId + block.transactionCount; ++sequence) {
                block.executionCompletedAt = std::max(block.executionCompletedAt, m_transactionExecutionTimes.at(sequence));
            }
            Time executionLatency = block.executionCompletedAt - block.executionStartedAt;
            m_phaseChangedTrace(m_nodeId, block.blockId, BIDL_EXECUTION_COMPLETE, executionLatency);
            m_phaseChangedTrace(m_nodeId, block.blockId, BIDL_P2_VERIFY_EXECUTE, executionLatency);
        }
        if (BlockContainsInvalidTransaction(block) && !block.reexecutionComplete) {
            if (!block.reexecutionScheduled) {
                block.reexecutionScheduled = true;
                block.reexecutionStartedAt = Simulator::Now();
                Time service = m_executionTimePerTransaction * block.transactionCount;
                for (const auto& record : block.canonicalTransactions) TraceTransaction(record.sequence, BIDL_CANDIDATE_CORRECTION_QUEUED);
                m_resourceModel->SubmitCpuTask(service, MakeCallback(&BidlApplication::CompleteBlockReexecution, this).Bind(block.blockId));
            }
            return;
        }
        if (!block.approvalComplete) {
            if (!block.approvalScheduled) {
                block.approvalScheduled = true;
                block.persistenceStartedAt = Simulator::Now();
                m_resourceModel->SubmitCpuTask(m_approvalTimePerBlock,
                                               MakeCallback(&BidlApplication::CompleteApproval, this).Bind(block.blockId));
            }
            return;
        }
        SubmitExecutionResult(block);
    }

    void BidlApplication::CompleteStorage(uint64_t blockId) {
        auto iterator = m_blocks.find(blockId);
        if (iterator == m_blocks.end() || iterator->second.persistSent || iterator->second.aborted) {
            return;
        }
        BidlBlockState& block = iterator->second;
        block.persistSent = true;
        block.storageCompletedAt = Simulator::Now();
        m_phaseChangedTrace(m_nodeId, blockId, BIDL_STORAGE_COMPLETE, Simulator::Now() - block.firstSeen);
        Time persistenceLatency = block.storageCompletedAt - block.persistenceStartedAt;
        Time executionLatency = block.executionCompletedAt - block.executionStartedAt;
        m_phaseChangedTrace(m_nodeId, blockId, BIDL_P3_PERSIST, persistenceLatency);
        m_phaseChangedTrace(m_nodeId, blockId, BIDL_P4_EXECUTION_TOTAL, executionLatency + persistenceLatency);

        TryCommit(block);
    }

    void BidlApplication::TryCommit(BidlBlockState& block) {
        if (block.committed || block.aborted || block.commitScheduled || !block.persistSent || !block.approvalComplete ||
            block.persistSenders.size() < 2 * m_faultTolerance + 1) {
            return;
        }
        block.commitScheduled = true;
        m_resourceModel->SubmitCpuTask(m_commitProcessingTime,
                                       MakeCallback(&BidlApplication::CompleteCommit, this).Bind(block.blockId));
    }

    void BidlApplication::CompleteCommit(uint64_t blockId) {
        auto iterator = m_blocks.find(blockId);
        if (iterator == m_blocks.end() || iterator->second.committed) {
            return;
        }
        BidlBlockState& block = iterator->second;
        block.committed = true;
        m_committedBlockIds.insert(block.blockId);
        ++m_committedBlocks;
        m_committedTransactions += block.transactionCount;
        m_lastCommit = Simulator::Now();
        m_blockCommittedTrace(m_nodeId, block.blockId, block.transactionCount, m_lastCommit - block.firstSeen);
        m_blockCommitTimeTrace(m_nodeId, block.blockId, block.transactionCount, m_lastCommit, m_lastCommit - block.firstSeen);
        for (uint64_t sequence = block.blockId; sequence < block.blockId + block.transactionCount; ++sequence) {
            auto arrival = m_transactionArrivalTimes.find(sequence);
            Time start = arrival == m_transactionArrivalTimes.end() ? block.firstSeen : arrival->second;
            const ClientRequest* identity = nullptr;
            for (const auto& record : block.canonicalTransactions)
                if (record.sequence == sequence) { identity = &record.request; break; }
            if (!identity) {
                auto local = m_transactions.find(sequence);
                if (local != m_transactions.end()) identity = &local->second;
            }
            if (identity) m_transactionFinalizedTrace(m_nodeId, sequence, block.blockId,
                identity->clientId, identity->requestId, identity->transactionCount);
            m_transactionCommittedTrace(m_nodeId, sequence, m_lastCommit - start);
            TraceTransaction(sequence, BIDL_CANDIDATE_NORMAL_COMMIT);
        }
        m_phaseChangedTrace(m_nodeId, block.blockId, BIDL_BLOCK_COMMITTED, m_lastCommit - block.firstSeen);
        m_phaseChangedTrace(m_nodeId, block.blockId, BIDL_P5_COMMIT, m_lastCommit - block.storageCompletedAt);
        m_phaseChangedTrace(m_nodeId, block.blockId, BIDL_END_TO_END, m_lastCommit - block.firstSeen);

        ConsensusMessage committed;
        committed.protocol = ConsensusProtocol::GENERIC;
        committed.type = ConsensusMessageType::BIDL_COMMIT;
        committed.instanceId = block.blockId;
        committed.digest = block.digest;
        committed.transactionCount = block.transactionCount;
        Broadcast(m_sequencerIds, committed);
    }

    void BidlApplication::Send(uint32_t peerId, ConsensusMessage message) {
        if (!m_running || peerId == m_nodeId) {
            return;
        }
        message.senderId = m_nodeId;
        message.receiverId = peerId;
        if (message.messageId == 0) {
            message.messageId = m_nextMessageId++;
        }
        Ptr<Packet> packet = message.ToPacket();
        m_messageTxTrace(m_nodeId, peerId, static_cast<uint16_t>(message.type), message.instanceId, packet->GetSize());
        m_messageEventTrace(true, message);
        if (message.type == ConsensusMessageType::BIDL_TRANSACTION || message.type == ConsensusMessageType::BIDL_TRANSACTION_REPAIR)
            m_candidateTrace(m_nodeId, peerId, static_cast<uint16_t>(message.type), message.view,
                             message.instanceId, message.digest, message.clientId, message.requestId, 0, message.iorsMarked ? 1 : 0);
        m_transport->Send(peerId, packet, ConsensusTransportMetadata::FromMessage(message));
    }

    void BidlApplication::TraceTransaction(uint64_t sequence, uint8_t phase) {
        auto transaction = m_transactions.find(sequence);
        if (transaction == m_transactions.end()) return;
        const ClientRequest& request = transaction->second;
        m_candidateTrace(m_transactionSource[sequence], m_nodeId, static_cast<uint16_t>(ConsensusMessageType::BIDL_TRANSACTION),
                         m_transactionEpoch[sequence], sequence, request.digest, request.clientId, request.requestId, phase, 0);
    }

    void BidlApplication::Broadcast(const std::vector<uint32_t>& peers, ConsensusMessage message) {
        for (uint32_t peerId : peers) {
            Send(peerId, message);
        }
    }

} // namespace ns3
