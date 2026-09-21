#include "bidl-application.h"

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

    NS_LOG_COMPONENT_DEFINE("BidlApplication");
    NS_OBJECT_ENSURE_REGISTERED(BidlApplication);

    TypeId BidlApplication::GetTypeId() {
        static TypeId tid =
            TypeId("ns3::BidlApplication")
                .SetParent<Application>()
                .SetGroupName("Consensus")
                .AddConstructor<BidlApplication>()
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
                .AddTraceSource("TransactionCommitted", "A sequence-numbered transaction commits with sequencer-to-commit latency.",
                                MakeTraceSourceAccessor(&BidlApplication::m_transactionCommittedTrace),
                                "ns3::BidlApplication::TransactionCommitTracedCallback")
                .AddTraceSource("BlockCommitTime", "An executor commits a block with its absolute simulation time.",
                                MakeTraceSourceAccessor(&BidlApplication::m_blockCommitTimeTrace),
                                "ns3::BidlApplication::BlockCommitTimeTracedCallback")
                .AddTraceSource("MessageTx", "A BIDL message is passed to the transport.",
                                MakeTraceSourceAccessor(&BidlApplication::m_messageTxTrace), "ns3::BidlApplication::MessageTracedCallback")
                .AddTraceSource("MessageRx", "A complete BIDL message is received.",
                                MakeTraceSourceAccessor(&BidlApplication::m_messageRxTrace), "ns3::BidlApplication::MessageTracedCallback")
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

    uint64_t BidlApplication::GetSequencedTransactions() const {
        return m_role == BIDL_SEQUENCER ? m_transactions.size() : 0;
    }

    uint64_t BidlApplication::GetSequencerCommittedTransactions() const {
        return m_role == BIDL_SEQUENCER ? m_committedTransactionBlocks.size() : 0;
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
            m_engine->Start();
        }
        if (m_role == BIDL_EXECUTOR) {
            m_backlogEvent = Simulator::Schedule(MilliSeconds(10), &BidlApplication::SampleExecutionBacklog, this);
        }
    }

    void BidlApplication::StopApplication() {
        m_running = false;
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
        EnqueueTransaction(request);
    }

    void BidlApplication::EnqueueTransaction(const ClientRequest& transaction) {
        if (!m_running || m_role != BIDL_SEQUENCER) {
            return;
        }
        const auto key = std::make_pair(transaction.clientId, transaction.requestId);
        m_ingressArrivalTimes.emplace(key, Simulator::Now());
        m_resourceModel->SubmitCpuTask(
            m_sequencerServiceTime,
            MakeCallback(&BidlApplication::CompleteSequencerService, this)
                .Bind(transaction.clientId, transaction.requestId, transaction.payloadSize, transaction.transactionCount,
                      transaction.digest));
    }

    void BidlApplication::CompleteSequencerService(uint32_t clientId, uint64_t requestId, uint32_t payloadSize,
                                                   uint32_t transactionCount, uint64_t digest) {
        ClientRequest transaction;
        transaction.clientId = clientId;
        transaction.requestId = requestId;
        transaction.payloadSize = payloadSize;
        transaction.transactionCount = transactionCount;
        transaction.digest = digest;
        SequenceTransaction(transaction);
    }

    void BidlApplication::SequenceTransaction(const ClientRequest& transaction) {
        if (!m_running || m_role != BIDL_SEQUENCER) {
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
            }
            return;
        }

        uint64_t sequence = m_nextTransactionSequence++;
        m_transactions[sequence] = transaction;
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
        message.view = ingress.GetNanoSeconds(); // Carries the internal-latency origin on generic BIDL traffic.
        Broadcast(m_ordererIds, message);
        Broadcast(m_executorIds, message);
        Broadcast(m_broadcasterIds, message);
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
        transaction.clientId = message.clientId;
        transaction.requestId = message.requestId;
        transaction.payloadSize = message.payloadSize;
        transaction.transactionCount = message.transactionCount;
        transaction.digest = message.digest;
        m_transactions[message.instanceId] = transaction;
        if (message.digest == INVALID_TRANSACTION_DIGEST) {
            m_invalidTransactions.insert(message.instanceId);
        } else {
            m_digestSequences[message.digest] = message.instanceId;
        }
        m_transactionArrivalTimes[message.instanceId] = message.view == 0 ? Simulator::Now() : NanoSeconds(message.view);
        return true;
    }

    void BidlApplication::VerifyTransaction(ConsensusMessage message) {
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
        m_transactionReceiveTimes.emplace(message.instanceId, Simulator::Now());
        m_resourceModel->SubmitCpuTask(m_transactionVerificationTime,
                                       MakeCallback(&BidlApplication::CompleteTransactionVerification, this).Bind(message));
    }

    void BidlApplication::CompleteTransactionVerification(ConsensusMessage message) {
        m_verificationPending.erase(message.instanceId);
        m_verificationDigests.erase(message.instanceId);
        if (!StoreTransaction(message)) {
            return;
        }
        if (m_role == BIDL_ORDERER) {
            TryProposeBlocks(false);
            TryProcessPendingProposals();
        } else if (m_role == BIDL_EXECUTOR) {
            TryExecuteNextTransaction();
            TryProcessPendingExecutorMessages();
        }
    }

    void BidlApplication::TryProcessPendingExecutorMessages() {
        std::vector<ConsensusMessage> ready;
        for (auto iterator = m_pendingExecutorMessages.begin(); iterator != m_pendingExecutorMessages.end();) {
            const ConsensusMessage& message = iterator->second;
            uint64_t digest = ComputeBlockDigest(message.instanceId, message.transactionCount);
            if (digest == 0) {
                ++iterator;
            } else if (digest == message.digest || message.type == ConsensusMessageType::BIDL_ORDERED_BLOCK) {
                ready.push_back(message);
                iterator = m_pendingExecutorMessages.erase(iterator);
            } else {
                auto block = m_blocks.find(message.instanceId);
                if (block != m_blocks.end() && block->second.reexecutionComplete && block->second.digest == message.digest) {
                    ready.push_back(message);
                    iterator = m_pendingExecutorMessages.erase(iterator);
                } else {
                    ++iterator;
                }
            }
        }
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
        if (!m_running || m_role != BIDL_ORDERER) {
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

    void BidlApplication::TryProcessPendingProposals() {
        for (auto iterator = m_pendingBlockProposals.begin(); iterator != m_pendingBlockProposals.end();) {
            const ConsensusMessage& proposal = iterator->second;
            uint64_t digest = ComputeBlockDigest(proposal.requestId, proposal.transactionCount);
            if (digest == 0) {
                ++iterator;
            } else if (digest != proposal.digest) {
                iterator = m_pendingBlockProposals.erase(iterator);
            } else {
                m_engine->Receive(proposal);
                iterator = m_pendingBlockProposals.erase(iterator);
            }
        }
    }

    void BidlApplication::OnTransportReceive(uint32_t peerId, Ptr<Packet> packet) {
        ConsensusMessage message;
        if (!m_running || !ConsensusMessage::FromPacket(packet, message) || message.senderId != peerId || message.receiverId != m_nodeId) {
            return;
        }
        m_messageRxTrace(message.senderId, m_nodeId, static_cast<uint16_t>(message.type), message.instanceId, packet->GetSize());
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
        ordered.instanceId = decision.requestId;
        ordered.payloadSize = decision.transactionCount * 32;
        ordered.digest = decision.digest;
        ordered.requestId = decision.requestId;
        ordered.transactionCount = decision.transactionCount;
        Broadcast(m_executorIds, ordered);
    }

    void BidlApplication::HandleSequencerMessage(const ConsensusMessage& message) {
        if (message.type == ConsensusMessageType::CLIENT_REQUEST) {
            ClientRequest request;
            request.clientId = message.clientId;
            request.requestId = message.requestId;
            request.payloadSize = message.payloadSize;
            request.transactionCount = message.transactionCount;
            request.digest = message.digest;
            EnqueueTransaction(request);
            return;
        }

        if (message.type != ConsensusMessageType::BIDL_COMMIT || !ValidateBlock(message.instanceId, message.transactionCount, message.digest) ||
            !m_committedBlockIds.insert(message.instanceId).second) {
            return;
        }

        for (uint64_t sequence = message.instanceId; sequence < message.instanceId + message.transactionCount; ++sequence) {
            m_committedTransactionBlocks[sequence] = message.instanceId;
            ReplyToClient(sequence, message.instanceId);
        }
    }

    void BidlApplication::HandleOrdererMessage(const ConsensusMessage& message) {
        if (message.type == ConsensusMessageType::BIDL_TRANSACTION) {
            if (std::find(m_sequencerIds.begin(), m_sequencerIds.end(), message.senderId) != m_sequencerIds.end()) {
                VerifyTransaction(message);
            }
            return;
        }

        if (message.type == ConsensusMessageType::PRE_PREPARE) {
            uint64_t digest = ComputeBlockDigest(message.requestId, message.transactionCount);
            if (digest == 0) {
                m_pendingBlockProposals[message.requestId] = message;
                return;
            }
            if (!ValidateBlock(message.requestId, message.transactionCount, message.digest)) {
                return;
            }
        }
        m_engine->Receive(message);
    }

    void BidlApplication::HandleExecutorMessage(const ConsensusMessage& message) {
        if (message.type == ConsensusMessageType::BIDL_TRANSACTION) {
            bool fromSequencer = std::find(m_sequencerIds.begin(), m_sequencerIds.end(), message.senderId) != m_sequencerIds.end();
            bool fromBroadcaster =
                std::find(m_broadcasterIds.begin(), m_broadcasterIds.end(), message.senderId) != m_broadcasterIds.end();
            if (fromSequencer || fromBroadcaster) {
                VerifyTransaction(message);
            }
            return;
        }

        if (message.type != ConsensusMessageType::BIDL_ORDERED_BLOCK && message.type != ConsensusMessageType::BIDL_PERSIST) {
            return;
        }
        uint64_t localDigest = ComputeBlockDigest(message.instanceId, message.transactionCount);
        if (localDigest == 0) {
            m_pendingExecutorMessages.emplace(message.instanceId, message);
            return;
        }
        bool digestMatches = localDigest == message.digest;
        if (message.type == ConsensusMessageType::BIDL_PERSIST) {
            auto known = m_blocks.find(message.instanceId);
            if (!digestMatches &&
                (known == m_blocks.end() || !known->second.reexecutionComplete || known->second.digest != message.digest)) {
                m_pendingExecutorMessages.emplace(message.instanceId, message);
                return;
            }
        }

        BidlBlockState& block = m_blocks[message.instanceId];
        if (block.blockId == 0) {
            block.blockId = message.instanceId;
            block.digest = message.digest;
            block.transactionCount = message.transactionCount;
            block.requiresReexecution = !digestMatches;
            auto firstSeen = m_transactionArrivalTimes.find(block.blockId);
            block.firstSeen = firstSeen == m_transactionArrivalTimes.end() ? Simulator::Now() : firstSeen->second;
            auto executionStarted = m_transactionReceiveTimes.find(block.blockId);
            block.executionStartedAt =
                executionStarted == m_transactionReceiveTimes.end() ? block.firstSeen : executionStarted->second;
            if (!m_hasCommitWindow) {
                m_firstCommitWindowStart = block.firstSeen;
                m_hasCommitWindow = true;
            }
        }
        if (block.digest != message.digest || block.transactionCount != message.transactionCount) {
            return;
        }

        if (message.type == ConsensusMessageType::BIDL_ORDERED_BLOCK && !digestMatches) {
            block.requiresReexecution = true;
        }

        if (message.type == ConsensusMessageType::BIDL_ORDERED_BLOCK) {
            bool wasOrdered = block.ordered;
            block.orderedSenders.insert(message.senderId);
            block.ordered = block.orderedSenders.size() >= 2 * m_faultTolerance + 1;
            if (!wasOrdered && block.ordered) {
                m_phaseChangedTrace(m_nodeId, block.blockId, BIDL_ORDERED, Simulator::Now() - block.firstSeen);
            }
            TryPersist(block);
        } else {
            block.persistSenders.insert(message.senderId);
            TryCommit(block);
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
        message.digest ^= 0xd6e8feb86659fd93ULL;
        if (message.digest == 0 || message.digest == INVALID_TRANSACTION_DIGEST) {
            message.digest ^= 0x94d049bb133111ebULL;
        }
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
        }
    }

    void BidlApplication::TryExecuteNextTransaction() {
        if (!m_running || m_executionBusy) {
            return;
        }
        auto transaction = m_transactions.find(m_nextExecutionSequence);
        if (transaction == m_transactions.end()) {
            return;
        }

        m_executionBusy = true;
        Time service = m_executionTimePerTransaction * std::max<uint32_t>(1, transaction->second.transactionCount);
        m_resourceModel->SubmitCpuTask(service, MakeCallback(&BidlApplication::CompleteTransaction, this).Bind(m_nextExecutionSequence));
    }

    void BidlApplication::CompleteTransaction(uint64_t sequence) {
        if (!m_running || sequence != m_nextExecutionSequence) {
            return;
        }
        m_executedTransactions.insert(sequence);
        m_transactionExecutionTimes[sequence] = Simulator::Now();
        ++m_nextExecutionSequence;
        m_executionBusy = false;

        for (auto& [blockId, block] : m_blocks) {
            (void)blockId;
            TryPersist(block);
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
        if (block == m_blocks.end()) {
            return;
        }
        block->second.reexecutionComplete = true;
        m_blockReexecutionTrace(m_nodeId, blockId, block->second.transactionCount,
                                Simulator::Now() - block->second.reexecutionStartedAt);
        TryPersist(block->second);
        TryProcessPendingExecutorMessages();
    }

    void BidlApplication::CompleteApproval(uint64_t blockId) {
        auto block = m_blocks.find(blockId);
        if (block == m_blocks.end()) {
            return;
        }
        block->second.approvalComplete = true;
        m_phaseChangedTrace(m_nodeId, blockId, BIDL_APPROVAL_COMPLETE, Simulator::Now() - block->second.firstSeen);
        TryPersist(block->second);
    }

    void BidlApplication::TryPersist(BidlBlockState& block) {
        if (!block.ordered || block.storageScheduled || !AllTransactionsExecuted(block)) {
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
        block.storageScheduled = true;
        m_resourceModel->SubmitStorageTask(m_storageTime, MakeCallback(&BidlApplication::CompleteStorage, this).Bind(block.blockId));
    }

    void BidlApplication::CompleteStorage(uint64_t blockId) {
        auto iterator = m_blocks.find(blockId);
        if (iterator == m_blocks.end() || iterator->second.persistSent) {
            return;
        }
        BidlBlockState& block = iterator->second;
        block.persistSent = true;
        block.persistSenders.insert(m_nodeId);
        block.storageCompletedAt = Simulator::Now();
        m_phaseChangedTrace(m_nodeId, blockId, BIDL_STORAGE_COMPLETE, Simulator::Now() - block.firstSeen);
        Time persistenceLatency = block.storageCompletedAt - block.persistenceStartedAt;
        Time executionLatency = block.executionCompletedAt - block.executionStartedAt;
        m_phaseChangedTrace(m_nodeId, blockId, BIDL_P3_PERSIST, persistenceLatency);
        m_phaseChangedTrace(m_nodeId, blockId, BIDL_P4_EXECUTION_TOTAL, executionLatency + persistenceLatency);

        ConsensusMessage persist;
        persist.protocol = ConsensusProtocol::GENERIC;
        persist.type = ConsensusMessageType::BIDL_PERSIST;
        persist.instanceId = block.blockId;
        persist.payloadSize = 64;
        persist.digest = block.digest;
        persist.transactionCount = block.transactionCount;
        Broadcast(m_executorIds, persist);
        TryCommit(block);
    }

    void BidlApplication::TryCommit(BidlBlockState& block) {
        if (block.committed || block.commitScheduled || !block.persistSent ||
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
        ++m_committedBlocks;
        m_committedTransactions += block.transactionCount;
        m_lastCommit = Simulator::Now();
        m_blockCommittedTrace(m_nodeId, block.blockId, block.transactionCount, m_lastCommit - block.firstSeen);
        m_blockCommitTimeTrace(m_nodeId, block.blockId, block.transactionCount, m_lastCommit, m_lastCommit - block.firstSeen);
        for (uint64_t sequence = block.blockId; sequence < block.blockId + block.transactionCount; ++sequence) {
            auto arrival = m_transactionArrivalTimes.find(sequence);
            Time start = arrival == m_transactionArrivalTimes.end() ? block.firstSeen : arrival->second;
            m_transactionCommittedTrace(m_nodeId, sequence, m_lastCommit - start);
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
        m_transport->Send(peerId, packet, ConsensusTransportMetadata::FromMessage(message));
    }

    void BidlApplication::Broadcast(const std::vector<uint32_t>& peers, ConsensusMessage message) {
        for (uint32_t peerId : peers) {
            Send(peerId, message);
        }
    }

} // namespace ns3
