#include "ns3/ipv4.h"
#include "ns3/packet.h"
#include "ns3/ipv4-header.h"
#include "ns3/pause-header.h"
#include "ns3/interface-tag.h"
#include "ns3/boolean.h"
#include "ns3/uinteger.h"
#include "ns3/double.h"
#include "switch-node.h"
#include "qbb-net-device.h"
#include "ppp-header.h"
#include "ns3/int-header.h"
#include "ns3/simulator.h"
#include <cmath>
#include <algorithm>
#include "ns3/tcp-header.h"
#include "ns3/udp-header.h"
#include "ns3/custom-priority-tag.h"
#include "ns3/feedback-tag.h"
#include "ns3/unsched-tag.h"

namespace ns3 {

TypeId SwitchNode::GetTypeId (void)
{
	static TypeId tid = TypeId ("ns3::SwitchNode")
	                    .SetParent<Node> ()
	                    .AddConstructor<SwitchNode> ()
	                    .AddAttribute("EcnEnabled",
	                                  "Enable ECN marking.",
	                                  BooleanValue(false),
	                                  MakeBooleanAccessor(&SwitchNode::m_ecnEnabled),
	                                  MakeBooleanChecker())
	                    .AddAttribute("CcMode",
	                                  "CC mode.",
	                                  UintegerValue(0),
	                                  MakeUintegerAccessor(&SwitchNode::m_ccMode),
	                                  MakeUintegerChecker<uint32_t>())
	                    .AddAttribute("AckHighPrio",
	                                  "Set high priority for ACK/NACK or not",
	                                  UintegerValue(0),
	                                  MakeUintegerAccessor(&SwitchNode::m_ackHighPrio),
	                                  MakeUintegerChecker<uint32_t>())
	                    .AddAttribute("MaxRtt",
	                                  "Max Rtt of the network",
	                                  UintegerValue(9000),
	                                  MakeUintegerAccessor(&SwitchNode::m_maxRtt),
	                                  MakeUintegerChecker<uint32_t>())
	                    .AddAttribute("PowerEnabled",
	                                  "Inserts Rxbytes instead of Txbytes in INT header",
	                                  BooleanValue(false),
	                                  MakeBooleanAccessor(&SwitchNode::PowerEnabled),
	                                  MakeBooleanChecker())
	                    .AddAttribute("IorsMode", "0=off, 1=mark, 2=fixed bounded wait, 3=RTT bounded wait.",
	                                  UintegerValue(0), MakeUintegerAccessor(&SwitchNode::m_iorsMode),
	                                  MakeUintegerChecker<uint32_t>(0, 3))
	                    .AddAttribute("IorsDetectOnly", "Count conflicts without marking or changing packets.",
	                                  BooleanValue(false), MakeBooleanAccessor(&SwitchNode::m_iorsDetectOnly),
	                                  MakeBooleanChecker())
	                    .AddAttribute("IorsCapacity", "Maximum candidate bindings.", UintegerValue(4096),
	                                  MakeUintegerAccessor(&SwitchNode::m_iorsCapacity), MakeUintegerChecker<uint32_t>())
	                    .AddAttribute("IorsFingerprintBits", "Bits retained in the key fingerprint; collisions bypass detection.",
	                                  UintegerValue(64), MakeUintegerAccessor(&SwitchNode::m_iorsFingerprintBits),
	                                  MakeUintegerChecker<uint32_t>(1, 64))
	                    .AddAttribute("IorsHoldCapacity", "Maximum packets awaiting bounded forwarding.", UintegerValue(128),
	                                  MakeUintegerAccessor(&SwitchNode::m_iorsHoldCapacity), MakeUintegerChecker<uint32_t>())
	                    .AddAttribute("IorsTtl", "Binding lifetime.", TimeValue(MilliSeconds(10)),
	                                  MakeTimeAccessor(&SwitchNode::m_iorsTtl), MakeTimeChecker())
	                    .AddAttribute("IorsFixedWait", "Fixed intervention duration.", TimeValue(MicroSeconds(10)),
	                                  MakeTimeAccessor(&SwitchNode::m_iorsFixedWait), MakeTimeChecker())
	                    .AddAttribute("IorsMaxWait", "Maximum intervention duration.", TimeValue(MicroSeconds(100)),
	                                  MakeTimeAccessor(&SwitchNode::m_iorsMaxWait), MakeTimeChecker())
	                    .AddTraceSource("EcnMark",
	                                    "Packet marked CE by the switch.",
	                                    MakeTraceSourceAccessor(&SwitchNode::m_ecnMarkTrace),
	                                    "ns3::Packet::TracedCallback")
	                    .AddTraceSource("IorsConflict",
	                                    "An IORS conflict candidate or unauthorized ordering message was observed.",
	                                    MakeTraceSourceAccessor(&SwitchNode::m_iorsConflictTrace),
	                                    "ns3::SwitchNode::IorsConflictTracedCallback")

	                    ;
	return tid;
}

SwitchNode::SwitchNode() {
	m_ecmpSeed = m_id;
	m_node_type = 1;
	m_mmu = CreateObject<SwitchMmu>();
	for (uint32_t i = 0; i < pCnt; i++)
		for (uint32_t j = 0; j < pCnt; j++)
			for (uint32_t k = 0; k < qCnt; k++)
				m_bytes[i][j][k] = 0;
	for (uint32_t i = 0; i < pCnt; i++)
		m_txBytes[i] = 0;
	for (uint32_t i = 0; i < pCnt; i++)
		m_lastPktSize[i] = m_lastPktTs[i] = 0;
	for (uint32_t i = 0; i < pCnt; i++)
		m_u[i] = 0;
}

int SwitchNode::GetOutDev(Ptr<const Packet> p, CustomHeader &ch) {
	// look up entries
	Ptr<Packet> cp = p->Copy();

	PppHeader ph; cp->RemoveHeader(ph);
	Ipv4Header ih; cp->RemoveHeader(ih);
	auto entry = m_rtTable.find(ih.GetDestination().Get());

	// no matching entry
	if (entry == m_rtTable.end())
		return -1;

	// entry found
	auto &nexthops = entry->second;

	// pick one next hop based on hash
	union {
		uint8_t u8[4 + 4 + 2 + 2];
		uint32_t u32[3];
	} buf;
	buf.u32[0] = ih.GetSource().Get();
	buf.u32[1] = ih.GetDestination().Get();
	if (ih.GetProtocol() == 0x6) {
		TcpHeader th; cp->PeekHeader(th);
		buf.u32[2] = th.GetSourcePort() | ((uint32_t)th.GetDestinationPort() << 16);
	}
	else if (ch.l3Prot == 0x11) {
		buf.u32[2] = ch.udp.sport | ((uint32_t)ch.udp.dport << 16);
	}
	else if (ch.l3Prot == 0xFC || ch.l3Prot == 0xFD)
		buf.u32[2] = ch.ack.sport | ((uint32_t)ch.ack.dport << 16);

	uint32_t idx = EcmpHash(buf.u8, 12, m_ecmpSeed) % nexthops.size();
	// if (nexthops.size()>1){ std::cout << "selected " << idx << std::endl; }
	return nexthops[idx];
}

void SwitchNode::CheckAndSendPfc(uint32_t inDev, uint32_t qIndex) {
	Ptr<QbbNetDevice> device = DynamicCast<QbbNetDevice>(m_devices[inDev]);
	if (m_mmu->CheckShouldPause(inDev, qIndex)) {
		device->SendPfc(qIndex, 0);
		// std::cout << "sending PFC" << std::endl;
		m_mmu->SetPause(inDev, qIndex);
	}
}
void SwitchNode::CheckAndSendResume(uint32_t inDev, uint32_t qIndex) {
	Ptr<QbbNetDevice> device = DynamicCast<QbbNetDevice>(m_devices[inDev]);
	if (m_mmu->CheckShouldResume(inDev, qIndex)) {
		device->SendPfc(qIndex, 1);
		m_mmu->SetResume(inDev, qIndex);
	}
}

void SwitchNode::SendToDev(Ptr<Packet>p, CustomHeader &ch) {
	int idx = GetOutDev(p, ch);
	if (idx >= 0) {
		NS_ASSERT_MSG(m_devices[idx]->IsLinkUp(), "The routing table look up should return link that is up");

		// determine the qIndex
		uint32_t qIndex=0;
		MyPriorityTag priotag;
		// IMPORTANT: MyPriorityTag should only be attached by lossy traffic. This tag indicates the qIndex but also indicates that it is "lossy". Never attach MyPriorityTag on lossless traffic.
		bool found = p->PeekPacketTag(priotag);

		// UnSchedTag is used by ABM. End-hosts explicitly tag packets of the first BDP so that ABM then prioritizes these packets in the buffer allocation.
		uint32_t unsched = 0;
		UnSchedTag tag;
		bool foundunSched = p->PeekPacketTag (tag);
		if (foundunSched) {
			unsched = tag.GetValue();
		}

		if (ch.l3Prot == 0xFF || ch.l3Prot == 0xFE || (m_ackHighPrio && (ch.l3Prot == 0xFD || ch.l3Prot == 0xFC))) { //QCN or PFC or NACK, go highest priority
			qIndex = 0;
		}
		else if (found) {
			qIndex = priotag.GetPriority();
			// std::cout << "using queue " << qIndex << std::endl;
		}
		else {
			qIndex = (ch.l3Prot == 0x06 ? 1 : ch.udp.pg); // For TCP/IP if the stack did not attach MyPriorityTag, put to queue 1.
		}

		// admission control
		InterfaceTag t;
		p->PeekPacketTag(t);
		uint32_t inDev = t.GetPortId();
		if (qIndex != 0) { //not highest priority
			// IMPORTANT: MyPriorityTag should only be attached by lossy traffic. This tag indicates the qIndex but also indicates that it is "lossy". Never attach MyPriorityTag on lossless traffic.
			if (m_mmu->CheckIngressAdmission(inDev, qIndex, p->GetSize(), found,unsched) && m_mmu->CheckEgressAdmission(idx, qIndex, p->GetSize(), found,unsched)) {			// Admission control
				m_mmu->UpdateIngressAdmission(inDev, qIndex, p->GetSize(), found, unsched);
				m_mmu->UpdateEgressAdmission(idx, qIndex, p->GetSize(), found);
			} else {
				return; // Drop
			}
			CheckAndSendPfc(inDev, qIndex);
		}
		m_bytes[inDev][idx][qIndex] += p->GetSize();
		m_devices[idx]->SwitchSend(qIndex, p, ch);
		DynamicCast<QbbNetDevice>(m_devices[idx])->totalBytesRcvd += p->GetSize(); // Attention: this is the egress port's total received packets. Not the ingress port.
	} else
		std::cout << "outdev not found! Dropped. This should not happen. Debugging required!" << std::endl;
	return; // Drop
}

uint32_t SwitchNode::EcmpHash(const uint8_t* key, size_t len, uint32_t seed) {
	uint32_t h = seed;
	if (len > 3) {
		const uint32_t* key_x4 = (const uint32_t*) key;
		size_t i = len >> 2;
		do {
			uint32_t k = *key_x4++;
			k *= 0xcc9e2d51;
			k = (k << 15) | (k >> 17);
			k *= 0x1b873593;
			h ^= k;
			h = (h << 13) | (h >> 19);
			h += (h << 2) + 0xe6546b64;
		} while (--i);
		key = (const uint8_t*) key_x4;
	}
	if (len & 3) {
		size_t i = len & 3;
		uint32_t k = 0;
		key = &key[i - 1];
		do {
			k <<= 8;
			k |= *key--;
		} while (--i);
		k *= 0xcc9e2d51;
		k = (k << 15) | (k >> 17);
		k *= 0x1b873593;
		h ^= k;
	}
	h ^= len;
	h ^= h >> 16;
	h *= 0x85ebca6b;
	h ^= h >> 13;
	h *= 0xc2b2ae35;
	h ^= h >> 16;
	return h;
}

void SwitchNode::SetEcmpSeed(uint32_t seed) {
	m_ecmpSeed = seed;
}

void SwitchNode::AddTableEntry(Ipv4Address &dstAddr, uint32_t intf_idx) {
	uint32_t dip = dstAddr.Get();
	m_rtTable[dip].push_back(intf_idx);
}

void SwitchNode::ClearTable() {
	m_rtTable.clear();
}

// This function can only be called in switch mode
bool SwitchNode::SwitchReceiveFromDevice(Ptr<NetDevice> device, Ptr<Packet> packet, CustomHeader &ch) {
	if (m_iorsMode != 0 && ch.l3Prot == 0x11 && IorsInspect(packet, ch, device->GetIfIndex()))
		return true;
	SendToDev(packet, ch);
	return true;
}

bool SwitchNode::IorsBind(const IorsKey& key, uint64_t value, bool& conflict) {
	auto existing = m_iorsEntries.find(key);
	if (existing != m_iorsEntries.end() && existing->second.expires <= Simulator::Now()) {
		m_iorsEntries.erase(existing);
		++m_iorsExpired;
		existing = m_iorsEntries.end();
	}
	if (existing != m_iorsEntries.end()) {
		conflict = existing->second.value != value;
		return true;
	}
	uint64_t fingerprint = 1469598103934665603ULL;
	auto mix = [&fingerprint](uint64_t word) {
		fingerprint ^= word;
		fingerprint *= 1099511628211ULL;
	};
	mix(std::get<0>(key)); mix(std::get<1>(key)); mix(std::get<2>(key)); mix(std::get<3>(key));
	if (m_iorsFingerprintBits < 64) fingerprint &= (1ULL << m_iorsFingerprintBits) - 1;
	auto owner = m_iorsFingerprints.find(fingerprint);
	if (owner != m_iorsFingerprints.end() && owner->second.second <= Simulator::Now()) {
		m_iorsFingerprints.erase(owner);
		owner = m_iorsFingerprints.end();
	}
	if (owner != m_iorsFingerprints.end() && owner->second.first != key) {
		++m_iorsCollisionBypass;
		return false;
	}
	if (m_iorsEntries.size() >= m_iorsCapacity) {
		for (auto it = m_iorsEntries.begin(); it != m_iorsEntries.end();) {
			if (it->second.expires <= Simulator::Now()) {
				it = m_iorsEntries.erase(it);
				++m_iorsExpired;
			}
			else ++it;
		}
	}
	if (m_iorsFingerprints.size() >= m_iorsCapacity) {
		for (auto it = m_iorsFingerprints.begin(); it != m_iorsFingerprints.end();) {
			if (it->second.second <= Simulator::Now()) it = m_iorsFingerprints.erase(it);
			else ++it;
		}
	}
	if (m_iorsEntries.size() >= m_iorsCapacity) {
		++m_iorsCapacityBypass;
		return false;
	}
	m_iorsEntries.emplace(key, IorsEntry{value, Simulator::Now() + m_iorsTtl});
	m_iorsFingerprints[fingerprint] = {key, Simulator::Now() + m_iorsTtl};
	conflict = false;
	return true;
}

void SwitchNode::IorsForward(Ptr<Packet> packet, CustomHeader header) {
	if (m_iorsHeld > 0) --m_iorsHeld;
	SendToDev(packet, header);
}

bool SwitchNode::IorsInspect(Ptr<Packet> packet, const CustomHeader& header, uint32_t) {
	const uint32_t length = packet->GetSize();
	if (length < 74) return false;
	uint8_t* bytes = packet->GetBuffer();
	auto read16 = [bytes](uint32_t pos) { return (uint32_t(bytes[pos]) << 8) | bytes[pos + 1]; };
	auto read32 = [bytes](uint32_t pos) { return (uint32_t(bytes[pos]) << 24) | (uint32_t(bytes[pos + 1]) << 16) |
		(uint32_t(bytes[pos + 2]) << 8) | bytes[pos + 3]; };
	auto read64 = [&read32](uint32_t pos) { return (uint64_t(read32(pos)) << 32) | read32(pos + 4); };
	const uint32_t searchEnd = std::min<uint32_t>(128, length - 74);
	for (uint32_t base = 0; base <= searchEnd; ++base) {
		if (read16(base) != 0xc05e || bytes[base + 2] != 1) continue;
		uint16_t kind = read16(base + 4);
		if (kind != 30 && kind != 65 && kind != 40 && kind != 50) continue;
		if (((kind == 30 || kind == 65) && bytes[base + 3] != 0) || (kind == 40 && bytes[base + 3] != 3) ||
			(kind == 50 && bytes[base + 3] != 4)) continue;
		if (read32(base + 38) + 66ULL != length - base) continue;
		uint32_t ext = base + 66;
		if (bytes[ext] != 'I' || bytes[ext + 1] != 'O' || bytes[ext + 2] != 'R' || bytes[ext + 3] != 'S') continue;
		++m_iorsObserved;
		if (bytes[ext + 4] & 1) {
			++m_iorsAlreadyMarked;
			return false; // Already marked: no lookup and no state insertion.
		}
		uint32_t sender = read32(base + 6);
		uint64_t view = read64(base + 14);
		uint64_t slot = read64(base + 22);
		uint64_t digest = read64(base + 42);
		uint32_t clientId = read32(base + 50);
		uint64_t requestId = read64(base + 54);
		bool conflict = false;
		bool reverseConflict = false;
		if (kind == 30 || kind == 65) {
			if (!IorsBind(IorsKey{0, view, slot, 0}, digest, conflict)) return false;
			if (!IorsBind(IorsKey{4, view, digest, 0}, slot, reverseConflict)) return false;
			bool clientSequenceConflict = false;
			IorsBind(IorsKey{1, view, requestId, clientId}, slot, clientSequenceConflict);
		} else {
			const uint8_t keySpace = kind == 40 ? 2 : 3;
			if (!IorsBind(IorsKey{keySpace, view, slot, 0}, digest, conflict)) return false;
			if (!IorsBind(IorsKey{static_cast<uint8_t>(keySpace + 3), view, digest, 0}, slot, reverseConflict)) return false;
		}
		conflict = conflict || reverseConflict;
		if (!conflict) return false;
		if (m_iorsDetectOnly) {
			++m_iorsEquivocation;
			m_iorsConflictTrace(GetId(), bytes[base + 3], sender, view, slot, digest, 2, Simulator::Now());
			return false;
		}
		bytes[ext + 4] |= 1;
		++m_iorsMarked;
		++m_iorsEquivocation;
		m_iorsConflictTrace(GetId(), bytes[base + 3], sender, view, slot, digest, 2, Simulator::Now());
		bytes[ext + 5] = 2;
		if ((kind == 30 || kind == 65) && m_iorsMode >= 2 && !bytes[ext + 6] &&
			m_iorsHeld < m_iorsHoldCapacity) {
			Time delay = m_iorsMode == 2 ? m_iorsFixedWait : NanoSeconds(m_maxRtt / 2);
			delay = std::min(delay, m_iorsMaxWait);
			if (delay.IsPositive()) {
				bytes[ext + 6] = 1;
				++m_iorsHeld;
				++m_iorsWaited;
				Simulator::Schedule(delay, &SwitchNode::IorsForward, this, packet, header);
				return true;
			}
		}
	if ((kind == 30 || kind == 65) && m_iorsMode >= 2 && m_iorsHeld >= m_iorsHoldCapacity)
		++m_iorsHoldBypass;
		return false;
	}
	return false;
}

void SwitchNode::SwitchNotifyDequeue(uint32_t ifIndex, uint32_t qIndex, Ptr<Packet> p) {
	InterfaceTag t;
	p->PeekPacketTag(t);

	MyPriorityTag priotag;
	bool found = p->PeekPacketTag(priotag);

	if (qIndex != 0) {
		uint32_t inDev = t.GetPortId();
		m_mmu->RemoveFromIngressAdmission(inDev, qIndex, p->GetSize(), found);
		m_mmu->RemoveFromEgressAdmission(ifIndex, qIndex, p->GetSize(), found);
		m_bytes[inDev][ifIndex][qIndex] -= p->GetSize();
		if (m_ecnEnabled) {
			bool egressCongested = m_mmu->ShouldSendCN(ifIndex, qIndex);
			if (egressCongested) {
				PppHeader ppp;
				Ipv4Header h;
				p->RemoveHeader(ppp);
				p->RemoveHeader(h);
				h.SetEcn((Ipv4Header::EcnType)0x03);
				p->AddHeader(h);
				p->AddHeader(ppp);
				m_ecnMarkTrace(p, ifIndex, qIndex);
			}
		}
		//CheckAndSendPfc(inDev, qIndex);
		CheckAndSendResume(inDev, qIndex);
	}
	if (1) {
		uint8_t* buf = p->GetBuffer();
		if (buf[PppHeader::GetStaticSize() + 9] == 0x11) { // udp packet
			IntHeader *ih = (IntHeader*)&buf[PppHeader::GetStaticSize() + 20 + 8 + 6]; // ppp, ip, udp, SeqTs, INT
			Ptr<QbbNetDevice> dev = DynamicCast<QbbNetDevice>(m_devices[ifIndex]);
			if (m_ccMode == 3) { // HPCC or PowerTCP-INT
				if (!PowerEnabled)
					ih->PushHop(Simulator::Now().GetTimeStep(), m_txBytes[ifIndex], dev->GetQueue()->GetNBytesTotal(), dev->GetDataRate().GetBitRate());
				else
					ih->PushHop(Simulator::Now().GetTimeStep(), dev->GetQueue()->GetNBytesRxTotal(), dev->GetQueue()->GetNBytesTotal(), dev->GetDataRate().GetBitRate());
				// ih->PushHop(Simulator::Now().GetTimeStep(), m_txBytes[ifIndex], dev->GetQueue()->GetNBytesTotal(), dev->GetDataRate().GetBitRate());
			} else if (m_ccMode == 10) { // HPCC-PINT
				uint64_t t = Simulator::Now().GetTimeStep();
				uint64_t dt = t - m_lastPktTs[ifIndex];
				if (dt > m_maxRtt)
					dt = m_maxRtt;
				uint64_t B = dev->GetDataRate().GetBitRate() / 8; //Bps
				uint64_t qlen = dev->GetQueue()->GetNBytesTotal();
				double newU;

				/**************************
				 * approximate calc
				 *************************/
				int b = 20, m = 16, l = 20; // see log2apprx's paremeters
				int sft = logres_shift(b, l);
				double fct = 1 << sft; // (multiplication factor corresponding to sft)
				double log_T = log2(m_maxRtt) * fct; // log2(T)*fct
				double log_B = log2(B) * fct; // log2(B)*fct
				double log_1e9 = log2(1e9) * fct; // log2(1e9)*fct
				double qterm = 0;
				double byteTerm = 0;
				double uTerm = 0;
				if ((qlen >> 8) > 0) {
					int log_dt = log2apprx(dt, b, m, l); // ~log2(dt)*fct
					int log_qlen = log2apprx(qlen >> 8, b, m, l); // ~log2(qlen / 256)*fct
					qterm = pow(2, (
					                log_dt + log_qlen + log_1e9 - log_B - 2 * log_T
					            ) / fct
					           ) * 256;
					// 2^((log2(dt)*fct+log2(qlen/256)*fct+log2(1e9)*fct-log2(B)*fct-2*log2(T)*fct)/fct)*256 ~= dt*qlen*1e9/(B*T^2)
				}
				if (m_lastPktSize[ifIndex] > 0) {
					int byte = m_lastPktSize[ifIndex];
					int log_byte = log2apprx(byte, b, m, l);
					byteTerm = pow(2, (
					                   log_byte + log_1e9 - log_B - log_T
					               ) / fct
					              );
					// 2^((log2(byte)*fct+log2(1e9)*fct-log2(B)*fct-log2(T)*fct)/fct) ~= byte*1e9 / (B*T)
				}
				if (m_maxRtt > dt && m_u[ifIndex] > 0) {
					int log_T_dt = log2apprx(m_maxRtt - dt, b, m, l); // ~log2(T-dt)*fct
					int log_u = log2apprx(int(round(m_u[ifIndex] * 8192)), b, m, l); // ~log2(u*512)*fct
					uTerm = pow(2, (
					                log_T_dt + log_u - log_T
					            ) / fct
					           ) / 8192;
					// 2^((log2(T-dt)*fct+log2(u*512)*fct-log2(T)*fct)/fct)/512 = (T-dt)*u/T
				}
				newU = qterm + byteTerm + uTerm;

#if 0
				/**************************
				 * accurate calc
				 *************************/
				double weight_ewma = double(dt) / m_maxRtt;
				double u;
				if (m_lastPktSize[ifIndex] == 0)
					u = 0;
				else {
					double txRate = m_lastPktSize[ifIndex] / double(dt); // B/ns
					u = (qlen / m_maxRtt + txRate) * 1e9 / B;
				}
				newU = m_u[ifIndex] * (1 - weight_ewma) + u * weight_ewma;
				printf(" %lf\n", newU);
#endif

				/************************
				 * update PINT header
				 ***********************/
				uint16_t power = Pint::encode_u(newU);
				if (power > ih->GetPower())
					ih->SetPower(power);

				m_u[ifIndex] = newU;
			}
		}
		else {
			FeedbackTag Int;
			bool found;
			found = p->PeekPacketTag(Int);
			if (found) {
				Ptr<QbbNetDevice> dev = DynamicCast<QbbNetDevice>(m_devices[ifIndex]);
				Int.setTelemetryQlenDeq(Int.getHopCount(), dev->GetQueue()->GetNBytesTotal()); // queue length at dequeue
				Int.setTelemetryTsDeq(Int.getHopCount(), Simulator::Now().GetNanoSeconds()); // timestamp at dequeue
				Int.setTelemetryBw(Int.getHopCount(), dev->GetDataRate().GetBitRate());
				Int.setTelemetryTxBytes(Int.getHopCount(), m_txBytes[ifIndex]);
				Int.incrementHopCount(); // Incrementing hop count at Dequeue. Don't do this at enqueue.
				p->ReplacePacketTag(Int); // replacing the tag with new values
				// std::cout << "found " << Int.getHopCount() << std::endl;
			}
		}
	}
	m_txBytes[ifIndex] += p->GetSize();
	m_lastPktSize[ifIndex] = p->GetSize();
	m_lastPktTs[ifIndex] = Simulator::Now().GetTimeStep();
}

int SwitchNode::logres_shift(int b, int l) {
	static int data[] = {0, 0, 1, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5};
	return l - data[b];
}

int SwitchNode::log2apprx(int x, int b, int m, int l) {
	int x0 = x;
	int msb = int(log2(x)) + 1;
	if (msb > m) {
		x = (x >> (msb - m) << (msb - m));
#if 0
		x += + (1 << (msb - m - 1));
#else
		int mask = (1 << (msb - m)) - 1;
		if ((x0 & mask) > (rand() & mask))
			x += 1 << (msb - m);
#endif
	}
	return int(log2(x) * (1 << logres_shift(b, l)));
}

} /* namespace ns3 */
