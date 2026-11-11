#ifndef SWITCH_NODE_H
#define SWITCH_NODE_H

#include <unordered_map>
#include <map>
#include <tuple>
#include <ns3/node.h>
#include <ns3/traced-callback.h>
#include <ns3/nstime.h>
#include "qbb-net-device.h"
#include "switch-mmu.h"
#include "pint.h"

namespace ns3 {

class Packet;

class SwitchNode : public Node{
	static const uint32_t pCnt = 257;	// Number of ports used
	static const uint32_t qCnt = 8;	// Number of queues/priorities used
	uint32_t m_ecmpSeed;
	std::unordered_map<uint32_t, std::vector<int> > m_rtTable; // map from ip address (u32) to possible ECMP port (index of dev)

	// monitor of PFC
	uint32_t m_bytes[pCnt][pCnt][qCnt]; // m_bytes[inDev][outDev][qidx] is the bytes from inDev enqueued for outDev at qidx
	
	uint64_t m_txBytes[pCnt]; // counter of tx bytes


	uint32_t m_lastPktSize[pCnt];
	uint64_t m_lastPktTs[pCnt]; // ns
	double m_u[pCnt];

protected:
	bool m_ecnEnabled;
	uint32_t m_ccMode;
	uint64_t m_maxRtt;
	TracedCallback<Ptr<const Packet>, uint32_t, uint32_t> m_ecnMarkTrace;
	TracedCallback<uint32_t, uint8_t, uint32_t, uint64_t, uint64_t, uint64_t, uint8_t, Time> m_iorsConflictTrace;

	uint32_t m_ackHighPrio; // set high priority for ACK/NACK

	// vamsi
	bool PowerEnabled;

private:
	using IorsKey = std::tuple<uint8_t, uint64_t, uint64_t, uint32_t>;
	struct IorsEntry { uint64_t value; Time expires; };
	std::map<IorsKey, IorsEntry> m_iorsEntries;
	std::map<uint64_t, std::pair<IorsKey, Time>> m_iorsFingerprints;
	std::map<uint32_t, uint32_t> m_iorsSources;
	std::map<uint32_t, uint32_t> m_iorsIngressPorts;
	uint32_t m_iorsMode{0};
	bool m_iorsDetectOnly{false};
	uint32_t m_iorsCapacity{4096};
	uint32_t m_iorsFingerprintBits{64};
	uint32_t m_iorsHoldCapacity{128};
	uint32_t m_iorsHeld{0};
	uint32_t m_iorsZyzReplicaCount{4};
	uint32_t m_iorsBidlSequencerId{200};
	uint32_t m_iorsBidlOrdererCount{0};
	uint64_t m_iorsObserved{0};
	uint64_t m_iorsMarked{0};
	uint64_t m_iorsWaited{0};
	uint64_t m_iorsAlreadyMarked{0};
	uint64_t m_iorsCapacityBypass{0};
	uint64_t m_iorsCollisionBypass{0};
	uint64_t m_iorsUnauthorized{0};
	uint64_t m_iorsEquivocation{0};
	uint64_t m_iorsHoldBypass{0};
	uint64_t m_iorsExpired{0};
	Time m_iorsTtl{MilliSeconds(10)};
	Time m_iorsFixedWait{MicroSeconds(10)};
	Time m_iorsMaxWait{MicroSeconds(100)};
	bool IorsInspect(Ptr<Packet> packet, const CustomHeader& header, uint32_t ingressPort);
	bool IorsBind(const IorsKey& key, uint64_t value, bool& conflict);
	void IorsForward(Ptr<Packet> packet, CustomHeader header);
	int GetOutDev(Ptr<const Packet>, CustomHeader &ch);
	void SendToDev(Ptr<Packet>p, CustomHeader &ch);
	static uint32_t EcmpHash(const uint8_t* key, size_t len, uint32_t seed);
	void CheckAndSendPfc(uint32_t inDev, uint32_t qIndex);
	void CheckAndSendResume(uint32_t inDev, uint32_t qIndex);
public:
	Ptr<SwitchMmu> m_mmu;

	static TypeId GetTypeId (void);
	SwitchNode();
	void SetEcmpSeed(uint32_t seed);
	void AddTableEntry(Ipv4Address &dstAddr, uint32_t intf_idx);
	void ClearTable();
	void SetIorsSource(uint32_t ipv4Address, uint32_t senderId);
	void SetIorsIngressPort(uint32_t port, uint32_t ipv4Address);
	uint64_t GetIorsObserved() const { return m_iorsObserved; }
	uint64_t GetIorsMarked() const { return m_iorsMarked; }
	uint64_t GetIorsWaited() const { return m_iorsWaited; }
	uint64_t GetIorsAlreadyMarked() const { return m_iorsAlreadyMarked; }
	uint64_t GetIorsCapacityBypass() const { return m_iorsCapacityBypass; }
	uint64_t GetIorsCollisionBypass() const { return m_iorsCollisionBypass; }
	uint64_t GetIorsUnauthorized() const { return m_iorsUnauthorized; }
	uint64_t GetIorsEquivocation() const { return m_iorsEquivocation; }
	uint64_t GetIorsHoldBypass() const { return m_iorsHoldBypass; }
	uint64_t GetIorsExpired() const { return m_iorsExpired; }
	bool SwitchReceiveFromDevice(Ptr<NetDevice> device, Ptr<Packet> packet, CustomHeader &ch);
	void SwitchNotifyDequeue(uint32_t ifIndex, uint32_t qIndex, Ptr<Packet> p);

	// for approximate calc in PINT
	int logres_shift(int b, int l);
	int log2apprx(int x, int b, int m, int l); // given x of at most b bits, use most significant m bits of x, calc the result in l bits
};

} /* namespace ns3 */

#endif /* SWITCH_NODE_H */
