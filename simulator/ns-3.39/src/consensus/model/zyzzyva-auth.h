#ifndef ZYZZYVA_AUTH_H
#define ZYZZYVA_AUTH_H

#include "consensus-message.h"

namespace ns3 {

    /** Deterministic simulated authentication bound to replica identity and message bytes. */
    class ZyzzyvaAuth {
      public:
        static constexpr size_t SIGNATURE_SIZE = 64;
        static bool Sign(ConsensusMessage& message);
        static bool Verify(ConsensusMessage& message);
        static bool VerifyContent(const ConsensusMessage& message, const std::vector<uint8_t>& signature);
        static bool SignDetached(uint32_t sender, const std::vector<uint8_t>& data, std::vector<uint8_t>& signature);
        static bool VerifyDetached(uint32_t sender, const std::vector<uint8_t>& data,
                                   const std::vector<uint8_t>& signature);
        static void SignClientRequest(ClientRequest& request);
        static bool VerifyClientRequest(const ClientRequest& request);
        static std::vector<uint8_t> EncodeClientRequest(const ClientRequest& request);
        static bool DecodeClientRequest(const ConsensusMessage& message, size_t offset, ClientRequest& request);
    };

} // namespace ns3

#endif
