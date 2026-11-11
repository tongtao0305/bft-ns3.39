#include "zyzzyva-auth.h"

#include <array>
#include <openssl/hmac.h>
#include <openssl/sha.h>

namespace ns3 {

    namespace {
        std::array<uint8_t, 32> MakeKey(uint32_t sender) {
            std::array<uint8_t, 32> seed{};
            const uint8_t label[] = {'n','s','3','-','z','y','z','z','y','v','a'};
            uint8_t material[sizeof(label) + 4];
            for (size_t i = 0; i < sizeof(label); ++i) material[i] = label[i];
            for (int shift = 24, i = 0; shift >= 0; shift -= 8, ++i)
                material[sizeof(label) + i] = static_cast<uint8_t>(sender >> shift);
            SHA256(material, sizeof(material), seed.data());
            return seed;
        }

        void Write32(std::vector<uint8_t>& bytes, uint32_t value) {
            for (int shift = 24; shift >= 0; shift -= 8) bytes.push_back(static_cast<uint8_t>(value >> shift));
        }
        void Write64(std::vector<uint8_t>& bytes, uint64_t value) {
            for (int shift = 56; shift >= 0; shift -= 8) bytes.push_back(static_cast<uint8_t>(value >> shift));
        }
        std::vector<uint8_t> SignedBytes(const ConsensusMessage& message) {
            std::vector<uint8_t> bytes;
            bytes.push_back(static_cast<uint8_t>(message.protocol));
            Write32(bytes, static_cast<uint16_t>(message.type));
            Write32(bytes, message.senderId);
            Write32(bytes, message.receiverId);
            Write64(bytes, message.view);
            Write64(bytes, message.instanceId);
            Write64(bytes, message.digest);
            Write32(bytes, message.clientId);
            Write64(bytes, message.requestId);
            Write32(bytes, message.transactionCount);
            bytes.insert(bytes.end(), message.payloadBytes.begin(), message.payloadBytes.end());
            return bytes;
        }
        std::vector<uint8_t> ClientBytes(const ClientRequest& request) {
            std::vector<uint8_t> bytes;
            Write32(bytes, 0x5a524551);
            Write32(bytes, request.clientId);
            Write64(bytes, request.requestId);
            Write32(bytes, request.payloadSize);
            Write32(bytes, request.transactionCount);
            Write64(bytes, request.digest);
            return bytes;
        }
    }

    bool ZyzzyvaAuth::SignDetached(uint32_t sender, const std::vector<uint8_t>& data,
                                   std::vector<uint8_t>& signature) {
        const auto key = MakeKey(sender);
        signature.resize(SIGNATURE_SIZE);
        unsigned int length = 0;
        return HMAC(EVP_sha512(), key.data(), static_cast<int>(key.size()), data.data(), data.size(),
                    signature.data(), &length) != nullptr && length == SIGNATURE_SIZE;
    }

    bool ZyzzyvaAuth::VerifyDetached(uint32_t sender, const std::vector<uint8_t>& data,
                                     const std::vector<uint8_t>& signature) {
        if (signature.size() != SIGNATURE_SIZE) return false;
        const auto key = MakeKey(sender);
        std::array<uint8_t, SIGNATURE_SIZE> expected{};
        unsigned int length = 0;
        if (!HMAC(EVP_sha512(), key.data(), static_cast<int>(key.size()), data.data(), data.size(),
                  expected.data(), &length) || length != SIGNATURE_SIZE) return false;
        uint8_t difference = 0;
        for (size_t i = 0; i < expected.size(); ++i) difference |= expected[i] ^ signature[i];
        return difference == 0;
    }

    bool ZyzzyvaAuth::Sign(ConsensusMessage& message) {
        message.authentication.clear();
        if (!SignDetached(message.senderId, SignedBytes(message), message.authentication)) return false;
        message.payloadBytes.insert(message.payloadBytes.end(), message.authentication.begin(), message.authentication.end());
        return true;
    }

    bool ZyzzyvaAuth::Verify(ConsensusMessage& message) {
        if (message.payloadBytes.size() < SIGNATURE_SIZE) return false;
        auto begin = message.payloadBytes.end() - SIGNATURE_SIZE;
        message.authentication.assign(begin, message.payloadBytes.end());
        message.payloadBytes.erase(begin, message.payloadBytes.end());
        message.payloadSize = message.payloadBytes.size();
        return VerifyDetached(message.senderId, SignedBytes(message), message.authentication);
    }

    bool ZyzzyvaAuth::VerifyContent(const ConsensusMessage& message, const std::vector<uint8_t>& signature) {
        return VerifyDetached(message.senderId, SignedBytes(message), signature);
    }

    void ZyzzyvaAuth::SignClientRequest(ClientRequest& request) {
        SignDetached(request.clientId, ClientBytes(request), request.authentication);
    }
    bool ZyzzyvaAuth::VerifyClientRequest(const ClientRequest& request) {
        return VerifyDetached(request.clientId, ClientBytes(request), request.authentication);
    }
    std::vector<uint8_t> ZyzzyvaAuth::EncodeClientRequest(const ClientRequest& request) {
        std::vector<uint8_t> bytes;
        Write32(bytes, 0x5a524551);
        Write32(bytes, request.payloadSize);
        bytes.insert(bytes.end(), request.authentication.begin(), request.authentication.end());
        bytes.resize(bytes.size() + request.payloadSize, 0);
        return bytes;
    }
    bool ZyzzyvaAuth::DecodeClientRequest(const ConsensusMessage& message, size_t offset, ClientRequest& request) {
        const auto& bytes = message.payloadBytes;
        if (offset > bytes.size() || bytes.size() - offset < 8 + SIGNATURE_SIZE) return false;
        uint32_t magic = 0, length = 0;
        for (size_t i = 0; i < 4; ++i) magic = (magic << 8) | bytes[offset + i];
        for (size_t i = 4; i < 8; ++i) length = (length << 8) | bytes[offset + i];
        if (magic != 0x5a524551 || !length || bytes.size() - offset != uint64_t(length) + 8 + SIGNATURE_SIZE) return false;
        request = ClientRequest{message.clientId, message.requestId, length, message.transactionCount, message.digest};
        request.authentication.assign(bytes.begin() + offset + 8, bytes.begin() + offset + 8 + SIGNATURE_SIZE);
        return VerifyClientRequest(request);
    }

} // namespace ns3
