// auth.hpp — pre-shared-key frame authentication (see orderer-rust auth.rs
// and spec/REPLICATION.md 13.7). Authentication and integrity, not secrecy.
// Handshake: initiator sends nonce_i; responder replies nonce_r and
// MAC(K, "chal" | nonce_i | nonce_r | ids); both derive
// SK = HMAC(K, "sess" | ...). Every later frame carries a 128-bit MAC over
// direction, sequence, kind and body.
#pragma once

#include <vector>

#include "sha256.hpp"

namespace orderer::cluster {

inline constexpr std::size_t MAC_LEN = 16;
inline constexpr std::size_t NONCE_LEN = 16;

/// HMAC over `label | parts...` with the cluster key (handshake derivations).
inline Digest handshake(const std::vector<std::uint8_t>& key, const std::string& label,
                        std::initializer_list<std::pair<const std::uint8_t*, std::size_t>> parts) {
    std::vector<std::uint8_t> m(label.begin(), label.end());
    for (auto [p, n] : parts) m.insert(m.end(), p, p + n);
    return hmac_sha256(key.data(), key.size(), m.data(), m.size());
}

class Session {
public:
    /// send_dir: 0 for the initiator, 1 for the responder.
    Session(const Digest& key, std::uint8_t send_dir) : key_(key), send_dir_(send_dir) {}

    std::array<std::uint8_t, MAC_LEN> seal(std::uint8_t kind, const std::uint8_t* body, std::size_t n) {
        auto m = mac(send_dir_, send_seq_, kind, body, n);
        ++send_seq_;
        return m;
    }

    /// Check the next incoming frame; the sequence advances only on success.
    bool open(std::uint8_t kind, const std::uint8_t* body, std::size_t n, const std::uint8_t* tag) {
        auto want = mac(static_cast<std::uint8_t>(1 - send_dir_), recv_seq_, kind, body, n);
        const bool ok = ct_eq(want.data(), tag, MAC_LEN);
        if (ok) ++recv_seq_;
        return ok;
    }

private:
    std::array<std::uint8_t, MAC_LEN> mac(std::uint8_t dir, std::uint64_t seq, std::uint8_t kind,
                                          const std::uint8_t* body, std::size_t n) const {
        std::vector<std::uint8_t> m;
        m.reserve(10 + n);
        m.push_back(dir);
        for (int i = 0; i < 8; ++i) m.push_back(static_cast<std::uint8_t>(seq >> (8 * i)));
        m.push_back(kind);
        m.insert(m.end(), body, body + n);
        auto d = hmac_sha256(key_.data(), key_.size(), m.data(), m.size());
        std::array<std::uint8_t, MAC_LEN> out;
        std::memcpy(out.data(), d.data(), MAC_LEN);
        return out;
    }
    Digest key_;
    std::uint8_t send_dir_;
    std::uint64_t send_seq_ = 0, recv_seq_ = 0;
};

}  // namespace orderer::cluster
