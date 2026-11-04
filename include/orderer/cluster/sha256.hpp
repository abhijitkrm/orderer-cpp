// sha256.hpp — SHA-256, HMAC-SHA256, constant-time compare and nonces, for
// divergence digests and the pre-shared-key handshake (see auth.hpp).
#pragma once

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <random>
#include <string>
#include <vector>

namespace orderer::cluster {

using Digest = std::array<std::uint8_t, 32>;

class Sha256 {
public:
    Sha256() = default;
    void update(const std::uint8_t* data, std::size_t n) {
        len_ += n;
        if (fill_) {
            std::size_t take = std::min<std::size_t>(64 - fill_, n);
            std::memcpy(buf_ + fill_, data, take);
            fill_ += take; data += take; n -= take;
            if (fill_ == 64) { block(buf_); fill_ = 0; }
        }
        for (; n >= 64; data += 64, n -= 64) block(data);
        if (n) { std::memcpy(buf_, data, n); fill_ = n; }
    }
    Digest finish() {
        const std::uint64_t bits = len_ * 8;
        std::uint8_t pad[72] = {0x80};
        const std::size_t used = (len_ + 1) % 64;
        const std::size_t zeros = used <= 56 ? 56 - used : 120 - used;
        const std::size_t total = 1 + zeros;
        for (int i = 0; i < 8; ++i) pad[total + i] = static_cast<std::uint8_t>(bits >> (56 - 8 * i));
        const std::uint64_t keep = len_;
        update(pad, total + 8);
        len_ = keep;
        Digest out;
        for (int i = 0; i < 8; ++i)
            for (int j = 0; j < 4; ++j) out[i * 4 + j] = static_cast<std::uint8_t>(h_[i] >> (24 - 8 * j));
        return out;
    }

private:
    static std::uint32_t rotr(std::uint32_t x, int n) { return (x >> n) | (x << (32 - n)); }
    void block(const std::uint8_t* b) {
        static const std::uint32_t K[64] = {
            0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
            0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
            0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
            0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
            0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
            0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
            0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
            0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2};
        std::uint32_t w[64];
        for (int i = 0; i < 16; ++i)
            w[i] = (std::uint32_t(b[4 * i]) << 24) | (std::uint32_t(b[4 * i + 1]) << 16) | (std::uint32_t(b[4 * i + 2]) << 8) | b[4 * i + 3];
        for (int i = 16; i < 64; ++i) {
            const std::uint32_t s0 = rotr(w[i - 15], 7) ^ rotr(w[i - 15], 18) ^ (w[i - 15] >> 3);
            const std::uint32_t s1 = rotr(w[i - 2], 17) ^ rotr(w[i - 2], 19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16] + s0 + w[i - 7] + s1;
        }
        std::uint32_t a = h_[0], bb = h_[1], c = h_[2], d = h_[3], e = h_[4], f = h_[5], g = h_[6], h = h_[7];
        for (int i = 0; i < 64; ++i) {
            const std::uint32_t s1 = rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25);
            const std::uint32_t ch = (e & f) ^ (~e & g);
            const std::uint32_t t1 = h + s1 + ch + K[i] + w[i];
            const std::uint32_t s0 = rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22);
            const std::uint32_t maj = (a & bb) ^ (a & c) ^ (bb & c);
            const std::uint32_t t2 = s0 + maj;
            h = g; g = f; f = e; e = d + t1; d = c; c = bb; bb = a; a = t1 + t2;
        }
        h_[0] += a; h_[1] += bb; h_[2] += c; h_[3] += d; h_[4] += e; h_[5] += f; h_[6] += g; h_[7] += h;
    }
    std::uint32_t h_[8] = {0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19};
    std::uint8_t buf_[64];
    std::size_t fill_ = 0;
    std::uint64_t len_ = 0;
};

inline Digest sha256(const std::uint8_t* d, std::size_t n) {
    Sha256 h;
    h.update(d, n);
    return h.finish();
}
inline Digest sha256(const std::vector<std::uint8_t>& v) { return sha256(v.data(), v.size()); }

inline std::string hex(const std::uint8_t* d, std::size_t n) {
    static const char* h = "0123456789abcdef";
    std::string s;
    for (std::size_t i = 0; i < n; ++i) { s += h[d[i] >> 4]; s += h[d[i] & 15]; }
    return s;
}
inline std::string hex(const Digest& d) { return hex(d.data(), d.size()); }

inline Digest hmac_sha256(const std::uint8_t* key, std::size_t klen, const std::uint8_t* msg, std::size_t mlen) {
    std::uint8_t k[64] = {0};
    if (klen > 64) { auto d = sha256(key, klen); std::memcpy(k, d.data(), 32); }
    else if (klen) std::memcpy(k, key, klen);
    Sha256 inner;
    std::uint8_t pad[64];
    for (int i = 0; i < 64; ++i) pad[i] = k[i] ^ 0x36;
    inner.update(pad, 64);
    inner.update(msg, mlen);
    const Digest ih = inner.finish();
    Sha256 outer;
    for (int i = 0; i < 64; ++i) pad[i] = k[i] ^ 0x5c;
    outer.update(pad, 64);
    outer.update(ih.data(), 32);
    return outer.finish();
}

inline bool ct_eq(const std::uint8_t* a, const std::uint8_t* b, std::size_t n) {
    std::uint8_t acc = 0;
    for (std::size_t i = 0; i < n; ++i) acc |= a[i] ^ b[i];
    return acc == 0;
}

inline std::array<std::uint8_t, 16> nonce() {
    static std::random_device rd;
    std::array<std::uint8_t, 16> n;
    for (std::size_t i = 0; i < 16; i += 4) {
        std::uint32_t v = rd() ^ static_cast<std::uint32_t>(std::chrono::steady_clock::now().time_since_epoch().count());
        std::memcpy(n.data() + i, &v, 4);
    }
    return n;
}

}  // namespace orderer::cluster
