// crc.hpp — CRC-32C (Castagnoli, reflected 0x82F63B78) with an incremental
// update, using the CPU instruction where the build targets it and a
// slicing-by-8 table otherwise. Results are identical to journal.hpp's
// bytewise crc32c.
#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>

#if defined(__ARM_FEATURE_CRC32)
#include <arm_acle.h>
#elif defined(__SSE4_2__)
#include <nmmintrin.h>
#endif

namespace orderer::cluster {

namespace detail {
inline const std::array<std::array<std::uint32_t, 256>, 8>& crc_tables() {
    static const auto t = [] {
        std::array<std::array<std::uint32_t, 256>, 8> t{};
        for (std::uint32_t i = 0; i < 256; ++i) {
            std::uint32_t c = i;
            for (int k = 0; k < 8; ++k) c = (c & 1) ? (c >> 1) ^ 0x82F63B78u : c >> 1;
            t[0][i] = c;
        }
        for (std::uint32_t i = 0; i < 256; ++i)
            for (int k = 1; k < 8; ++k) t[k][i] = (t[k - 1][i] >> 8) ^ t[0][t[k - 1][i] & 0xFF];
        return t;
    }();
    return t;
}
}  // namespace detail

/// Fold `n` bytes into the running state `c` (start from ~0, finish with ~).
inline std::uint32_t crc32c_update(std::uint32_t c, const std::uint8_t* d, std::size_t n) {
#if defined(__ARM_FEATURE_CRC32)
    for (; n >= 8; d += 8, n -= 8) {
        std::uint64_t v;
        std::memcpy(&v, d, 8);
        c = __crc32cd(c, v);
    }
    for (; n; --n) c = __crc32cb(c, *d++);
    return c;
#elif defined(__SSE4_2__)
    std::uint64_t cc = c;
    for (; n >= 8; d += 8, n -= 8) {
        std::uint64_t v;
        std::memcpy(&v, d, 8);
        cc = _mm_crc32_u64(cc, v);
    }
    c = static_cast<std::uint32_t>(cc);
    for (; n; --n) c = _mm_crc32_u8(c, *d++);
    return c;
#else
    const auto& t = detail::crc_tables();
    for (; n >= 8; d += 8, n -= 8) {
        std::uint32_t lo, hi;
        std::memcpy(&lo, d, 4);
        std::memcpy(&hi, d + 4, 4);
        lo ^= c;
        c = t[7][lo & 0xFF] ^ t[6][(lo >> 8) & 0xFF] ^ t[5][(lo >> 16) & 0xFF] ^ t[4][lo >> 24] ^
            t[3][hi & 0xFF] ^ t[2][(hi >> 8) & 0xFF] ^ t[1][(hi >> 16) & 0xFF] ^ t[0][hi >> 24];
    }
    for (; n; --n) c = t[0][(c ^ *d++) & 0xFF] ^ (c >> 8);
    return c;
#endif
}

inline std::uint32_t crc32c(const std::uint8_t* d, std::size_t n) { return ~crc32c_update(~0u, d, n); }

}  // namespace orderer::cluster
