// routing.hpp — symbol → partition (spec/ROUTING.md).
#pragma once

#include <cstdint>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "flat.hpp"

namespace orderer {

inline constexpr std::uint32_t MAX_PARTITIONS = 1024;

/// spec/ROUTING.md §2: multiply-shift Fibonacci hashing, unsigned 64-bit.
inline std::uint32_t hash_partition(std::uint32_t symbol, std::uint32_t partitions) {
    std::uint64_t h = std::uint64_t(symbol) * 0x9E3779B97F4A7C15ull;
    return std::uint32_t(((h >> 32) * std::uint64_t(partitions)) >> 32);
}

/// A fixed symbol → partition map: table overrides, hash for the rest.
class PartitionMap {
  public:
    /// Build hash routing (+ overrides); error text on invalid input.
    static std::optional<std::string> make(std::uint32_t partitions,
                                           const std::vector<std::pair<std::uint32_t, std::uint32_t>>& table,
                                           PartitionMap& out) {
        if (partitions == 0 || partitions > MAX_PARTITIONS)
            return "partitions must be 1..=" + std::to_string(MAX_PARTITIONS) + ", got " +
                   std::to_string(partitions);
        PartitionMap m;
        m.p_ = partitions;
        m.dense_.resize(DENSE);
        for (std::uint32_t s = 0; s < DENSE; ++s) m.dense_[s] = hash_partition(s, partitions);
        std::set<std::uint32_t> seen;
        for (auto [sym, p] : table) {
            if (p >= partitions)
                return "symbol " + std::to_string(sym) + ": partition " + std::to_string(p) +
                       " out of range for " + std::to_string(partitions) + " partitions";
            if (!seen.insert(sym).second) return "symbol " + std::to_string(sym) + " listed twice";
            if (sym < DENSE) m.dense_[sym] = p;
            m.sparse_[sym] = p;
        }
        out = std::move(m);
        return std::nullopt;
    }

    /// Parse an orderer-partition-map/1 file for a pipeline of `partitions`.
    static std::optional<std::string> parse_table(std::string_view text, std::uint32_t partitions,
                                                  PartitionMap& out) {
        std::vector<std::pair<std::uint32_t, std::uint32_t>> table;
        bool first = true;
        std::size_t pos = 0;
        while (pos < text.size()) {
            auto e = text.find('\n', pos);
            if (e == std::string_view::npos) e = text.size();
            auto line = flat::trim(text.substr(pos, e - pos));
            pos = e + 1;
            if (line.empty()) continue;
            if (first) {
                first = false;
                if (flat::get_str(line, "format") != std::optional<std::string_view>("orderer-partition-map/1"))
                    return "not an orderer-partition-map/1 header";
                auto p = flat::get_u64(line, "partitions");
                if (!p) return "partition map header lacks partitions";
                if (*p != partitions)
                    return "partition map is for " + std::to_string(*p) + " partitions, pipeline has " +
                           std::to_string(partitions);
                continue;
            }
            auto s = flat::get_u64(line, "symbol");
            auto p = flat::get_u64(line, "partition");
            if (!s || *s > UINT32_MAX) return "bad symbol in: " + std::string(line);
            if (!p || *p > UINT32_MAX) return "bad partition in: " + std::string(line);
            table.emplace_back(std::uint32_t(*s), std::uint32_t(*p));
        }
        if (first) return "empty partition map";
        return make(partitions, table, out);
    }

    std::uint32_t partitions() const { return p_; }

    std::uint32_t partition(std::uint32_t sym) const {
        if (sym < DENSE) return dense_[sym];
        if (!sparse_.empty()) {
            auto it = sparse_.find(sym);
            if (it != sparse_.end()) return it->second;
        }
        return hash_partition(sym, p_);
    }

  private:
    static constexpr std::uint32_t DENSE = 4096;
    std::uint32_t p_ = 1;
    std::vector<std::uint32_t> dense_;
    std::unordered_map<std::uint32_t, std::uint32_t> sparse_;
};

}  // namespace orderer
