// Internal machinery: order pool, intrusive levels, order map, price index.
#pragma once

#include <map>
#include <vector>

#include "../types.hpp"

namespace matcher::detail {

inline constexpr std::uint32_t NIL = UINT32_MAX;

// ---- order + slab pool -----------------------------------------------------

struct Order {
    OrderId id;
    Side side;
    Price price;
    Qty qty;
    Tif tif;
    std::uint32_t prev, next;
};

struct Pool {
    std::vector<Order> slots;
    std::uint32_t free_head = NIL;
    std::size_t live = 0;
    std::size_t cap;

    explicit Pool(std::size_t cap_) : cap(cap_) {
        slots.reserve(cap_ < (1u << 20) ? cap_ : (1u << 20));
    }

    std::uint32_t alloc() {
        if (free_head != NIL) {
            auto idx = free_head;
            free_head = slots[idx].next;
            ++live;
            return idx;
        }
        if (slots.size() < cap) {
            auto idx = static_cast<std::uint32_t>(slots.size());
            slots.push_back(Order{0, Side::Bid, 0, 0, Tif::Gtc, NIL, NIL});
            ++live;
            return idx;
        }
        return NIL; // caller checked live < cap (book_full)
    }

    void free(std::uint32_t idx) {
        slots[idx].next = free_head;
        slots[idx].prev = NIL;
        free_head = idx;
        --live;
    }
};

// ---- price level (intrusive FIFO) ------------------------------------------

struct Level {
    std::uint32_t head = NIL, tail = NIL;
    Qty total = 0;
    bool empty() const { return head == NIL; }
};

inline void level_push(Pool& p, Level& l, std::uint32_t idx) {
    Qty q = p.slots[idx].qty;
    p.slots[idx].prev = l.tail;
    p.slots[idx].next = NIL;
    if (l.tail != NIL) p.slots[l.tail].next = idx;
    else l.head = idx;
    l.tail = idx;
    l.total += q;
}

inline void level_unlink(Pool& p, Level& l, std::uint32_t idx) {
    Order& o = p.slots[idx];
    auto prev = o.prev, next = o.next;
    Qty q = o.qty;
    if (prev != NIL) p.slots[prev].next = next;
    else l.head = next;
    if (next != NIL) p.slots[next].prev = prev;
    else l.tail = prev;
    o.prev = o.next = NIL;
    l.total -= q;
}

// ---- order_id -> pool index (open-addressed) --------------------------------

struct OrderMap {
    std::vector<OrderId> keys;
    std::vector<std::uint32_t> vals;
    std::vector<std::uint8_t> used;
    std::size_t mask, len = 0;

    explicit OrderMap(std::size_t live_max) {
        std::size_t cap = 16;
        while (cap < live_max * 2) cap <<= 1;
        keys.assign(cap, 0);
        vals.assign(cap, 0);
        used.assign(cap, 0);
        mask = cap - 1;
    }

    static std::uint64_t mix(std::uint64_t k) {
        std::uint64_t h = k * 0x9E3779B97F4A7C15ull;
        return h ^ (h >> 32);
    }

    const std::uint32_t* get(OrderId k) const {
        std::size_t i = mix(k) & mask;
        while (used[i]) {
            if (keys[i] == k) return &vals[i];
            i = (i + 1) & mask;
        }
        return nullptr;
    }

    bool contains(OrderId k) const { return get(k) != nullptr; }

    void insert(OrderId k, std::uint32_t v) {
        std::size_t i = mix(k) & mask;
        while (used[i]) {
            if (keys[i] == k) { vals[i] = v; return; }
            i = (i + 1) & mask;
        }
        used[i] = 1; keys[i] = k; vals[i] = v; ++len;
    }

    void remove(OrderId k) {
        std::size_t i = mix(k) & mask;
        while (used[i] && keys[i] != k) i = (i + 1) & mask;
        if (!used[i]) return;
        used[i] = 0;
        --len;
        // Backward-shift deletion (Knuth 6.4R) — no tombstones. Scan forward
        // from the hole; move back any entry whose probe chain crosses it.
        // An entry that cannot move leaves the hole where it is — only the
        // scan advances. (Previously the hole also moved onto that live
        // slot, so a later shift overwrote it and a key silently vanished.)
        std::size_t hole = i;
        std::size_t j = i;
        for (;;) {
            j = (j + 1) & mask;
            if (!used[j]) return;
            std::size_t home = mix(keys[j]) & mask;
            bool in_interval = hole < j ? (home > hole && home <= j)
                                        : (home > hole || home <= j);
            if (!in_interval) {
                keys[hole] = keys[j]; vals[hole] = vals[j];
                used[hole] = 1; used[j] = 0;
                hole = j;
            }
        }
    }
};

// ---- price index ------------------------------------------------------------

struct LevelDepth {
    Price price;
    Qty qty;
};

// Direct-indexed ladder + occupancy bitmap + top-of-book cursor. summary bit
// w is set iff bits[w] != 0, so rescan skips 4096 empty ticks per word.
struct LadderIndex {
    Price base;
    Side side;
    std::vector<Level> levels;
    std::vector<std::uint64_t> bits;
    std::vector<std::uint64_t> summary;
    std::uint32_t count = 0;
    std::uint32_t best = NIL;

    LadderIndex(Side s, Price pmin, Price pmax) : base(pmin), side(s) {
        std::size_t span = std::size_t(pmax - pmin) + 1;
        levels.assign(span, Level{});
        bits.assign((span + 63) / 64, 0);
        summary.assign((bits.size() + 63) / 64, 0);
    }

    std::size_t idx(Price p) const { return std::size_t(p - base); }

    bool best_price(Price& out) const {
        if (best == NIL) return false;
        out = base + best;
        return true;
    }

    Level* level_mut(Price p) {
        auto i = idx(p);
        if (i >= levels.size()) return nullptr;
        Level& l = levels[i];
        return l.empty() ? nullptr : &l;
    }

    Level* level_insert(Price p) {
        auto i = idx(p);
        Level& l = levels[i];
        if (l.empty()) {
            bits[i / 64] |= 1ull << (i % 64);
            summary[i / 4096] |= 1ull << ((i / 64) % 64);
            ++count;
            if (best == NIL || (side == Side::Bid && std::uint32_t(i) > best) ||
                (side == Side::Ask && std::uint32_t(i) < best)) {
                best = std::uint32_t(i);
            }
        }
        return &l;
    }

    void unlink_level(Price p) {
        auto i = idx(p);
        if (!levels[i].empty()) return;
        std::size_t w = i / 64;
        bits[w] &= ~(1ull << (i % 64));
        if (!bits[w]) summary[w / 64] &= ~(1ull << (w % 64));
        --count;
        if (count == 0) best = NIL;
        else if (std::uint32_t(i) == best) best = rescan(i);
    }

    // Next occupied index moving inward from `from` (inclusive): higher for
    // asks, lower for bids.
    std::uint32_t rescan(std::size_t from) const {
        std::size_t w = from / 64;
        if (side == Side::Ask) {
            if (std::uint64_t word = bits[w] & (~0ull << (from % 64)))
                return std::uint32_t(w * 64 + __builtin_ctzll(word));
            // next non-empty word above w, via the summary
            for (std::size_t s = w + 1; s < bits.size();) {
                std::size_t sw = s / 64;
                if (std::uint64_t sword = summary[sw] & (~0ull << (s % 64))) {
                    std::size_t nw = sw * 64 + __builtin_ctzll(sword);
                    return std::uint32_t(nw * 64 + __builtin_ctzll(bits[nw]));
                }
                s = sw * 64 + 64;
            }
            return NIL;
        }
        if (std::uint64_t word = bits[w] & (~0ull >> (63 - from % 64)))
            return std::uint32_t(w * 64 + 63 - __builtin_clzll(word));
        // next non-empty word below w, via the summary
        for (std::int64_t s = std::int64_t(w) - 1; s >= 0;) {
            std::size_t sw = std::size_t(s) / 64;
            if (std::uint64_t sword = summary[sw] & (~0ull >> (63 - std::size_t(s) % 64))) {
                std::size_t nw = sw * 64 + 63 - __builtin_clzll(sword);
                return std::uint32_t(nw * 64 + 63 - __builtin_clzll(bits[nw]));
            }
            s = std::int64_t(sw) * 64 - 1;
        }
        return NIL;
    }

    Qty sum_range(Price lo, Price hi) const {
        long loI = long(idx(lo)) < 0 ? 0 : long(idx(lo));
        long hiI = long(idx(hi)) > long(levels.size()) - 1 ? long(levels.size()) - 1 : long(idx(hi));
        if (loI > hiI) return 0;
        Qty sum = 0;
        for (std::size_t i = std::size_t(loI); i <= std::size_t(hiI);) {
            std::size_t w = i / 64;
            std::size_t hiBit = std::min(w * 64 + 63, std::size_t(hiI));
            std::uint64_t word = bits[w] & (~0ull << (i % 64)) & (~0ull >> (63 - hiBit % 64));
            while (word) {
                std::size_t b = __builtin_ctzll(word);
                sum += levels[w * 64 + b].total;
                word &= word - 1;
            }
            i = w * 64 + 64;
        }
        return sum;
    }

    std::size_t len() const { return count; }

    std::vector<LevelDepth> depth(std::size_t n) const {
        std::vector<LevelDepth> out;
        if (side == Side::Ask) {
            for (std::size_t w = 0; w < bits.size() && out.size() < n; ++w) {
                std::uint64_t word = bits[w];
                while (word && out.size() < n) {
                    std::size_t b = __builtin_ctzll(word);
                    std::size_t i = w * 64 + b;
                    out.push_back({base + Price(i), levels[i].total});
                    word &= word - 1;
                }
            }
        } else {
            for (std::size_t w = bits.size(); w-- > 0 && out.size() < n;) {
                std::uint64_t word = bits[w];
                while (word && out.size() < n) {
                    std::size_t b = 63 - __builtin_clzll(word);
                    std::size_t i = w * 64 + b;
                    out.push_back({base + Price(i), levels[i].total});
                    word &= ~(1ull << b);
                }
            }
        }
        return out;
    }
};

// std::map fallback for unbounded domains.
struct TreeIndex {
    Side side;
    std::map<Price, Level> map;

    explicit TreeIndex(Side s) : side(s) {}

    bool best_price(Price& out) const {
        if (map.empty()) return false;
        out = side == Side::Bid ? map.rbegin()->first : map.begin()->first;
        return true;
    }

    Level* level_mut(Price p) {
        auto it = map.find(p);
        if (it == map.end() || it->second.empty()) return nullptr;
        return &it->second;
    }

    Level* level_insert(Price p) { return &map[p]; }

    void unlink_level(Price p) {
        auto it = map.find(p);
        if (it != map.end() && it->second.empty()) map.erase(it);
    }

    Qty sum_range(Price lo, Price hi) const {
        Qty sum = 0;
        for (auto it = map.lower_bound(lo); it != map.end() && it->first <= hi; ++it)
            sum += it->second.total;
        return sum;
    }

    std::size_t len() const { return map.size(); }

    std::vector<LevelDepth> depth(std::size_t n) const {
        std::vector<LevelDepth> out;
        if (side == Side::Ask) {
            for (auto it = map.begin(); it != map.end() && out.size() < n; ++it)
                out.push_back({it->first, it->second.total});
        } else {
            for (auto it = map.rbegin(); it != map.rend() && out.size() < n; ++it)
                out.push_back({it->first, it->second.total});
        }
        return out;
    }
};


// Enum-dispatched union of the two indexes (mirrors Rust `enum PriceIndex`):
// branch-predictable, no virtuals, fully inlinable. Holds both impls — the
// unused one is ~48 bytes.
struct PriceIndex {
    IndexKind kind;
    LadderIndex lad;
    TreeIndex tree;

    PriceIndex(IndexKind k, Side side, Price pmin, Price pmax)
        : kind(k), lad(side, pmin, pmax), tree(side) {}

    bool best_price(Price& out) const {
        return kind == IndexKind::Ladder ? lad.best_price(out) : tree.best_price(out);
    }
    Level* level_mut(Price p) {
        return kind == IndexKind::Ladder ? lad.level_mut(p) : tree.level_mut(p);
    }
    Level* level_insert(Price p) {
        return kind == IndexKind::Ladder ? lad.level_insert(p) : tree.level_insert(p);
    }
    void unlink_level(Price p) {
        kind == IndexKind::Ladder ? lad.unlink_level(p) : tree.unlink_level(p);
    }
    Qty sum_range(Price lo, Price hi) const {
        return kind == IndexKind::Ladder ? lad.sum_range(lo, hi) : tree.sum_range(lo, hi);
    }
    std::size_t len() const {
        return kind == IndexKind::Ladder ? lad.len() : tree.len();
    }
    std::vector<LevelDepth> depth(std::size_t n) const {
        return kind == IndexKind::Ladder ? lad.depth(n) : tree.depth(n);
    }
};

} // namespace matcher::detail
