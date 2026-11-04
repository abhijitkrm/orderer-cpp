// snapstore.hpp — durable snapshots, byte-compatible with orderer-rust
// (snapstore.rs). snap-<index>.snap = "ORDS" | index u64 | term u32 | len u64
// | crc u32 | data, written to a temp name, fsynced, renamed, dir fsynced.
#pragma once

#include <algorithm>
#include <cstdio>
#include <filesystem>

#include "wal.hpp"

namespace orderer::cluster {

struct SnapMeta {
    Index index = 0;
    Term term = 0;
    std::uint64_t len = 0;
    std::uint32_t crc = 0;
};

class SnapStore {
public:
    explicit SnapStore(std::filesystem::path dir) : dir_(std::move(dir)) {
        std::filesystem::create_directories(dir_);
        // Leftover temp files are incomplete writes.
        std::error_code ec;
        for (auto& e : std::filesystem::directory_iterator(dir_))
            if (e.path().extension() == ".tmp") std::filesystem::remove(e.path(), ec);
    }

    void put(Index index, Term term, const std::uint8_t* data, std::size_t n) const {
        namespace fs = std::filesystem;
        char name[64];
        std::snprintf(name, sizeof name, "snap-%020llu", static_cast<unsigned long long>(index));
        const fs::path tmp = dir_ / (std::string(name) + ".tmp");
        std::vector<std::uint8_t> file = header(index, term, n, crc32c(data, n));
        file.insert(file.end(), data, data + n);
        int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) throw WalError(WalError::Kind::Io, "create " + tmp.string());
        write_all(fd, file.data(), file.size());
        full_sync(fd);
        ::close(fd);
        fs::rename(tmp, path(index));
        sync_dir(dir_);
    }

    /// Indexes of intact snapshot files, ascending. A corrupt file is skipped.
    std::vector<Index> list() const {
        std::vector<Index> v;
        for (auto& e : std::filesystem::directory_iterator(dir_)) {
            const std::string n = e.path().filename().string();
            if (n.rfind("snap-", 0) == 0 && e.path().extension() == ".snap") {
                const Index i = std::stoull(n.substr(5, n.size() - 10));
                if (meta(i)) v.push_back(i);
            }
        }
        std::sort(v.begin(), v.end());
        return v;
    }

    std::optional<SnapMeta> meta(Index index) const {
        std::error_code ec;
        const auto p = path(index);
        const auto size = std::filesystem::file_size(p, ec);
        if (ec || size < HEADER) return std::nullopt;
        std::ifstream f(p, std::ios::binary);
        std::uint8_t h[HEADER];
        f.read(reinterpret_cast<char*>(h), HEADER);
        auto m = parse(h);
        if (!m || m->index != index || size != HEADER + m->len) return std::nullopt;
        return m;
    }

    /// Load and verify a snapshot; nullopt if absent or corrupt.
    std::optional<std::pair<SnapMeta, std::vector<std::uint8_t>>> get(Index index) const {
        std::error_code ec;
        if (!std::filesystem::exists(path(index), ec)) return std::nullopt;
        const std::string all = read_file(path(index));
        if (all.size() < HEADER) return std::nullopt;
        auto m = parse(reinterpret_cast<const std::uint8_t*>(all.data()));
        if (!m || m->index != index || all.size() - HEADER != m->len) return std::nullopt;
        const auto* d = reinterpret_cast<const std::uint8_t*>(all.data()) + HEADER;
        if (crc32c(d, m->len) != m->crc) return std::nullopt;
        return std::pair{*m, std::vector<std::uint8_t>(d, d + m->len)};
    }

    /// Delete snapshots older than `keep_from`.
    void gc(Index keep_from) const {
        std::error_code ec;
        for (Index i : list()) if (i < keep_from) std::filesystem::remove(path(i), ec);
    }

    /// A snapshot being received from a peer, verified before it is published.
    class Incoming {
    public:
        void write(const std::uint8_t* d, std::size_t n) {
            if (got_ + n > len_) throw WalError(WalError::Kind::Io, "snapshot longer than announced");
            got_ += n;
            running_ = crc32c_update(running_, d, n);
            write_all(fd_, d, n);
        }
        /// Verify length and checksum and publish the file.
        void finish() {
            ::close(std::exchange(fd_, -1));
            if (got_ != len_ || ~running_ != crc_) {
                std::error_code ec;
                std::filesystem::remove(tmp_, ec);
                throw WalError(WalError::Kind::Io, got_ != len_ ? "snapshot shorter than announced" : "snapshot checksum mismatch");
            }
            int fd = ::open(tmp_.c_str(), O_RDWR);
            full_sync(fd);
            ::close(fd);
            std::filesystem::rename(tmp_, dst_);
            sync_dir(dir_);
        }
        void abort() {
            if (fd_ >= 0) ::close(std::exchange(fd_, -1));
            std::error_code ec;
            std::filesystem::remove(tmp_, ec);
        }
        ~Incoming() { if (fd_ >= 0) abort(); }
        Incoming(Incoming&& o) noexcept
            : fd_(std::exchange(o.fd_, -1)), tmp_(std::move(o.tmp_)), dst_(std::move(o.dst_)), dir_(std::move(o.dir_)),
              len_(o.len_), crc_(o.crc_), got_(o.got_), running_(o.running_) {}

    private:
        friend class SnapStore;
        Incoming() = default;
        int fd_ = -1;
        std::filesystem::path tmp_, dst_, dir_;
        std::uint64_t len_ = 0;
        std::uint32_t crc_ = 0;
        std::uint64_t got_ = 0;
        std::uint32_t running_ = ~0u;
    };

    Incoming receive(Index index, Term term, std::uint64_t len, std::uint32_t crc) const {
        char name[64];
        std::snprintf(name, sizeof name, "snap-%020llu", static_cast<unsigned long long>(index));
        Incoming in;
        in.tmp_ = dir_ / (std::string(name) + ".tmp");
        in.dst_ = path(index);
        in.dir_ = dir_;
        in.len_ = len;
        in.crc_ = crc;
        in.fd_ = ::open(in.tmp_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (in.fd_ < 0) throw WalError(WalError::Kind::Io, "create " + in.tmp_.string());
        auto h = header(index, term, len, crc);
        write_all(in.fd_, h.data(), h.size());
        return in;
    }

    std::filesystem::path path(Index index) const {
        char name[64];
        std::snprintf(name, sizeof name, "snap-%020llu.snap", static_cast<unsigned long long>(index));
        return dir_ / name;
    }

    static constexpr std::size_t HEADER = 4 + 8 + 4 + 8 + 4;

private:
    static std::vector<std::uint8_t> header(Index index, Term term, std::uint64_t len, std::uint32_t crc) {
        std::vector<std::uint8_t> h = {'O', 'R', 'D', 'S'};
        put_u64(h, index); put_u32(h, term); put_u64(h, len); put_u32(h, crc);
        return h;
    }
    static std::optional<SnapMeta> parse(const std::uint8_t* h) {
        if (std::memcmp(h, "ORDS", 4) != 0) return std::nullopt;
        SnapMeta m;
        std::memcpy(&m.index, h + 4, 8);
        std::memcpy(&m.term, h + 12, 4);
        std::memcpy(&m.len, h + 16, 8);
        std::memcpy(&m.crc, h + 24, 4);
        return m;
    }
    std::filesystem::path dir_;
};

}  // namespace orderer::cluster
