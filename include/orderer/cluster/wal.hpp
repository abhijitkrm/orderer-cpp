// wal.hpp — the replicated log's write-ahead file, byte-compatible with
// orderer-rust (orderer-cluster wal.rs; spec/REPLICATION.md 13.3).
//
// One consensus write (Persist) is one frame: len u32 | crc32c u32 | body.
// Recovery cuts a torn tail (including a zero-filled one that a SIGKILL during
// an append can leave) and refuses to start when valid frames follow a bad one.
#pragma once

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>

#include "codec.hpp"
#include "crc.hpp"

namespace orderer::cluster {

namespace wal_detail {
inline constexpr char MAGIC[8] = {'O', 'R', 'D', 'W', 'A', 'L', 0, 1};
inline constexpr std::size_t FRAME_HEAD = 8;
inline constexpr std::size_t MAX_FRAME = 256u << 20;
// term u32, vote u8, compact flag u8, from u64, entry count u32.
inline constexpr std::size_t MIN_BODY = 4 + 1 + 1 + 8 + 4;
}  // namespace wal_detail

struct WalError : std::runtime_error {
    enum class Kind { Io, Corrupt, BadHeader } kind;
    std::uint64_t offset = 0;
    WalError(Kind k, const std::string& m, std::uint64_t off = 0) : std::runtime_error(m), kind(k), offset(off) {}
};

struct Repair {
    std::uint64_t frames = 0, torn_bytes = 0;
};

/// fsync a file descriptor as durably as the platform allows.
inline void full_sync(int fd) {
#ifdef F_FULLFSYNC
    if (::fcntl(fd, F_FULLFSYNC) == 0) return;
#endif
    if (::fsync(fd) != 0) throw WalError(WalError::Kind::Io, std::string("fsync: ") + std::strerror(errno));
}

inline void sync_dir(const std::filesystem::path& dir) {
    int fd = ::open(dir.c_str(), O_RDONLY);
    if (fd < 0) throw WalError(WalError::Kind::Io, "open dir: " + dir.string());
    try { full_sync(fd); } catch (...) { ::close(fd); throw; }
    ::close(fd);
}

inline std::string read_file(const std::filesystem::path& p) {
    std::ifstream f(p, std::ios::binary);
    if (!f) throw WalError(WalError::Kind::Io, "cannot read " + p.string());
    return std::string(std::istreambuf_iterator<char>(f), {});
}

inline void write_all(int fd, const void* data, std::size_t n) {
    const char* p = static_cast<const char*>(data);
    while (n) {
        ssize_t w = ::write(fd, p, n);
        if (w < 0) {
            if (errno == EINTR) continue;
            throw WalError(WalError::Kind::Io, std::string("write: ") + std::strerror(errno));
        }
        p += w; n -= static_cast<std::size_t>(w);
    }
}

class Wal {
public:
    /// Open (creating if absent) the WAL in `dir`, repairing a torn tail.
    /// The recovered disk image comes back through `disk`.
    static Wal open(const std::filesystem::path& dir, Disk& disk, Repair& rep) {
        namespace fs = std::filesystem;
        using namespace wal_detail;
        fs::create_directories(dir);
        const fs::path path = dir / "raft.wal", tmp = dir / "raft.wal.tmp";
        std::error_code ec;
        fs::remove(tmp, ec);
        if (!fs::exists(path)) {
            int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
            if (fd < 0) throw WalError(WalError::Kind::Io, "create " + tmp.string());
            write_all(fd, MAGIC, sizeof MAGIC);
            full_sync(fd);
            ::close(fd);
            fs::rename(tmp, path);
            sync_dir(dir);
        }
        const std::string data = read_file(path);
        std::size_t valid = 0;
        std::uint64_t frames = 0;
        disk = replay(data, valid, frames);
        const std::uint64_t torn = data.size() - valid;
        int fd = ::open(path.c_str(), O_RDWR);
        if (fd < 0) throw WalError(WalError::Kind::Io, "open " + path.string());
        if (torn > 0) {
            if (::ftruncate(fd, static_cast<off_t>(valid)) != 0) { ::close(fd); throw WalError(WalError::Kind::Io, "ftruncate"); }
            full_sync(fd);
        }
        ::lseek(fd, static_cast<off_t>(valid), SEEK_SET);
        rep = Repair{frames, torn};
        Wal w;
        w.fd_ = fd;
        w.path_ = path;
        w.len_ = valid;
        return w;
    }

    Wal() = default;
    Wal(const Wal&) = delete;
    Wal& operator=(const Wal&) = delete;
    Wal(Wal&& o) noexcept : fd_(std::exchange(o.fd_, -1)), path_(std::move(o.path_)), len_(o.len_), buf_(std::move(o.buf_)) {}
    Wal& operator=(Wal&& o) noexcept {
        if (this != &o) { close(); fd_ = std::exchange(o.fd_, -1); path_ = std::move(o.path_); len_ = o.len_; buf_ = std::move(o.buf_); }
        return *this;
    }
    ~Wal() { close(); }

    /// Buffer one write. Nothing is durable until sync().
    void append(const Persist& p) { frame(p, buf_); }

    /// Write buffered frames and fsync (or only write, for benchmarks).
    void sync(bool durable = true) {
        if (!buf_.empty()) {
            write_all(fd_, buf_.data(), buf_.size());
            len_ += buf_.size();
            buf_.clear();
        }
        if (durable) full_sync(fd_);
    }

    std::uint64_t len() const { return len_; }

    /// Replace the file with one frame holding the current state, so a
    /// compacted log's prefix no longer occupies disk. Crash-safe.
    void rewrite() {
        namespace fs = std::filesystem;
        using namespace wal_detail;
        sync();
        const std::string data = read_file(path_);
        std::size_t valid = 0;
        std::uint64_t frames = 0;
        const Disk disk = replay(data, valid, frames);
        Persist p;
        p.term = disk.term;
        p.voted_for = disk.voted_for;
        p.from = disk.base + 1;
        p.entries = disk.log;
        if (disk.base > 0) p.compact = std::pair<Index, Term>{disk.base, disk.base_term};
        std::vector<std::uint8_t> out(MAGIC, MAGIC + sizeof MAGIC);
        frame(p, out);
        const fs::path dir = path_.parent_path(), tmp = dir / "raft.wal.tmp";
        int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) throw WalError(WalError::Kind::Io, "create " + tmp.string());
        write_all(fd, out.data(), out.size());
        full_sync(fd);
        ::close(fd);
        fs::rename(tmp, path_);
        sync_dir(dir);
        close();
        fd_ = ::open(path_.c_str(), O_RDWR);
        if (fd_ < 0) throw WalError(WalError::Kind::Io, "reopen " + path_.string());
        len_ = out.size();
        ::lseek(fd_, static_cast<off_t>(len_), SEEK_SET);
    }

    /// Replay a whole WAL image. `valid` = length of the intact prefix.
    static Disk replay(const std::string& data, std::size_t& valid, std::uint64_t& frames) {
        using namespace wal_detail;
        if (data.size() < sizeof MAGIC || std::memcmp(data.data(), MAGIC, sizeof MAGIC) != 0)
            throw WalError(WalError::Kind::BadHeader, "wal header missing or unrecognised");
        const auto* d = reinterpret_cast<const std::uint8_t*>(data.data());
        Disk disk;
        std::size_t off = sizeof MAGIC;
        frames = 0;
        while (off < data.size()) {
            const std::uint8_t* body = nullptr;
            std::size_t blen = 0;
            if (frame_at(d, data.size(), off, body, blen)) {
                Persist p;
                try { p = decode_persist(body, blen); }
                catch (const CodecError&) { throw WalError(WalError::Kind::Corrupt, "wal corrupt", off); }
                if (!fits(disk, p)) throw WalError(WalError::Kind::Corrupt, "wal corrupt", off);
                apply_persist(disk, p);
                off += FRAME_HEAD + blen;
                ++frames;
            } else {
                // Torn only if nothing valid follows.
                if (valid_frame_follows(d, data.size(), off)) throw WalError(WalError::Kind::Corrupt, "wal corrupt", off);
                break;
            }
        }
        valid = off;
        return disk;
    }

private:
    void close() { if (fd_ >= 0) { ::close(fd_); fd_ = -1; } }

    static void frame(const Persist& p, std::vector<std::uint8_t>& out) {
        const std::size_t start = out.size();
        out.resize(start + wal_detail::FRAME_HEAD);
        encode_persist(p, out);
        const std::uint8_t* body = out.data() + start + wal_detail::FRAME_HEAD;
        const std::size_t len = out.size() - start - wal_detail::FRAME_HEAD;
        const std::uint32_t l = static_cast<std::uint32_t>(len), c = crc32c(body, len);
        std::memcpy(out.data() + start, &l, 4);
        std::memcpy(out.data() + start + 4, &c, 4);
    }

    static bool frame_at(const std::uint8_t* d, std::size_t n, std::size_t off, const std::uint8_t*& body, std::size_t& len) {
        using namespace wal_detail;
        if (off + FRAME_HEAD > n) return false;
        std::uint32_t l, c;
        std::memcpy(&l, d + off, 4);
        std::memcpy(&c, d + off + 4, 4);
        // A real body is never shorter than the fixed fields of a write. That
        // also rejects zeros: an all-zero header (length 0, checksum 0) would
        // otherwise pass as a valid empty frame.
        if (l < MIN_BODY || l > MAX_FRAME) return false;
        if (off + FRAME_HEAD + l > n) return false;
        body = d + off + FRAME_HEAD;
        len = l;
        return crc32c(body, l) == c;
    }

    static bool fits(const Disk& disk, const Persist& p) {
        if (p.from == 0) return false;
        Index base = disk.base;
        std::size_t len = disk.log.size();
        if (p.compact && p.compact->first > disk.base) {
            base = p.compact->first;
            const std::size_t drop = p.compact->first - disk.base;
            len = len > drop ? len - drop : 0;
        }
        return p.from > base && p.from - 1 - base <= len;
    }

    static bool valid_frame_follows(const std::uint8_t* d, std::size_t n, std::size_t bad) {
        using namespace wal_detail;
        const std::uint8_t* body;
        std::size_t blen;
        if (bad + FRAME_HEAD <= n) {
            std::uint32_t l;
            std::memcpy(&l, d + bad, 4);
            if (l >= MIN_BODY && l <= MAX_FRAME) {
                const std::size_t next = bad + FRAME_HEAD + l;
                if (next < n && frame_at(d, n, next, body, blen)) return true;
            }
        }
        // Bounded scan for a frame starting anywhere in the next 1 MiB.
        const std::size_t end = std::min(n, bad + 1 + (1u << 20));
        for (std::size_t o = bad + 1; o < end; ++o) {
            if (o + 4 > n) break;
            std::uint32_t l;
            std::memcpy(&l, d + o, 4);
            if (l > n - o) continue;  // cheap reject before checksumming
            if (frame_at(d, n, o, body, blen)) {
                try { (void)decode_persist(body, blen); return true; } catch (const CodecError&) {}
            }
        }
        return false;
    }

    int fd_ = -1;
    std::filesystem::path path_;
    std::uint64_t len_ = 0;
    std::vector<std::uint8_t> buf_;
};

}  // namespace orderer::cluster
