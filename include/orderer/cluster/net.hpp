// net.hpp — blocking TCP helpers and the framing shared by the replica
// transport and the client protocol: len u32 | crc32c u32 | kind u8 | body
// [| 16-byte MAC once the connection is authenticated]. len and crc cover
// kind, body and MAC. Byte-compatible with orderer-rust.
#pragma once

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <optional>
#include <string>
#include <vector>

#include "auth.hpp"
#include "crc.hpp"

namespace orderer::cluster {

inline constexpr std::size_t MAX_NET_FRAME = 64u << 20;

struct NetError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

/// A TCP address; IPv4 "host:port" (host a dotted quad or "localhost").
struct Addr {
    sockaddr_in sa{};
    static Addr parse(const std::string& s) {
        Addr a;
        auto c = s.rfind(':');
        if (c == std::string::npos) throw NetError("bad address " + s);
        std::string host = s.substr(0, c);
        if (host == "localhost") host = "127.0.0.1";
        a.sa.sin_family = AF_INET;
        a.sa.sin_port = htons(static_cast<std::uint16_t>(std::stoi(s.substr(c + 1))));
        if (::inet_pton(AF_INET, host.c_str(), &a.sa.sin_addr) != 1) throw NetError("bad address " + s);
        return a;
    }
    std::string str() const {
        char buf[64];
        ::inet_ntop(AF_INET, &sa.sin_addr, buf, sizeof buf);
        return std::string(buf) + ":" + std::to_string(ntohs(sa.sin_port));
    }
    bool operator==(const Addr& o) const { return str() == o.str(); }
};

inline void ignore_sigpipe() {
    static const bool once = [] { ::signal(SIGPIPE, SIG_IGN); return true; }();
    (void)once;
}

class Fd {
public:
    Fd() = default;
    explicit Fd(int fd) : fd_(fd) {}
    Fd(Fd&& o) noexcept : fd_(std::exchange(o.fd_, -1)) {}
    Fd& operator=(Fd&& o) noexcept { if (this != &o) { reset(); fd_ = std::exchange(o.fd_, -1); } return *this; }
    ~Fd() { reset(); }
    int get() const { return fd_; }
    bool ok() const { return fd_ >= 0; }
    void reset() { if (fd_ >= 0) { ::close(fd_); fd_ = -1; } }
    void shutdown() { if (fd_ >= 0) ::shutdown(fd_, SHUT_RDWR); }

private:
    int fd_ = -1;
};

inline void set_timeouts(int fd, int ms) {
    timeval tv{ms / 1000, (ms % 1000) * 1000};
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
}

inline void tune(int fd) {
    int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
#ifdef SO_NOSIGPIPE
    ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof one);
#endif
}

/// Bind and listen (reuse-addr). The listener is non-blocking so accept loops
/// can notice shutdown.
inline Fd listen_on(const Addr& a) {
    Fd fd(::socket(AF_INET, SOCK_STREAM, 0));
    if (!fd.ok()) throw NetError("socket");
    int one = 1;
    ::setsockopt(fd.get(), SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    if (::bind(fd.get(), reinterpret_cast<const sockaddr*>(&a.sa), sizeof a.sa) != 0) throw NetError("bind " + a.str());
    if (::listen(fd.get(), 128) != 0) throw NetError("listen " + a.str());
    ::fcntl(fd.get(), F_SETFL, ::fcntl(fd.get(), F_GETFL) | O_NONBLOCK);
    return fd;
}

/// Accept one connection, or an invalid Fd after `ms` without one. The
/// accepted socket is made blocking explicitly (it can inherit non-blocking
/// mode on macOS).
inline Fd accept_for(int listener, int ms) {
    pollfd p{listener, POLLIN, 0};
    if (::poll(&p, 1, ms) <= 0) return Fd();
    int c = ::accept(listener, nullptr, nullptr);
    if (c < 0) return Fd();
    ::fcntl(c, F_SETFL, ::fcntl(c, F_GETFL) & ~O_NONBLOCK);
    tune(c);
    return Fd(c);
}

inline Fd connect_to(const Addr& a, int timeout_ms) {
    Fd fd(::socket(AF_INET, SOCK_STREAM, 0));
    if (!fd.ok()) return Fd();
    ::fcntl(fd.get(), F_SETFL, ::fcntl(fd.get(), F_GETFL) | O_NONBLOCK);
    int r = ::connect(fd.get(), reinterpret_cast<const sockaddr*>(&a.sa), sizeof a.sa);
    if (r != 0 && errno != EINPROGRESS) return Fd();
    pollfd p{fd.get(), POLLOUT, 0};
    if (::poll(&p, 1, timeout_ms) <= 0) return Fd();
    int err = 0;
    socklen_t l = sizeof err;
    ::getsockopt(fd.get(), SOL_SOCKET, SO_ERROR, &err, &l);
    if (err != 0) return Fd();
    ::fcntl(fd.get(), F_SETFL, ::fcntl(fd.get(), F_GETFL) & ~O_NONBLOCK);
    tune(fd.get());
    return fd;
}

using Clock = std::chrono::steady_clock;

inline void send_all(int fd, const std::uint8_t* d, std::size_t n) {
    while (n) {
#ifdef MSG_NOSIGNAL
        ssize_t w = ::send(fd, d, n, MSG_NOSIGNAL);
#else
        ssize_t w = ::send(fd, d, n, 0);
#endif
        if (w < 0) {
            if (errno == EINTR) continue;
            throw NetError("send failed");
        }
        d += w; n -= static_cast<std::size_t>(w);
    }
}

/// Fill `buf` unless asked to stop. false = stopped, or clean EOF at a frame
/// boundary. Throws on error, EOF mid-frame, or passing `deadline`.
inline bool read_full(int fd, std::uint8_t* buf, std::size_t n, const std::atomic<bool>* stop,
                      bool at_boundary, std::optional<Clock::time_point> deadline) {
    std::size_t got = 0;
    while (got < n) {
        if (stop && stop->load(std::memory_order_relaxed)) return false;
        if (deadline && Clock::now() >= *deadline) throw NetError("timed out");
        ssize_t r = ::recv(fd, buf + got, n - got, 0);
        if (r == 0) {
            if (got == 0 && at_boundary) return false;
            throw NetError("unexpected eof");
        }
        if (r < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) continue;  // SO_RCVTIMEO tick
            throw NetError("recv failed");
        }
        got += static_cast<std::size_t>(r);
    }
    return true;
}

struct Frame {
    std::uint8_t kind = 0;
    std::vector<std::uint8_t> body;
};

/// Append one frame (sealed when `sess` is set) to `out` without sending.
inline void put_frame(std::vector<std::uint8_t>& out, std::uint8_t kind, const std::uint8_t* body, std::size_t n, Session* sess) {
    const std::size_t extra = sess ? MAC_LEN : 0;
    const std::uint32_t len = static_cast<std::uint32_t>(1 + n + extra);
    const std::size_t start = out.size();
    out.resize(start + 8);
    out.push_back(kind);
    out.insert(out.end(), body, body + n);
    if (sess) {
        auto mac = sess->seal(kind, body, n);
        out.insert(out.end(), mac.begin(), mac.end());
    }
    const std::uint32_t crc = crc32c(out.data() + start + 8, len);
    std::memcpy(out.data() + start, &len, 4);
    std::memcpy(out.data() + start + 4, &crc, 4);
}

inline void write_frame(int fd, std::uint8_t kind, const std::uint8_t* body, std::size_t n, Session* sess) {
    std::vector<std::uint8_t> f;
    put_frame(f, kind, body, n, sess);
    send_all(fd, f.data(), f.size());
}

/// Read one frame, verifying length, checksum and (when `sess`) the MAC.
/// nullopt = stopped or clean close at a frame boundary.
inline std::optional<Frame> read_frame(int fd, const std::atomic<bool>* stop, Session* sess,
                                       std::optional<Clock::time_point> deadline = std::nullopt) {
    std::uint8_t head[8];
    if (!read_full(fd, head, 8, stop, true, deadline)) return std::nullopt;
    std::uint32_t len, crc;
    std::memcpy(&len, head, 4);
    std::memcpy(&crc, head + 4, 4);
    if (len == 0 || len > MAX_NET_FRAME) throw NetError("frame length");
    std::vector<std::uint8_t> body(len);
    if (!read_full(fd, body.data(), len, stop, false, deadline)) return std::nullopt;
    if (crc32c(body.data(), len) != crc) throw NetError("frame checksum");
    Frame f;
    f.kind = body[0];
    f.body.assign(body.begin() + 1, body.end());
    if (sess) {
        if (f.body.size() < MAC_LEN) throw NetError("frame authentication");
        const std::size_t n = f.body.size() - MAC_LEN;
        if (!sess->open(f.kind, f.body.data(), n, f.body.data() + n)) throw NetError("frame authentication");
        f.body.resize(n);
    }
    return f;
}

}  // namespace orderer::cluster
