// client.hpp — the client protocol, wire-compatible with orderer-rust
// (client.rs): requests PROPOSE / STATUS / HASH / BARRIER over the framing of
// net.hpp, a leader-following Client, and exactly-once sessions. A server
// holding a cluster key requires the challenge-response handshake.
#pragma once

#include <sstream>

#include "replica.hpp"

namespace orderer::cluster {

inline constexpr std::uint8_t Q_HELLO = 0, Q_PROPOSE = 1, Q_STATUS = 2, Q_HASH = 3, Q_BARRIER = 4;
inline constexpr std::uint8_t CR_OK = 1, CR_NOT_LEADER = 2, CR_BUSY = 3, CR_UNKNOWN = 4, CR_ERR = 5, CR_TEXT = 6, CR_HASH = 7,
                              CR_CHALLENGE = 8;

/// Serves one replica's client port. The Replica must outlive the server.
class ClientServer {
public:
    ClientServer(const Addr& addr, Replica& replica, std::optional<std::vector<std::uint8_t>> key = std::nullopt)
        : replica_(replica), key_(std::move(key)) {
        ignore_sigpipe();
        listener_ = listen_on(addr);
        thread_ = std::thread([this] {
            std::vector<std::thread> conns;
            while (!stop_) {
                Fd c = accept_for(listener_.get(), 50);
                if (!c.ok()) continue;
                auto fd = std::make_shared<Fd>(std::move(c));
                conns.emplace_back([this, fd] { try { serve(*fd); } catch (const std::exception&) {} });
            }
            for (auto& t : conns) if (t.joinable()) t.join();
        });
    }
    ~ClientServer() { shutdown(); }
    void shutdown() {
        if (stop_.exchange(true)) return;
        if (thread_.joinable()) thread_.join();
    }

private:
    static std::string role_name(consensus::Role r) { return consensus::role_name(r); }

    void reply(int fd, Session* s, std::uint8_t kind, const std::vector<std::uint8_t>& body) {
        write_frame(fd, kind, body.data(), body.size(), s);
    }
    void reply(int fd, Session* s, std::uint8_t kind, const std::string& text) {
        write_frame(fd, kind, reinterpret_cast<const std::uint8_t*>(text.data()), text.size(), s);
    }

    void serve(Fd& conn) {
        set_timeouts(conn.get(), 200);
        std::optional<Session> sess;
        if (key_) {
            auto hello = read_frame(conn.get(), &stop_, nullptr, Clock::now() + std::chrono::seconds(5));
            if (!hello || hello->kind != Q_HELLO || hello->body.size() != NONCE_LEN) return;
            const auto ns = nonce();
            const auto mac = handshake(*key_, "cchal", {{hello->body.data(), NONCE_LEN}, {ns.data(), NONCE_LEN}});
            std::vector<std::uint8_t> r(ns.begin(), ns.end());
            r.insert(r.end(), mac.begin(), mac.begin() + MAC_LEN);
            write_frame(conn.get(), CR_CHALLENGE, r.data(), r.size(), nullptr);
            sess.emplace(handshake(*key_, "csess", {{hello->body.data(), NONCE_LEN}, {ns.data(), NONCE_LEN}}), 1);
        }
        Session* sp = sess ? &*sess : nullptr;
        while (!stop_) {
            auto f = read_frame(conn.get(), &stop_, sp);
            if (!f) return;
            switch (f->kind) {
                case Q_HELLO: reply(conn.get(), sp, CR_ERR, std::string("this server does not use authentication")); return;
                case Q_PROPOSE: answer(conn.get(), sp, replica_.propose(std::move(f->body))); break;
                case Q_BARRIER: answer(conn.get(), sp, replica_.barrier()); break;
                case Q_STATUS: {
                    const Status s = replica_.status();
                    std::ostringstream o;
                    o << "id=" << int(s.id) << " role=" << role_name(s.role) << " term=" << s.term << " leader="
                      << (s.leader ? int(*s.leader) : -1) << " commit=" << s.commit << " applied=" << s.applied << " base=" << s.base
                      << " last=" << s.last_index << " pending=" << s.pending << " halted=" << (s.halted.empty() ? "-" : s.halted)
                      << " digests_verified=" << s.digests_verified << "\n";
                    reply(conn.get(), sp, CR_TEXT, o.str());
                    break;
                }
                case Q_HASH: {
                    if (auto h = replica_.state_hash()) {
                        std::vector<std::uint8_t> b;
                        put_u64(b, h->first);
                        b.insert(b.end(), h->second.begin(), h->second.end());
                        reply(conn.get(), sp, CR_HASH, b);
                    } else reply(conn.get(), sp, CR_ERR, std::string("state hash unsupported"));
                    break;
                }
                default: reply(conn.get(), sp, CR_ERR, std::string("unknown request"));
            }
        }
    }

    void answer(int fd, Session* sp, const ProposeResult& r) {
        using K = ProposeResult::Kind;
        switch (r.kind) {
            case K::Ok: { std::vector<std::uint8_t> b; put_u64(b, r.index); reply(fd, sp, CR_OK, b); break; }
            case K::NotLeader: reply(fd, sp, CR_NOT_LEADER, std::vector<std::uint8_t>{r.hint ? *r.hint : std::uint8_t(255)}); break;
            case K::Stopped: reply(fd, sp, CR_NOT_LEADER, std::vector<std::uint8_t>{255}); break;
            case K::Busy: reply(fd, sp, CR_BUSY, std::vector<std::uint8_t>{}); break;
            default: reply(fd, sp, CR_UNKNOWN, std::string("unknown outcome"));
        }
    }

    Replica& replica_;
    std::optional<std::vector<std::uint8_t>> key_;
    std::atomic<bool> stop_{false};
    Fd listener_;
    std::thread thread_;
};

struct ClientResult {
    enum class Kind { Ok, NoLeader, Unknown, Protocol } kind = Kind::Ok;
    Index index = 0;
    std::string text;
    bool ok() const { return kind == Kind::Ok; }
};

/// A connection to one replica.
class Conn {
public:
    static std::optional<Conn> connect(const Addr& addr, const std::optional<std::vector<std::uint8_t>>& key = std::nullopt) {
        Fd fd = connect_to(addr, 500);
        if (!fd.ok()) return std::nullopt;
        set_timeouts(fd.get(), 30000);
        std::optional<Session> sess;
        if (key) {
            const auto nc = nonce();
            write_frame(fd.get(), Q_HELLO, nc.data(), nc.size(), nullptr);
            auto f = read_frame(fd.get(), nullptr, nullptr);
            if (!f || f->kind != CR_CHALLENGE || f->body.size() != NONCE_LEN + MAC_LEN) return std::nullopt;
            const auto want = handshake(*key, "cchal", {{nc.data(), NONCE_LEN}, {f->body.data(), NONCE_LEN}});
            if (!ct_eq(want.data(), f->body.data() + NONCE_LEN, MAC_LEN)) return std::nullopt;
            sess.emplace(handshake(*key, "csess", {{nc.data(), NONCE_LEN}, {f->body.data(), NONCE_LEN}}), 0);
        }
        return Conn(std::move(fd), std::move(sess));
    }

    /// nullopt on any I/O or protocol failure.
    std::optional<Frame> call(std::uint8_t kind, const std::vector<std::uint8_t>& body = {}) {
        try {
            write_frame(fd_.get(), kind, body.data(), body.size(), sess_ ? &*sess_ : nullptr);
            return read_frame(fd_.get(), nullptr, sess_ ? &*sess_ : nullptr);
        } catch (const std::exception&) { return std::nullopt; }
    }

    std::optional<std::string> status() {
        auto f = call(Q_STATUS);
        if (!f || f->kind != CR_TEXT) return std::nullopt;
        return std::string(f->body.begin(), f->body.end());
    }
    /// A linearizable read barrier (this node must be the leader).
    std::optional<Index> barrier() {
        auto f = call(Q_BARRIER);
        if (!f || f->kind != CR_OK || f->body.size() != 8) return std::nullopt;
        Index i; std::memcpy(&i, f->body.data(), 8); return i;
    }
    std::optional<std::pair<Index, Digest>> hash() {
        auto f = call(Q_HASH);
        if (!f || f->kind != CR_HASH || f->body.size() != 40) return std::nullopt;
        std::pair<Index, Digest> r;
        std::memcpy(&r.first, f->body.data(), 8);
        std::memcpy(r.second.data(), f->body.data() + 8, 32);
        return r;
    }

private:
    friend class Client;
    Conn(Fd fd, std::optional<Session> s) : fd_(std::move(fd)), sess_(std::move(s)) {}
    Fd fd_;
    std::optional<Session> sess_;
};

/// Submits batches to a cluster, following leader hints and surviving restarts.
class Client {
public:
    explicit Client(std::vector<std::pair<NodeId, Addr>> addrs) : addrs_(std::move(addrs)), conns_(addrs_.size()) {}

    Client& with_key(std::vector<std::uint8_t> key) { key_ = std::move(key); return *this; }
    /// Exactly-once submission under session id `id` (one sequential stream per id).
    Client& with_session(std::uint64_t id) { session_ = {id, 0}; return *this; }
    int retry_budget_ms = 30000;

    /// Submit an encoded payload. Ok = committed on a quorum. Unknown = may still commit.
    ClientResult propose(const std::vector<std::uint8_t>& payload) {
        for (int attempt = 0; attempt < 600; ++attempt) {
            const std::size_t i = current_ % addrs_.size();
            if (!conns_[i]) conns_[i] = Conn::connect(addrs_[i].second, key_);
            if (!conns_[i]) { ++current_; std::this_thread::sleep_for(std::chrono::milliseconds(10)); continue; }
            auto f = conns_[i]->call(Q_PROPOSE, payload);
            if (!f) {
                // Died mid-request: it may have been accepted. Unknown, never a blind retry.
                conns_[i].reset();
                ++current_;
                return {ClientResult::Kind::Unknown, 0, "connection lost"};
            }
            switch (f->kind) {
                case CR_OK: {
                    if (f->body.size() != 8) return {ClientResult::Kind::Protocol, 0, "bad reply"};
                    Index idx; std::memcpy(&idx, f->body.data(), 8);
                    return {ClientResult::Kind::Ok, idx, ""};
                }
                case CR_NOT_LEADER: {
                    std::size_t pos = addrs_.size();
                    if (!f->body.empty())
                        for (std::size_t k = 0; k < addrs_.size(); ++k) if (addrs_[k].first == f->body[0]) pos = k;
                    if (pos < addrs_.size()) current_ = pos; else ++current_;
                    std::this_thread::sleep_for(std::chrono::milliseconds(5));
                    break;
                }
                case CR_BUSY: std::this_thread::sleep_for(std::chrono::milliseconds(2)); break;
                case CR_UNKNOWN: conns_[i].reset(); return {ClientResult::Kind::Unknown, 0, std::string(f->body.begin(), f->body.end())};
                default: return {ClientResult::Kind::Protocol, 0, "reply " + std::to_string(f->kind)};
            }
        }
        return {ClientResult::Kind::NoLeader, 0, ""};
    }

    /// Submit a batch with exactly-once effect: the same (session, seq) is
    /// retried through lost connections and leader changes until it is
    /// definitely committed.
    ClientResult propose_session(const CmdSym* cmds, std::size_t n) {
        if (!session_) return {ClientResult::Kind::Protocol, 0, "no session: call with_session first"};
        if (!pending_) {
            const std::uint64_t seq = session_->second + 1;
            std::vector<std::uint8_t> payload;
            encode_batch(cmds, n, payload, std::pair{session_->first, seq});
            session_->second = seq;
            pending_ = std::move(payload);
        }
        const auto deadline = Clock::now() + std::chrono::milliseconds(retry_budget_ms);
        for (;;) {
            ClientResult r = propose(*pending_);
            if (r.ok()) { pending_.reset(); return r; }
            if (r.kind == ClientResult::Kind::Protocol) return r;
            if (Clock::now() >= deadline) return r;
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }

private:
    std::vector<std::pair<NodeId, Addr>> addrs_;
    std::vector<std::optional<Conn>> conns_;
    std::size_t current_ = 0;
    std::optional<std::vector<std::uint8_t>> key_;
    std::optional<std::pair<std::uint64_t, std::uint64_t>> session_;
    std::optional<std::vector<std::uint8_t>> pending_;
};

}  // namespace orderer::cluster
