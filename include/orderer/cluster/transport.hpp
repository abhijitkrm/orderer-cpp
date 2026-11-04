// transport.hpp — TCP transport between replicas, wire-compatible with
// orderer-rust (transport.rs). Sends never block the protocol: each peer has a
// bounded queue and a thread that reconnects with backoff; a full queue or a
// down peer drops messages, which the protocol tolerates. Snapshots stream in
// chunks and are verified by length and checksum before they are accepted.
// With a cluster key, every connection runs the challenge-response handshake
// of auth.hpp and every frame is MAC'd.
#pragma once

#include <algorithm>
#include <condition_variable>
#include <fcntl.h>
#include <set>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <variant>

#include "net.hpp"
#include "snapstore.hpp"

namespace orderer::cluster {

inline constexpr std::uint8_t K_MSG = 1, K_SNAP_BEGIN = 2, K_SNAP_CHUNK = 3, K_SNAP_END = 4, K_DIGEST = 5, K_HELLO = 6,
                              K_CHALLENGE = 7, K_AUTH = 8;
inline constexpr std::size_t SNAP_CHUNK = 1u << 20;
inline constexpr std::size_t PEER_QUEUE = 8192;

/// What the transport hands to the node.
struct Inbound {
    enum class Kind { Msg, Snapshot, Digest } kind = Kind::Msg;
    NodeId from = 0;
    Msg msg;                                        // Msg
    Term term = 0, last_term = 0;                   // Snapshot
    Index index = 0;                                // Snapshot, Digest
    Digest digest{};                                // Digest
};

/// Test hook that cuts links: allows(from, to) is checked on every send and receive.
class Filter {
public:
    void block(NodeId a, NodeId b) {
        std::lock_guard g(m_);
        blocked_.insert({a, b});
        blocked_.insert({b, a});
    }
    void isolate(NodeId n, const std::vector<NodeId>& all) { for (NodeId o : all) if (o != n) block(n, o); }
    void heal() { std::lock_guard g(m_); blocked_.clear(); }
    bool allows(NodeId from, NodeId to) const {
        std::lock_guard g(m_);
        return !blocked_.count({from, to});
    }

private:
    mutable std::mutex m_;
    std::set<std::pair<NodeId, NodeId>> blocked_;
};

struct NetStats {
    std::atomic<std::uint64_t> sent{0}, dropped{0}, received{0}, bad_frames{0};
};

class Transport {
public:
    using Sink = std::function<void(Inbound)>;

    Transport(NodeId me, std::vector<std::pair<NodeId, Addr>> members, std::uint64_t cluster_id, Sink sink,
              std::shared_ptr<SnapStore> snaps, std::shared_ptr<Filter> filter, std::optional<std::vector<std::uint8_t>> key)
        : me_(me), members_(std::move(members)), cluster_id_(cluster_id), sink_(std::move(sink)),
          snaps_(std::move(snaps)), filter_(std::move(filter)), key_(std::move(key)) {
        ignore_sigpipe();
        Addr mine;
        for (auto& [id, a] : members_) { ids_.push_back(id); if (id == me_) mine = a; }
        listener_ = listen_on(mine);
        acceptor_ = std::thread([this] { accept_loop(); });
        for (auto& [id, a] : members_) {
            if (id == me_) continue;
            auto peer = std::make_unique<Peer>();
            peer->addr = a;
            peer->thread = std::thread([this, id = id, p = peer.get()] { sender_loop(id, *p); });
            peers_[id] = std::move(peer);
        }
    }
    ~Transport() { shutdown(); }

    void send(NodeId to, Msg m) { enqueue(to, Job{Job::Kind::Msg, std::move(m), 0, 0, 0, {}, 0}); }
    /// Stream the stored snapshot at `index` to `to`.
    void send_snapshot(NodeId to, Term term, Index index, Term last_term) {
        Job j; j.kind = Job::Kind::Snapshot; j.term = term; j.index = index; j.last_term = last_term;
        enqueue(to, std::move(j));
    }
    void send_digest(NodeId to, Index index, const Digest& d) {
        Job j; j.kind = Job::Kind::Digest; j.index = index; j.digest = d;
        enqueue(to, std::move(j));
    }

    void shutdown() {
        if (stop_.exchange(true)) return;
        for (auto& [id, p] : peers_) { std::lock_guard g(p->m); p->cv.notify_all(); }
        if (acceptor_.joinable()) acceptor_.join();
        for (auto& [id, p] : peers_) if (p->thread.joinable()) p->thread.join();
        std::vector<std::thread> rs;
        { std::lock_guard g(readers_m_); rs.swap(readers_); }
        for (auto& t : rs) if (t.joinable()) t.join();
    }

    NetStats stats;

private:
    struct Job {
        enum class Kind { Msg, Snapshot, Digest } kind;
        Msg msg;
        Term term, last_term;
        Index index;
        Digest digest;
        int pad;
    };
    struct Peer {
        Addr addr;
        std::mutex m;
        std::condition_variable cv;
        std::deque<Job> q;
        std::thread thread;
    };

    void enqueue(NodeId to, Job j) {
        if (!filter_->allows(me_, to)) { ++stats.dropped; return; }
        auto it = peers_.find(to);
        if (it == peers_.end()) return;
        Peer& p = *it->second;
        {
            std::lock_guard g(p.m);
            if (p.q.size() >= PEER_QUEUE) { ++stats.dropped; return; }
            p.q.push_back(std::move(j));
        }
        p.cv.notify_one();
    }

    std::vector<std::uint8_t> hello_body() const {
        std::vector<std::uint8_t> b = {me_};
        put_u64(b, cluster_id_);
        return b;
    }

    // ---- outbound ----
    std::optional<Session> initiator_handshake(int fd, NodeId peer) {
        const auto ns = nonce();
        auto h = hello_body();
        h.insert(h.end(), ns.begin(), ns.end());
        write_frame(fd, K_HELLO, h.data(), h.size(), nullptr);
        set_timeouts(fd, 100);
        auto f = read_frame(fd, &stop_, nullptr, Clock::now() + std::chrono::seconds(2));
        if (!f || f->kind != K_CHALLENGE || f->body.size() != NONCE_LEN + MAC_LEN) return std::nullopt;
        const std::uint8_t ids[2] = {me_, peer};
        const auto want = handshake(*key_, "chal", {{ns.data(), NONCE_LEN}, {f->body.data(), NONCE_LEN}, {ids, 2}});
        if (!ct_eq(want.data(), f->body.data() + NONCE_LEN, MAC_LEN)) return std::nullopt;
        Session s(handshake(*key_, "sess", {{ns.data(), NONCE_LEN}, {f->body.data(), NONCE_LEN}, {ids, 2}}), 0);
        write_frame(fd, K_AUTH, nullptr, 0, &s);  // proves this side knows the key too
        return s;
    }

    void sender_loop(NodeId peer, Peer& p) {
        int backoff_ms = 20;
        while (!stop_) {
            Fd conn = connect_to(p.addr, 300);
            if (!conn.ok()) {
                { std::lock_guard g(p.m); stats.dropped += p.q.size(); p.q.clear(); }  // shed what queued meanwhile
                std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
                backoff_ms = std::min(backoff_ms * 2, 500);
                continue;
            }
            backoff_ms = 20;
            std::optional<Session> sess;
            try {
                if (key_) {
                    sess = initiator_handshake(conn.get(), peer);
                    if (!sess) { ++stats.bad_frames; std::this_thread::sleep_for(std::chrono::milliseconds(200)); continue; }
                } else {
                    auto h = hello_body();
                    write_frame(conn.get(), K_HELLO, h.data(), h.size(), nullptr);
                }
                Session* sp = sess ? &*sess : nullptr;
                std::vector<std::uint8_t> out;
                while (!stop_) {
                    std::deque<Job> batch;
                    {
                        std::unique_lock g(p.m);
                        p.cv.wait_for(g, std::chrono::milliseconds(50), [&] { return !p.q.empty() || stop_; });
                        batch.swap(p.q);
                    }
                    for (auto& job : batch) {
                        if (!filter_->allows(me_, peer)) continue;
                        ++stats.sent;
                        write_job(conn.get(), out, sp, job);
                        if (out.size() > (256u << 10)) { send_all(conn.get(), out.data(), out.size()); out.clear(); }
                    }
                    if (!out.empty()) { send_all(conn.get(), out.data(), out.size()); out.clear(); }
                }
            } catch (const std::exception&) {
                // connection lost: reconnect
            }
        }
    }

    void write_job(int fd, std::vector<std::uint8_t>& out, Session* sp, const Job& job) {
        std::vector<std::uint8_t> b;
        switch (job.kind) {
            case Job::Kind::Msg:
                encode_msg(job.msg, b);
                put_frame(out, K_MSG, b.data(), b.size(), sp);
                break;
            case Job::Kind::Digest:
                put_u64(b, job.index);
                b.insert(b.end(), job.digest.begin(), job.digest.end());
                put_frame(out, K_DIGEST, b.data(), b.size(), sp);
                break;
            case Job::Kind::Snapshot: stream_snapshot(fd, out, sp, job); break;
        }
    }

    void stream_snapshot(int fd, std::vector<std::uint8_t>& out, Session* sp, const Job& job) {
        auto snap = snaps_->get(job.index);
        if (!snap) return;  // gone (superseded): the peer will ask again
        std::vector<std::uint8_t> b;
        put_u32(b, job.term); put_u64(b, job.index); put_u32(b, job.last_term);
        put_u64(b, snap->first.len); put_u32(b, snap->first.crc);
        put_frame(out, K_SNAP_BEGIN, b.data(), b.size(), sp);
        const auto& data = snap->second;
        for (std::size_t at = 0; at < data.size(); at += SNAP_CHUNK) {
            put_frame(out, K_SNAP_CHUNK, data.data() + at, std::min(SNAP_CHUNK, data.size() - at), sp);
            send_all(fd, out.data(), out.size());
            out.clear();
        }
        put_frame(out, K_SNAP_END, nullptr, 0, sp);
    }

    // ---- inbound ----
    void accept_loop() {
        while (!stop_) {
            Fd c = accept_for(listener_.get(), 50);
            if (!c.ok()) continue;
            std::lock_guard g(readers_m_);
            auto fd = std::make_shared<Fd>(std::move(c));
            readers_.emplace_back([this, fd] {
                try { reader_loop(*fd); } catch (const std::exception&) {}
            });
        }
    }

    void reader_loop(Fd& conn) {
        set_timeouts(conn.get(), 100);
        const auto first = read_frame(conn.get(), &stop_, nullptr, Clock::now() + std::chrono::seconds(5));
        if (!first) return;
        const std::size_t want_len = key_ ? 9 + NONCE_LEN : 9;
        auto bad = [&] { ++stats.bad_frames; };
        if (first->kind != K_HELLO || first->body.size() != want_len) { bad(); return; }
        const NodeId from = first->body[0];
        std::uint64_t cid;
        std::memcpy(&cid, first->body.data() + 1, 8);
        if (cid != cluster_id_ || from == me_ || std::find(ids_.begin(), ids_.end(), from) == ids_.end()) { bad(); return; }
        std::optional<Session> sess;
        if (key_) {
            const std::uint8_t* ns = first->body.data() + 9;
            const auto nr = nonce();
            const std::uint8_t ids[2] = {from, me_};
            const auto mac = handshake(*key_, "chal", {{ns, NONCE_LEN}, {nr.data(), NONCE_LEN}, {ids, 2}});
            std::vector<std::uint8_t> reply(nr.begin(), nr.end());
            reply.insert(reply.end(), mac.begin(), mac.begin() + MAC_LEN);
            write_frame(conn.get(), K_CHALLENGE, reply.data(), reply.size(), nullptr);
            sess.emplace(handshake(*key_, "sess", {{ns, NONCE_LEN}, {nr.data(), NONCE_LEN}, {ids, 2}}), 1);
            auto a = read_frame(conn.get(), &stop_, &*sess, Clock::now() + std::chrono::seconds(2));
            if (!a || a->kind != K_AUTH || !a->body.empty()) { bad(); return; }
        }
        std::optional<SnapStore::Incoming> incoming;
        Term in_term = 0, in_last_term = 0;
        Index in_index = 0;
        while (!stop_) {
            std::optional<Frame> f;
            try { f = read_frame(conn.get(), &stop_, sess ? &*sess : nullptr); }
            catch (const NetError& e) {
                if (std::string(e.what()).find("frame") != std::string::npos) bad();
                throw;
            }
            if (!f) return;
            if (!filter_->allows(from, me_)) continue;
            ++stats.received;
            Reader r(f->body.data(), f->body.size());
            try {
                switch (f->kind) {
                    case K_MSG: {
                        Inbound in; in.kind = Inbound::Kind::Msg; in.from = from; in.msg = decode_msg(r);
                        r.finish();
                        sink_(std::move(in));
                        break;
                    }
                    case K_DIGEST: {
                        Inbound in; in.kind = Inbound::Kind::Digest; in.from = from; in.index = r.u64();
                        std::memcpy(in.digest.data(), r.bytes(32), 32);
                        sink_(std::move(in));
                        break;
                    }
                    case K_SNAP_BEGIN: {
                        in_term = r.u32(); in_index = r.u64(); in_last_term = r.u32();
                        const std::uint64_t len = r.u64();
                        const std::uint32_t crc = r.u32();
                        incoming.reset();
                        incoming.emplace(snaps_->receive(in_index, in_last_term, len, crc));
                        break;
                    }
                    case K_SNAP_CHUNK:
                        if (!incoming) { bad(); return; }
                        incoming->write(f->body.data(), f->body.size());
                        break;
                    case K_SNAP_END: {
                        if (!incoming) { bad(); return; }
                        incoming->finish();
                        incoming.reset();
                        Inbound in; in.kind = Inbound::Kind::Snapshot; in.from = from; in.term = in_term;
                        in.index = in_index; in.last_term = in_last_term;
                        sink_(std::move(in));
                        break;
                    }
                    default: bad(); return;
                }
            } catch (const CodecError&) { bad(); return; }
        }
    }

    NodeId me_;
    std::vector<std::pair<NodeId, Addr>> members_;
    std::vector<NodeId> ids_;
    std::uint64_t cluster_id_;
    Sink sink_;
    std::shared_ptr<SnapStore> snaps_;
    std::shared_ptr<Filter> filter_;
    std::optional<std::vector<std::uint8_t>> key_;
    std::atomic<bool> stop_{false};
    Fd listener_;
    std::thread acceptor_;
    std::map<NodeId, std::unique_ptr<Peer>> peers_;
    std::mutex readers_m_;
    std::vector<std::thread> readers_;
};

}  // namespace orderer::cluster
