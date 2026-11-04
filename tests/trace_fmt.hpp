// trace_fmt.hpp — the text formats of the replication conformance vectors
// (vectors/replication/README.md): shared by the trace replayer and the wire
// fixture tests.
#pragma once

#include <sstream>
#include <string>
#include <vector>

#include "orderer/consensus.hpp"

namespace tracefmt {
using namespace orderer::consensus;


inline std::string hexs(const std::vector<std::uint8_t>& d) {
    if (d.empty()) return "-";
    static const char* h = "0123456789abcdef";
    std::string s;
    for (auto b : d) { s += h[b >> 4]; s += h[b & 15]; }
    return s;
}

inline std::vector<std::uint8_t> unhex(const std::string& s) {
    std::vector<std::uint8_t> v;
    if (s == "-") return v;
    for (std::size_t i = 0; i + 1 < s.size(); i += 2) v.push_back(static_cast<std::uint8_t>(std::stoi(s.substr(i, 2), nullptr, 16)));
    return v;
}

inline std::string fmt_entry(const Entry& e) { return std::to_string(e.term) + ":" + hexs(e.data); }

inline std::string fmt_entries(const std::vector<Entry>& es) {
    if (es.empty()) return "-";
    std::string s;
    for (std::size_t i = 0; i < es.size(); ++i) { if (i) s += ","; s += fmt_entry(es[i]); }
    return s;
}

inline std::vector<Entry> parse_entries(const std::string& s) {
    std::vector<Entry> v;
    if (s == "-") return v;
    std::stringstream ss(s);
    std::string part;
    while (std::getline(ss, part, ',')) {
        auto c = part.find(':');
        v.push_back(Entry{static_cast<Term>(std::stoul(part.substr(0, c))), unhex(part.substr(c + 1))});
    }
    return v;
}

inline std::string fmt_msg(const Msg& m) {
    std::ostringstream o;
    switch (m.kind) {
        case Msg::Kind::Vote: o << "vote " << m.term << " " << m.last_index << " " << m.last_term << " " << int(m.pre); break;
        case Msg::Kind::VoteReply: o << "votereply " << m.term << " " << int(m.granted) << " " << int(m.pre); break;
        case Msg::Kind::Append:
            o << "append " << m.term << " " << m.prev_index << " " << m.prev_term << " " << m.commit << " " << fmt_entries(m.entries);
            break;
        case Msg::Kind::AppendReply: o << "appendreply " << m.term << " " << int(m.success) << " " << m.index; break;
    }
    return o.str();
}

inline Msg parse_msg(const std::vector<std::string>& w, std::size_t at) {
    auto n = [&](std::size_t i) { return std::stoull(w.at(at + i)); };
    const std::string& k = w.at(at);
    if (k == "vote") return Msg::vote(static_cast<Term>(n(1)), n(2), static_cast<Term>(n(3)), n(4) == 1);
    if (k == "votereply") return Msg::vote_reply(static_cast<Term>(n(1)), n(2) == 1, n(3) == 1);
    if (k == "append") return Msg::append(static_cast<Term>(n(1)), n(2), static_cast<Term>(n(3)), parse_entries(w.at(at + 5)), n(4));
    if (k == "appendreply") return Msg::append_reply(static_cast<Term>(n(1)), n(2) == 1, n(3));
    throw std::runtime_error("unknown message " + k);
}

inline std::string fmt_effect(const Effect& e) {
    std::ostringstream o;
    switch (e.kind) {
        case Effect::Kind::Send: o << "send " << int(e.to) << " " << fmt_msg(e.msg); break;
        case Effect::Kind::Persist: {
            const auto& p = e.persist;
            o << "persist " << p.token << " " << p.term << " ";
            if (p.voted_for) o << int(*p.voted_for); else o << "-";
            o << " " << p.from << " ";
            if (p.compact) o << p.compact->first << ":" << p.compact->second; else o << "-";
            o << " " << fmt_entries(p.entries);
            break;
        }
        case Effect::Kind::Apply: o << "apply " << e.index << " " << fmt_entry(e.entry); break;
        case Effect::Kind::NeedSnapshot: o << "needsnap " << int(e.to) << " " << e.index << " " << e.term; break;
        case Effect::Kind::Restore: o << "restore " << e.index << " " << e.term; break;
    }
    return o.str();
}

inline std::string fmt_state(const Node& n) {
    std::ostringstream o;
    o << "state role=" << role_name(n.role()) << " term=" << n.term() << " leader=";
    if (n.leader()) o << int(*n.leader()); else o << "-";
    o << " commit=" << n.commit_index() << " applied=" << n.applied_index() << " base=" << n.base() << " last=" << n.last_index();
    return o.str();
}

inline std::vector<std::string> words(const std::string& line) {
    std::vector<std::string> w;
    std::istringstream is(line);
    std::string t;
    while (is >> t) w.push_back(t);
    return w;
}

inline std::string field(const std::string& line, const std::string& key) {
    for (const auto& w : words(line)) if (w.rfind(key + "=", 0) == 0) return w.substr(key.size() + 1);
    throw std::runtime_error("missing " + key);
}


inline std::string fmt_disk(const Disk& d) {
    std::ostringstream o;
    o << d.term << ":";
    if (d.voted_for) o << int(*d.voted_for); else o << "-";
    o << ":" << d.base << ":" << d.base_term << ":" << fmt_entries(d.log);
    return o.str();
}

}  // namespace tracefmt
