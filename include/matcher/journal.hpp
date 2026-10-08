// Journaling (spec/JOURNAL.md): append-only canonical command/event lines.
// The journal is I/O policy on top of the deterministic core — it never
// changes matching semantics.
#pragma once

#include <cstdio>
#include <string>

#include "sink.hpp"
#include "types.hpp"

namespace matcher {

/// Writes one canonical command line per record() call. sym != nullptr emits
/// the `engine:true` tagged form.
template <class Writer>
class CmdJournal {
  public:
    explicit CmdJournal(Writer& w, const Symbol* sym = nullptr) : w_(w), sym_(sym) {}

    void record(const Command& c) {
        scratch_.clear();
        write_cmd(c, sym_, scratch_);
        scratch_.push_back('\n');
        w_(scratch_);
    }

  private:
    Writer& w_;
    const Symbol* sym_;
    std::string scratch_;

    static void write_cmd(const Command& c, const Symbol* sym, std::string& b) {
        auto symf = [&](std::string_view tag) {
            b += tag;
            if (sym) {
                b += ",\"symbol\":";
                b += std::to_string(*sym);
            }
        };
        switch (c.kind) {
            case Command::Kind::New:
                symf("{\"cmd\":\"new\"");
                b += ",\"order_id\":" + std::to_string(c.order_id);
                b += ",\"side\":\"";
                b += c.side == Side::Ask ? "ask" : "bid";
                b += "\",\"otype\":\"";
                b += c.otype == OType::Market ? "market" : "limit";
                if (c.otype == OType::Limit) {
                    b += "\",\"price\":" + std::to_string(c.price);
                    b += ",\"qty\":" + std::to_string(c.qty);
                } else {
                    b += "\",\"qty\":" + std::to_string(c.qty);
                }
                b += ",\"tif\":\"";
                b += tif_str(c.tif);
                b += "\"}";
                break;
            case Command::Kind::Cancel:
                symf("{\"cmd\":\"cancel\"");
                b += ",\"order_id\":" + std::to_string(c.order_id) + "}";
                break;
            case Command::Kind::Replace:
                symf("{\"cmd\":\"replace\"");
                b += ",\"order_id\":" + std::to_string(c.order_id);
                b += ",\"price\":" + std::to_string(c.price);
                b += ",\"qty\":" + std::to_string(c.qty) + "}";
                break;
        }
    }

    static const char* tif_str(Tif t) {
        switch (t) {
            case Tif::Ioc: return "ioc";
            case Tif::Fok: return "fok";
            case Tif::PostOnly: return "post_only";
            default: return "gtc";
        }
    }
};

/// Sink decorator: journals each event line then forwards to the inner sink.
template <class Inner, class Writer>
class EvtJournal {
  public:
    EvtJournal(Inner& inner, Writer& w) : inner_(inner), w_(w) {}

    void on_event(std::uint64_t seq, const Event& e) {
        scratch_.clear();
        write_canonical(seq, e, scratch_);
        scratch_.push_back('\n');
        w_(scratch_);
        inner_.on_event(seq, e);
    }

  private:
    Inner& inner_;
    Writer& w_;
    std::string scratch_;
};

/// Journals one symbol-tagged engine event — for submit_tagged closures.
template <class Writer>
inline void journal_event(Symbol sym, std::uint64_t seq, const Event& e, Writer& w,
                          std::string& scratch) {
    scratch.clear();
    write_canonical_sym(seq, sym, e, scratch);
    scratch.push_back('\n');
    w(scratch);
}

} // namespace matcher
