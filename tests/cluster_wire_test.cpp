// The replication runtime's storage and wire layer against fixtures written by
// orderer-rust (vectors/replication-wire, from `wirefix`): messages, WAL frame
// bodies, command batches, WAL files in every damaged state, and snapshots.
// Decoding must give the same values, re-encoding the same bytes, recovery the
// same disk image, and files written here must equal the Rust files byte for
// byte.
//
//   cluster_wire_test <vectors-dir>
#include <filesystem>
#include <fstream>
#include <random>
#include <sstream>

#include "check.hpp"
#include "orderer/cluster/auth.hpp"
#include "orderer/cluster/snapstore.hpp"
#include "trace_fmt.hpp"

using namespace orderer::cluster;
namespace fs = std::filesystem;

namespace {

fs::path WIRE;

std::string slurp(const fs::path& p) {
    std::ifstream f(p, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(f), {});
}

std::vector<std::uint8_t> unhex(const std::string& s) { return tracefmt::unhex(s.empty() ? "-" : s); }

// Lines of "hex | rest"
std::vector<std::pair<std::string, std::string>> fixture_lines(const std::string& name) {
    std::vector<std::pair<std::string, std::string>> v;
    std::istringstream in(slurp(WIRE / name));
    std::string line;
    while (std::getline(in, line)) {
        auto bar = line.find(" | ");
        if (bar == std::string::npos) continue;
        v.emplace_back(line.substr(0, bar), line.substr(bar + 3));
    }
    return v;
}

fs::path scratch(const std::string& name) {
    auto d = fs::temp_directory_path() / ("orderer-cpp-wire-" + name + "-" + std::to_string(::getpid()));
    fs::remove_all(d);
    fs::create_directories(d);
    return d;
}

std::string hexstr(const std::vector<std::uint8_t>& b) { return b.empty() ? "" : hex(b.data(), b.size()); }

}  // namespace

TEST(crc_matches_the_check_value_and_the_bytewise_journal_crc) {
    const char* nine = "123456789";
    CHECK(crc32c(reinterpret_cast<const std::uint8_t*>(nine), 9) == 0xE3069283u);
    std::mt19937_64 rng(7);
    std::vector<std::uint8_t> data(5000);
    for (auto& b : data) b = static_cast<std::uint8_t>(rng());
    for (std::size_t start = 0; start < 9; ++start)
        for (std::size_t len = 0; len < 300; ++len)
            CHECK(crc32c(data.data() + start, len) == orderer::crc32c(data.data() + start, len)) << start << "/" << len;
    for (std::size_t step : {1u, 3u, 8u, 13u, 999u}) {
        std::uint32_t c = ~0u;
        for (std::size_t i = 0; i < data.size(); i += step) c = crc32c_update(c, data.data() + i, std::min(step, data.size() - i));
        CHECK(~c == crc32c(data.data(), data.size())) << "step " << step;
    }
}

TEST(sha256_and_hmac_match_the_standard_vectors) {
    auto h = [](const std::string& s) { return hex(sha256(reinterpret_cast<const std::uint8_t*>(s.data()), s.size())); };
    CHECK(h("") == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    CHECK(h("abc") == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    CHECK(h(std::string(1000000, 'a')) == "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0");
    auto m = [](const std::string& k, const std::string& s) {
        return hex(hmac_sha256(reinterpret_cast<const std::uint8_t*>(k.data()), k.size(), reinterpret_cast<const std::uint8_t*>(s.data()), s.size()));
    };
    CHECK(m(std::string(20, '\x0b'), "Hi There") == "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7");
    CHECK(m("Jefe", "what do ya want for nothing?") == "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843");
    CHECK(m(std::string(131, '\xaa'), "Test Using Larger Than Block-Size Key - Hash Key First") ==
          "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54");
}

TEST(sessions_detect_tampering_replay_reordering_and_reflection) {
    Digest k{};
    k.fill(7);
    Session a(k, 0), b(k, 1);
    const std::uint8_t one[] = {'o', 'n', 'e'}, two[] = {'t', 'w', 'o'}, twp[] = {'t', 'w', 'p'};
    auto m1 = a.seal(1, one, 3);
    auto m2 = a.seal(1, two, 3);
    CHECK(b.open(1, one, 3, m1.data()));
    CHECK(!b.open(1, one, 3, m1.data())) << "replay";
    CHECK(!b.open(1, twp, 3, m2.data())) << "altered body";
    CHECK(!b.open(2, two, 3, m2.data())) << "altered kind";
    CHECK(b.open(1, two, 3, m2.data())) << "rejections must not advance state";
    (void)a.seal(1, one, 3);
    auto m4 = a.seal(1, one, 3);
    CHECK(!b.open(1, one, 3, m4.data())) << "a dropped frame";
    Session a2(k, 0);
    auto m = a2.seal(1, one, 3);
    CHECK(!a2.open(1, one, 3, m.data())) << "reflection";
    Digest k2{};
    k2.fill(8);
    Session c(k2, 1);
    Session a3(k, 0);
    auto mm = a3.seal(1, one, 3);
    CHECK(!c.open(1, one, 3, mm.data())) << "wrong key";
}

TEST(messages_decode_and_re_encode_exactly_as_rust_wrote_them) {
    auto lines = fixture_lines("msgs.txt");
    CHECK(lines.size() >= 8);
    for (auto& [hx, text] : lines) {
        auto bytes = unhex(hx);
        Reader r(bytes.data(), bytes.size());
        Msg m = decode_msg(r);
        r.finish();
        CHECK(tracefmt::fmt_msg(m) == text) << text;
        std::vector<std::uint8_t> again;
        encode_msg(m, again);
        CHECK(again == bytes) << text;
        for (std::size_t n = 0; n < bytes.size(); ++n) {
            bool threw = false;
            try { Reader rr(bytes.data(), n); (void)decode_msg(rr); rr.finish(); } catch (const CodecError&) { threw = true; }
            CHECK(threw) << text << " prefix " << n << " decoded";
        }
    }
}

TEST(hostile_input_never_crashes_the_decoders) {
    std::mt19937_64 rng(99);
    for (int i = 0; i < 20000; ++i) {
        std::vector<std::uint8_t> junk(rng() % 64);
        for (auto& b : junk) b = static_cast<std::uint8_t>(rng());
        try { Reader r(junk.data(), junk.size()); (void)decode_msg(r); } catch (const CodecError&) {}
        try { (void)decode_persist(junk.data(), junk.size()); } catch (const CodecError&) {}
        try { (void)decode_batch(junk.data(), junk.size()); } catch (const CodecError&) {}
    }
    // A hostile entry count must not allocate.
    std::vector<std::uint8_t> evil = {3};
    evil.resize(1 + 24, 0);
    put_u32(evil, 0xFFFFFFFFu);
    bool threw = false;
    try { Reader r(evil.data(), evil.size()); (void)decode_msg(r); } catch (const CodecError&) { threw = true; }
    CHECK(threw);
}

TEST(wal_frame_bodies_round_trip) {
    for (auto& [hx, text] : fixture_lines("persists.txt")) {
        auto bytes = unhex(hx);
        Persist p = decode_persist(bytes.data(), bytes.size());
        std::ostringstream o;
        o << p.term << " ";
        if (p.voted_for) o << int(*p.voted_for); else o << "-";
        o << " " << p.from << " ";
        if (p.compact) o << p.compact->first << ":" << p.compact->second; else o << "-";
        o << " " << tracefmt::fmt_entries(p.entries);
        CHECK(o.str() == text) << text;
        std::vector<std::uint8_t> again;
        encode_persist(p, again);
        CHECK(again == bytes) << text;
    }
}

TEST(command_batches_round_trip_including_sessions) {
    for (auto& [hx, rest] : fixture_lines("batches.txt")) {
        auto bytes = unhex(hx);
        std::istringstream in(rest);
        std::string a, b, bar;
        in >> a >> b >> bar;  // "client seq |"
        auto cmds = decode_batch(bytes.data(), bytes.size());
        auto sess = session_of(bytes.data(), bytes.size());
        if (a == "-") CHECK(!sess) << hx;
        else {
            CHECK(sess && sess->first == std::stoull(a) && sess->second == std::stoull(b)) << hx;
        }
        // Expected command descriptions: sym:kind:side:otype:tif:id:price:qty
        std::vector<std::string> want;
        for (std::string w; in >> w;) want.push_back(w);
        CHECK(cmds.size() == want.size()) << hx;
        for (std::size_t i = 0; i < cmds.size() && i < want.size(); ++i) {
            const auto& [sym, c] = cmds[i];
            std::string kind = c.kind == matcher::Command::Kind::New ? "new" : c.kind == matcher::Command::Kind::Cancel ? "cancel" : "replace";
            std::string got = std::to_string(sym) + ":" + kind + ":";
            if (c.kind == matcher::Command::Kind::New) {
                const char* tifs[] = {"gtc", "ioc", "fok", "post_only"};
                got += std::string(c.side == matcher::Side::Bid ? "bid" : "ask") + ":" + (c.otype == matcher::OType::Market ? "market" : "limit") + ":" + tifs[int(c.tif)];
            } else got += "-:-:-";
            got += ":" + std::to_string(c.order_id) + ":" + std::to_string(c.price) + ":" + std::to_string(c.qty);
            CHECK(got == want[i]) << got << " vs " << want[i];
        }
        std::vector<std::uint8_t> again;
        if (!bytes.empty()) {
            std::optional<std::pair<std::uint64_t, std::uint64_t>> s;
            if (sess) s = *sess;
            encode_batch(cmds.data(), cmds.size(), again, s);
        }
        CHECK(again == bytes) << "re-encode " << hx.substr(0, 40);
    }
}

TEST(wal_recovery_matches_rust_for_every_damage_pattern) {
    std::size_t cases = 0;
    for (auto& e : fs::directory_iterator(WIRE)) {
        const std::string name = e.path().filename().string();
        if (name.rfind("wal-", 0) != 0 || e.path().extension() != ".bin") continue;
        const std::string tag = name.substr(4, name.size() - 8);
        std::string want = slurp(WIRE / ("wal-" + tag + ".expect"));
        while (!want.empty() && want.back() == '\n') want.pop_back();
        auto dir = scratch(tag);
        fs::copy_file(e.path(), dir / "raft.wal");
        std::string got;
        try {
            Disk disk;
            Repair rep;
            Wal w = Wal::open(dir, disk, rep);
            got = "ok " + tracefmt::fmt_disk(disk) + " frames=" + std::to_string(rep.frames) + " torn=" + std::to_string(rep.torn_bytes);
        } catch (const WalError& err) {
            got = err.kind == WalError::Kind::Corrupt ? "corrupt " + std::to_string(err.offset)
                  : err.kind == WalError::Kind::BadHeader ? "badheader" : std::string("io ") + err.what();
        }
        CHECK(got == want) << tag << "\n  want: " << want << "\n  got:  " << got;
        fs::remove_all(dir);
        ++cases;
    }
    CHECK(cases >= 15) << "only " << cases << " WAL fixtures";
}

TEST(a_wal_written_here_is_byte_identical_to_the_one_rust_wrote) {
    auto e = [](Term t, std::vector<std::uint8_t> d) { return Entry{t, std::move(d)}; };
    auto mk = [&](Term term, std::optional<NodeId> vote, Index from, std::vector<Entry> es, std::optional<std::pair<Index, Term>> comp) {
        Persist p; p.term = term; p.voted_for = vote; p.from = from; p.entries = std::move(es); p.compact = comp; return p;
    };
    auto dir = scratch("identical");
    Disk disk;
    Repair rep;
    {
        Wal w = Wal::open(dir, disk, rep);
        w.append(mk(1, 1, 1, {e(1, {1}), e(1, {2}), e(1, {3})}, std::nullopt));
        w.append(mk(2, 2, 3, {e(2, {9, 9})}, std::nullopt));
        w.append(mk(2, 2, 4, {e(2, {}), e(2, {4})}, std::nullopt));
        w.append(mk(3, std::nullopt, 6, {}, std::pair<Index, Term>{3, 2}));
        w.sync();
    }
    CHECK(slurp(dir / "raft.wal") == slurp(WIRE / "wal-good.bin")) << "WAL bytes differ from Rust's";
    // Appending after recovery lands cleanly; a rewrite keeps the state.
    {
        Wal w = Wal::open(dir, disk, rep);
        w.append(mk(3, std::nullopt, 6, {e(3, {7})}, std::nullopt));
        w.sync();
        w.rewrite();
        w.append(mk(3, std::nullopt, 7, {e(3, {8})}, std::nullopt));
        w.sync();
    }
    Wal w = Wal::open(dir, disk, rep);
    CHECK(disk.base == 3 && disk.base_term == 2 && disk.log.size() == 4) << tracefmt::fmt_disk(disk);
    CHECK(rep.torn_bytes == 0);
    fs::remove_all(dir);
}

TEST(snapshots_read_and_write_identically_to_rust) {
    auto dir = scratch("snap");
    SnapStore store(dir);
    for (const char* idx : {"42", "7"}) {
        std::istringstream in(slurp(WIRE / (std::string("snap-") + idx + ".expect")));
        std::uint64_t index, len;
        std::uint32_t term;
        std::string crc, hx;
        in >> index >> term >> len >> crc >> hx;
        auto data = unhex(hx);
        // Read the Rust-written file.
        char name[64];
        std::snprintf(name, sizeof name, "snap-%020llu.snap", static_cast<unsigned long long>(index));
        fs::copy_file(WIRE / name, dir / name);
        auto got = store.get(index);
        CHECK(got.has_value()) << idx;
        if (got) {
            CHECK(got->first.term == term && got->first.len == len) << idx;
            CHECK(got->second == data) << idx;
        }
        // Write the same snapshot here: the file must equal Rust's.
        auto dir2 = scratch(std::string("snap2-") + idx);
        SnapStore s2(dir2);
        s2.put(index, term, data.data(), data.size());
        CHECK(slurp(dir2 / name) == slurp(WIRE / name)) << "snapshot " << idx << " differs";
        fs::remove_all(dir2);
    }
    CHECK((store.list() == std::vector<Index>{7, 42}));
    // Corruption and truncation are detected.
    char name[64];
    std::snprintf(name, sizeof name, "snap-%020llu.snap", 42ull);
    std::string bytes = slurp(dir / name);
    bytes.back() ^= 1;
    { std::ofstream f(dir / name, std::ios::binary | std::ios::trunc); f.write(bytes.data(), static_cast<std::streamsize>(bytes.size())); }
    CHECK(!store.get(42)) << "bit flip";
    { std::ofstream f(dir / name, std::ios::binary | std::ios::trunc); f.write(bytes.data(), static_cast<std::streamsize>(bytes.size() - 3)); }
    CHECK(!store.get(42)) << "truncation";
    // Streaming receive verifies length and checksum.
    std::vector<std::uint8_t> blob(100000);
    for (std::size_t i = 0; i < blob.size(); ++i) blob[i] = static_cast<std::uint8_t>(i);
    const auto crc = crc32c(blob.data(), blob.size());
    auto in = store.receive(9, 1, blob.size(), crc);
    for (std::size_t i = 0; i < blob.size(); i += 4096) in.write(blob.data() + i, std::min<std::size_t>(4096, blob.size() - i));
    in.finish();
    CHECK(store.get(9) && store.get(9)->second == blob);
    auto bad = store.receive(10, 1, 4, crc);
    bad.write(reinterpret_cast<const std::uint8_t*>("abcd"), 4);
    bool threw = false;
    try { bad.finish(); } catch (const WalError&) { threw = true; }
    CHECK(threw && !store.get(10));
    fs::remove_all(dir);
}

int main(int argc, char** argv) {
    WIRE = fs::path(argc > 1 ? argv[1] : "vectors") / "replication-wire";
    return check::run_all();
}
