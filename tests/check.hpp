// check.hpp — a minimal test runner: TEST(name) { CHECK(cond) << "msg"; }
#pragma once

#include <functional>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace check {

struct Case {
    const char* name;
    std::function<void()> fn;
};
inline std::vector<Case>& cases() {
    static std::vector<Case> v;
    return v;
}
inline int failures = 0;
struct Reg {
    Reg(const char* n, std::function<void()> f) { cases().push_back({n, std::move(f)}); }
};
struct Fail {
    std::ostringstream os;
    const char* file;
    int line;
    Fail(const char* f, int l, const char* expr) : file(f), line(l) { os << expr << " — "; }
    ~Fail() {
        std::cerr << file << ":" << line << ": FAIL " << os.str() << "\n";
        ++failures;
    }
    template <class T>
    Fail& operator<<(const T& v) {
        os << v;
        return *this;
    }
};

inline int run_all() {
    // abort the whole binary if anything deadlocks
    std::thread([] {
        std::this_thread::sleep_for(std::chrono::seconds(300));
        std::cerr << "watchdog: tests exceeded 300s\n";
        std::abort();
    }).detach();
    for (auto& c : cases()) {
        int before = failures;
        c.fn();
        std::cout << (failures == before ? "ok   " : "FAIL ") << c.name << "\n";
    }
    std::cout << (failures ? "FAILED" : "all passed") << " (" << cases().size() << " tests)\n";
    return failures ? 1 : 0;
}

}  // namespace check

#define TEST(name)                                   \
    static void name();                              \
    static check::Reg reg_##name(#name, name);       \
    static void name()
#define CHECK(c) \
    if (c) {     \
    } else       \
        check::Fail(__FILE__, __LINE__, #c)
