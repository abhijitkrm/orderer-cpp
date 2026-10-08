// The README quick start, compiled by CMake (target `quickstart`).
#include <orderer/orderer.hpp>

#include <cstdio>

using namespace orderer;

int main() {
    auto dir = fs::temp_directory_path() / "orderer-cpp-quickstart";
    JournalConfig journal;
    journal.dir = dir;  // binary, durable: fsync every 1024 records
    auto [events_plug, events] = collect(true);

    auto p = Pipeline<FifoCore>::builder()
                 .partitions(2)
                 .journal(journal)
                 .egress(events_plug)  // or acks(...), metrics(...), callback(...), your own Egress
                 .build();

    Handle h = p->handle();  // copyable; publish from any thread
    h.publish(7, Command::new_limit(1, Side::Ask, 100, 10, Tif::Gtc));
    h.publish(7, Command::new_limit(2, Side::Bid, 100, 4, Tif::Gtc));
    h.publish(9, Command::new_limit(1, Side::Bid, 50, 1, Tif::Gtc));

    p->drain();                                // applied and delivered
    p->snapshot().write(dir / "books.snap");  // consistent cut: matcher-snap/1 + .meta
    p->shutdown();

    std::fputs(events->listing().c_str(), stdout);
}
