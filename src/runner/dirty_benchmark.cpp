#include <faabric/util/string_tools.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/util/dirty.h>
#include <faabric/util/environment.h>
#include <faabric/util/logging.h>
#include <faabric/util/timing.h>

#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <inttypes.h>
#include <memory>
#include <poll.h>
#include <shared_mutex>
#include <signal.h>
#include <sys/eventfd.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace faabric::util;
using namespace faabric::snapshot;

namespace faabric::runner {

#define ONE_MB (1024L * 1024L)
#define _TARGET_MEM_SIZE (1024L * ONE_MB)

#define NUM_PAGES (_TARGET_MEM_SIZE / HOST_PAGE_SIZE)
#define MEM_SIZE (HOST_PAGE_SIZE * NUM_PAGES)

struct BenchConf
{
    std::string mode;
    int nThreads = faabric::util::getUsableCores() - 1;
    float readPct = 0.2;
    float writePct = 0.8;
};

struct BenchResult
{
    int nWrites = 0;
    int nReads = 0;
    int nPages = 0;
    std::vector<char> dirtyPages;
};

std::string benchToString(BenchConf c)
{
    std::string res = c.mode;

    res += fmt::format(
      " READ {:.1f}% WRITE {:.1f}%", 100 * c.readPct, 100 * c.writePct);

    res += fmt::format(" {} THREADS", c.nThreads);

    return res;
}

void doBenchInner(BenchConf conf)
{
    SPDLOG_INFO("---------------");
    SPDLOG_INFO("{}", benchToString(conf));

    // Configure dirty tracking
    SystemConfig& c = getSystemConfig();
    c.dirtyTrackingMode = conf.mode;
    resetDirtyTracker();

    bool isUffd = faabric::util::startsWith(conf.mode, "uffd");

    std::shared_ptr<DirtyTracker> tracker = getDirtyTracker();

    if (tracker->getType() != conf.mode) {
        SPDLOG_ERROR(
          "Tracker not expected mode: {} != {}", tracker->getType(), conf.mode);
        exit(1);
    }

    // Create some dummy data and a snapshot
    std::vector<uint8_t> dummyData(MEM_SIZE, 1);
    auto snap = std::make_shared<SnapshotData>(dummyData);

    // Allocate memory
    MemoryRegion mem = allocatePrivateMemory(MEM_SIZE);
    std::span<uint8_t> memView = std::span<uint8_t>(mem.get(), MEM_SIZE);

    // Map to the snapshot *before* for some types
    if (!isUffd) {
        snap->mapToMemory({ mem.get(), (size_t)MEM_SIZE });
    }

    // ------- Initialisation -------
    TimePoint startStart = startTimer();

    tracker->clearAll();
    tracker->startTracking(memView);

    // Map to the snapshot
    // TODO this should be possible to do before all the tracking initialisation
    if (isUffd) {
        snap->mapToMemory({ mem.get(), (size_t)MEM_SIZE });
    }

    long startNanos = getTimeDiffNanos(startStart);
    float startMillis = float(startNanos) / (1000L * 1000L);

    // ------- Running -------
    std::vector<std::thread> threads;
    std::vector<BenchResult> results;

    threads.reserve(conf.nThreads);
    results.resize(conf.nThreads);

    for (int t = 0; t < conf.nThreads; t++) {
        results[t].dirtyPages = std::vector<char>(NUM_PAGES, 0);
    }

    TimePoint runStart = startTimer();

    for (int t = 0; t < conf.nThreads; t++) {
        threads.emplace_back([&results, &conf, t, &tracker, &memView] {
            tracker->startThreadLocalTracking(memView);

            BenchResult& res = results[t];
            int threadChunkSize = NUM_PAGES / conf.nThreads;
            int offset = t * threadChunkSize;

            int targetReads = threadChunkSize * conf.readPct;
            int targetWrites = threadChunkSize * conf.writePct;

            for (int i = offset; i < offset + threadChunkSize; i++) {
                res.nPages++;

                bool isWrite = res.nWrites < targetWrites;
                bool isRead = !isWrite && res.nReads < targetReads;

                // Skip if necessary
                if (!isWrite && !isRead) {
                    SPDLOG_TRACE("Skipping {}");
                    continue;
                }

                // Perform the relevant operation
                uint8_t* ptr = memView.data() + (i * HOST_PAGE_SIZE) + 5;
                if (isWrite) {
                    res.nWrites++;

                    *ptr = (uint8_t)5;
                } else {
                    res.nReads++;

                    uint8_t loopRes = *ptr;
                    if (loopRes == 9) {
                        printf("This should never happen %u\n", loopRes);
                    }
                }
            }

            // Get dirty pages for this thread
            res.dirtyPages = tracker->getThreadLocalDirtyPages(memView);
        });
    }

    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }

    long runNanos = getTimeDiffNanos(runStart);
    float runMillis = float(runNanos) / (1000L * 1000L);

    // ------- Diffing --------
    TimePoint diffingStart = startTimer();

    tracker->stopTracking(memView);
    tracker->stopThreadLocalTracking(memView);

    std::vector<char> dirtyPages = tracker->getDirtyPages(memView);
    std::vector<SnapshotDiff> diffs =
      snap->diffWithDirtyRegions(memView, dirtyPages);

    long diffingNanos = getTimeDiffNanos(diffingStart);
    float diffingMillis = float(diffingNanos) / (1000L * 1000L);
    // ----------

    // ------- Writing diffs --------
    TimePoint writingStart = startTimer();

    snap->queueDiffs(diffs);
    snap->writeQueuedDiffs();

    long writingNanos = getTimeDiffNanos(writingStart);
    float writingMillis = float(writingNanos) / (1000L * 1000L);
    // ----------

    float totalMillis = startMillis + runMillis + diffingMillis + writingMillis;

    SPDLOG_INFO("S {:.2f}ms R {:.2f}ms D {:.2f}ms W {:.2f}ms; TOT {:.2f}ms",
                startMillis,
                runMillis,
                diffingMillis,
                writingMillis,
                totalMillis);

    // -----------------------------
    // VALIDATION
    // -----------------------------
    int actualDirty = std::count(dirtyPages.begin(), dirtyPages.end(), 1);

    int expectedDirty = 0;
    for (int t = 0; t < conf.nThreads; t++) {
        BenchResult& res = results[t];

        int nDirty =
          std::count(res.dirtyPages.begin(), res.dirtyPages.end(), 1);
        actualDirty += nDirty;

        SPDLOG_TRACE(
          "Thread {} processed {} pages ({} writes, {} reads, {} dirty (TLS))",
          t,
          res.nPages,
          res.nWrites,
          res.nReads,
          nDirty);

        expectedDirty += res.nWrites;
    }

    if (actualDirty != expectedDirty) {
        SPDLOG_ERROR(
          "FAILED: on {}: {} != {}", conf.mode, actualDirty, expectedDirty);

        throw std::runtime_error("Failed");
    }
}

void doBench(const std::string& mode)
{
    int reps = 20;
    for (int r = 0; r < reps; r++) {
        BenchConf conf;
        conf.mode = mode;

        conf.readPct = 0.2;
        conf.writePct = 0.8;
        doBenchInner(conf);

        //conf.readPct = 0.8;
        //conf.writePct = 0.2;
        //doBenchInner(conf);
    }
}
}

int main(int argc, char* argv[])
{
    initLogging();

    std::vector<std::string> modes;
    if (argc > 1) {
        modes = { argv[1] };
    } else {
        modes = { "uffd-thread-demand", "uffd-demand", "segfault", "soft-pte" };
    }

    for (const auto& m : modes) {
        faabric::runner::doBench(m);
    }

    return 0;
}
