#include <faabric/util/dirty.h>
#include <faabric/util/logging.h>
#include <faabric/util/timing.h>

#include <thread>

using namespace faabric::util;

namespace faabric::runner {

#define ONE_MB (1024L * 1024L)
#define _TARGET_MEM_SIZE (1024L * ONE_MB)

#define NUM_PAGES (_TARGET_MEM_SIZE / HOST_PAGE_SIZE)
#define MEM_SIZE (HOST_PAGE_SIZE * NUM_PAGES)

struct BenchConf
{
    std::string mode;
    bool mapMemory = false;
    bool sharedMemory = false;
    bool dirtyReads = false;
    int nThreads = 2;
    int readPct = 20;
    int writePct = 80;
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

    res += fmt::format(" READ {}% WRITE {}%", c.readPct, c.writePct);

    res += fmt::format(" {} THREADS", c.nThreads);

    res += c.mapMemory ? " MAP" : "";
    res += c.sharedMemory ? " SHARED" : "";

    return res;
}

void doBenchInner(BenchConf conf)
{
    SPDLOG_INFO("---------------");
    SPDLOG_INFO("{}", benchToString(conf));

    SystemConfig& c = getSystemConfig();
    c.dirtyTrackingMode = conf.mode;
    resetDirtyTracker();

    std::shared_ptr<DirtyTracker> tracker = getDirtyTracker();

    if (tracker->getType() != conf.mode) {
        SPDLOG_ERROR(
          "Tracker not expected mode: {} != {}", tracker->getType(), conf.mode);
        exit(1);
    }

    MemoryRegion sharedMemPtr = allocateSharedMemory(MEM_SIZE);
    MemoryRegion privateMemPtr = allocatePrivateMemory(MEM_SIZE);

    std::span<uint8_t> memView;
    if (conf.sharedMemory) {
        memView = std::span<uint8_t>(sharedMemPtr.get(), MEM_SIZE);
    } else {
        memView = std::span<uint8_t>(privateMemPtr.get(), MEM_SIZE);
    }

    if (conf.mapMemory) {
        int fd = createFd(MEM_SIZE, "foobar");
        mapMemoryPrivate(memView, fd);
    }

    // ---------
    TimePoint startStart = startTimer();
    tracker->clearAll();
    tracker->startTracking(memView);

    long startNanos = getTimeDiffNanos(startStart);
    float startMillis = float(startNanos) / (1000L * 1000L);

    std::vector<std::thread> threads;
    std::vector<BenchResult> results;

    threads.reserve(conf.nThreads);
    results.resize(conf.nThreads);

    for (int t = 0; t < conf.nThreads; t++) {
        results[t].dirtyPages = std::vector<char>(NUM_PAGES, 0);
    }

    // ---------
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
                    SPDLOG_TRACE("Writing {}", i);
                    res.nWrites++;

                    *ptr = (uint8_t)5;
                } else {
                    SPDLOG_TRACE("Reading {}", i);
                    res.nReads++;

                    uint8_t loopRes = *ptr;
                    if (loopRes == 9) {
                        printf("This will never happen %u\n", loopRes);
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

    // ----------
    TimePoint stopStart = startTimer();

    tracker->stopTracking(memView);
    tracker->stopThreadLocalTracking(memView);

    long stopNanos = getTimeDiffNanos(stopStart);
    float stopMillis = float(stopNanos) / (1000L * 1000L);

    float totalMillis = startMillis + runMillis + stopMillis;
    // ----------

    SPDLOG_INFO("{:.2f}ms {:.2f}ms {:.2f}ms; TOT {:.2f}ms",
                startMillis,
                runMillis,
                stopMillis,
                totalMillis);

    std::vector<char> dirtyPages = tracker->getDirtyPages(memView);
    int actualDirty = std::count(dirtyPages.begin(), dirtyPages.end(), 1);

    int expectedDirty = 0;
    for (int t = 0; t < conf.nThreads; t++) {
        BenchResult& res = results[t];

        int nDirty =
          std::count(res.dirtyPages.begin(), res.dirtyPages.end(), 1);
        actualDirty += nDirty;

        SPDLOG_DEBUG(
          "Thread {} processed {} pages ({} writes, {} reads, {} dirty (TLS))",
          t,
          res.nPages,
          res.nWrites,
          res.nReads,
          nDirty);

        expectedDirty += res.nWrites;
        if (conf.dirtyReads) {
            expectedDirty += res.nReads;
        }
    }

    if (actualDirty != expectedDirty) {
        SPDLOG_ERROR("FAILED: on {} {} {}: {} != {}",
                     conf.mode,
                     conf.mapMemory,
                     conf.sharedMemory,
                     actualDirty,
                     expectedDirty);
    }
}

void doBench(BenchConf conf)
{
    // Shared, write-heavy
    conf.readPct = 20;
    conf.writePct = 80;
    conf.sharedMemory = true;
    doBenchInner(conf);

    // Shared, read-heavy
    conf.readPct = 80;
    conf.writePct = 20;
    conf.sharedMemory = true;
    doBenchInner(conf);

    // Mapped, write-heavy
    conf.readPct = 20;
    conf.writePct = 80;
    conf.mapMemory = true;
    doBenchInner(conf);

    // Mapped, read-heavy
    conf.readPct = 80;
    conf.writePct = 20;
    conf.mapMemory = false;
    doBenchInner(conf);
}
}

int main()
{
    initLogging();

    faabric::runner::doBench({ .mode = "segfault", .dirtyReads = false });

    faabric::runner::doBench({ .mode = "softpte", .dirtyReads = false });

    return 0;
}
