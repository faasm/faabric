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

#define READ_INTERVAL 100
#define SKIP_INTERVAL 11

struct BenchConf
{
    std::string mode;
    bool mapMemory = false;
    bool sharedMemory = false;
    bool dirtyReads = false;
    int nThreads = 2;
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
    res += c.mapMemory ? " MAP " : "";
    res += c.sharedMemory ? " SHARED " : "";

    return res;
}

void doBench(BenchConf conf)
{
    SPDLOG_ERROR("RUN: {}", benchToString(conf));

    SystemConfig& c = getSystemConfig();
    c.dirtyTrackingMode = conf.mode;

    // TODO - resetDirtyTracker

    DirtyTracker& tracker = getDirtyTracker();

    if (tracker.getType() != conf.mode) {
        SPDLOG_ERROR(
          "Tracker not expected mode: {} != {}", tracker.getType(), conf.mode);
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
    tracker.clearAll();
    tracker.startTracking(memView);

    long startNanos = getTimeDiffNanos(startStart);
    float startMicros = float(startNanos) / 1000;

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
            tracker.startThreadLocalTracking(memView);

            BenchResult& res = results[t];
            int threadChunkSize = NUM_PAGES / conf.nThreads;
            int offset = t * threadChunkSize;

            for (int i = offset; i < offset + threadChunkSize; i++) {
                res.nPages++;

                // Skip if necessary
                if (i % SKIP_INTERVAL == 0) {
                    SPDLOG_TRACE("Skipping {} ({})", i, SKIP_INTERVAL);
                    continue;
                }

                // Work out if this is a read or write
                bool isWrite = true;
                if (i % READ_INTERVAL == 0) {
                    SPDLOG_TRACE("Reading {}", i);
                    isWrite = false;
                    res.nReads++;
                } else {
                    SPDLOG_TRACE("Writing {}", i);
                    isWrite = true;
                    res.nWrites++;
                }

                // Perform the relevant operation
                uint8_t* ptr = memView.data() + (i * HOST_PAGE_SIZE) + 5;
                if (isWrite) {
                    *ptr = (uint8_t)5;
                } else {
                    uint8_t loopRes = *ptr;
                    if (loopRes == 9) {
                        printf("This will never happen %u\n", loopRes);
                    }
                }
            }

            // Get dirty pages for this thread
            res.dirtyPages = tracker.getThreadLocalDirtyPages(memView);
        });
    }

    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }

    long runNanos = getTimeDiffNanos(runStart);
    float runMicros = float(runNanos) / 1000;

    // ----------
    TimePoint stopStart = startTimer();

    tracker.stopTracking(memView);
    tracker.stopThreadLocalTracking(memView);

    long stopNanos = getTimeDiffNanos(stopStart);
    float stopMicros = float(stopNanos) / 1000;

    float totalMicros = startMicros + runMicros + stopMicros;
    // ----------

    SPDLOG_INFO("{}us {}us {}us; TOT {}us",
                startMicros,
                runMicros,
                stopMicros,
                totalMicros);

    std::vector<char> dirtyPages = tracker.getDirtyPages(memView);
    int actualDirty = std::count(dirtyPages.begin(), dirtyPages.end(), 1);

    int expectedDirty = 0;
    for (int t = 0; t < conf.nThreads; t++) {
        BenchResult& res = results[t];

        int nDirty =
          std::count(res.dirtyPages.begin(), res.dirtyPages.end(), 1);
        actualDirty += nDirty;

        SPDLOG_DEBUG(
          "Thread {} processed {} pages ({} writes, {} reads, {} dirty)",
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
}

int main()
{
    initLogging();

    faabric::runner::doBench({ .mode = "segfault",
                               .mapMemory = false,
                               .sharedMemory = false,
                               .dirtyReads = false });

    faabric::runner::doBench({ .mode = "segfault",
                               .mapMemory = false,
                               .sharedMemory = true,
                               .dirtyReads = false });

    faabric::runner::doBench({ .mode = "softpte",
                               .mapMemory = false,
                               .sharedMemory = false,
                               .dirtyReads = false });

    faabric::runner::doBench({ .mode = "softpte",
                               .mapMemory = false,
                               .sharedMemory = true,
                               .dirtyReads = false });
    return 0;
}
