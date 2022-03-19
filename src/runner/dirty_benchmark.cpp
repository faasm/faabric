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

    res += fmt::format(" READ {:.1f}% WRITE {:.1f}%", c.readPct, c.writePct);

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

    MemoryRegion mappingSourceMemPtr = allocateSharedMemory(MEM_SIZE);
    MemoryRegion sharedMemPtr = allocateSharedMemory(MEM_SIZE);
    MemoryRegion privateMemPtr = allocatePrivateMemory(MEM_SIZE);

    std::span<uint8_t> memView;
    if (conf.sharedMemory) {
        memView = std::span<uint8_t>(sharedMemPtr.get(), MEM_SIZE);
    } else {
        memView = std::span<uint8_t>(privateMemPtr.get(), MEM_SIZE);
    }

    std::span<uint8_t> mappingView =
      std::span<uint8_t>(mappingSourceMemPtr.get(), MEM_SIZE);

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
        threads.emplace_back(
          [&mappingView, &results, &conf, t, &tracker, &memView] {
              tracker->startThreadLocalTracking(memView);

              // Set up source mapping for demand paging
              if (conf.mode == "uffd-demand" ||
                  conf.mode == "uffd-thread-demand") {
                  tracker->mapRegions(mappingView, memView);
              }

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
                      SPDLOG_TRACE("Write to page {}", i);
                      res.nWrites++;

                      *ptr = (uint8_t)5;
                  } else {
                      SPDLOG_TRACE("Read on page {}", i);
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

        SPDLOG_TRACE(
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

        throw std::runtime_error("Failed");
    }
}

void doBench(BenchConf conf)
{
    // Private, write-heavy
    conf.readPct = 0.2;
    conf.writePct = 0.8;
    doBenchInner(conf);

    // Private, read-heavy
    conf.readPct = 0.8;
    conf.writePct = 0.2;
    doBenchInner(conf);

    // Shared, read-heavy
    conf.readPct = 0.8;
    conf.writePct = 0.2;
    conf.sharedMemory = true;
    // doBenchInner(conf);

    // Shared, write-heavy
    conf.readPct = 0.2;
    conf.writePct = 0.8;
    conf.sharedMemory = true;
    // doBenchInner(conf);

    // Shared, read-heavy
    conf.readPct = 0.8;
    conf.writePct = 0.2;
    conf.sharedMemory = true;
    // doBenchInner(conf);

    // Mapped, write-heavy
    conf.readPct = 0.2;
    conf.writePct = 0.8;
    conf.mapMemory = true;
    // doBenchInner(conf);

    // Mapped, low writes and reads
    conf.readPct = 0.1;
    conf.writePct = 0.1;
    conf.mapMemory = true;
    // doBenchInner(conf);

    // Mapped, read-heavy
    conf.readPct = 0.8;
    conf.writePct = 0.2;
    conf.mapMemory = true;
    // doBenchInner(conf);
}
}

int main()
{
    initLogging();

    faabric::runner::doBench(
      { .mode = "uffd-thread-demand", .dirtyReads = false });

    faabric::runner::doBench({ .mode = "uffd-demand", .dirtyReads = false });

    faabric::runner::doBench({ .mode = "segfault", .dirtyReads = false });

    faabric::runner::doBench({ .mode = "softpte", .dirtyReads = false });

    faabric::runner::doBench({ .mode = "uffd", .dirtyReads = true });

    faabric::runner::doBench({ .mode = "uffd-thread", .dirtyReads = false });

    faabric::runner::doBench({ .mode = "uffd-wp", .dirtyReads = true });

    faabric::runner::doBench({ .mode = "uffd-thread-wp", .dirtyReads = false });

    return 0;
}
