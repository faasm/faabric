#include <catch2/catch.hpp>

#include "faabric_utils.h"

#include <faabric/util/PeriodicBackgroundThread.h>
#include <faabric/util/barrier.h>

#include <thread>
#include <unistd.h>

using namespace faabric::util;

namespace tests {

class DummyPeriodicThread : public PeriodicBackgroundThread
{
  public:
    DummyPeriodicThread(std::shared_ptr<Barrier> barrierIn)
      : barrier(barrierIn)
    {}

    void doWork() override
    {
        workCount++;
        barrier->wait();
    }

    void tidyUp() override {}

    int getWorkCount() { return workCount.load(); }

  private:
    std::shared_ptr<Barrier> barrier;

    std::atomic<int> workCount = 0;
};

TEST_CASE("Test periodic background operation", "[util]")
{
    int intervalSeconds = 1;

    auto b = Barrier::create(2);

    DummyPeriodicThread t(b);
    REQUIRE(t.getWorkCount() == 0);

    // Start and wait on the barrier twice
    t.start(intervalSeconds);
    b->wait();
    REQUIRE(t.getWorkCount() == 1);

    b->wait();
    REQUIRE(t.getWorkCount() == 2);

    // Stop the thread
    t.stop();

    // Check the count again
    REQUIRE(t.getWorkCount() == 2);
}
}
