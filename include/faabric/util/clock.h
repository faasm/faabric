#pragma once

#include <chrono>

namespace faabric::util {
typedef std::chrono::steady_clock::time_point TimePoint;

class Clock
{
  public:
    Clock();

    const TimePoint now();

    long epochMillis();

    long timeDiff(const TimePoint& t1, const TimePoint& t2);

    long timeDiffNano(const TimePoint& t1, const TimePoint& t2);

    long timeDiffMicro(const TimePoint& t1, const TimePoint& t2);
};

Clock& getGlobalClock();
}
