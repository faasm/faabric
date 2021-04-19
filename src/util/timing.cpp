#include <faabric/util/logging.h>
#include <faabric/util/timing.h>

faabric::util::TimePoint globalStart;
std::unordered_map<std::string, std::atomic<long>> timerTotals;
std::unordered_map<std::string, std::atomic<int>> timerCounts;

namespace faabric::util {
faabric::util::TimePoint startTimer()
{
    faabric::util::Clock& clock = faabric::util::getGlobalClock();
    return clock.now();
}

long getTimeDiffNanos(const faabric::util::TimePoint& begin)
{
    faabric::util::Clock& clock = faabric::util::getGlobalClock();
    return clock.timeDiffNano(clock.now(), begin);
}

long getTimeDiffMicros(const faabric::util::TimePoint& begin)
{
    faabric::util::Clock& clock = faabric::util::getGlobalClock();
    faabric::util::TimePoint end = clock.now();

    long micros = clock.timeDiffMicro(end, begin);
    return micros;
}

double getTimeDiffMillis(const faabric::util::TimePoint& begin)
{
    long micros = getTimeDiffMicros(begin);
    double millis = ((double)micros) / 1000;
    return millis;
}

void logEndTimer(const std::string& label,
                 const faabric::util::TimePoint& begin)
{
    double millis = getTimeDiffMillis(begin);
    const std::shared_ptr<spdlog::logger>& l = faabric::util::getLogger();
    l->trace("TIME = {:.2f}ms ({})", millis, label);

    // Record microseconds total
    timerTotals[label] += long(millis * 1000L);
    timerCounts[label]++;
}

void startGlobalTimer()
{
    globalStart = startTimer();
}

void printTimerTotals()
{
    // Stop global timer
    double totalMillis = getTimeDiffMillis(globalStart);
    double totalSeconds = totalMillis / 1000.0;

    // Switch the pairs so we can use std::sort
    std::vector<std::pair<long, std::string>> totals;
    for (auto& p : timerTotals) {
        totals.push_back({ p.second, p.first });
    }

    std::sort(totals.begin(), totals.end());

    printf("---------- TIMER TOTALS ----------\n");
    printf("Total (ms)  Avg (ms)   Count  Label\n");
    for (auto& p : totals) {
        double millis = double(p.first) / 1000.0;
        int count = timerCounts[p.second];
        double avg = millis / count;
        printf(
          "%-11.2f %-10.3f %5i  %s\n", millis, avg, count, p.second.c_str());
    }

    printf("Total running time: %.2fs\n\n", totalSeconds);
}

uint64_t timespecToNanos(struct timespec* nativeTimespec)
{
    uint64_t nanos = nativeTimespec->tv_sec * 1000000000;
    nanos += nativeTimespec->tv_nsec;

    return nanos;
}

void nanosToTimespec(uint64_t nanos, struct timespec* nativeTimespec)
{
    nativeTimespec->tv_sec = nanos / 1000000000;
    nativeTimespec->tv_nsec = nanos % 1000000000;
}
}
