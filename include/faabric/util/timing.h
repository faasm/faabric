#pragma once

#include <faabric/util/clock.h>
#include <string>

#ifdef TRACE_ALL
#define PROF_START(name)                                                       \
    const faabric::util::TimePoint name = faabric::util::startTimer();
#define PROF_END(name) faabric::util::logEndTimer(#name, name);
#define PROF_SUMMARY faabric::util::printTimerTotals();
#else
#define PROF_START(name)
#define PROF_END(name)
#define PROF_SUMMARY
#endif

namespace faabric::util {
faabric::util::TimePoint startTimer();

long getTimeDiffNanos(const faabric::util::TimePoint& begin);

long getTimeDiffMicros(const faabric::util::TimePoint& begin);

double getTimeDiffMillis(const faabric::util::TimePoint& begin);

void logEndTimer(const std::string& label,
                 const faabric::util::TimePoint& begin);

void printTimerTotals();

uint64_t timespecToNanos(struct timespec* nativeTimespec);

void nanosToTimespec(uint64_t nanos, struct timespec* nativeTimespec);
}
