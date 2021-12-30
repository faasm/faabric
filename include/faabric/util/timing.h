#pragma once

#include <faabric/util/clock.h>
#include <string>

#ifdef TRACE_ALL
#define PROF_START(name)                                                       \
    const faabric::util::TimePoint name = faabric::util::startTimer();
#define PROF_END(name) faabric::util::logEndTimer(#name, name);
#define PROF_SUMMARY_START faabric::util::startGlobalTimer();
#define PROF_SUMMARY_END faabric::util::printTimerTotals();
#define PROF_CLEAR faabric::util::clearTimerTotals();
#else
#define PROF_START(name)
#define PROF_END(name)
#define PROF_SUMMARY_START
#define PROF_SUMMARY_END
#define PROF_CLEAR
#endif

namespace faabric::util {
faabric::util::TimePoint startTimer();

long getTimeDiffNanos(const faabric::util::TimePoint& begin);

long getTimeDiffMicros(const faabric::util::TimePoint& begin);

double getTimeDiffMillis(const faabric::util::TimePoint& begin);

void logEndTimer(const std::string& label,
                 const faabric::util::TimePoint& begin);

void startGlobalTimer();

void printTimerTotals();

void clearTimerTotals();

uint64_t timespecToNanos(struct timespec* nativeTimespec);

void nanosToTimespec(uint64_t nanos, struct timespec* nativeTimespec);
}
