#include "timing.h"

#include <faabric/util/logging.h>

namespace faabric::util {
    faabric::util::TimePoint startTimer() {
        faabric::util::Clock &clock = faabric::util::getGlobalClock();
        return clock.now();
    }

    long getTimeDiffNanos(const faabric::util::TimePoint &begin) {
        faabric::util::Clock &clock = faabric::util::getGlobalClock();
        return clock.timeDiffNano(clock.now(), begin);
    }

    long getTimeDiffMicros(const faabric::util::TimePoint &begin) {
        faabric::util::Clock &clock = faabric::util::getGlobalClock();
        faabric::util::TimePoint end = clock.now();

        long micros = clock.timeDiffMicro(end, begin);
        return micros;
    }

    double getTimeDiffMillis(const faabric::util::TimePoint &begin) {
        long micros = getTimeDiffMicros(begin);
        double millis = ((double) micros) / 1000;
        return millis;
    }

    void logEndTimer(const std::string &label, const faabric::util::TimePoint &begin) {
        double millis = getTimeDiffMillis(begin);
        const std::shared_ptr<spdlog::logger> &l = faabric::util::getLogger();
        l->trace("TIME = {:.2f}ms ({})", millis, label);
    }

    uint64_t timespecToNanos(struct timespec *nativeTimespec) {
        uint64_t nanos = nativeTimespec->tv_sec * 1000000000;
        nanos += nativeTimespec->tv_nsec;

        return nanos;
    }

    void nanosToTimespec(uint64_t nanos, struct timespec *nativeTimespec) {
        nativeTimespec->tv_sec = nanos / 1000000000;
        nativeTimespec->tv_nsec = nanos % 1000000000;
    }
}