#pragma once

#include "fixtures.h"

#include <faabric/scheduler/ExecGraph.h>
#include <faabric/state/State.h>
#include <faabric/state/StateServer.h>
#include <faabric/util/func.h>
#include <faabric/util/testing.h>

using namespace faabric;

#define SHORT_TEST_TIMEOUT_MS 1000

#define REQUIRE_RETRY_MAX 5
#define REQUIRE_RETRY_SLEEP_MS 1000

#define REQUIRE_RETRY(updater, check)                                          \
    {                                                                          \
        {                                                                      \
            updater;                                                           \
        };                                                                     \
        bool res = (check);                                                    \
        int count = 0;                                                         \
        while (!res && count < REQUIRE_RETRY_MAX) {                            \
            count++;                                                           \
            SLEEP_MS(REQUIRE_RETRY_SLEEP_MS);                                  \
            {                                                                  \
                updater;                                                       \
            };                                                                 \
            res = (check);                                                     \
        }                                                                      \
        if (!res) {                                                            \
            FAIL();                                                            \
        }                                                                      \
    }

#define FAABRIC_CATCH_LOGGER                                                   \
    struct LogListener : Catch::TestEventListenerBase                          \
    {                                                                          \
        using TestEventListenerBase::TestEventListenerBase;                    \
        void testCaseStarting(Catch::TestCaseInfo const& testInfo) override    \
        {                                                                      \
            this->Catch::TestEventListenerBase::testCaseStarting(testInfo);    \
            SPDLOG_INFO("=============================================");      \
            SPDLOG_INFO("TEST: {}", testInfo.name);                            \
            SPDLOG_INFO("=============================================");      \
        }                                                                      \
                                                                               \
        void sectionStarting(Catch::SectionInfo const& sectionInfo) override   \
        {                                                                      \
            this->Catch::TestEventListenerBase::sectionStarting(sectionInfo);  \
            if (sectionInfo.name != currentTestCaseInfo->name) {               \
                SPDLOG_INFO("---------------------------------------------");  \
                SPDLOG_INFO("SECTION: {}", sectionInfo.name);                  \
                SPDLOG_INFO("---------------------------------------------");  \
            }                                                                  \
        }                                                                      \
    };                                                                         \
    CATCH_REGISTER_LISTENER(LogListener)

namespace tests {
void cleanFaabric();

void checkMessageEquality(const faabric::Message& msgA,
                          const faabric::Message& msgB);

void checkMpiMessageEquivalence(const faabric::Message& msgA,
                                const faabric::Message& msgB);

void checkExecGraphNodeEquality(const scheduler::ExecGraphNode& nodeA,
                                const scheduler::ExecGraphNode& nodeB,
                                bool isMpi = false);

void checkExecGraphEquality(const scheduler::ExecGraph& graphA,
                            const scheduler::ExecGraph& graphB,
                            bool isMpi = false);

std::pair<int, std::string> submitGetRequestToUrl(const std::string& host,
                                                  int port,
                                                  const std::string& body);
}
