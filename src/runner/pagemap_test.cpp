#include <faabric/util/logging.h>

int main()
{
    faabric::util::initLogging();

    SPDLOG_DEBUG("Hi");

    return 0;
}
