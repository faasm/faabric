#include <faabric/util/func.h>
#include <faabric/util/logging.h>

int main()
{
    faabric::util::initLogging();

    // Build a message just to check things work
    faabric::Message msg = faabric::util::messageFactory("foo", "bar");
    std::string msgString = faabric::util::funcToString(msg, true);

    SPDLOG_DEBUG("Message: {}", msgString);

    return 0;
}
