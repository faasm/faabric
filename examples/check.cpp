#include <faabric/util/logging.h>
#include <faabric/util/func.h>

int main()
{
    faabric::util::initLogging();
    auto logger = faabric::util::getLogger();

    // Build a message just to check things work
    faabric::Message msg = faabric::util::messageFactory("foo", "bar");
    std::string msgString = faabric::util::funcToString(msg, true);

    logger->debug("Message: {}", msgString);

    return 0;
}
