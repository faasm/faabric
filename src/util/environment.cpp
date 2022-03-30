#include <faabric/util/config.h>
#include <faabric/util/environment.h>

#include <stdexcept>
#include <thread>

namespace faabric::util {
std::string getEnvVar(std::string const& key, std::string const& deflt)
{
    char const* val = getenv(key.c_str());

    if (val == nullptr) {
        return deflt;
    }

    std::string retVal(val);

    if (retVal.length() == 0) {
        return deflt;
    } else {
        return retVal;
    }
}

std::string setEnvVar(const std::string& varName, const std::string& value)
{
    std::string original = getEnvVar(varName, "");
    setenv(varName.c_str(), value.c_str(), 1);

    return original;
}

void unsetEnvVar(const std::string& varName)
{
    unsetenv(varName.c_str());
}

unsigned int getUsableCores()
{
    auto conf = faabric::util::getSystemConfig();
    unsigned int nCores;

    if (conf.overrideCpuCount == 0) {
        nCores = std::jthread::hardware_concurrency();
    } else {
        nCores = conf.overrideCpuCount;
    }

    // Returns zero when unable to detect
    if (nCores == 0) {
        throw std::runtime_error("Unable to detect number of cores");
    }

    return nCores;
}
}
