#include <faabric/util/logging.h>
#include <faabric/util/state.h>

namespace faabric::util {
std::string keyForUser(const std::string& user, const std::string& key)
{
    if (user.empty() || key.empty()) {
        throw std::runtime_error(
          fmt::format("Cannot have empty user or key ({}/{})", user, key));
    }

    std::string fullKey = user + "_" + key;

    return fullKey;
}

void maskDouble(unsigned int* maskArray, unsigned long idx)
{
    // We assume int is half size of double
    unsigned long intIdx = 2 * idx;
    maskArray[intIdx] |= STATE_MASK_32;
    maskArray[intIdx + 1] |= STATE_MASK_32;
}

}
