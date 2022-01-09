#pragma once

#include <faabric/proto/faabric.pb.h>

namespace faabric::util {
void copyMessage(const faabric::Message* src, faabric::Message* dst);
}
