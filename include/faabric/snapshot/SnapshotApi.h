#pragma once

namespace faabric::snapshot {
enum SnapshotCalls
{
    NoSnapshotCall = 0,
    PushSnapshot = 1,
    PushSnapshotDiffs = 2,
    DeleteSnapshot = 3,
    ThreadResult = 4,
};
}
