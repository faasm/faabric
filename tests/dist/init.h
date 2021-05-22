#pragma once

// Note - these must match the container config
#define MASTER_IP "172.50.0.4"
#define WORKER_IP "172.50.0.5"

namespace tests {

void initDistTests();

// Specific test functions
void registerThreadFunctions();

}
