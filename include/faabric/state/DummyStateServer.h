#pragma once

#include "StateServer.h"


namespace faabric::state {
    class DummyStateServer {
    public:
        DummyStateServer();

        std::shared_ptr <state::StateKeyValue> getRemoteKv();

        std::shared_ptr <state::StateKeyValue> getLocalKv();

        std::vector <uint8_t> getRemoteKvValue();

        std::vector <uint8_t> getLocalKvValue();

        std::vector <uint8_t> dummyData;
        std::string dummyUser;
        std::string dummyKey;

        void start();

        void stop();

        std::thread serverThread;
        state::State remoteState;
        state::StateServer stateServer;
    };

}