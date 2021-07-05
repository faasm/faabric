#pragma once

#define PARSE_MSG(T, data, size)                                               \
    T msg;                                                                     \
    if (!msg.ParseFromArray(data, size)) {                                     \
        throw std::runtime_error("Error deserialising message");               \
    }

#define SERIALISE_MSG(msg)                                                     \
    size_t msgSize = msg.ByteSizeLong();                                       \
    uint8_t buffer[msgSize];                                                   \
    if (!msg.SerializeToArray(buffer, msgSize)) {                              \
        throw std::runtime_error("Error serialising message");                 \
    }

#define SERIALISE_MSG_PTR(msg)                                                 \
    size_t msgSize = msg->ByteSizeLong();                                      \
    uint8_t buffer[msgSize];                                                   \
    if (!msg->SerializeToArray(buffer, msgSize)) {                             \
        throw std::runtime_error("Error serialising message");                 \
    }

#define SEND_FB_MSG(T, mb)                                                     \
    {                                                                          \
        const uint8_t* buffer = mb.GetBufferPointer();                         \
        int size = mb.GetSize();                                               \
        faabric::EmptyResponse response;                                       \
        syncSend(T, buffer, size, &response);                                  \
    }

#define SEND_FB_MSG_ASYNC(T, mb)                                               \
    {                                                                          \
        const uint8_t* buffer = mb.GetBufferPointer();                         \
        int size = mb.GetSize();                                               \
        asyncSend(T, buffer, size);                                            \
    }
