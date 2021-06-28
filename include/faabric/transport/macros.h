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
