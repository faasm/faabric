#pragma once

#define PARSE_MSG(T, data, size)                                               \
    T msg;                                                                     \
    if (!msg.ParseFromArray(data, size)) {                                     \
        throw std::runtime_error("Error deserialising message");               \
    }
