#pragma once

#define PARSE_MSG(T, data, size)                                               \
    T parsedMsg;                                                               \
    if (!parsedMsg.ParseFromArray(data, size)) {                               \
        throw std::runtime_error("Error deserialising message");               \
    }

#define SERIALISE_MSG(_msg)                                                    \
    size_t serialisedSize = _msg.ByteSizeLong();                               \
    uint8_t serialisedBuffer[serialisedSize];                                  \
    if (!_msg.SerializeToArray(serialisedBuffer, serialisedSize)) {            \
        throw std::runtime_error("Error serialising message");                 \
    }

#define SERIALISE_MSG_PTR(_msg)                                                \
    size_t serialisedSize = _msg->ByteSizeLong();                              \
    uint8_t serialisedBuffer[serialisedSize];                                  \
    if (!_msg->SerializeToArray(serialisedBuffer, serialisedSize)) {           \
        throw std::runtime_error("Error serialising message");                 \
    }

#define SEND_FB_MSG(T, _mb)                                                    \
    {                                                                          \
        const uint8_t* _buffer = _mb.GetBufferPointer();                       \
        int _size = _mb.GetSize();                                             \
        faabric::EmptyResponse _response;                                      \
        syncSend(T, _buffer, _size, &_response);                               \
    }

#define SEND_FB_MSG_ASYNC(T, _mb)                                              \
    {                                                                          \
        const uint8_t* _buffer = _mb.GetBufferPointer();                       \
        int _size = _mb.GetSize();                                             \
        asyncSend(T, _buffer, _size);                                          \
    }
