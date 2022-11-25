#pragma once

#define PARSE_MSG(T, data, size)                                               \
    T parsedMsg;                                                               \
    if (!parsedMsg.ParseFromArray(data, size)) {                               \
        throw std::runtime_error("Error deserialising message");               \
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
