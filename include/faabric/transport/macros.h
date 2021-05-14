#pragma once

#define SEND_MESSAGE(header, msg)  \
    sendHeader(header); \
    size_t msgSize = msg.ByteSizeLong();  \
    { \
        char *sMsg = new char[msgSize]; \
        if(!msg.SerializeToArray(sMsg, msgSize)) { \
            throw std::runtime_error("Error serialising message"); \
        } \
        send(sMsg, msgSize); \
    } \
    
#define SEND_MESSAGE_PTR(header, msg)  \
    sendHeader(header); \
    size_t msgSize = msg->ByteSizeLong();  \
    char *sMsg = new char[msgSize]; \
    if(!msg->SerializeToArray(sMsg, msgSize)) { \
        throw std::runtime_error("Error serialising message"); \
    } \
    send(sMsg, msgSize); \

#define PARSE_MSG(T, data, size) \
    T msg; \
    if (!msg.ParseFromArray(data, size)) { \
        throw std::runtime_error("Error deserialising message"); \
    } \

