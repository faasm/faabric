#include <chrono>
#include <iostream>
#include <stdexcept>
#include <thread>

#include "./build/msg.pb.h"

#include <zmq.hpp>

#define NUM_MSG 1000000
#define URL "tcp://127.0.0.1:5555"

void push(zmq::context_t& context)
{
    zmq::socket_t sock(context, zmq::socket_type::push);
    sock.bind(URL);

    example::MPIMessage msg;
    int bufferSize = 3;
    std::vector<int> buffer(bufferSize, 3);
    msg.set_count(bufferSize);
    msg.set_buffer(buffer.data(), sizeof(int) * bufferSize);

    for (int i = 0; i < NUM_MSG; i++) {
        msg.set_worldid(i);

        // Serialize message
        std::string serializedMsg;
        if (!msg.SerializeToString(&serializedMsg)) {
            throw std::runtime_error("Error serialising message!");
        }

        // Send message
        zmq::message_t zmqMsg(serializedMsg.data(), serializedMsg.size());
        if (!sock.send(zmqMsg, zmq::send_flags::none)) {
            throw std::runtime_error("Error sending message!");
        }
    }
}

void pull(zmq::context_t& context)
{
    zmq::socket_t sock(context, zmq::socket_type::pull);
    sock.connect(URL);
    
    for (int i = 0; i < NUM_MSG; i++) {
        zmq::message_t msgOut;
        if (!sock.recv(msgOut)) {
            throw std::runtime_error("Error receiving message!");
        }

        // De-serialize message
        std::string deserializedMsg((char*) msgOut.data(), msgOut.size());
        example::MPIMessage out;
        if (!out.ParseFromString(deserializedMsg)) {
            throw std::runtime_error("Error deserialising message!");
        }

        // Sanity-checks
        if (out.worldid() != i) {
            throw std::runtime_error("Malformed emssage");
        }
        if (i > 0 && i % 100000 == 0) {
            std::cout << "ACKed " << i << " messages" << std::endl;
        }
    }
}

int main(int argc, char *argv[])
{
    // 1 IO thread. As a rule of thumb use one thread per Gbps of data sent
    zmq::context_t context(1);

    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    std::thread thPush([&context] {
        push(context);         
    });
    std::thread thPull([&context] {
        pull(context);         
    });

    // Join threads
    thPush.join();
    thPull.join();

    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds> (end - begin).count();
    std::cout << "Duration: " << duration << std::endl;
    std::cout << "Throughput: "
              << std::setprecision(3)
              << (float) NUM_MSG / duration 
              << " MPS" << std::endl;


    return 0;
}
