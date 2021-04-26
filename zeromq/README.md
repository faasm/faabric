# ZeroMQ micro-benchmark

## Details

Inside loop:
* Serialize/deserialize
* Update message
* Error-checking

Out of loop:
* Initialise message
* Initialise socket

## Build

Compile with:
```
protoc -cpp_out $(pwd) -I $(pwd) msg.proto
g++ -o cli push.cc msg.pb.cc -L/usr/local/lib -lzmq -lprotobuf -lpthread
```

## Results

Preliminary results show throughputs of upwards of 1e6 messages per second.
