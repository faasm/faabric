
faabric_lib(redis Redis.cpp)

target_link_libraries(redis PRIVATE faabric::util)
