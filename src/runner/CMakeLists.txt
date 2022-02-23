
faabric_lib(runner FaabricMain.cpp)

target_link_libraries(runner PRIVATE faabric::scheduler faabric::util faabric::endpoint)

add_executable(dirty_bench dirty_benchmark.cpp)
target_link_libraries(dirty_bench PRIVATE faabric::scheduler faabric::util faabric::endpoint)

