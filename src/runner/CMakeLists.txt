faabric_lib(runner FaabricMain.cpp)

target_link_libraries(runner PRIVATE faabric::scheduler faabric::util faabric::endpoint)

