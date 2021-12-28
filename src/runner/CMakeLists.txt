
faabric_lib(runner FaabricMain.cpp)
target_link_libraries(runner PRIVATE faabric::scheduler faabric::util faabric::endpoint)

add_executable(queue_prof queue_prof.cpp)
target_link_libraries(queue_prof
    faabric::scheduler
    faabric::util
    faabric::endpoint)
