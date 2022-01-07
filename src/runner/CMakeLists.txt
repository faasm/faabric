
faabric_lib(runner FaabricMain.cpp)

target_link_libraries(runner PRIVATE faabric::scheduler faabric::util faabric::endpoint)

add_executable(diff_stress diff_stress.cpp)
target_link_libraries(diff_stress faabric::util)
