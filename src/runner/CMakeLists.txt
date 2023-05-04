
faabric_lib(runner FaabricMain.cpp)

target_link_libraries(runner PRIVATE
    faabric::planner
    faabric::scheduler
    faabric::util
    faabric::endpoint
)
