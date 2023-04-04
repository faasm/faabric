# TODO: eventually expose an endpoint as well
faabric_lib(planner
    Planner.cpp
    PlannerClient.cpp
    PlannerServer.cpp
)

target_link_libraries(planner PRIVATE
    faabric::transport
    faabric::util
)

add_executable(planner_server planner_server.cpp)
target_link_libraries(planner_server PRIVATE faabric::planner)
