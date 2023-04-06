# ----------------------------------------------
# Protobuf generation
# ----------------------------------------------

set(PB_HEADER_COPIED "${FAABRIC_INCLUDE_DIR}/faabric/planner/planner.pb.h")

protobuf_generate_cpp(PB_SRC PB_HEADER planner.proto)

# Copy the generated headers into place
add_custom_command(
    OUTPUT "${PB_HEADER_COPIED}"
    DEPENDS "${PB_HEADER}"
    COMMAND ${CMAKE_COMMAND}
    ARGS -E copy ${PB_HEADER} ${FAABRIC_INCLUDE_DIR}/faabric/planner/
)

add_custom_target(
    planner_pbh_copied
    DEPENDS ${PB_HEADER_COPIED}
)

# TODO: eventually expose an endpoint as well
faabric_lib(planner
    Planner.cpp
    PlannerClient.cpp
    PlannerServer.cpp
    PlannerState.cpp
    ${PB_SRC}
)

# Make sure the protobuf headers are built before we build the planner
add_dependencies(planner planner_pbh_copied)

target_link_libraries(planner PRIVATE
    faabric::endpoint
    # TODO: do we want this link-time dependency here?
    # At this point we need it for the endpoint
    faabric::scheduler
    faabric::transport
    faabric::util
)

add_executable(planner_server planner_server.cpp)
target_link_libraries(planner_server PRIVATE faabric::planner)
