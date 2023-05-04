# ----------------------------------------------
# Protobuf generation
# ----------------------------------------------

set(PLANNER_PB_HEADER_COPIED "${FAABRIC_INCLUDE_DIR}/faabric/planner/planner.pb.h")

protobuf_generate_cpp(PLANNER_PB_SRC PLANNER_PB_HEADER planner.proto)

# Copy the generated headers into place
add_custom_command(
    OUTPUT "${PLANNER_PB_HEADER_COPIED}"
    DEPENDS "${PLANNER_PB_HEADER}"
    COMMAND ${CMAKE_COMMAND}
    ARGS -E copy ${PLANNER_PB_HEADER} ${FAABRIC_INCLUDE_DIR}/faabric/planner/
)

add_custom_target(
    planner_pbh_copied
    DEPENDS ${PLANNER_PB_HEADER_COPIED}
)

faabric_lib(planner
    Planner.cpp
    PlannerClient.cpp
    PlannerEndpointHandler.cpp
    PlannerServer.cpp
    ${PLANNER_PB_SRC}
)

# Make sure the protobuf headers are built before we build anything else.
# Otherwise some pre-processor checks will fail
add_dependencies(faabric_common_dependencies planner_pbh_copied)

target_link_libraries(planner PRIVATE
    faabric::endpoint
    faabric::transport
    faabric::util
)

add_executable(planner_server planner_server.cpp)
target_link_libraries(planner_server PRIVATE faabric::faabric)
