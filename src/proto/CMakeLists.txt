# ----------------------------------------------
# Protobuf/gRPC generation
# ----------------------------------------------

set(PB_HEADER "${CMAKE_CURRENT_BINARY_DIR}/faabric.pb.h")
set(PB_HEADER_COPIED "${FAABRIC_INCLUDE_DIR}/faabric/proto/faabric.pb.h")
set(PB_SRC "${CMAKE_CURRENT_BINARY_DIR}/faabric.pb.cc")

add_custom_command(
    OUTPUT "${PB_HEADER}" "${PB_SRC}"
    COMMAND protoc
    ARGS --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
    -I "${CMAKE_CURRENT_LIST_DIR}"
    faabric.proto
    DEPENDS faabric.proto
)

# Copy the generated headers into place
add_custom_command(
    OUTPUT "${PB_HEADER_COPIED}"
    DEPENDS "${PB_HEADER}"
    COMMAND ${CMAKE_COMMAND} -E copy
    ${PB_HEADER}
    ${FAABRIC_INCLUDE_DIR}/faabric/proto/
    )

# ----------------------------------------------
# Faabric wrapper library
# ----------------------------------------------

set(HEADERS
    ${PB_HEADER_COPIED}
    ${GRPC_HEADER_COPIED}
    )

set(LIB_FILES
    ${HEADERS}
    ${PB_SRC}
    ${GRPC_SRC}
    )

faabric_lib(proto "${LIB_FILES}")

add_dependencies(proto spdlog_ext)

target_link_libraries(proto protobuf)
