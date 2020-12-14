# ----------------------------------------------
# Protobuf/gRPC generation
# ----------------------------------------------

set(PB_HEADER "${CMAKE_CURRENT_BINARY_DIR}/faabric.pb.h")
set(GRPC_HEADER "${CMAKE_CURRENT_BINARY_DIR}/faabric.grpc.pb.h")

set(PB_HEADER_COPIED "${FAABRIC_INCLUDE_DIR}/faabric/proto/faabric.pb.h")
set(GRPC_HEADER_COPIED "${FAABRIC_INCLUDE_DIR}/faabric/proto/faabric.grpc.pb.h")

set(PB_SRC "${CMAKE_CURRENT_BINARY_DIR}/faabric.pb.cc")
set(GRPC_SRC "${CMAKE_CURRENT_BINARY_DIR}/faabric.grpc.pb.cc")

add_custom_command(
    OUTPUT "${PB_HEADER}" "${GRPC_HEADER}" "${PB_SRC}" "${GRPC_SRC}"
    COMMAND protoc
    ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
    --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
    -I "${CMAKE_CURRENT_LIST_DIR}"
    --plugin=protoc-gen-grpc=${GRPC_PLUGIN}
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

add_custom_command(
    OUTPUT "${GRPC_HEADER_COPIED}"
    DEPENDS "${GRPC_HEADER}"
    COMMAND ${CMAKE_COMMAND} -E copy
    ${GRPC_HEADER}
    ${FAABRIC_INCLUDE_DIR}/faabric/proto/
    )

# ----------------------------------------------
# Faabric wrapper library
# ----------------------------------------------

set(HEADERS
    ${FAABRIC_INCLUDE_DIR}/faabric/proto/macros.h
    ${FAABRIC_INCLUDE_DIR}/faabric/proto/RPCServer.h
    ${PB_HEADER_COPIED}
    ${GRPC_HEADER_COPIED}
    )

set(LIB_FILES
    RPCServer.cpp
    ${HEADERS}
    ${PB_SRC}
    ${GRPC_SRC}
    )

faabric_lib(proto "${LIB_FILES}")

add_dependencies(proto spdlog_ext)

target_link_libraries(proto
    ${Protobuf_LIBRARIES}
    gRPC::grpc++
    gRPC::grpc++_reflection
    )

