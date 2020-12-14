# ----------------------------------------------
# Protobuf/gRPC generation
# ----------------------------------------------

set(PB_HEADER "${CMAKE_CURRENT_BINARY_DIR}/faabric.pb.h")
set(GRPC_HEADER "${CMAKE_CURRENT_BINARY_DIR}/faabric.grpc.pb.h")
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

# ----------------------------------------------
# Faabric wrapper library
# ----------------------------------------------

file(GLOB HEADERS "${FAABRIC_INCLUDE_DIR}/faabric/proto/*.h")

set(LIB_FILES
    RPCServer.cpp
    ${HEADERS}
    ${PB_HEADER}
    ${GRPC_HEADER}
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

# Copy the generated headers into place
add_custom_command(TARGET proto POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy 
    ${PB_HEADER}
    ${FAABRIC_INCLUDE_DIR}/faabric/proto/
    )

add_custom_command(TARGET proto POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy 
    ${GRPC_HEADER}
    ${FAABRIC_INCLUDE_DIR}/faabric/proto/
    )
