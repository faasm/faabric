# ----------------------------------------------
# Flatbuffers and gRPC
# ----------------------------------------------

target_link_libraries(faabric_flat PRIVATE flatbuffers)

target_include_directories(faabric_flat ${FLATBUFFERS_INCLUDE_DIRS}/include)

set(FB_HEADER "${CMAKE_CURRENT_BINARY_DIR}/faabric.fb.h")
set(GRPC_HEADER "${CMAKE_CURRENT_BINARY_DIR}/faabric.grpc.fb.h")

set(FB_HEADER_COPIED "${FAABRIC_INCLUDE_DIR}/faabric/proto/faabric.fb.h")
set(GRPC_HEADER_COPIED "${FAABRIC_INCLUDE_DIR}/faabric/proto/faabric.grpc.fb.h")

set(FB_SRC "${CMAKE_CURRENT_BINARY_DIR}/faabric.fb.cc")
set(GRPC_SRC "${CMAKE_CURRENT_BINARY_DIR}/faabric.grpc.fb.cc")

add_custom_command(
    OUTPUT "${FB_HEADER}" "${GRPC_HEADER}" "${FB_SRC}" "${GRPC_SRC}"
    COMMAND ${FLATBUFFERS_FLATC_EXECUTABLE}
    ARGS --grpc "${CMAKE_CURRENT_BINARY_DIR}"
    --cpp "${CMAKE_CURRENT_BINARY_DIR}"
    -I "${CMAKE_CURRENT_LIST_DIR}"
    faabric.fbs
    DEPENDS faabric.fbs
)

# Copy the generated headers into place
add_custom_command(
    OUTPUT "${FB_HEADER_COPIED}"
    DEPENDS "${FB_HEADER}"
    COMMAND ${CMAKE_COMMAND} -E copy
    ${FB_HEADER}
    ${FAABRIC_INCLUDE_DIR}/faabric/flat/
    )

add_custom_command(
    OUTPUT "${GRPC_HEADER_COPIED}"
    DEPENDS "${GRPC_HEADER}"
    COMMAND ${CMAKE_COMMAND} -E copy
    ${GRPC_HEADER}
    ${FAABRIC_INCLUDE_DIR}/faabric/flat/
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

