# ----------------------------------------------
# Flatbuffers and gRPC
# ----------------------------------------------

set(FB_HEADER "${CMAKE_CURRENT_BINARY_DIR}/faabric_generated.h")
set(GRPC_HEADER "${CMAKE_CURRENT_BINARY_DIR}/faabric.grpc.fb.h")
set(GRPC_SRC "${CMAKE_CURRENT_BINARY_DIR}/faabric.grpc.fb.cc")

set(FB_HEADER_COPIED "${FAABRIC_INCLUDE_DIR}/faabric/proto/faabric_generated.h")
set(GRPC_HEADER_COPIED "${FAABRIC_INCLUDE_DIR}/faabric/proto/faabric.grpc.fb.h")

add_custom_command(
    OUTPUT "${FB_HEADER}" "${GRPC_HEADER}" "${GRPC_SRC}"
    DEPENDS faabric.fbs
    COMMAND ${FLATBUFFERS_FLATC_EXECUTABLE}
    ARGS --grpc --cpp faabric.fbs
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
    ${FAABRIC_INCLUDE_DIR}/faabric/flat/FlatRPCServer.h
    ${FB_HEADER_COPIED}
    ${GRPC_HEADER_COPIED}
    )

set(LIB_FILES
    FlatRPCServer.cpp
    ${HEADERS}
    ${GRPC_SRC}
    )

faabric_lib(flat "${LIB_FILES}")

add_dependencies(flat spdlog_ext)

target_include_directories(flat PUBLIC ${FLATBUFFERS_INCLUDE_DIRS})

target_link_libraries(flat
    gRPC::grpc++
    gRPC::grpc++_reflection
    )

