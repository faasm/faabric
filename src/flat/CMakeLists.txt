# ----------------------------------------------
# Flatbuffers and gRPC
# ----------------------------------------------

set(FB_HEADER "${CMAKE_CURRENT_LIST_DIR}/faabric_generated.h")
set(GRPC_HEADER "${CMAKE_CURRENT_LIST_DIR}/faabric.grpc.fb.h")
set(GRPC_SRC "${CMAKE_CURRENT_LIST_DIR}/faabric.grpc.fb.cc")

set(FB_HEADER_COPIED "${FAABRIC_INCLUDE_DIR}/faabric/flat/faabric_generated.h")
set(GRPC_HEADER_COPIED "${FAABRIC_INCLUDE_DIR}/faabric/flat/faabric.grpc.fb.h")

add_custom_command(
    OUTPUT "${FB_HEADER}" "${GRPC_HEADER}" "${GRPC_SRC}"
    DEPENDS faabric.fbs
    WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}
    COMMAND ${FLATBUFFERS_FLATC_EXECUTABLE}
    ARGS --grpc --cpp --raw-binary faabric.fbs
)

# Copy the generated headers into place
add_custom_command(
    OUTPUT "${FB_HEADER_COPIED}"
    DEPENDS "${FB_HEADER}"
    COMMAND ${CMAKE_COMMAND} -E copy
    ${FB_HEADER}
    ${FB_HEADER_COPIED}
    )

add_custom_command(
    OUTPUT "${GRPC_HEADER_COPIED}"
    DEPENDS "${GRPC_HEADER}"
    COMMAND ${CMAKE_COMMAND} -E copy
    ${GRPC_HEADER}
    ${GRPC_HEADER_COPIED}
    )

# ----------------------------------------------
# Faabric wrapper library
# ----------------------------------------------

set(HEADERS
    ${FB_HEADER_COPIED}
    ${GRPC_HEADER_COPIED}
    )

set(LIB_FILES
    ${HEADERS}
    ${GRPC_SRC}
    )

# To get faabric_lib to work here we have to set this include_directory.
include_directories(${FAABRIC_INCLUDE_DIR}/faabric/flat)
faabric_lib(flat "${LIB_FILES}")

# Add include directories for flatbuffers themselves
target_include_directories(flat PUBLIC ${FLATBUFFERS_INCLUDE_DIRS})

target_link_libraries(flat grpc)

