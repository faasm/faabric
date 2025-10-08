# ----------------------------------------------
# Flatbuffers
# ----------------------------------------------

set(FB_HEADER "${CMAKE_CURRENT_LIST_DIR}/faabric_generated.h")
set(FB_HEADER_COPIED "${FAABRIC_INCLUDE_DIR}/faabric/flat/faabric_generated.h")

# flatc command to generate flatbuffers files
add_custom_command(
    OUTPUT "${FB_HEADER}"
    DEPENDS faabric.fbs
    WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}
    COMMAND flatbuffers::flatc
    ARGS --cpp --gen-mutable --raw-binary faabric.fbs
)

# Copy the generated headers into place
add_custom_command(
    OUTPUT "${FB_HEADER_COPIED}"
    DEPENDS "${FB_HEADER}"
    COMMAND ${CMAKE_COMMAND} -E copy ${FB_HEADER} ${FB_HEADER_COPIED}
)

add_custom_target(
    faabric_fbs_copied
    DEPENDS ${FB_HEADER_COPIED}
)

# ----------------------------------------------
# Faabric wrapper library
# ----------------------------------------------

faabric_lib(flat flat.cpp)
add_dependencies(faabric_common_dependencies faabric_fbs_copied)
