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
    COMMAND ${FLATBUFFERS_FLATC_EXECUTABLE}
    ARGS --cpp --gen-mutable --raw-binary faabric.fbs
)

# Copy the generated headers into place
add_custom_command(
    OUTPUT "${FB_HEADER_COPIED}"
    DEPENDS "${FB_HEADER}"
    COMMAND ${CMAKE_COMMAND} -E copy
    ${FB_HEADER}
    ${FB_HEADER_COPIED}
    )

# ----------------------------------------------
# Faabric wrapper library
# ----------------------------------------------

set(HEADERS
    ${FB_HEADER_COPIED}
    )

set(LIB_FILES
    ${HEADERS}
    flat.cpp
    )

# To get faabric_lib to work here we have to set this include_directory.
include_directories(${FAABRIC_INCLUDE_DIR}/faabric/flat)
faabric_lib(flat "${LIB_FILES}")

# Add include directories for flatbuffers themselves
target_include_directories(flat PUBLIC ${FLATBUFFERS_INCLUDE_DIRS})

target_link_libraries(flat flatbuffers_imported)
