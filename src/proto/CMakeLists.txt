# ----------------------------------------------
# Protobuf/gRPC generation
# ----------------------------------------------

set(PB_HEADER_COPIED "${FAABRIC_INCLUDE_DIR}/faabric/proto/faabric.pb.h")

protobuf_generate_cpp(PB_SRC PB_HEADER faabric.proto)

# Copy the generated headers into place
add_custom_command(
    OUTPUT "${PB_HEADER_COPIED}"
    DEPENDS "${PB_HEADER}"
    COMMAND ${CMAKE_COMMAND}
    ARGS -E copy ${PB_HEADER} ${FAABRIC_INCLUDE_DIR}/faabric/proto/
)

add_custom_target(
    faabric_pbh_copied
    DEPENDS ${PB_HEADER_COPIED}
)

# ----------------------------------------------
# Faabric wrapper library
# ----------------------------------------------

faabric_lib(proto ${PB_SRC})
add_dependencies(faabric_common_dependencies faabric_pbh_copied)
target_include_directories(proto_deps INTERFACE ${CMAKE_CURRENT_BINARY_DIR})
