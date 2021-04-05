set(HEADERS
    ${FAABRIC_INCLUDE_DIR}/faabric/rpc/macros.h
    ${FAABRIC_INCLUDE_DIR}/faabric/rpc/RPCServer.h
    )

set(LIB_FILES
    RPCServer.cpp
    ${HEADERS}
    )

faabric_lib(rpc "${LIB_FILES}")

add_dependencies(rpc spdlog_ext)

target_link_libraries(rpc
    ${Protobuf_LIBRARIES}
    gRPC::grpc++
    gRPC::grpc++_reflection
    )

