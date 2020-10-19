include_directories(${FAABRIC_INCLUDE_DIR}/faabric/endpoint)

file(GLOB HEADERS "${FAABRIC_INCLUDE_DIR}/faabric/endpoint/*.h")

set(LIB_FILES
        Endpoint.cpp
        FaabricEndpoint.cpp
        FaabricEndpointHandler.cpp
        ${HEADERS}
        )

faabric_private_lib(endpoint "${LIB_FILES}")

add_dependencies(endpoint pistache_ext)
target_link_directories(endpoint PUBLIC ${THIRD_PARTY_INSTALL_DIR}/lib)
target_link_libraries(endpoint pistache pthread util)
