include_directories(${FAABRIC_INCLUDE_DIR}/endpoint)

set(LIB_FILES
        Endpoint.cpp
        ${FAABRIC_INCLUDE_DIR}/faabric/endpoint/Endpoint.h
        )

faabric_private_lib(endpoint "${LIB_FILES}")

target_link_libraries(endpoint pistache pthread)
