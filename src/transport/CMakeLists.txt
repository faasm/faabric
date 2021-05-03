# ----------------------------------------------
# Faabric transport library: ZeroMQ + Protobuf
# ----------------------------------------------

set(HEADERS
    "${FAABRIC_INCLUDE_DIR}/faabric/transport/MessageContext.h"
    "${FAABRIC_INCLUDE_DIR}/faabric/transport/MessageEndpoint.h"
    "${FAABRIC_INCLUDE_DIR}/faabric/transport/MessageEndpointServer.h"
    "${FAABRIC_INCLUDE_DIR}/faabric/transport/SimpleMessageEndpoint.h"
)

set(LIB_FILES 
    MessageContext.cpp
    MessageEndpoint.cpp
    MessageEndpointServer.cpp
    SimpleMessageEndpoint.cpp
    ${HEADERS}
)

faabric_lib(transport "${LIB_FILES}")

# Add include directories for zeromq themselves
target_include_directories(transport PUBLIC ${ZEROMQ_INCLUDE_DIRS})

# ----------------------------------------------
# Add dependency with ZeroMQ
# ----------------------------------------------

add_dependencies(transport libzeromq_ext)
add_dependencies(transport cppzeromq_ext)

target_link_libraries(transport proto zmq)

