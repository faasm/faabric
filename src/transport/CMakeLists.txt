# ----------------------------------------------
# Faabric transport library: ZeroMQ + Protobuf
# ----------------------------------------------

set(HEADERS
    "${FAABRIC_INCLUDE_DIR}/faabric/transport/common.h"
    "${FAABRIC_INCLUDE_DIR}/faabric/transport/context.h"
    "${FAABRIC_INCLUDE_DIR}/faabric/transport/macros.h"
    "${FAABRIC_INCLUDE_DIR}/faabric/transport/Message.h"
    "${FAABRIC_INCLUDE_DIR}/faabric/transport/MessageEndpoint.h"
    "${FAABRIC_INCLUDE_DIR}/faabric/transport/MessageEndpointClient.h"
    "${FAABRIC_INCLUDE_DIR}/faabric/transport/MessageEndpointServer.h"
    "${FAABRIC_INCLUDE_DIR}/faabric/transport/MpiMessageEndpoint.h"
    "${FAABRIC_INCLUDE_DIR}/faabric/transport/PointToPointBroker.h"
    "${FAABRIC_INCLUDE_DIR}/faabric/transport/PointToPointClient.h"
    "${FAABRIC_INCLUDE_DIR}/faabric/transport/PointToPointServer.h"
)

set(LIB_FILES
    context.cpp
    Message.cpp
    MessageEndpoint.cpp
    MessageEndpointClient.cpp
    MessageEndpointServer.cpp
    MpiMessageEndpoint.cpp
    PointToPointBroker.cpp
    PointToPointClient.cpp
    PointToPointServer.cpp
    ${HEADERS}
)

faabric_lib(transport "${LIB_FILES}")

target_link_libraries(transport util proto zeromq_imported)

