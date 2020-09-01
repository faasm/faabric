include_directories(${FAABRIC_INCLUDE_DIR}/faabric/mpi)

set(LIB_FILES
        mpi.cpp
        ${FAABRIC_INCLUDE_DIR}/faabric/mpi/mpi.h
    )

faabric_private_lib(faabricmpi "${LIB_FILES}")

target_link_libraries(faabricmpi util)
