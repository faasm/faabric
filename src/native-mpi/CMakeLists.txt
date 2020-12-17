set(LIB_FILES
        mpi.cpp
        ${FAABRIC_INCLUDE_DIR}/faabric/mpi/mpi.h
    )

faabric_lib(mpi "${LIB_FILES}")

#target_link_libraries(mpi)
