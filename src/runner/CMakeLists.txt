set(LIB_FILES
        "${FAABRIC_INCLUDE_DIR}/faabric/runner/FaabricMain.h"
        FaabricMain.cpp
        )

faabric_lib(runner "${LIB_FILES}")

target_link_libraries(runner scheduler util endpoint)
