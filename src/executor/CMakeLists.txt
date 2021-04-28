set(LIB_FILES
        "${FAABRIC_INCLUDE_DIR}/faabric/executor/FaabricMain.h"
        FaabricMain.cpp
        )

faabric_lib(executor "${LIB_FILES}")

target_link_libraries(executor scheduler)

