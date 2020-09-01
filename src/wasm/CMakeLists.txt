cmake_minimum_required(VERSION 3.0)
project(faabricwasm)

set(CMAKE_CXX_STANDARD 17)

set(PUBLIC_HEADERS
        ${FAABRIC_INCLUDE_DIR}/wasm/wasm.h
        )

set(LIB_FILES
        wasm.cpp
        )

add_library(faabricwasm STATIC ${LIB_FILES})
set_target_properties(faabricwasm PROPERTIES PUBLIC_HEADER "${PUBLIC_HEADERS}")

# Only do the install if we're doing a WASM build
# WARNING - this overwrites the existing mpi.h on the system if used incorrectly
if(FAABRIC_WASM_BUILD)
    # Install in sysroot
    install(TARGETS faabricwasm
        ARCHIVE DESTINATION ${CMAKE_SYSROOT}/lib/wasm32-wasi
        LIBRARY DESTINATION ${CMAKE_SYSROOT}/lib/wasm32-wasi
        PUBLIC_HEADER DESTINATION ${CMAKE_SYSROOT}/include/faabricwasm
        )

    # Add imports
    install(FILES faabricwasm.imports DESTINATION ${CMAKE_SYSROOT}/lib/wasm32-wasi RENAME faabricwasm.imports)

    # Add mpi header
    install(FILES ${PROJ_INCLUDE}/mpi.h DESTINATION ${CMAKE_SYSROOT}/include)
endif()