cmake_minimum_required (VERSION 2.8)

set(CMAKE_CXX_COMPILER "g++")
set(CMAKE_C_COMPILER "gcc")

project (vlog)
include(ExternalProject)
set(CMAKE_MACOSX_RPATH 1)

#Set to release mode if nothing is specified
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    message(STATUS "Setting build type to 'Release' as none was specified.")
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
    # Set the possible values of build type for cmake-gui
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif()

#TBB
#find_library(tbb tbb PATHS ENV LD_LIBRARY_PATH)
#find_path (tbbh tbb/task_scheduler_init.h PATHS ENV CPATH)
#IF ((${tbb} STREQUAL "tbb-NOTFOUND") OR (${tbbh} STREQUAL "tbbh-NOTFOUND"))
#    message(FATAL_ERROR "Could not find TBB (thread building blocks)")
#ENDIF()

#ZLIB
find_package(ZLIB REQUIRED)

#Trident
IF(TRIDENT_LIB)
    message("Using KOGNAC provided installation: ${KOGNAC_LIB} ${KOGNAC_INC}")
    include_directories(${KOGNAC_INC})
    LINK_DIRECTORIES("${KOGNAC_LIB}")
    add_library(kognac STATIC IMPORTED)
    set_property(TARGET kognac PROPERTY IMPORTED_LOCATION ${KOGNAC_LIB}/libkognac.a)
    add_library(kognac-log SHARED IMPORTED)
    set_property(TARGET kognac-log PROPERTY IMPORTED_LOCATION "${KOGNAC_LIB}/libkognac-log${CMAKE_SHARED_LIBRARY_SUFFIX}")

    message("Using Trident provided installation: ${TRIDENT_LIB} ${TRIDENT_INC}")
    include_directories(${TRIDENT_INC})
    include_directories(${TRIDENT_INC}/layers)
    include_directories(${TRIDENT_INC}/../rdf3x/include)
    add_library(trident SHARED IMPORTED)
    set_property(TARGET trident PROPERTY IMPORTED_LOCATION ${TRIDENT_LIB}/libtrident-core${CMAKE_SHARED_LIBRARY_SUFFIX})
    add_library(trident-sparql SHARED IMPORTED)
    set_property(TARGET trident-sparql PROPERTY IMPORTED_LOCATION ${TRIDENT_LIB}/libtrident-sparql${CMAKE_SHARED_LIBRARY_SUFFIX})
    LINK_DIRECTORIES("${TRIDENT_LIB}/")
ELSE()
    ExternalProject_Add(git-trident
        DOWNLOAD_COMMAND git clone https://github.com/karmaresearch/trident.git
        DOWNLOAD_DIR .
        SOURCE_DIR trident
        BUILD_IN_SOURCE 1
        CMAKE_ARGS "-DSPARQL=1"
        INSTALL_COMMAND ""
        )
    ExternalProject_Get_Property(git-trident SOURCE_DIR)
    include_directories(${SOURCE_DIR}/include)
    include_directories(${SOURCE_DIR}/include/layers)
    include_directories(${SOURCE_DIR}/rdf3x/include)
    # TRIDENT
    add_library(trident SHARED IMPORTED)
    add_dependencies(trident git-trident)
    set_property(TARGET trident PROPERTY IMPORTED_LOCATION "${CMAKE_BINARY_DIR}/trident/libtrident-core${CMAKE_SHARED_LIBRARY_SUFFIX}")
    LINK_DIRECTORIES("${CMAKE_BINARY_DIR}/trident/")
    # TRIDENT SPARQL
    add_library(trident-sparql SHARED IMPORTED)
    add_dependencies(trident-sparql git-trident)
    set_property(TARGET trident-sparql PROPERTY IMPORTED_LOCATION "${CMAKE_BINARY_DIR}/trident/libtrident-sparql${CMAKE_SHARED_LIBRARY_SUFFIX}")
     # KOGNAC
    add_library(kognac STATIC IMPORTED)
    add_dependencies(kognac git-trident)
    include_directories(${SOURCE_DIR}/kognac/include)
    LINK_DIRECTORIES("${CMAKE_BINARY_DIR}/trident/kognac/")
    set_property(TARGET kognac PROPERTY IMPORTED_LOCATION trident/kognac/libkognac.a)
    # KOGNAC LOG
    add_library(kognac-log SHARED IMPORTED)
    add_dependencies(kognac-log git-trident)
    set_property(TARGET kognac-log PROPERTY IMPORTED_LOCATION "${CMAKE_BINARY_DIR}/trident/kognac/libkognac-log${CMAKE_SHARED_LIBRARY_SUFFIX}")
ENDIF()

#SPARSEHASH
find_path (sparsehash sparsehash/sparse_hash_map)
IF (${sparsehash} STREQUAL "sparsehash-NOTFOUND")
    message("Could not find sparsehash. I'm going to use the version from kognac ...")
    include_directories(${CMAKE_BINARY_DIR}/trident/kognac/external/sparsehash/src/)
ENDIF()

#LZ4
find_library(lz4 lz4)
find_path (lz4h lz4.h)
IF ((${lz4} STREQUAL "lz4-NOTFOUND") OR (${lz4h} STREQUAL "lz4h-NOTFOUND"))
    message("Could not find LZ4. I'm going to use the version from kognac")
    include_directories(${CMAKE_BINARY_DIR}/trident/kognac/external/lz4/lib/)
    LINK_DIRECTORIES(${CMAKE_BINARY_DIR}/trident/kognac/external/lz4/lib)
    add_library(lz4 STATIC IMPORTED)
    set_property(TARGET lz4 PROPERTY IMPORTED_LOCATION trident/kognac/external/lz4/lib/liblz4.a)
ELSE()
    include_directories(lz4h)
    message("-- Found LZ4")
ENDIF()

#Webserver support
if(WEBINTERFACE)
    set(COMPILE_FLAGS "${COMPILE_FLAGS} -DWEBINTERFACE=1")
ENDIF()

if(MYSQL)
    set(COMPILE_FLAGS "${COMPILE_FLAGS} -DMYSQL=1")
    link_libraries("-lmysqlcppconn")
ENDIF()

if(ODBC)
    set(COMPILE_FLAGS "${COMPILE_FLAGS} -DODBC=1")
    link_libraries("-lodbc")
ENDIF()

if(MAPI)
    set(COMPILE_FLAGS "${COMPILE_FLAGS} -DMAPI=1")
    link_libraries("-lmapi")
ENDIF()

if(MDLITE)
    set(COMPILE_FLAGS "${COMPILE_FLAGS} -DMDLITE=1")
    link_libraries("-lmonetdb5")
ENDIF()

#add files to compile
file(GLOB vlog_SRC
    "src/vlog/*.cpp"
    "src/vlog/common/*.cpp"
    "src/vlog/backward/*.cpp"
    "src/vlog/forward/*.cpp"
    "src/vlog/magic/*.cpp"
    "src/vlog/inmemory/*.cpp"
    "src/vlog/trident/*.cpp"
    "src/vlog/web/*.cpp"
    "src/launcher/vloglayer.cpp"
    "src/launcher/vlogscan.cpp"
    "src/vlog/mysql/*.cpp"
    "src/vlog/odbc/*.cpp"
    "src/vlog/mapi/*.cpp"
    "src/vlog/mdlite/*.cpp"
)

#Create both a library and the executable program
add_library(vlog STATIC ${vlog_SRC})
add_executable(vlog_exec src/launcher/main.cpp)

#PTHREADS
find_package(Threads REQUIRED)
if (THREADS_HAVE_PTHREAD_ARG)
    target_compile_options(PUBLIC vlog "-pthread")
endif()
if (CMAKE_THREAD_LIBS_INIT)
    target_link_libraries(vlog "${CMAKE_THREAD_LIBS_INIT}")
endif()

#Set compiler options
set(CMAKE_CXX_FLAGS_RELEASE "-DNDEBUG -O3")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g -DDEBUG")
set(COMPILE_FLAGS "${COMPILE_FLAGS} -c -MD -std=c++11 -DUSE_COMPRESSED_COLUMNS -DPRUNING_QSQR=1")
set_target_properties(vlog PROPERTIES COMPILE_FLAGS "${COMPILE_FLAGS}")
set_target_properties(vlog_exec PROPERTIES COMPILE_FLAGS "${COMPILE_FLAGS}" OUTPUT_NAME "vlog")

#standard include
include_directories(include/)
if(WEBINTERFACE)
TARGET_LINK_LIBRARIES(vlog trident trident-sparql ${ZLIB_LIBRARIES} kognac kognac-log)
else()
TARGET_LINK_LIBRARIES(vlog trident trident-sparql ${ZLIB_LIBRARIES} kognac kognac-log)
endif()
TARGET_LINK_LIBRARIES(vlog_exec vlog)
