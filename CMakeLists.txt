cmake_minimum_required (VERSION 2.8)

set(CMAKE_C_COMPILER "gcc")
set(CMAKE_CXX_COMPILER "g++")
if (NOT "$ENV{CC}" STREQUAL "")
    set(CMAKE_C_COMPILER "$ENV{CC}")
endif()
if (NOT "$ENV{CXX}" STREQUAL "")
    set(CMAKE_CXX_COMPILER "$ENV{CXX}")
endif()

project (vlog)
include(ExternalProject)
set(CMAKE_MACOSX_RPATH 1)

#Set to release mode if nothing is specified
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    message(STATUS "Setting build type to 'Release' as none was specified.")
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
    # Set the possible values of build type for cmake-gui
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release")
endif()

#Add macPorts
# Detect if the "port" command is valid on this system; if so, return full path
EXECUTE_PROCESS(COMMAND which port RESULT_VARIABLE DETECT_MACPORTS OUTPUT_VARIABLE MACPORTS_PREFIX ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE)

IF (${DETECT_MACPORTS} EQUAL 0)
    # "/opt/local/bin/port" doesn't have libs, so we get the parent directory
    GET_FILENAME_COMPONENT(MACPORTS_PREFIX ${MACPORTS_PREFIX} DIRECTORY)

    # "/opt/local/bin" doesn't have libs, so we get the parent directory
    GET_FILENAME_COMPONENT(MACPORTS_PREFIX ${MACPORTS_PREFIX} DIRECTORY)

    # "/opt/local" is where MacPorts lives, add `/lib` suffix and link
    LINK_DIRECTORIES(${MACPORTS_PREFIX}/lib)
    INCLUDE_DIRECTORIES(${MACPORTS_PREFIX}/include)
ENDIF()

#Add homebrew
EXECUTE_PROCESS(COMMAND brew --prefix RESULT_VARIABLE exitstatus OUTPUT_VARIABLE BREW_PREFIX ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE)

IF (${exitstatus} EQUAL 0)
    LINK_DIRECTORIES(${BREW_PREFIX}/lib)
    INCLUDE_DIRECTORIES(${BREW_PREFIX}/include)
ENDIF()

#add files to compile
file(GLOB vlog_SRC
    "src/vlog/*.cpp"
    "src/vlog/common/*.cpp"
    "src/vlog/backward/*.cpp"
    "src/vlog/forward/*.cpp"
    "src/vlog/magic/*.cpp"
    "src/vlog/incremental/*.cpp"
    "src/vlog/inmemory/*.cpp"
    "src/vlog/trident/*.cpp"
    "src/vlog/cycles/*.cpp"
    "src/vlog/ml/*.cpp"
    "src/vlog/trigger/*.cpp"
    "src/launcher/vloglayer.cpp"
    "src/launcher/vlogscan.cpp"
    "src/vlog/prediction/*.cpp"
    "src/vlog/util/*.cpp"
    )

#ZLIB
find_package(ZLIB REQUIRED)

#Trident
IF(TRIDENT_LIB)
    message("Using KOGNAC provided installation: ${KOGNAC_LIB} ${KOGNAC_INC}")
    include_directories(${KOGNAC_INC})
    LINK_DIRECTORIES("${KOGNAC_LIB}")
    add_library(kognac-core SHARED IMPORTED)
    set_property(TARGET kognac-core PROPERTY IMPORTED_LOCATION ${KOGNAC_LIB}/${CMAKE_SHARED_LIBRARY_PREFIX}kognac-core${CMAKE_SHARED_LIBRARY_SUFFIX})

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
        CMAKE_ARGS "-DSPARQL=1" "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
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
    set(KOGNAC_LIB ${CMAKE_BINARY_DIR}/trident/kognac)
    add_library(kognac-core SHARED IMPORTED)
    add_dependencies(kognac-core git-trident)
    include_directories(${SOURCE_DIR}/kognac/include)
    LINK_DIRECTORIES("${KOGNAC_LIB}")
    set_property(TARGET kognac-core PROPERTY IMPORTED_LOCATION ${KOGNAC_LIB}/${CMAKE_SHARED_LIBRARY_PREFIX}kognac-core${CMAKE_SHARED_LIBRARY_SUFFIX})
ENDIF()

#SPARSEHASH
find_path(sparsehash sparsehash/sparse_hash_map)
IF (${sparsehash} STREQUAL "sparsehash-NOTFOUND")
    message("Could not find sparsehash. I'm going to use the version from kognac ...")
    include_directories(${KOGNAC_LIB}/external/sparsehash/src/)
ENDIF()

#LZ4
# find_library(lz4 lz4)
# find_path(lz4h lz4.h)
# IF (${lz4h} STREQUAL "lz4h-NOTFOUND")
#     message("Could not find LZ4. I'm going to use the version from kognac")
    message("I'm going to use LZ4 the version from kognac")
    include_directories(${KOGNAC_LIB}/external/lz4/lib/)
    LINK_DIRECTORIES(${KOGNAC_LIB}/external/lz4/lib)
    add_library(lz4 STATIC IMPORTED)
    set_property(TARGET lz4 PROPERTY IMPORTED_LOCATION ${KOGNAC_LIB}/external/lz4/lib/${CMAKE_STATIC_LIBRARY_PREFIX}lz4${CMAKE_STATIC_LIBRARY_SUFFIX})
# ELSE()
# include_directories(lz4h)
#    message("-- Found LZ4")
# ENDIF()

IF (JAVA)
    find_package(Java REQUIRED)
    include(UseJava)
    find_package(JNI REQUIRED)
    include_directories(${JNI_INCLUDE_DIRS})
ENDIF()

#Webserver support
IF(WEBINTERFACE)
    file(GLOB vlog_webSRC "src/vlog/web/*.cpp")
    set(vlog_SRC ${vlog_webSRC} ${vlog_SRC})
    set(COMPILE_FLAGS "${COMPILE_FLAGS} -DWEBINTERFACE=1")
ENDIF()

IF(MYSQL)
    file(GLOB vlog_mysqlSRC "src/vlog/mysql/*.cpp")
    set(vlog_SRC ${vlog_mysqlSRC} ${vlog_SRC})
    set(COMPILE_FLAGS "${COMPILE_FLAGS} -DMYSQL=1")
    link_libraries("-lmysqlcppconn")
ENDIF()

IF(ODBC)
    file(GLOB vlog_odbcSRC "src/vlog/odbc/*.cpp")
    set(vlog_SRC ${vlog_odbcSRC} ${vlog_SRC})
    set(COMPILE_FLAGS "${COMPILE_FLAGS} -DODBC=1")
    link_libraries("-lodbc")
ENDIF()

IF(MAPI)
    file(GLOB vlog_mapiSRC "src/vlog/mapi/*.cpp")
    set(vlog_SRC ${vlog_mapiSRC} ${vlog_SRC})
    set(COMPILE_FLAGS "${COMPILE_FLAGS} -DMAPI=1")
    link_libraries("-lmapi")
ENDIF()

IF(SPARQL)
    find_package(CURL REQUIRED)
    include_directories(${CURL_INCLUDE_DIR})
    file(GLOB vlog_sparqlSRC "src/vlog/sparql/*.cpp")
    set(vlog_SRC ${vlog_sparqlSRC} ${vlog_SRC})
    set(COMPILE_FLAGS "${COMPILE_FLAGS} -DSPARQL=1")
ENDIF()

IF(MDLITE)
    file(GLOB vlog_mdliteSRC "src/vlog/mdlite/*.cpp")
    set(vlog_SRC ${vlog_mdliteSRC} ${vlog_SRC})
    set(COMPILE_FLAGS "${COMPILE_FLAGS} -DMDLITE=1")
    link_libraries("-lmonetdb5")
ENDIF()

IF(ELASTIC)
    file(GLOB vlog_elasticSRC "src/vlog/text/elastictable.cpp")
    set(vlog_SRC ${vlog_elasticSRC} ${vlog_SRC})
    set(COMPILE_FLAGS "${COMPILE_FLAGS} -DELASTIC=1")
ENDIF()

IF(JAVA)
    file(GLOB vlog_javaSRC "src/vlog/java/native/*.cpp")
    add_library(vlog-java SHARED ${vlog_javaSRC})
ENDIF()

#Create both a library and the executable program
add_library(vlog-core SHARED ${vlog_SRC})
add_executable(vlog src/launcher/main.cpp)

IF(SPARQL)
    target_link_libraries(vlog-core ${CURL_LIBRARIES})
ENDIF()

#PTHREADS
find_package(Threads REQUIRED)
if (THREADS_HAVE_PTHREAD_ARG)
    target_compile_options(PUBLIC vlog-core "-pthread")
endif()
if (CMAKE_THREAD_LIBS_INIT)
    target_link_libraries(vlog-core "${CMAKE_THREAD_LIBS_INIT}")
endif()

#Set compiler options
if((${CMAKE_CXX_COMPILER_ID} STREQUAL "Clang") OR (${CMAKE_CXX_COMPILER_ID} STREQUAL "GNU"))
    set(CMAKE_CXX_FLAGS_RELEASE "-DNDEBUG -O3")
    set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g -DDEBUG")
    set(COMPILE_FLAGS "${COMPILE_FLAGS} -c -fPIC -MD -std=c++11 -DPRUNING_QSQR=1")
    #set(COMPILE_FLAGS "${COMPILE_FLAGS} -c -fPIC -MD -std=c++11 -DUSE_COMPRESSED_COLUMNS -DPRUNING_QSQR=1")
endif()

IF (JAVA)
    set(COMPILE_FLAGS "${COMPILE_FLAGS} -fPIC")
ENDIF()
set_target_properties(vlog-core PROPERTIES COMPILE_FLAGS "${COMPILE_FLAGS}")
IF(JAVA)
    set_target_properties(vlog-java PROPERTIES COMPILE_FLAGS "${COMPILE_FLAGS}")
ENDIF()
set_target_properties(vlog PROPERTIES COMPILE_FLAGS "${COMPILE_FLAGS}" OUTPUT_NAME "vlog")

#standard include
include_directories(include/)
TARGET_LINK_LIBRARIES(vlog-core trident trident-sparql ${ZLIB_LIBRARIES} kognac-core lz4)

if(JAVA)
    file(GLOB javaSRC "src/vlog/java/karmaresearch/vlog/*.java")
    Create_javadoc(vlog PACKAGES karmaresearch.vlog SOURCEPATH ${CMAKE_CURRENT_SOURCE_DIR}/src/vlog/java WINDOWTITLE "VLog JNI Interface")
    # add_dependencies(vlog-java headers)
    TARGET_LINK_LIBRARIES(vlog-java vlog-core trident trident-sparql kognac-core)
    #Terrible hack to make cmake actually find the shared library when adding it to the jar.
    #It won't find it in the binary directory ... so we copy it to the source directory, add it to the jar, and then remove it from the source directory
    add_custom_command(TARGET vlog-java COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_SOURCE_DIR}/resources)
    add_custom_command(TARGET vlog-java COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:vlog-java> ${CMAKE_SOURCE_DIR}/resources)
    add_custom_command(TARGET vlog-java COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:vlog-core> ${CMAKE_SOURCE_DIR}/resources)
    add_custom_command(TARGET vlog-java COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:trident> ${CMAKE_SOURCE_DIR}/resources)
    add_custom_command(TARGET vlog-java COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:trident-sparql> ${CMAKE_SOURCE_DIR}/resources)
    add_custom_command(TARGET vlog-java COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:kognac-core> ${CMAKE_SOURCE_DIR}/resources)
    add_jar(jvlog ${javaSRC} resources/${CMAKE_SHARED_LIBRARY_PREFIX}vlog-java${CMAKE_SHARED_LIBRARY_SUFFIX} resources/${CMAKE_SHARED_LIBRARY_PREFIX}vlog-core${CMAKE_SHARED_LIBRARY_SUFFIX} resources/${CMAKE_SHARED_LIBRARY_PREFIX}trident-core${CMAKE_SHARED_LIBRARY_SUFFIX} resources/${CMAKE_SHARED_LIBRARY_PREFIX}trident-sparql${CMAKE_SHARED_LIBRARY_SUFFIX} resources/${CMAKE_SHARED_LIBRARY_PREFIX}kognac-core${CMAKE_SHARED_LIBRARY_SUFFIX} GENERATE_NATIVE_HEADERS headers DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/src/vlog/java/native/)
    add_custom_command(TARGET jvlog POST_BUILD COMMAND ${CMAKE_COMMAND} -E remove_directory ${CMAKE_SOURCE_DIR}/resources)
    add_custom_command(TARGET jvlog POST_BUILD COMMAND ${CMAKE_COMMAND} -E remove $<TARGET_FILE:vlog-java>)
    add_dependencies(jvlog vlog-java)
ENDIF()
TARGET_LINK_LIBRARIES(vlog vlog-core)
