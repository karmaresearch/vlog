#!/bin/sh

HERE=`pwd`
echo $HERE

KOGNAC=${HERE}/../../kognac
TRIDENT=${HERE}/../../trident

#cmake -DKOGNAC_LIB=${KOGNAC}/build -DKOGNAC_INC=${KOGNAC}/include -DTRIDENT_LIB=${TRIDENT}/build -DTRIDENT_INC=${TRIDENT}/include ..

cmake -DCMAKE_BUILD_TYPE=Debug -DKOGNAC_LIB=${KOGNAC}/build_debug -DKOGNAC_INC=${KOGNAC}/include -DTRIDENT_LIB=${TRIDENT}/build_debug -DTRIDENT_INC=${TRIDENT}/include -DCMAKE_EXPORT_COMPILE_COMMANDS=ON ..
