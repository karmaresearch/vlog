#!/bin/sh
# Make sure trident is updated
/app/trident/scripts/docker/update_and_make.sh
cd /app/vlog
git pull
cd build
cmake ..
make
cd ../build_debug
cmake ..
make
