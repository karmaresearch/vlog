# VLog

## Installation instructions

We used CMake to ease the installation process. To build VLog, the following
commands should suffice:

```
mkdir build
cd build
cmake ..
make
```

The only library VLog depends on is zlib, which is usually present by default.

To enable the web-interface, you need to use the -DWEBINTERFACE=1 option to cmake.

If you want to for instance build the DEBUG version, including the web interface: proceed as follows:

```
mkdir build_debug
cd build_debug
cmake DWEBINTERFACE=1 -DCMAKE_BUILD_TYPE=Debug ..
make
```

## License

Vlog is released under the Apache 2 license.
