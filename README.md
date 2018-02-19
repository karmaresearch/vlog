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

If you want to build the DEBUG version of the program, including the web interface: proceed as follows:

```
mkdir build_debug
cd build_debug
cmake DWEBINTERFACE=1 -DCMAKE_BUILD_TYPE=Debug ..
make
```

## Docker

In case you do not want to compile the program, you can use a Docker image that
contains a precompiled version of the program. After you install Docker, you can launch
the following commands:

```
docker pull karmaresearch/vlog
docker run -ti karmaresearch/vlog
```

## Usage

Please check the [Wiki](https://github.com/karmaresearch/vlog/wiki) for some instructions on how to run the program.

## License

Vlog is released under the Apache 2 license.
