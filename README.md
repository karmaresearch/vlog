# VLog

[![Build Status](https://travis-ci.org/karmaresearch/vlog.svg?branch=master)](https://travis-ci.org/karmaresearch/vlog)

## Installation 

We used CMake to ease the installation process. To build VLog, the following
commands should suffice:

```
mkdir build
cd build
cmake ..
make
```

### Dependencies
*   zlib
*   bison (3.0 or higher)
*   flex (2.6 or higher)
*   Other external libraries should be automatically downloaded and installed in the same directory.

### Options for cmake
*   `-DWEBINTERFACE=1` to enable the web interface
*   `-DCMAKE_BUILD_TYPE=Debug` to build the DEBUG version of the program
*   `-DSPARQL=1` to support SPARQL queries to external data sources
*   `-DJAVA=1` to build the java modules
*   For other options, please check the file `CMakeLists.txt`

#### Example

In this example we show how to install VLog in debug mode with support for the web interface from the command line.

```
$ git clone https://github.com/karmaresearch/vlog.git
$ cd vlog
$ mkdir build_debug
$ cd build_debug
$ cmake -DWEBINTERFACE=1 -DCMAKE_BUILD_TYPE=Debug ..
$ make -j
```

Please note that the option `-j` for make will build VLog in parallel

## Docker

In case you do not want to compile the program, you can use a Docker image that
contains a precompiled version of the program. After you install Docker, you can launch
the following commands:

```
docker pull karmaresearch/vlog
docker run -ti karmaresearch/vlog
```

## Usage

Please check:
*   the [Wiki](https://github.com/karmaresearch/vlog/wiki) for some instructions on how to run the program.
*   a [screencast](https://iccl.inf.tu-dresden.de/w/images/1/18/Vlog-demo-iswc2016.mp4) presented at *ISWC'16, Posters and Demos*

## License

Vlog is released under the Apache license, Version 2.0.  A copy of the license may be obtained
from [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0).
