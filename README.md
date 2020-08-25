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

External libraries should be automatically downloaded and installed in the same directory. The only library that should be already installed is zlib, which is necessary to read gzip files. This library is usually already present by default.

To enable the web-interface, you need to use the -DWEBINTERFACE=1 option to cmake.

If you want to build the DEBUG version of the program, including the web interface: proceed as follows:

```
mkdir build_debug
cd build_debug
cmake -DWEBINTERFACE=1 -DCMAKE_BUILD_TYPE=Debug ..
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

Please check:
* The [Wiki](https://github.com/karmaresearch/vlog/wiki) for instructions on **how to run VLog from the command line**
* A [screencast](https://iccl.inf.tu-dresden.de/w/images/1/18/Vlog-demo-iswc2016.mp4) was presented at *ISWC'16, Posters and Demos*.
* You can **use VLog in Java** through the [Rulewerk](https://github.com/knowsys/rulewerk/) library, which also supports additional input formats for rules and data

## License

Vlog is released under the Apache license, Version 2.0.  A copy of the license may be obtained
from [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0).
