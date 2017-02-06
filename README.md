# VLog

## Installation instructions

In order to compile VLog, first you need trident. 

```
git clone https://github.com/jrbn/trident.git
```

should get it. Then, go to the trident directory and follow the README instructions there.
It will also require you to install some other packages.

Next, you also need curl. On Ubuntu/Debian,

```
sudo apt-get install libcurl3-openssl-dev
```

should do the trick. On OSX, the necessary files are present in Xcode.
Next,

```
make TRIDENT=<location of trident>
```

should compile everything. If you want to create a debug version of the program, run

```
make DEBUG=1
```

instead.

## License

Vlog is released under the Apache 2 license.
