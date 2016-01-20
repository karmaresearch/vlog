### Vlog

This is Vlog, an experimental column-oriented Datalog engine.

Please read the INSTALL file for compilation instructions.

### Usage Instructions

The "example" directory contains scripts to simplify usage. There are two
steps: loading and materialization.

The dataset must first be loaded into the EDB layer. This can be accomplished
using the "load" command of Vlog.
From the "example" directory:

    ../vlog load -f ttl -i indexDir

will create a directory "indexDir" with an index for the dataset in the
"ttl" directory (which, in this case, is a small 1 university LUBM dataset).

Materializing this dataset with for instance the LUBM1\_LE rules can then
be accomplished with:

    ../vlog mat -i indexDir --rules dlog/LUBM1_LE.dlog

The result can be stored on disk by adding the flags:

    --storemat materialization_lubm1 --decompressmat 1

which will create a directory "materialization\_lubm1 with the result of the
instantiation.
