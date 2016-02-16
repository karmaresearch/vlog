### Vlog

This is Vlog, an experimental column-oriented Datalog engine.

Please read the INSTALL file for compilation instructions.

### Usage Instructions

The "example" directory contains scripts to simplify usage. There are two
steps: loading and materialization.

The dataset must first be loaded into the EDB layer. This can be accomplished
using the "load" command of Vlog.
From the "example" directory:

    ../vlog load -i ttl -o indexDir

will create a directory "indexDir" with an index for the dataset in the
"ttl" directory (which, in this case, is a small 1 university LUBM dataset).

To materialize this dataset with for instance the LUBM1\_LE rules,
first an EDB layer configuration file "edb.conf" needs to be created, with the
following content:

EDB0\_predname=TE  
EDB0\_type=Trident  
EDB0\_param0=indexDir  

This specifies that the EDB predicate name is "TE", that the EDB layer name = "Trident",
and that the dataset can be found in the directory "indexDir".

Then, the materialization can be created with the following command:

    ../vlog mat -e edb.conf --rules dlog/LUBM1_LE.dlog

The result can be stored on disk by adding the flags:

    --storemat materialization_lubm1 --decompressmat 1

which will create a directory "materialization\_lubm1 with the result of the
instantiation.

### TODO

Describe usage of MySQL, MonetDB, ODBC layers.
