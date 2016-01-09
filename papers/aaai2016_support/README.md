### AAAI 2016 paper

This directory contains some support material for our AAAI 2016 paper titled
"Column-Oriented Datalog Materialization for Large Knowledge Graphs".

### Datasets

Most of the datasets are publically available on the
[RDFox 2014 evaluation page](http://www.cs.ox.ac.uk/isg/tools/RDFox/2014/AAAI/):

* [LUBM_1K](https://krr-nas.cs.ox.ac.uk/2014/AAAI/RDFox/LUBM-01K.zip) (698M)
* [LUBM 5K](https://krr-nas.cs.ox.ac.uk/2014/AAAI/RDFox/LUBM-05K.zip) (3.4G)
* [Claros](https://krr-nas.cs.ox.ac.uk/2014/AAAI/RDFox/Claros.zip) (161M)
* [DBpedia](https://krr-nas.cs.ox.ac.uk/2014/AAAI/RDFox/DBpedia.zip) (1.3G)

Note that these files are zipped, but Vlog currently can read only gzipped or
uncompressed files, so you would need to decompress them first.

The Claros-sample.nt.gz file contains the Claros-S sample mentioned in the paper.

### Rulesets

The "rulesets" directory
contains all the rulesets that we used in our experiments to compute the
materialization with our approach. They are rewritings of the RDFox rules
that use a slightly different syntax and that include additional rules for
transforming triples (stored in the EDB layer) into IDB facts.

### Running Vlog

See the README in the Vlog root directory and the example directory on
how to run Vlog.
