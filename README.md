This package contains utilities for estimating the number of counters
required by a datacube: http://en.wikipedia.org/wiki/OLAP_cube

The first utility is a MapReduce job that calculate the cardinality of
all fields in a data set.  The second utility calculates the powerset
of all fields, then multiplies each element in the powerset by its
associated cardinality.  This script produces a single value, which is
the total number of counters required to track all possible
combinations of all field values.

To build the software, run 'mvn package'.  This will generate a tar.gz package in the target/ directory. The tar.gz package contains scripts in the bin/ directory, and some sample files in samples/.

Example calculation run commands:
```bash
bin/calc-cardinality.sh '|' 42 mydata.txt cardinality
hadoop fs -cat cardinality/part-r-* > cardinality.txt
bin/calc-num-counters.py cardinality.txt
```
