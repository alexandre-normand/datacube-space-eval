#!/usr/bin/env python

USAGE = """calc-num-counters.py cardinality-input-file 
           Determine the number of datacube counters required to support the number
           of dimensions and associated cardinality specified in cardinality-input-file.
           cardinality input file must be structured as lines with:
          '<dimension>  <cardinality>' on each line. """

import sys

def powerSet(sequence):
    if sequence:
        head, tail = sequence[:1], sequence[1:]
        for smaller in powerSet(tail):
            yield smaller
            yield head + smaller
    else:
        yield []

def numCounters(dimensionsToCardinalityMap, verbose):
    totalCounters = 0
    for sequence in powerSet(dimensionsToCardinalityMap.keys()):
        if len(sequence):
            setCounter = 1
            if(verbose):
                print >> sys.stderr, [dimensionsToCardinalityMap[dim] for dim in sequence]
            for dimension in sequence:
                setCounter *= dimensionsToCardinalityMap[dimension]
            totalCounters += setCounter
    return totalCounters

def main():
    if len(sys.argv) != 2:
        print >> sys.stderr, USAGE
        sys.exit(99)

    verbose = True

    dimensionsToCardinality = {}
    for line in open(sys.argv[1]):
        (dimension,cardinality) = line.split()
        dimensionsToCardinality[dimension] = int(cardinality)

    print >> sys.stdout, numCounters(dimensionsToCardinality, verbose)

if __name__ == "__main__":
    main()
