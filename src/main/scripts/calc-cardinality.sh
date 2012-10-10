#!/usr/bin/env sh

runloc=`dirname $0`
JAR=$runloc/../lib/datacube-space-eval-*.jar

hadoop jar \
    $JAR \
    $*
