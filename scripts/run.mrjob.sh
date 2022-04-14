#!/bin/bash

basepath=~/DAT500-FinalProject

while [ $# -gt 0 ]; do
    if [[ $1 == "-"* ]]; then
        v="${1/-/}"
        declare "$v"="$2"
        shift
    fi
    shift
done

env="${env:-local}"
name="${name:-find_null_val}"
inputfile="${inputfile:-2015.sample.10.csv}"

mrjob="$basepath/mrJobs/$name.py"
outputfile="$name.output"

if [[ "$env" == "local" ]]; then
    inputfile="$basepath/data/$inputfile"
    exec "$basepath/scripts/run.local.mrjob.sh" -name "$name" -mrjob "$mrjob" -inputfile "$inputfile" -outputfile "$outputfile"
else
    inputfile="hdfs:///data/$inputfile"
    outputfile="/data/$outputfile"
    exec "$basepath/scripts/run.cluster.mrjob.sh" -name "$name" -mrjob "$mrjob" -inputfile "$inputfile" -outputfile "$outputfile"
fi