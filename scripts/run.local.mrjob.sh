#!/bin/sh
path=~/DAT500-FinalProject

mrjob=${1:-$path/mrJobs/find_null_val.mrjob.py}
outputfile=${2-local.mrjob.output}
inputfile=${3:-$path/data/2015.sample.10.csv}

outputPath=$path/data/$outputfile

echo "Creating outputfile"
touch $outputPath

echo "Running (local) mrjob with $mrjob, file $inputfile"
python3 $mrjob -r inline $inputfile > $outputPath
