#!/bin/sh
name=${1:-cluster-mrjob}
mrjob=${2:-find_null_val.mrjob.py}
inputfile=${3:-hdfs:///data/2015.sample.10.csv}
outputfile=${4:-/data/$name.out}


echo "Running $name mrjob with $mrjob, file $inputfile, outputfile $outputfile"

hadoop fs -rm -r $outputfile
python3 $mrjob --hadoop-streaming-jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar -r hadoop $inputfile --output-dir hdfs://$outputfile --no-output
