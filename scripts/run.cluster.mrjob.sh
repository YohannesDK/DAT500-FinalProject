#!/bin/bash
while [ $# -gt 0 ]; do
    if [[ $1 == "-"* ]]; then
        v="${1/-/}"
        declare "$v"="$2"
        export $v
        shift
    fi
    shift
done

name=${name:-cluster-mrjob}
mrjob=${mrjob:-find_null_val.mrjob.py}
inputfile=${inputfile:-hdfs:///data/2015.sample.10.csv}
outputfile=${outputfile:-/data/$name.out}

command=${command:-$mrjob --hadoop-streaming-jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar -r hadoop $inputfile --output-dir hdfs://$outputfile --no-output}
remove_mrjob_args=${mrjob_args:---cols_to_remove "hdfs:///data/find_null_val.output/part-00000"}

echo "Running $name mrjob with $mrjob, file $inputfile, outputfile $outputfile"

hadoop fs -rm -r $outputfile

if [[ "$name" == "remove_null_vals" ]]; then
    python3 $command $remove_mrjob_args
else 
    python3 $command 
fi

