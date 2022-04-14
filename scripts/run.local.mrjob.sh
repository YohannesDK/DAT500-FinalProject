#!/bin/bash
path=~/DAT500-FinalProject

while [ $# -gt 0 ]; do
    if [[ $1 == "-"* ]]; then
        v="${1/-/}"
        declare "$v"="$2"
        export $v
        shift
    fi
    shift
done

name="${name:-find_null_val}"
mrjob=${mrjob:-$path/mrJobs/find_null_val.py}
inputfile=${inputfile:-$path/data/2015.sample.10.csv}
outputfile=${outputfile-local.mrjob.output}
outputPath=$path/data/$outputfile

remove_mrjob_args=${mrjob_args:---cols_to_remove "$path/data/find_null_val.output"}
fill_mrjob_args=${mrjob_args:---mean_mode_values_path "$path/data/find_mean_mode.output"}

echo "Creating outputfile"
touch $outputPath

echo "Running (local) mrjob with $mrjob, file $inputfile"

if [[ "$name" == "remove_null_vals" ]]; then
    python3 $mrjob -r inline $inputfile > $outputPath $remove_mrjob_args
elif [[ "$name" == "fill_null_vals" ]]; then
    python3 $mrjob -r inline $inputfile > $outputPath $fill_mrjob_args
else 
    python3 $mrjob -r inline $inputfile > $outputPath
fi