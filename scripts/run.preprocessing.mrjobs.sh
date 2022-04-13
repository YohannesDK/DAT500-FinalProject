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
file"${file:-2015.sample.10.csv}"

echo "Running find_null_val.mrjob.py"
"$basepath/scripts/run.mrjob.sh" -env "$env" -name "find_null_val" -inputfile "$file" && \
printf '%*s\n' "${COLUMNS:-$(tput cols)}" '' | tr ' ' -

echo -e "\nRunning remove_null_vals.mrjob.py"
"$basepath/scripts/run.mrjob.sh" -env "$env" -name "remove_null_vals" -inputfile "$file" && \
printf '%*s\n' "${COLUMNS:-$(tput cols)}" '' | tr ' ' -

echo -e "\nRunning find_mean_mode.mrjob.py"
"$basepath/scripts/run.mrjob.sh" -env "$env" -name "find_mean_mode" -inputfile "remove_null_vals.output" && \
printf '%*s\n' "${COLUMNS:-$(tput cols)}" '' | tr ' ' -

echo -e "\nRunning fill_null_vals.mrjob.py"
"$basepath/scripts/run.mrjob.sh" -env "$env" -name "fill_null_vals" -inputfile "remove_null_vals.output"