#!/bin/bash
filename=${1:-find_null_val}
hadoop fs -text /data/$filename.output/part* | less
