#!/bin/sh
filename=${1:-find_null_val}
hadoop fs -text /data/$filename.out/part* | less
