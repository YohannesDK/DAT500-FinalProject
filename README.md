# DAT500-FinalProject
Final Project in DAT500 - Airline Delay Predictions, using Hadoop, MrJob and Spark


To run mrjob localy: (args are optional, have default run defined)
```
./scripts/run.local.mrjob.sh <mrjobfile> <outputfile> <inputfile> 
```

To run mrjob on the hadoop cluster use: (args are optional, have default run defined)
```
./scripts/run.cluster.mrjob.sh <nameOfMrjob> <mrjobfile> <inputfile> <outputfile> 
```
---

## Running Localy

To run **find_null_val.mrjob.py**:
```
./scripts/run.mrjob.sh -env local -name find_null_val -inputfile 2015.sample.10.csv
```

To run **remove_null_vals.mrjob.py**:
```
./scripts/run.mrjob.sh -env local -name remove_null_vals -inputfile 2015.sample.10.csv
```
---

## Running on Hadoop Cluster

To run **find_null_val.mrjob.py**:
```
./scripts/run.mrjob.sh -env cluster -name find_null_val -inputfile 2015.sample.10.csv
```

To run **remove_null_vals.mrjob.py**:
```
./scripts/run.mrjob.sh -env cluster -name remove_null_vals -inputfile 2015.sample.10.csv
```

To view output run,
```
./scripts/show.mrjob.results.sh <mrjob-name>
```