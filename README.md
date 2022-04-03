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

To run **remove_null_vals.mrjob.py** (localy -> from mrjobs folder):
```
python3 remove_null_vals.mrjob.py -r inline ../data/2015.sample.10.csv --find_null_val_out ~/DAT500-FinalProject/data/local.mrjob.output
```