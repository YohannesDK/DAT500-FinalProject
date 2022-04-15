# Profiling Notes

Data File - 2015.csv (719.07 MB)

### Find Null Values - MrJob
Running this mrjob took a lot of time, running the profiler we found that our mapper method
takes a lot of time.

It initially looked like this,
```py
def mapper_init(self):
    self.features = ["FL_DATE", "OP_CARRIER", "OP_CARRIER_FL_NUM", "ORIGIN", "DEST", "CRS_DEP_TIME", "DEP_TIME", "DEP_DELAY", "TAXI_OUT", "WHEELS_OFF", "WHEELS_ON", "TAXI_IN", "CRS_ARR_TIME", "ARR_TIME", "ARR_DELAY", "CANCELLED", "CANCELLATION_CODE", "DIVERTED", "CRS_ELAPSED_TIME", "ACTUAL_ELAPSED_TIME", "AIR_TIME", "DISTANCE", "CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY", "Unnamed: 27"] 

    if self.options.features == None:
      return

def mapper(self, key, line):
    row = pd.Series(line).replace('', np.nan)
    null_idx = np.where(row.isnull())[0]

    for col in range(len(self.features)):
      if col in null_idx:
        yield (self.features[col], 1)
        continue
      yield (self.features[col], 0)
```

This took about, 0:38:19 minutes

---

Then we optmilized the code by first changing the features list to a dict(index:column name).
...

```py
def mapper_init(self):
    # TODO need to figure out how to read this in, hard code it for now
    self.features = ["FL_DATE", "OP_CARRIER", "OP_CARRIER_FL_NUM", "ORIGIN", "DEST", "CRS_DEP_TIME", "DEP_TIME", "DEP_DELAY", "TAXI_OUT", "WHEELS_OFF", "WHEELS_ON", "TAXI_IN", "CRS_ARR_TIME", "ARR_TIME", "ARR_DELAY", "CANCELLED", "CANCELLATION_CODE", "DIVERTED", "CRS_ELAPSED_TIME", "ACTUAL_ELAPSED_TIME", "AIR_TIME", "DISTANCE", "CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY", "Unnamed: 27"] 
    self.features = {idx: feature for idx, feature in enumerate(self.features)}

```
We also changed the way we found empty values,
```py
def mapper(self, key, line):
    null_idx = {i for i, x in enumerate(line) if x == ""}

    for idx in self.features:
      if idx in null_idx:
        ...
```
now that our list of empty values, i.e null_idx, is a set, we have O(1) lookup time when we do our
check, before we yield any results.

This time it took about, 0:18:41 minutes. Doubled the speed (even though we are running with sub optimal configuration blocks / cores)


---

Running the same but with the entire dataset took about 1:00:37 hour. (optimized version)
We also ran the not optimized version earlier and it took about, 1:29:59 hour, so a 30 min difference.

One thing we noticed was that we are not using the number of possible vcores or memory, when the mappers are done
    - need to figure out how to speed this up

### Remove Null Vals
Not optimized version - about 0:19:00.433907 minutes
```py
class Remove_Null_Vals(MRJob):
    INPUT_PROTOCOL = CsvProtocol
    OUTPUT_PROTOCOL = CsvProtocol

    def configure_args(self):
        super(Remove_Null_Vals, self).configure_args()
        self.add_file_arg('--cols_to_remove_path', type=str, help="Path to file containing columns to remove")
        # self.add_passthru_arg('--count', type=int, help="Number of rows to output")

    def load_args(self, args):
        super(Remove_Null_Vals, self).load_args(args)
        self.cols_to_remove = None

        if self.options.cols_to_remove_path is None:
            self.arg_parser.error('You must specify the --cols_to_remove_path')
        else:
            self.cols_to_remove = self.options.cols_to_remove_path
    
    
    def reducer_init(self):
        self.features = ["FL_DATE", "OP_CARRIER", "OP_CARRIER_FL_NUM", "ORIGIN", "DEST", "CRS_DEP_TIME", "DEP_TIME", "DEP_DELAY", "TAXI_OUT", "WHEELS_OFF", "WHEELS_ON", "TAXI_IN", "CRS_ARR_TIME", "ARR_TIME", "ARR_DELAY", "CANCELLED", "CANCELLATION_CODE", "DIVERTED", "CRS_ELAPSED_TIME", "ACTUAL_ELAPSED_TIME", "AIR_TIME", "DISTANCE", "CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY", "Unnamed: 27"] 
        self.cols_idx_to_remove = []

        if self.cols_to_remove == None:
            return

        with open(self.cols_to_remove) as f:
            lines = f.readlines()
            for line in lines:
                col, _ = line.split("\t")
                col = col.replace('"', "") 
                self.cols_idx_to_remove.append(self.features.index(col))
        f.close()
    
    def reducer(self, _, rows):
        for row in rows: 
            yield None, [val for idx, val in enumerate(row) if idx not in self.cols_idx_to_remove]

if __name__ == '__main__':
    start_time = datetime.now()
    Remove_Null_Vals.run()
    end_time = datetime.now()
    elapsed_time = end_time - start_time
    sys.stderr.write("Time taken: " + str(elapsed_time))
```

Optimized version - 0:18:07.541699 minutes

not that much difference

but reducer is still running with 2 cpu vcores, and not using entire amount of available memory

```py
```

### Find mean mode
 time taken - 1:02:56.716699 hours


### Fill null vals
TODO... (should take about 1 hour, when looking at the job in comparison with the other mrjobs)
