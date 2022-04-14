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
