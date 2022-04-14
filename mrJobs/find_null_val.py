#!/usr/bin/python3

from mrjob.job import MRJob
from mrjob.step import MRStep
from mr3px.csvprotocol import CsvProtocol 

from datetime import datetime
import sys

class Find_Null_Vals(MRJob):
  INPUT_PROTOCOL = CsvProtocol
  #OUTPUT_PROTOCOL = CsvProtocol

  def configure_args(self):
    super(Find_Null_Vals, self).configure_args()
    self.add_file_arg("--features")

  def mapper_init(self):
    # TODO need to figure out how to read this in, hard code it for now
    self.features = ["FL_DATE", "OP_CARRIER", "OP_CARRIER_FL_NUM", "ORIGIN", "DEST", "CRS_DEP_TIME", "DEP_TIME", "DEP_DELAY", "TAXI_OUT", "WHEELS_OFF", "WHEELS_ON", "TAXI_IN", "CRS_ARR_TIME", "ARR_TIME", "ARR_DELAY", "CANCELLED", "CANCELLATION_CODE", "DIVERTED", "CRS_ELAPSED_TIME", "ACTUAL_ELAPSED_TIME", "AIR_TIME", "DISTANCE", "CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY", "Unnamed: 27"] 
    self.features = {idx: feature for idx, feature in enumerate(self.features)}

  def mapper(self, key, line):
    null_idx = {i for i, x in enumerate(line) if x == ""}

    for idx in self.features:
      if idx in null_idx:
        yield (self.features[idx], 1)
        continue
      yield (self.features[idx], 0)

  def reducer(self, key, value):
    value = list(value)
    n_values = len(value)
    n_null_values = sum(value)
    if n_null_values / n_values >= 0.9:
      yield(key, (n_null_values, n_values, n_null_values / n_values))


if __name__ == '__main__':
  start_time = datetime.now()
  Find_Null_Vals.run()
  end_time = datetime.now()
  elapsed_time = end_time - start_time
  sys.stderr.write("Time taken: " + str(elapsed_time))
