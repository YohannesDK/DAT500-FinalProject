from mrjob.job import MRJob
from mr3px.csvprotocol import CsvProtocol
from re import match as re_match
from re import compile as re_compile

import pandas as pd
import numpy as np

from datetime import datetime
import sys

class Find_Mean_Mode(MRJob):
    INPUT_PROTOCOL = CsvProtocol
    # OUTPUT_PROTOCOL = CsvProtocol

    def configure_args(self):
        super(Find_Mean_Mode, self).configure_args()
        self.features = ["FL_DATE","OP_CARRIER","OP_CARRIER_FL_NUM","ORIGIN","DEST","CRS_DEP_TIME","DEP_TIME","DEP_DELAY","TAXI_OUT","WHEELS_OFF","WHEELS_ON","TAXI_IN","CRS_ARR_TIME","ARR_TIME","ARR_DELAY","CANCELLED","DIVERTED","CRS_ELAPSED_TIME","ACTUAL_ELAPSED_TIME","AIR_TIME","DISTANCE","CARRIER_DELAY","WEATHER_DELAY","NAS_DELAY","SECURITY_DELAY","LATE_AIRCRAFT_DELAY"]
        # possible optimixation only find mean mode for empty values
    
    def is_number_regex(self, s):
        if re_match("^\d+?\.\d+?$", s) is None:
            isnumb = s.isdigit()
            if not isnumb:
                isnumb = s.lstrip("-").replace(".", "", 1).isdigit()
            return isnumb
        return True
    
    def combine_categorical_counts(self, values):
        # count number of times each value occurs
        d = {}
        for v in values:
                if not v[0] in d:
                    d[v[0]] = 1
                    continue
                d[v[0]] += 1
        return list(d.items())

    def mapper(self, key, line):
        # row
        if line != self.features:
            for idx, val in enumerate(line):
                if val == "":
                    continue

                if self.is_number_regex(val):
                    yield ["numeric", self.features[idx]], float(val)
                    continue

                yield ["categorical", self.features[idx]], (val, 1)
    
    def reducer(self, key, values):
        # numerical values 
        if key[0] == "numeric":
            yield key[1], round(np.mean(list(values)), 2)
        
        # categorical values
        if key[0] == "categorical":
            yield key[1], max(self.combine_categorical_counts(values), key=lambda x: x[1])[0]

if __name__ == '__main__':
    start_time = datetime.now()
    Find_Mean_Mode.run()
    end_time = datetime.now()
    elapsed_time = end_time - start_time
    sys.stderr.write("Time taken: " + str(elapsed_time))