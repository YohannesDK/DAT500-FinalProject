from mrjob.job import MRJob
from mr3px.csvprotocol import CsvProtocol
from re import match as re_match
from re import compile as re_compile

import pandas as pd
import numpy as np

# https://stackoverflow.com/questions/37996471/element-wise-test-of-numpy-array-is-numeric
# https://stackoverflow.com/questions/354038/how-do-i-check-if-a-string-is-a-number-float/25299619#25299619

class Find_Mean_Mode(MRJob):
    INPUT_PROTOCOL = CsvProtocol
    # OUTPUT_PROTOCOL = CsvProtocol

    def configure_args(self):
        super(Find_Mean_Mode, self).configure_args()
        self.features = ["FL_DATE","OP_CARRIER","OP_CARRIER_FL_NUM","ORIGIN","DEST","CRS_DEP_TIME","DEP_TIME","DEP_DELAY","TAXI_OUT","WHEELS_OFF","WHEELS_ON","TAXI_IN","CRS_ARR_TIME","ARR_TIME","ARR_DELAY","CANCELLED","DIVERTED","CRS_ELAPSED_TIME","ACTUAL_ELAPSED_TIME","AIR_TIME","DISTANCE","CARRIER_DELAY","WEATHER_DELAY","NAS_DELAY","SECURITY_DELAY","LATE_AIRCRAFT_DELAY"]
    
    def is_number_regex(self, s):
        if re_match("^\d+?\.\d+?$", s) is None:
            isnumb = s.isdigit()
            if not isnumb:
                isnumb = s.lstrip("-").replace(".", "", 1).isdigit()
            return isnumb
        return True
    
    def combine_categorical_counts(self, values):
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
    
    # maybe we can create a combiner that joines equal [value, count] pairs togehter
    def combiner(self, key, values):
        """
        we will get a value like,
        [["FLL", 1], ["FLL", 1], ["MCO", 1], ["LAS", 1], ["ORD", 1], ["STT", 1], ["BWI", 1]]
        we need to combine equal values, and sum up the counts
        """

        if key[0] == "numeric":
            # yield key, values
            return
        
        # categorical values
        if key[0] == "categorical":
            # we need to combine equal values, and sum up the counts
            yield key, self.combine_categorical_counts(values)

    def reducer(self, key, values):
        values = list(values)

        # numerical values 
        if key[0] == "numeric":
            return
        #     yield key[0], round(np.mean(values), 2)
        
        # categorical values
        # if len(key) == 2:
        yield key, values

if __name__ == '__main__':
    Find_Mean_Mode.run()