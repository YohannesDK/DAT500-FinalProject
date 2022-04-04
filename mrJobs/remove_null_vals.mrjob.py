#!/usr/bin/python3
from mr3px.csvprotocol import CsvProtocol 
from mrjob.job import MRJob

import pandas as pd
import numpy as np

class Remove_Null_Vals(MRJob):
    INPUT_PROTOCOL = CsvProtocol

    def configure_args(self):
        super(Remove_Null_Vals, self).configure_args()
        self.add_file_arg("--find_null_val_out")
    
    def reducer_init(self):
        self.features = ["FL_DATE", "OP_CARRIER", "OP_CARRIER_FL_NUM", "ORIGIN", "DEST", "CRS_DEP_TIME", "DEP_TIME", "DEP_DELAY", "TAXI_OUT", "WHEELS_OFF", "WHEELS_ON", "TAXI_IN", "CRS_ARR_TIME", "ARR_TIME", "ARR_DELAY", "CANCELLED", "CANCELLATION_CODE", "DIVERTED", "CRS_ELAPSED_TIME", "ACTUAL_ELAPSED_TIME", "AIR_TIME", "DISTANCE", "CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY", "Unnamed: 27"] 
        self.find_null_val_out = []
        self.deleted_count = 0

        if self.options.find_null_val_out == None:
            return

        with open(self.options.find_null_val_out) as f:
            lines = f.readlines()
            for line in lines:
                col, _ = line.split("\t")
                col = col.replace('"', "") 
                self.find_null_val_out.append(self.features.index(col))
        f.close()
    
    def reducer(self, _, rows):
        for row in rows: 
            yield _, [val for idx, val in enumerate(row) if idx not in self.find_null_val_out]

if __name__ == '__main__':
  Remove_Null_Vals.run()