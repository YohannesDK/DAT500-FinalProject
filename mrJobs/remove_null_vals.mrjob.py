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
                self.find_null_val_out.append((col, self.features.index(col)))
        f.close()
    
    def reducer(self, _, rows):
        for row in rows:
            for col in self.find_null_val_out:
                idx = col[1]
                if idx-self.deleted_count > 0 and idx-self.deleted_count < len(row)-1:

                    #TODO må fikse logikken her, må slette for hver rad, samtidig som vi opprettholder 
                    # indeksene (indeksene endrer seg for hver sletting)

                    del row[idx-self.deleted_count]
                    self.deleted_count += 1
            yield _, row

if __name__ == '__main__':
  Remove_Null_Vals.run()