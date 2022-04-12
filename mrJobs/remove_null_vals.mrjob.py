#!/usr/bin/python3
from mr3px.csvprotocol import CsvProtocol 
from mrjob.job import MRJob

import pandas as pd
import numpy as np

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
  Remove_Null_Vals.run()