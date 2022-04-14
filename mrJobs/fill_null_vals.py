#!/usr/bin/python3
from mr3px.csvprotocol import CsvProtocol 
from mrjob.job import MRJob

from datetime import datetime
import sys

class Fill_Null_Vals(MRJob):
    INPUT_PROTOCOL = CsvProtocol
    OUTPUT_PROTOCOL = CsvProtocol

    def configure_args(self):
        super(Fill_Null_Vals, self).configure_args()
        self.add_file_arg("--mean_mode_values_path", type=str, help="Path to file containing mean/mode values")
    
    def load_args(self, args):
        super(Fill_Null_Vals, self).load_args(args)
        self.mean_mode_values = None

        if self.options.mean_mode_values_path is None:
            self.arg_parser.error("You must specify the --mean_mode_values_path")
        else:
            self.mean_mode_values = self.options.mean_mode_values_path
        
    def reducer_init(self):
        self.features = ["FL_DATE","OP_CARRIER","OP_CARRIER_FL_NUM","ORIGIN","DEST","CRS_DEP_TIME","DEP_TIME","DEP_DELAY","TAXI_OUT","WHEELS_OFF","WHEELS_ON","TAXI_IN","CRS_ARR_TIME","ARR_TIME","ARR_DELAY","CANCELLED","DIVERTED","CRS_ELAPSED_TIME","ACTUAL_ELAPSED_TIME","AIR_TIME","DISTANCE","CARRIER_DELAY","WEATHER_DELAY","NAS_DELAY","SECURITY_DELAY","LATE_AIRCRAFT_DELAY"]
        self.d = {}

        if self.mean_mode_values == None:
            return
        
        with open(self.mean_mode_values) as f:
            lines = f.readlines()
            for line in lines:
                col, val = line.strip().split("\t")
                col = "".join(c for c in col if c.isalpha() or c == "_") #remove som backslashes
                self.d[col] = val
        f.close()
    
    def reducer(self, key, rows):
        # yield None, list(self.d.items())
        # return
        for row in rows:
            yield None, [val if val != "" else self.d[col] for col, val in zip(self.features, row)]

if __name__ == '__main__':
    start_time = datetime.now()
    Fill_Null_Vals.run()
    end_time = datetime.now()
    elapsed_time = end_time - start_time
    sys.stderr.write("Time taken: " + str(elapsed_time))