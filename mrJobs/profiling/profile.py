import sys
from threading import local

from mrJobs.find_null_val import Find_Null_Vals
from mrJobs.find_mean_mode import Find_Mean_Mode
from mrJobs.remove_null_vals import Remove_Null_Vals
from mrJobs.fill_null_vals import Fill_Null_Vals

def main(mrjob):

     basefolder = "/home/ubuntu/DAT500-FinalProject/data"

     if mrjob == "Find_Null_Vals":
          mr_job = Find_Null_Vals(args=['-r', 'inline', f"{basefolder}/2015.sample.csv"])

     elif mrjob == "Remove_Null_Vals":
          mr_job = Remove_Null_Vals(args=['-r', 'inline', f"{basefolder}/2015.sample.csv", "--cols_to_remove", f"{basefolder}/find_null_val.output"])

     elif mrjob == "Find_Mean_Mode":
          mr_job = Find_Mean_Mode(args=['-r', 'inline', f"{basefolder}/remove_null_vals.output"])

     elif mrjob == "Fill_Null_Vals":
          mr_job = Fill_Null_Vals(args=['-r', 'inline', f"{basefolder}/remove_null_vals.output", "--mean_mode_values_path", f"{basefolder}/find_mean_mode.output"])

     if mr_job is None:
          print("Error: mrJob is None")
          return
     
          
     with mr_job.make_runner() as runner:
          runner.run()



if __name__ == "__main__":
     mrJob = sys.argv[1]
     import cProfile
     print("Profiling started")
     cProfile.runctx('main(mrJob)', globals=globals(), locals=locals(), filename=f"./mrJobs/profiling/profile.{mrJob}.dat")

     import pstats
     from pstats import SortKey

     with open(f"./mrJobs/profiling/profile.{mrJob}.time.txt", "w") as f:
          p = pstats.Stats(f"./mrJobs/profiling/profile.{mrJob}.dat", stream=f)
          p.sort_stats(SortKey.TIME).print_stats()
     
     with open(f"./mrJobs/profiling/profile.{mrJob}.cumtime.txt", "w") as f:
          p = pstats.Stats(f"./mrJobs/profiling/profile.{mrJob}.dat", stream=f)
          p.sort_stats(SortKey.CUMULATIVE).print_stats()

     with open(f"./mrJobs/profiling/profile.{mrJob}.calls.txt", "w") as f:
          p = pstats.Stats(f"./mrJobs/profiling/profile.{mrJob}.dat", stream=f)
          p.sort_stats(SortKey.CALLS).print_stats()
     
     print("Profiling complete")