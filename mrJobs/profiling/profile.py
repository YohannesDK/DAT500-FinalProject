from mrJobs.find_null_val import Find_Null_Vals

def main():
     mr_job = Find_Null_Vals(args=['-r', 'inline', "/home/ubuntu/DAT500-FinalProject/data/2015.sample.csv"])
     with mr_job.make_runner() as runner:
          runner.run()



if __name__ == "__main__":
     import cProfile
     cProfile.run('main()', "./mrJobs/profiling/profile.dat")

     import pstats
     from pstats import SortKey

     with open("./mrJobs/profiling/profile.time.txt", "w") as f:
          p = pstats.Stats("./mrJobs/profiling/profile.dat", stream=f)
          p.sort_stats(SortKey.TIME).print_stats()
     
     with open("./mrJobs/profiling/profile.cumtime.txt", "w") as f:
          p = pstats.Stats("./mrJobs/profiling/profile.dat", stream=f)
          p.sort_stats(SortKey.CUMULATIVE).print_stats()

     with open("./mrJobs/profiling/profile.calls.txt", "w") as f:
          p = pstats.Stats("./mrJobs/profiling/profile.dat", stream=f)
          p.sort_stats(SortKey.CALLS).print_stats()