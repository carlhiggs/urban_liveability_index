# Purpose: create and maintain a running log of scripts as completed
# Author:  Carl Higgs
# Date:    09/03/2017

def script_running_log(script = '', task = '', start = ''):
  import time 
  import os
  import sys
  
  date_time = time.strftime("%Y%m%d-%H%M%S")
  duration = (time.time() - start)/60
  output = os.path.join(sys.path[0],'script_running_log.csv')
  
  if not os.path.isfile(output):
      try:
        with open(output, "w") as outfile:
          outfile.write('script,task,datetime_completed,duration_mins\n')
        print("Created script running log: {}".format(output))
      except:
        print("note: script running log does not already exist, but attempt to initiate failed.")  
  try:
    with open(output, "a") as outfile:
        outfile.write('{},{},{},{}\n'.format(script,task,date_time,duration))
        print("Processing complete (Task: {}); duration: {:04.2f} minutes".format(task,duration))
  except:
    print("note: unable to output to script_running_log.csv")
    raise