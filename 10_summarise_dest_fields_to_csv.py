# Purpose: Append header rows of all csv files in a folder to a new CSV file.
#          This is intended to provide a summary of available fields in each csv.
#          The first column in each row is the file name
# Author:  Carl Higgs
# Date:    7/12/2016


#import packages
import os
import time
import pandas as pd
import sys
from script_running_log import script_running_log
from ConfigParser import SafeConfigParser
parser = SafeConfigParser()
parser.read(os.path.join(sys.path[0],'config.ini'))

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Append header rows of all csv files in a folder (ie. respective destinations fields) to a new CSV file'

# INPUT PARAMETERS
folderPath = parser.get('data', 'folderPath')

outFolder = os.path.join(folderPath,parser.get('destinations', 'study_destinations'))  

filePath  = os.path.join(folderPath,outFolder)
 

#remember today's date
today = time.strftime("%Y%m%d")
todayhour = time.strftime("%Y%m%d_%H%M")

#Output directory
output = os.path.join(filePath,outFolder,'DestCSVfieldSummary'+todayhour+'.csv')
  
#Iterate through CSV files
for file in os.listdir(filePath):
  if "Log" not in file and "Rev" not in file and ".csv" in file:
    with open(os.path.join(filePath,file), 'r') as f:
      first_line = f.readline()
       # write to CSV
      if not os.path.isfile(output):
        try:
          with open(output, "w") as outfile:
            outfile.write('CSV_file\n')
          print("Now outputing to log file: {}".format(output))
        except:
          print("note: error occurred outputting to CSV logfile; perhaps it has not been defined.")  
      try:
        with open(output, "a") as outfile:
          outfile.write('{},{}'.format(file, first_line))
      except:
        print("note: error occurred outputting to CSV logfile; perhaps it has not been defined.")
		
# output to completion log					
script_running_log(script, task, start)

