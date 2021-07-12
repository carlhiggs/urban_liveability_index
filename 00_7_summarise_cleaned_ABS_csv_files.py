# Purpose: Following clean up and preparation of downloaded ABS data, this script will provide a summary
#          Display on screen and write a csv file with summary data for input csv file(s)
#          SUMMARY OUTPUT FIELDS, by variable excluding index (col 1):
#          count	mean	std	min	25%	50%	75%	max
# Author:  Carl Higgs
# Date:    7/12/2016


#import packages
import os
import pandas as pd
import matplotlib.pyplot as plt

import os
import time
import pandas as pd
import numpy as np
import sys
from script_running_log import script_running_log
from ConfigParser import SafeConfigParser
parser = SafeConfigParser()
parser.read(os.path.join(sys.path[0],'config.ini'))

# simple timer for log file
start  = time.time()
script = os.path.basename(sys.argv[0])
task   = "output summary csv tables for cleaned, derived ABS data; should cross-check these to ensure they are sensible before use.'"

# INPUT PARAMETERS
folderPath    = parser.get('data', 'folderPath')


# Specify file names - assumed to be csv files
infileList = [parser.get('abs', 'AffordableHousing'),
              parser.get('abs', 'OwnerOccupiedTORentalHousing'),
              parser.get('abs', 'CarOwnership'),
              parser.get('abs', 'MethodOfTravelToWorkPlace'),
              parser.get('abs', 'Adult18up_Employment')]
              
          
for file in infileList:
  df = pd.read_csv(os.path.join(folderPath,file), encoding='utf-8',index_col=0)
  
  summary = df.describe().T
  print('Final matrix summary statistics:')  
  print(summary.to_string())  
  
  summary.to_csv(os.path.join(folderPath,os.path.dirname(file),'SUMMARY_'+os.path.basename(file)), encoding='utf-8')
  print('Saved summary statistics for {}.'.format(file))
  
  # # Optional, non-refined code to plot and output histograms 
  # plot = df.plot.hist()
  # fig = plot.get_figure()
  # fig.savefig(os.path.join(folderPath,'SUMMARY_'+file+'.png'))


# output to completion log
script_running_log(script, task, start)
