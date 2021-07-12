# Purpose: Process a network matrix csv file w/ total column
#          NOTE: MAKE SURE THAT COLUMNS CORRESPOND TO ROWS, SUCH THAT DIAGONAL IS THE INTERSECTION OF LIVE AND WORK!!!!
#              otherwise, results will be false.
#          OUTPUT:
#          [id,  diagonal as a numerator column, total as denominator column, proportion = numerator/denominator]
#          Specifically, this is used to process ABS derived files:
#              ** UsualResidence_by_PlaceOfWork > LiveWorkSameSA3  **
# Author:  Carl Higgs
# Date:    7/12/2016


#import packages
import os
import time
import pandas as pd
import numpy as np
import sys
import psycopg2 

from script_running_log import script_running_log
from ConfigParser import SafeConfigParser
parser = SafeConfigParser()
parser.read(os.path.join(sys.path[0],'config.ini'))

# simple timer for log file
start  = time.time()
script = os.path.basename(sys.argv[0])
task   = "process ABS derived file 'UsualResidence_by_PlaceOfWork' into 'LiveWorkSameSA3'"

# SQL Settings - storing passwords in plain text is obviously not ideal
sqlDBName   = parser.get('postgresql', 'database')
sqlUserName = parser.get('postgresql', 'user')
sqlPWD      = parser.get('postgresql', 'password')

# INPUT PARAMETERS
folderPath    = parser.get('data', 'folderPath')

# Specify file names - assumed to be csv files
infileName  = os.path.join(folderPath,parser.get('abs', 'UsualResidence_by_PlaceOfWork'))
outfileName = os.path.join(folderPath,parser.get('abs', 'LiveWorkSameSA3'))

indexName = 'SA3_NAME11'
diagonalName = 'SA3_LiveWork'
totalName = 'SA3_TotalWork'

sa2UR_sa3POW = pd.read_csv(os.path.join(folderPath,infileName), encoding='utf-8',index_col=0)
sa2UR_sa3POW = sa2UR_sa3POW.reset_index()
# Get SA2 to SA3 look up table
conn = psycopg2.connect(database=sqlDBName, user=sqlUserName, password=sqlPWD)
curs = conn.cursor()
curs.execute("SELECT DISTINCT(sa2_name11),SA3_name11 FROM abs_linkage GROUP BY sa2_name11, sa3_name11 ORDER BY sa3_name11;")
area_lookup = pd.DataFrame(curs.fetchall(), columns=['sa2_name11', 'sa3_name11'])

# Merge lookup with usual residence (UR) by place of work (POW)
sa2UR_sa3POW__SA3match = pd.merge(area_lookup, sa2UR_sa3POW, how='left', left_on='sa2_name11', right_on='UsualResidenceSA2')
sa2UR_sa3POW__SA3match = sa2UR_sa3POW__SA3match.set_index('sa2_name11')

sa2UR_sa3POW__SA3match.loc["Bundoora - East"][sa2UR_sa3POW__SA3match.loc["Bundoora - East"]["sa3_name11"]]
sa2UR_sa3POW__SA3match['SA3_LiveWork'] = sa2UR_sa3POW__SA3match.apply(lambda x: x[x['sa3_name11']], axis=1)
sa2UR_sa3POW__SA3match['SA3_TotalWork'] = sa2UR_sa3POW__SA3match['Total']-sa2UR_sa3POW__SA3match['POW not applicable']-sa2UR_sa3POW__SA3match['POW not stated']
sa2UR_sa3POW__SA3match['SA3_propLiveWork'] = sa2UR_sa3POW__SA3match['SA3_LiveWork']/sa2UR_sa3POW__SA3match['SA3_TotalWork']

output = sa2UR_sa3POW__SA3match[['SA3_LiveWork','SA3_TotalWork','SA3_propLiveWork']]

try:
  output.to_csv(os.path.join(folderPath,outfileName), index=True, index_label = indexName, encoding='utf-8')
  print('Saved {}.'.format(outfileName))
except:
  print('Not saved.  Perhaps the file is open or name mis-specified?')
  
print('Final matrix summary statistics:')

summary = output.reset_index().describe().T

print(summary.to_string())


summary.to_csv(os.path.join(os.path.dirname(outfileName),'SUMMARY_'+os.path.basename(outfileName)), encoding='utf-8')
print('Saved summary statistics for {}.'.format(outfileName))

# output to completion log
script_running_log(script, task, start)
