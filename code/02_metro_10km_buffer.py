# Purpose: Create 10km metro buffer
# Author:  Carl Higgs
# Date:    2016 11 01

import arcpy
import os
import time
import sys
from script_running_log import script_running_log
from ConfigParser import SafeConfigParser
parser = SafeConfigParser()
parser.read(os.path.join(sys.path[0],'config.ini'))

# simple timer for log file
start  = time.time()
script = os.path.basename(sys.argv[0])

# INPUT PARAMETERS
folderPath    = parser.get('data', 'folderPath')
gdbName = parser.get('data', 'workspace')
destGdb = os.path.join(folderPath,gdbName)  
arcpy.env.workspace = destGdb
arcpy.env.overwriteOutput = True 

#specify input feature, buffer distance and output
input =  parser.get('data', 'study_region')
buffer = parser.get('data', 'study_buffer_metres')
output = parser.get('data', 'study_region_buffer')

# OUTPUT PROCESS
task = 'make ({}) unit buffer of feature ({}) to output feature ({})'.format(buffer,input,output)

# make buffer
arcpy.Buffer_analysis(input, output, buffer)

# output to completion log
script_running_log(script, task, start)

