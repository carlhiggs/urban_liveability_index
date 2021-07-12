# Purpose: buildVicUrbanMetro_PedRoads.txt
# Author:  Carl Higgs
# Date:    2017 03 10
#
# Note:  This script dissolves an existing network dataset into a new geodatabase,
#        and then rebuilds the network dataset.
#
#        It assumes that the existing network dataset to be processed has already 
#        been manually 'constructed' and built, and is located within the scratch.gdb
#        geodatabase within the folderPath directory.
#
#        Also, may require ArcGIS 10.5 -- so if you don't have this, do it manually.
#        The dissolve process may not work (didn't on ArcGIS 10.4.1 when attempted with Ballarat subregion)
#          - it isn't crucial, so if it doesn't work just build and push on.
#
#        See script metrourban_roadnetworksetup.py for details.

import arcpy
import os
import time
import sys
from script_running_log import script_running_log
from ConfigParser import SafeConfigParser
parser = SafeConfigParser()
parser.read(os.path.join(sys.path[0],'config.ini'))

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'dissolve an existing network dataset into a new geodatabase and then rebuild the network dataset'

# INPUT PARAMETERS
folderPath    = parser.get('data', 'folderPath')
destGdb = os.path.join(folderPath,parser.get('data', 'workspace'))  

arcpy.env.workspace = destGdb
arcpy.env.scratchWorkspace = folderPath  
arcpy.env.overwriteOutput = True 

networkFeature = parser.get('roads', 'pedestrian_road_stub')+"_ND"

try:
  arcpy.CheckOutExtension("network")
  
  print("dissolving {} to {}...".format(networkFeature,destGdb)),
  arcpy.DissolveNetwork_na(in_network_dataset=os.path.join(arcpy.env.scratchGDB,networkFeature), out_workspace_location=destGdb)
  print(" Done.")
  
  print("Building {}...".format(os.path.join(networkFeature,destGdb))),
  arcpy.BuildNetwork_na(in_network_dataset=os.path.join(destGdb,networkFeature))
  print(" Done.")

except:
  raise
	
finally:
  arcpy.CheckInExtension("network")

# output to completion log					
script_running_log(script, task, start)

