# Purpose: Python set up study region boundaries
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
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'create study region boundary files in new geodatabase'


# INPUT PARAMETERS
folderPath    = parser.get('data', 'folderPath')
arcpy.env.workspace = folderPath  
arcpy.env.scratchWorkspace = folderPath  
arcpy.env.overwriteOutput = True 


# output file geodatabase (gdb)
gdbName = parser.get('data', 'workspace')

# define spatial reference
SpatialRef = arcpy.SpatialReference(parser.get('workspace', 'SpatialRef'))

state = parser.get('data', 'state')
state_sos = state + parser.get('data', 'sos_region')
state_sd  = state + parser.get('data', 'sd_region')
region    = state + parser.get('data', 'sd_region') + parser.get('data', 'sos_region')

# Define locations of features to be projected to new geodatabase features
#  1. location of ABS Statistical Division 2011 .shp file 
#  2. location of ABS Section of State 2011 .shp file
features = [os.path.join(folderPath,parser.get('abs', 'abs_sos')),
            os.path.join(folderPath,parser.get('abs', 'abs_sd'))]

			
			
names = [state_sos, state_sd]
            
# SQL where_clause for each feature (inclusion criteria)
where_clause =  [''' "STE_NAME11" = \'Victoria\' AND "SOS_NAME11" IN( \'Major Urban\''' , \'Other Urban\' )  ''',''' "SD_NAME11"  = \'Melbourne\' ''']

where_clause =  [parser.get('abs', 'abs_sos_where_clause'),
                 parser.get('abs', 'abs_sd_where_clause')]
				
# OUTPUT PROCESS

# Create output gdb if not already existing
if gdbName.find('.') == -1:
    gdbName += ".gdb"
arcpy.CreateFileGDB_management(folderPath, gdbName)
arcpy.AddMessage("File Geodatabase Complete")

Output_Workspace=os.path.join(folderPath,gdbName)



input = ""
# loop over features
for feature in range (0, len(features)):
  arcpy.MakeFeatureLayer_management(r"%s" %features[feature],names[feature]) 
  print("Feature: "+features[feature])
  print("Query: "+where_clause[feature])
  # select subset of features to be included
  arcpy.SelectLayerByAttribute_management(in_layer_or_view  = names[feature], 
                                          selection_type    = "NEW_SELECTION", 
                                          where_clause      = " %s " %where_clause[feature])  
  # create copy of selected features as new feature class
  arcpy.CopyFeatures_management(names[feature], os.path.join("in_memory",names[feature]))
  
  # project subset feature class copy to desired spatial reference system (e.g. GDA 1994 VICGRID94)
  arcpy.Project_management(os.path.join("in_memory",names[feature]), 
                           os.path.join(Output_Workspace,names[feature]), 
                           SpatialRef)

# Clip Victorian Urban areas to Victorian Metro region as 'VicUrbanMetro'
arcpy.Clip_analysis(os.path.join(Output_Workspace,state_sos), 
                    os.path.join(Output_Workspace,state_sd), 
                    os.path.join(folderPath,gdbName,region))

# output to completion log					
script_running_log(script, task, start)

