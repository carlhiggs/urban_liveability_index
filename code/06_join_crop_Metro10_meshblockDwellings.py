# Purpose: Project meshblock shapefile to correct spatial reference, join w/ dwelling data, crop to Metro Urban area
# Author:  Carl Higgs
# Date:    21/12/2016

import os
import pandas
import arcpy
import time
import sys
from script_running_log import script_running_log
from ConfigParser import SafeConfigParser
parser = SafeConfigParser()
parser.read(os.path.join(sys.path[0],'config.ini'))

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Project meshblock shapefile to correct spatial reference; join w/ dwelling data; and crop to Metro Urban area'

# INPUT PARAMETERS
folderPath = parser.get('data', 'folderPath')
destGdb    = os.path.join(folderPath,parser.get('data', 'workspace'))  
arcpy.env.workspace = destGdb
arcpy.env.scratchWorkspace = folderPath
arcpy.env.qualifiedFieldNames = False  
arcpy.env.overwriteOutput = True 

spatial_reference = arcpy.SpatialReference(parser.get('workspace', 'SpatialRef'))

meshblocks    = os.path.join(folderPath,parser.get('abs', 'meshblocks'))
meshblock_id = parser.get('abs', 'meshblock_id')

dwellings     = os.path.join(folderPath,parser.get('abs', 'dwellings'))
dwellings_str = os.path.join(folderPath,parser.get('abs', 'dwellings_string'))
dwellings_id  = parser.get('abs', 'dwellings_id')

# metro urban area for selecting features to output to gdb feature
mb_dwellings = parser.get('workspace', 'mb_dwellings')

scratchShape  = os.path.join(arcpy.env.scratchGDB,mb_dwellings)
scratchTable  = os.path.join(arcpy.env.scratchGDB,'out_tab')

clippingFeature = parser.get('workspace', 'clippingBuffer_10km')

# make sure that the ID column of dwellings data is string to match with shapefile
# ArcGIS reads column as string if first cell identifies as such.
df = pandas.read_csv(dwellings)
if df[dwellings_id].dtype != 'object':
  df[dwellings_id] = df[dwellings_id].astype('str')
  df2 = df[:1].copy()
  df2.loc[0,dwellings_id] = 'dummy'
  df2.append(df).to_csv(dwellings_str, index = False)



# make feature layer and index  
arcpy.MakeFeatureLayer_management(meshblocks, 'layer')
arcpy.Project_management('layer', scratchShape, spatial_reference)
arcpy.Delete_management('layer')
arcpy.MakeFeatureLayer_management(scratchShape, 'layer')

# copy csv rows to memory -- endows w/ OID to facilitate attribute selection
#  also possibly avoids strange behaviour stalling access to joined table
arcpy.CopyRows_management(dwellings_str, scratchTable)
arcpy.MakeTableView_management(scratchTable, 'temp_csv', '"Dwellings" > 0')

arcpy.AddJoin_management(in_layer_or_view = 'layer', 
                         in_field         = meshblock_id,
                         join_table       = 'temp_csv',
                         join_field       = dwellings_id,
                         join_type        = "KEEP_ALL")                         
                         
arcpy.SelectLayerByLocation_management ('layer', select_features = clippingFeature )

# Copy joined, cropped Urban Metro meshblock + dwellings feature to project geodatabase
arcpy.CopyFeatures_management('layer', mb_dwellings)    

# output to completion log					
script_running_log(script, task, start)

