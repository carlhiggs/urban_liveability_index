# Purpose: Clip layers to Metro 10km buffer
#          - Intersections
#          - POS
#          - Roads (shapefile w/ freeways)
# Author:  Carl Higgs
# Date:    2016 12 15

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
task = 'Clip layers (intersections; roads w freeways; pos) to Metro 10km buffer'

# INPUT PARAMETERS
folderPath    = parser.get('data', 'folderPath')
destGdb = os.path.join(folderPath,parser.get('data', 'workspace'))  

arcpy.env.workspace = destGdb
arcpy.env.scratchWorkspace = folderPath  
arcpy.env.overwriteOutput = True 

spatial_reference = arcpy.SpatialReference(parser.get('workspace', 'SpatialRef'))
clippingFeature = parser.get('workspace', 'clippingBuffer_10km')

## DATA SOURCE LIST  - TO BE CLIPPED

intersections = os.path.join(folderPath,parser.get('roads', 'intersections'))

roads     = os.path.join(folderPath,parser.get('roads', 'roads_with_freeways'))
roads_out = parser.get('roads', 'roads_with_freeways_out')
roads_sql = parser.get('roads', 'roads_sql')

pos_entry       = os.path.join(folderPath,parser.get('pos', 'pos_entry_src'))

scratch_shape  = os.path.join(arcpy.env.scratchGDB,'scratch_shape')

def basename(filePath):
  '''strip a path to the basename of file, without the extension.  Requires OS '''
  try: 
    return os.path.basename(os.path.normpath(filePath)).split(".",1)[0]
  except:
    print('Return basename failed. Did you import os?')

def clipFeature(feature,clippingFeature,where_clause,output):
  cliptask = 'Clipping feature ({}) to shape ({})...'.format(feature,clippingFeature)
  print(cliptask),
  try:
    arcpy.MakeFeatureLayer_management(feature, 'feature') 
    arcpy.Project_management('feature', scratch_shape, spatial_reference)
    arcpy.Delete_management('feature')
    arcpy.MakeFeatureLayer_management(scratch_shape, 'feature')
    arcpy.SelectLayerByLocation_management('feature', 'intersect',clippingFeature)
    if where_clause != ' ':
      # SQL Query where applicable
      arcpy.SelectLayerByAttribute_management('feature','SUBSET_SELECTION',where_clause)  
    arcpy.CopyFeatures_management('feature', output)
    print("Done.")
  except:
    print("ERROR: "+str(sys.exc_info()[0]))

clipFeature(intersections,clippingFeature,' ',basename(intersections))
clipFeature(roads,clippingFeature,roads_sql, roads_out)
clipFeature(pos_entry,clippingFeature,' ', basename(pos_entry))

# output to completion log					
script_running_log(script, task, start)

