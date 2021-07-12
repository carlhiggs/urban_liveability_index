# Purpose: Set up road network for Vic Metro Urban region
# Author:  Carl Higgs
# Date:    2016 11 08
#
# Note:  This script copies the component features, however the network dataset 
#        must be created from scratch using ArcCatalog (as in Dec 2016 w/ ArcGIS 10.3).
#        The pilot used default settings (as per previous versions of the pedestrian network)
#        however, introducation of impedence and elevation paramaters could be appropriate.
#
#        Steps to build network following running this script:
#
#        First, ensure network analyst extension is enabled!
#        0. Locate the pedestrian_road_network dataset in the folderPath scratch.gdb
#        1. right click on pedestrian_road_network feature
#            and select 'New > Network Dataset'
#        2. Default name
#        3. Default classes
#        4. Yes, model global turns	
#        5. Default connectivity
#        6. No elevation modelling	
#        7. Default Length attribute (leave as is - should be in metres already)
#        8. No entries in travel mode
#        9. No driving directions
#       10. Select to build service area index (trialling this)
#       11. Click 'finish' and when prompted select to build network at this point
#       
#           Done!!
#


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
task = 'make metro urban road network'

# INPUT PARAMETERS
folderPath    = parser.get('data', 'folderPath')
destGdb = os.path.join(folderPath,parser.get('data', 'workspace'))  


SpatialRef = arcpy.SpatialReference(parser.get('workspace', 'SpatialRef'))


#specify input network dataset, contained features to be copied, and clipping features
inputNetwork = os.path.join(folderPath,parser.get('roads', 'network_source_data'))
clippingFeature = os.path.join(destGdb,parser.get('workspace', 'clippingBuffer_10km'))

# specify output path
outputNetwork = parser.get('roads', 'pedestrian_road_stub')

arcpy.env.workspace = inputNetwork
arcpy.env.scratchWorkspace = folderPath  
arcpy.env.overwriteOutput = True 

features = arcpy.ListFeatureClasses()
arcpy.Delete_management(arcpy.env.scratchGDB)

# OUTPUT PROCESS
# create empty Pedestrian Roads network dataset with spatial reference same as input (GDA 1994 VICGRID94)
arcpy.CreateFeatureDataset_management(arcpy.env.scratchGDB,outputNetwork, spatial_reference = SpatialRef)

# clip features
print("Cropping source network:")
print(inputNetwork)
print("to buffered study region as:")
print(os.path.join(arcpy.env.scratchGDB,outputNetwork))

for fc in features:
  print(fc)
  print(os.path.join(inputNetwork,fc))
  # arcpy.Clip_analysis(os.path.join(inputNetwork,fc), clippingFeature, outputNetwork)
  arcpy.MakeFeatureLayer_management(os.path.join(inputNetwork,fc), 'feature') 
  arcpy.SelectLayerByLocation_management('feature', 'intersect',clippingFeature)
  arcpy.CopyFeatures_management('feature', os.path.join(arcpy.env.scratchGDB,outputNetwork,fc))
  

# output to completion log					
script_running_log(script, task, start)

