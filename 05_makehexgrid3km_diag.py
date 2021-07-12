# Purpose: Make Hex grid corresponding to polygon feature
# Author:  Carl Higgs
# Date:    2017 03 10
#
# Note: Uses the 'Create Hexagon Tesselation' geoprocessing package
#       Author: Tim Whiteaker
#       http://www.arcgis.com/home/item.html?id=03388990d3274160afe240ac54763e57
#       freely available under the Berkeley Software Distribution license.
#       The toolbox should be located in folder with this script
#
#       It assumes that the units of analysis are in metres.

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
folderPath = parser.get('data', 'folderPath')
arcpy.ImportToolbox(os.path.join(folderPath,parser.get('data', 'create_hexagon_tbx')))

destGdb    = os.path.join(folderPath,parser.get('data', 'workspace'))  

arcpy.env.workspace = destGdb
arcpy.env.scratchWorkspace = folderPath  
arcpy.env.overwriteOutput = True 

#specify input feature
clippingFeature = parser.get('workspace', 'clippingBuffer_10km')

# specify hex parameters (diagonal width, output grid name, output buffered grid name)
hex_diag = parser.get('workspace', 'hex_diag')
hex_grid = parser.get('workspace', 'hex_grid')
hex_buffer = parser.get('workspace', 'hex_buffer') 
hex_grid_buffer =  parser.get('workspace', 'hex_grid_buffer')

# OUTPUT PROCESS
hex_side = float(hex_diag)*0.5

task1 = 'make {} km diagonal hex grid of feature {} to output feature {}'.format((float(hex_diag)*.001),clippingFeature,hex_grid)
print(task1)
arcpy.CreateHexagonsBySideLength_CreateHexagonsBySideLength(Study_Area=clippingFeature, 
                                                            Hexagon_Side_Length=hex_side, 
                                                            Output_Hexagons = hex_grid)

task2 = 'make {} km buffer of feature {} to output feature {}'.format(float(hex_buffer)*0.001,hex_grid,hex_grid_buffer)
print(task2)
arcpy.Buffer_analysis(hex_grid, hex_grid_buffer, hex_buffer)

task = task1 + " and " + task2

# output to completion log					
script_running_log(script, task, start)

