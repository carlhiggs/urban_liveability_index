# Purpose: This script creates OD matrix 
#          It is intended to find distance from parcel to closest large park (gr 1.5Ha)
# Input:   requires network dataset
# Authors: Carl Higgs, Koen Simons

import arcpy, arcinfo
import os
import time
import multiprocessing
import sys
import psycopg2 
import numpy as np
from progressor import progressor

from script_running_log import script_running_log
from ConfigParser import SafeConfigParser


parser = SafeConfigParser()
parser.read(os.path.join(sys.path[0],'config.ini'))

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])

# INPUT PARAMETERS
folderPath = parser.get('data', 'folderPath')
urbanGDB = os.path.join(folderPath,parser.get('data', 'workspace'))  

arcpy.env.workspace = urbanGDB  
arcpy.env.scratchWorkspace = folderPath   
arcpy.env.qualifiedFieldNames = False  
arcpy.env.overwriteOutput = True 

# Specify geodatabase with feature classes of "origins"
A_points = os.path.join(urbanGDB,parser.get('parcels', 'parcel_dwellings'))
A_pointsID = parser.get('parcels', 'parcel_id')


## specify "destinations"
B_points =  parser.get('pos', 'pos_entry')
B_pointsID = parser.get('pos', 'pos_entry_id')

# make POS feature layer where size is greater than 1.5 Ha, ie. 15000m2
arcpy.MakeFeatureLayer_management (B_points, "B_pointsLayer", " HA >= 1.5")  


## Network settings
in_network_dataset = parser.get('roads', 'pedestrian_road_network')

# specify street shape feature within network to find locations
locateShape = parser.get('roads', 'pedestrian_road_edges')

# specify junctions file which will be excluded from search
noLocateJunctions = parser.get('roads', 'pedestrian_road_junctions')

# specify search tolerance in units of input file (Features outside tolerance are not located)
searchTolerance =  parser.get('network', 'tolerance')

cutoff =  parser.get('network', 'limit')


## Hex details (polygon feature to iterate over)
polygons = parser.get('workspace', 'hex_grid')
polyBuffer =  parser.get('workspace', 'hex_grid_buffer')

hexStart = 0


# destination code, for use in log file 
dest_code = "pos_greq15000m2"

# SQL Settings
sqlDBName   = parser.get('postgresql', 'database')
sqlUserName = parser.get('postgresql', 'user')
sqlPWD      = parser.get('postgresql', 'password')

sqlTableName  = "dist_cl_od_parcel_pos_gr15km2"
log_table    = "log_dist_cl_od_parcel_dest"
queryPartA = "INSERT INTO {} VALUES ".format(sqlTableName)

sqlChunkify = 500

        
# initiate postgresql connection
conn = psycopg2.connect(database=sqlDBName, user=sqlUserName, password=sqlPWD)
curs = conn.cursor()

# get pid name
pid = multiprocessing.current_process().name
# create initial OD cost matrix layer on worker processors
if pid !='MainProcess':
  # Make OD cost matrix layer
  result_object = arcpy.MakeODCostMatrixLayer_na(in_network_dataset = in_network_dataset, 
                                                 out_network_analysis_layer = "ODmatrix", 
                                                 impedance_attribute = "Length", 
                                                 default_number_destinations_to_find = 1,
                                                 UTurn_policy = "ALLOW_UTURNS", 
                                                 hierarchy = "NO_HIERARCHY", 
                                                 output_path_shape = "NO_LINES")                                 
  outNALayer = result_object.getOutput(0)
  
  #Get the names of all the sublayers within the service area layer.
  subLayerNames = arcpy.na.GetNAClassNames(outNALayer)
  #Store the layer names that we will use later
  originsLayerName = subLayerNames["Origins"]
  destinationsLayerName = subLayerNames["Destinations"]
  linesLayerName = subLayerNames["ODLines"]
  
  # you may have to do this later in the script - but try now....
  ODLinesSubLayer = arcpy.mapping.ListLayers(outNALayer, linesLayerName)[0]
  fields = ['Name', 'Total_Length']
  
  
  arcpy.MakeFeatureLayer_management(polyBuffer, "buffer_layer")                
  
  
# Define query to create table
createTable     = '''
  CREATE TABLE IF NOT EXISTS {0}
  ({1} varchar PRIMARY KEY,
   VEAC_ID varchar NOT NULL,
   distance integer NOT NULL
   );
   '''.format(sqlTableName, A_pointsID.lower())
   
queryPartA      = '''
  INSERT INTO {} VALUES
  '''.format(sqlTableName)

# this is the same log table as used for other destinations.
#  It is only created if it does not already exist.
#  However, it probably does.  
createTable_log     = '''
        CREATE TABLE IF NOT EXISTS {}
          (hex integer NOT NULL, 
          parcel_count integer NOT NULL, 
          dest varchar, 
          status varchar, 
          mins double precision,
          PRIMARY KEY(hex,dest)
          );
          '''.format(log_table)     
  
queryInsert      = '''
  INSERT INTO {} VALUES
  '''.format(log_table)          
                    
queryUpdate      = '''
  ON CONFLICT ({0},{4}) 
  DO UPDATE SET {1}=EXCLUDED.{1},{2}=EXCLUDED.{2},{3}=EXCLUDED.{3}
  '''.format('hex','parcel_count','status','mins','dest')  

parcel_count = int(arcpy.GetCount_management(A_points).getOutput(0))  
      
## Functions defined for this script
# Define log file write method
def writeLog(hex = 0, AhexN = 'NULL', Bcode = 'NULL', status = 'NULL', mins= 0, create = log_table):
  try:
    if create == 'create':
      curs.execute(createTable_log)
      conn.commit()
      
    else:
      moment = time.strftime("%Y%m%d-%H%M%S")
  
      # write to sql table
      curs.execute("{0} ({1},{2},'{3}','{4}',{5}) {6}".format(queryInsert,hex, AhexN, Bcode,status, mins, queryUpdate))
      conn.commit()  
  except:
    print("ERROR: {}".format(sys.exc_info()))
    raise


def unique_values(table, field):
  data = arcpy.da.TableToNumPyArray(table, [field])
  return np.unique(data[field])    
    
# Worker/Child PROCESS
def ODMatrixWorkerFunction(hex): 
  # Connect to SQL database 
  try:
    conn = psycopg2.connect(database=sqlDBName, user=sqlUserName, password=sqlPWD)
    curs = conn.cursor()
  except:
    print("SQL connection error")
    print(sys.exc_info()[1])
    return 100
  # make sure Network Analyst licence is 'checked out'
  arcpy.CheckOutExtension('Network')
 
  # Worker Task is hex-specific by definition/parallel
  # 	Skip if hex was finished in previous run
  hexStartTime = time.time()
  if hex < hexStart:
    return(1)
    
  try:
    arcpy.MakeFeatureLayer_management (A_points, "A_pointsLayer")
  
    A_selection = arcpy.SelectLayerByAttribute_management("A_pointsLayer", where_clause = '"HEX_ID" = {}'.format(hex))   
    A_pointCount = int(arcpy.GetCount_management(A_selection).getOutput(0))
    
	  # Skip empty hexes
    if A_pointCount == 0:
	  writeLog(hex,0,dest_code,"no A points",(time.time()-hexStartTime)/60)
	  return(2)
	

    buffer = arcpy.SelectLayerByAttribute_management("buffer_layer", where_clause = '"ORIG_FID" = {}'.format(hex))
    B_selection = arcpy.SelectLayerByLocation_management('B_pointsLayer', 'intersect', buffer)
    B_pointCount = int(arcpy.GetCount_management(B_selection).getOutput(0))
    
	  # Skip empty hexes
    if B_pointCount == 0:
	  writeLog(hex,A_pointCount,dest_code,"no B points",(time.time()-hexStartTime)/60)
	  return(3)
      
	# If we're still in the loop at this point, it means we have the right hex and buffer combo and both contain at least one valid element
	# Process OD Matrix Setup
    arcpy.AddLocations_na(in_network_analysis_layer = outNALayer, 
        sub_layer                      = originsLayerName, 
        in_table                       = A_selection, 
        field_mappings                 = "Name {} #".format(A_pointsID), 
        search_tolerance               = "{} Meters".format(searchTolerance), 
        search_criteria                = "{} SHAPE;{} NONE".format(locateShape,noLocateJunctions), 
        append                         = "CLEAR", 
        snap_to_position_along_network = "NO_SNAP", 
        exclude_restricted_elements    = "INCLUDE",
        search_query                   = "{} #;{} #".format(locateShape,noLocateJunctions))

    arcpy.AddLocations_na(in_network_analysis_layer = outNALayer, 
      sub_layer                      = destinationsLayerName, 
      in_table                       = B_selection, 
      field_mappings                 = "Name {} #".format(B_pointsID), 
      search_tolerance               = "{} Meters".format(searchTolerance), 
      search_criteria                = "{} SHAPE;{} NONE".format(locateShape,noLocateJunctions), 
      append                         = "CLEAR", 
      snap_to_position_along_network = "NO_SNAP", 
      exclude_restricted_elements    = "INCLUDE",
      search_query                   = "{} #;{} #".format(locateShape,noLocateJunctions))    
    # Process: Solve
    result = arcpy.Solve_na(outNALayer, terminate_on_solve_error = "CONTINUE")
    if result[1] == u'false':
      writeLog(hex,A_pointCount,dest_code,"no solution",(time.time()-hexStartTime)/60)
      return(4)

    # Extract lines layer, export to SQL database
    outputLines = arcpy.da.SearchCursor(ODLinesSubLayer, fields)
    curs = conn.cursor()
    count = 0
    chunkedLines = list()
    
    for outputLine in outputLines :
      count += 1
      place = "before id"
      ID_A = outputLine[0].split('-')[0].encode('utf-8').strip(' ')
      ID_B = outputLine[0].split('-')[1].split(',')[0].strip(' ').encode('utf-8')
      place = "after ID"
      chunkedLines.append("('{}','{}',{})".format(ID_A,ID_B,int(round(outputLine[1]))))
      if(count % sqlChunkify == 0):
        curs.execute(queryPartA + ','.join(rowOfChunk for rowOfChunk in chunkedLines)+' ON CONFLICT DO NOTHING')
        conn.commit()
        chunkedLines = list()
        
    if(count % sqlChunkify != 0):
      curs.execute(queryPartA + ','.join(rowOfChunk for rowOfChunk in chunkedLines)+' ON CONFLICT DO NOTHING')
      conn.commit()
    writeLog(hex,A_pointCount,B_pointCount,"Solved",(time.time()-hexStartTime)/60)
    
    curs.execute("SELECT COUNT(*) FROM {}".format(sqlTableName))
    numerator = list(curs)
    numerator = int(numerator[0][0])
    progressor(numerator,parcel_count,start,"{}/{}; last hex processed: {}, at {}".format(numerator,parcel_count,hex,time.strftime("%Y%m%d-%H%M%S"))) 
    return 0
    
  except:
    print('''Error: {}
             String: {}
             Line example: {}
      '''.format( sys.exc_info(),chunkedLines,outputLine))   
    writeLog(hex, multiprocessing.current_process().pid, "ERROR", (time.time()-hexStartTime)/60)
    return(multiprocessing.current_process().pid)
  finally:
    arcpy.CheckInExtension('Network')
    conn.close()


nWorkers = 4  
hex_list = unique_values(A_points, 'HEX_ID')

# MAIN PROCESS
if __name__ == '__main__':
  try:
    conn = psycopg2.connect(database=sqlDBName, user=sqlUserName, password=sqlPWD)
    curs = conn.cursor()
    
    # create OD matrix table (Closest POS > 1.5Ha)
    curs.execute(createTable)
    conn.commit()
    
  except:
    print("SQL connection error")
    print(sys.exc_info())
    raise
    
  # initiate log file
  writeLog(create='create')  
  
  # Setup a pool of workers/child processes and split log output
  pool = multiprocessing.Pool(nWorkers)
    
  # Task name is now defined
  task = 'Create OD cost matrix for parcel points to POS > 1.5Ha'  # Do stuff
  print("Commencing task ({}): {} at {}".format(sqlDBName,task,time.strftime("%Y%m%d-%H%M%S")))

  # Divide work by hexes
  # Note: if a restricted list of hexes are wished to be processed, just supply a subset of hex_list including only the relevant hex id numbers.
  hexCursor = hex_list
  pool.map(ODMatrixWorkerFunction, hexCursor, chunksize=1)
  
  # output to completion log    
  script_running_log(script, task, start)
  conn.close()