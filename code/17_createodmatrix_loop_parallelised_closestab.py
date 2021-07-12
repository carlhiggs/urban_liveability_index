# Purpose: This script finds for each A point the closest B point along a network.
#              - it uses parallel processing
#              - it outputs to an sql database 
# Authors: Carl Higgs, Koen Simons

import arcpy, arcinfo
import os
import time
import multiprocessing
import sys
import psycopg2 
import numpy as np

from script_running_log import script_running_log
from ConfigParser import SafeConfigParser


parser = SafeConfigParser()
parser.read(os.path.join(sys.path[0],'config.ini'))

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])

# INPUT PARAMETERS
folderPath = parser.get('data', 'folderPath')
destGdb    = os.path.join(folderPath,parser.get('destinations', 'study_destinations'))  
urbanGdb    = os.path.join(folderPath,parser.get('data', 'workspace'))  

arcpy.env.workspace = destGdb
arcpy.env.scratchWorkspace = folderPath
arcpy.env.qualifiedFieldNames = False  
arcpy.env.overwriteOutput = True 

# Specify geodatabase with feature classes of "origins"
A_points = os.path.join(urbanGdb,parser.get('parcels', 'parcel_dwellings'))
A_pointsID = parser.get('parcels', 'parcel_id')

## specify "B_points" (e.g. destinations)
B_pointsID = parser.get('destinations', 'destination_id')

featureClasses = arcpy.ListFeatureClasses()

# List of 31 potentially relevant destinations: see destinations.csv in LI scripts folder for details
destination_list = parser.get('destinations', 'destination_list').split(',')

## Network settings
in_network_dataset = os.path.join(urbanGdb,parser.get('roads', 'pedestrian_road_network'))
locateShape = parser.get('roads', 'pedestrian_road_edges')
noLocateJunctions = parser.get('roads', 'pedestrian_road_junctions')
searchTolerance =  parser.get('network', 'tolerance')

hexStart = 0


# SQL Settings
sqlDBName   = parser.get('postgresql', 'database')
sqlUserName = parser.get('postgresql', 'user')
sqlPWD      = parser.get('postgresql', 'password')

sqlTableName  = "dist_cl_od_parcel_dest"
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
  
  # store list of destinations with counts (so as to overlook destinations for which zero data exists!)
  curs.execute("SELECT dest_name,dest_count FROM dest_type")
  count_list = list(curs)
  

# Define query to create table
createTable     = '''
  CREATE TABLE IF NOT EXISTS {0}
  ({1} varchar NOT NULL ,
   dest smallint NOT NULL ,
   oid bigint NOT NULL ,
   distance integer NOT NULL, 
   PRIMARY KEY({1},dest)
   );
   '''.format(sqlTableName, A_pointsID)
   
queryPartA      = '''
  INSERT INTO {} VALUES
  '''.format(sqlTableName)
  
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
           
          
## Functions defined for this script
# Define log file write method
def writeLog(hex = 0, AhexN = 'NULL', Bcode = 'NULL', status = 'NULL', mins= 0, create = log_table):
  try:
    if create == 'create':
      curs.execute(createTable_log)
      conn.commit()
      
    else:
      moment = time.strftime("%Y%m%d-%H%M%S")
      # print to screen regardless
      print('Hex:{:5d} A:{:8s} Dest:{:8s} {:15s} {:15s}'.format(hex, str(AhexN), str(Bcode), status, moment))     
      # write to sql table
      curs.execute("{0} ({1},{2},{3},'{4}',{5}) {6}".format(queryInsert,hex, AhexN, Bcode,status, mins, queryUpdate))
      conn.commit()  
  except:
    print("ERROR: {}".format(sys.exc_info()))
    raise


def unique_values(table, field):
  data = arcpy.da.TableToNumPyArray(table, [field])
  return np.unique(data[field])    
    
# Define make reduced feature layer method
def renameSkinny(is_geo, in_obj, out_obj, keep_fields_list=[''], rename_fields_list=None, where_clause=''):
  ''' Make an ArcGIS Feature Layer or Table View, containing only the fields
      specified in keep_fields_list, using an optional SQL query. Default
      will create a layer/view with NO fields. Method amended (Carl 17 Nov 2016) to include a rename clause - all fields supplied in rename must correspond to names in keep_fields'''
  field_info_str = ''
  input_fields = arcpy.ListFields(in_obj)
  if not keep_fields_list:
      keep_fields_list = []
  i = 0
  for field in input_fields:
      if field.name in keep_fields_list:
          possibleNewName = (rename_fields_list[i],field.name)[rename_fields_list==None]
          field_info_str += field.name + ' ' + possibleNewName + ' VISIBLE;'
          i += 1
      else:
          field_info_str += field.name + ' ' + field.name + ' HIDDEN;'
  field_info_str.rstrip(';')  # Remove trailing semicolon
  if is_geo:
      arcpy.MakeFeatureLayer_management(in_obj, out_obj, where_clause, field_info=field_info_str)
  else:
      arcpy.MakeTableView_management(in_obj, out_obj, where_clause, field_info=field_info_str)
  return out_obj

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
      # note: make sure A_selection is deleted at end of this... 
      #  ALSO: I commented out the following as no longer made sense: selection_type  = 'ADD_TO_SELECTION'
    A_selection = arcpy.SelectLayerByAttribute_management("A_pointsLayer", where_clause = '"HEX_ID" = {}'.format(hex))
    A_pointCount = int(arcpy.GetCount_management(A_selection).getOutput(0))
	  # Skip empty hexes
    if A_pointCount == 0:
	    writeLog(hex,0,'NULL',"no A points",(time.time()-hexStartTime)/60)
	    return(2)
	
    # fetch list of successfully processed destinations for this hex, if any
    # curs.execute("SELECT dest FROM {} WHERE hex = {}".format(log_table,hex))
    curs.execute("SELECT dest FROM {} WHERE hex = {}".format(log_table,hex))
    completed_dest_in_hex = list(curs)
    
    # completed destination IDs need to be selected as first element in tuple, and converted to integer
    completed_dest = [destination_list[int(x[0])] for x in completed_dest_in_hex if destination_list[int(x[0])] not in completed_dest_in_hex]
    remaining_dest_list = [x for x in destination_list if x not in completed_dest]
    
    for B_points in featureClasses:
      # set B_points index based on position in destination list
      if B_points in remaining_dest_list:
        destNum = destination_list.index(B_points)
        # only procede if > 0 destinations of this type are present in study region
        if count_list[destNum][1] > 0:
	      # OD Matrix Setup
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
              in_table                       = B_points, 
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
            writeLog(hex,A_pointCount,destNum,"no solution",(time.time()-hexStartTime)/60)
            return(4)
          
          # Extract lines layer, export to SQL database
          outputLines = arcpy.da.SearchCursor(ODLinesSubLayer, fields)
          count = 0
          chunkedLines = list()
          for outputLine in outputLines :
            count += 1
            ID = outputLine[0].split('-')
            ID1 = ID[1].split(',')
            chunkedLines.append("('{}',{},{},{})".format(ID[0].strip(' '),ID1[0],ID1[1],int(round(outputLine[1]))))
            if(count % sqlChunkify == 0):
              curs.execute(queryPartA + ','.join(rowOfChunk for rowOfChunk in chunkedLines))
              conn.commit()
              chunkedLines = list()
          
          if(count % sqlChunkify != 0):
            curs.execute(queryPartA + ','.join(rowOfChunk for rowOfChunk in chunkedLines))
            conn.commit()
          writeLog(hex,A_pointCount,destNum,"Solved",(time.time()-hexStartTime)/60)
  
    # return worker function as completed once all destinations processed
    return 0
  
  except:
    writeLog(hex, multiprocessing.current_process().pid, "Error: ",sys.exc_info()[1], (time.time()-hexStartTime)/60)
    return(multiprocessing.current_process().pid)
  finally:
    arcpy.CheckInExtension('Network')
    conn.close()

# Child/Worker Init functions
# Iterator must exist on Workers

nWorkers = 4
hex_list = unique_values(A_points, 'HEX_ID')
  
# MAIN PROCESS
if __name__ == '__main__':
  try:
    conn = psycopg2.connect(database=sqlDBName, user=sqlUserName, password=sqlPWD)
    curs = conn.cursor()

    # create OD matrix table
    curs.execute(createTable)
    conn.commit()

  except:
    print("SQL connection error")
    print(sys.exc_info()[0])
    raise
    
  # initiate log file
  writeLog(create='create')
  
  # Setup a pool of workers/child processes and split log output
  pool = multiprocessing.Pool(nWorkers)
  
  # Task name is now defined
  task = 'Create OD cost matrix for A points to B points.'  # Do stuff
  print("Commencing task ({}): {} at {}".format(sqlDBName,task,time.strftime("%Y%m%d-%H%M%S")))

  # Divide work by hexes
  # Note: if a restricted list of hexes are wished to be processed, just supply a subset of hex_list including only the relevant hex id numbers.
  hexCursor = hex_list
  pool.map(ODMatrixWorkerFunction, hexCursor, chunksize=1)
  
  # output to completion log    
  script_running_log(script, task, start)
  conn.close()
