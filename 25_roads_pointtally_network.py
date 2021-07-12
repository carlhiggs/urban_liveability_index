# Purpose: This script tallies length of roads intersecting sausage buffer polygons.
#            -- Lengths are categorised by road code 
#                -- (0-1: high/freeway; 2-4: heavy; 5: local)
#            -- Results are indexed by Sausage Buffer parcel PFI
#               and written as rows in an SQL table within a postgresql database
#            -- the script is parallelised.
#
#          ** make sure the roadsaspoints table has a GIST spatial index!
#
# Author: Koen Simons, Carl Higgs

#import packages
import arcpy
import os
import time
import psycopg2 
from progressor import progressor
import math

import sys
import numpy as np
import multiprocessing

from script_running_log import script_running_log
from ConfigParser import SafeConfigParser

parser = SafeConfigParser()
parser.read(os.path.join(sys.path[0],'config.ini'))

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'tally road lengths in network buffer'

# INPUT PARAMETERS
folderPath = parser.get('data', 'folderPath')
urbanGDB    = os.path.join(folderPath,parser.get('data', 'workspace'))  

arcpy.env.workspace = urbanGDB
arcpy.env.scratchWorkspace = os.path.join(folderPath,'altScratch2')
arcpy.env.overwriteOutput = True 

points = parser.get('parcels', 'parcel_dwellings')
points_id = parser.get('parcels', 'parcel_id')

# SQL Settings - storing passwords in plain text is obviously not ideal
sqlDBName   = parser.get('postgresql', 'database')
sqlUserName = parser.get('postgresql', 'user')
sqlPWD      = parser.get('postgresql', 'password')
roadPoints_table = "roadsaspoints"
roadLengths_table = "road_length"
distance = int(parser.get('network', 'distance'))
sausage_buffer_table = "sausagebuffer_{}".format(distance)

#  Size of tuple chunk sent to postgresql 
sqlChunkify = 500

 
createTable_roadLengths = '''
  CREATE TABLE IF NOT EXISTS {0} 
  ({1} varchar PRIMARY KEY,
  roads_hfreeways integer NOT NULL,
  roads_heavy integer NOT NULL,
  roads_local integer NOT NULL);
'''.format(roadLengths_table, points_id.lower())  

queryInsert_roadLengths      = '''
  INSERT INTO {} VALUES
  '''.format(roadLengths_table)
  
 
spatialQueryA = '''
INSERT INTO {0} ({1},roads_hfreeways,roads_heavy,roads_local)
(SELECT {1}, 
        COALESCE(SUM(CASE WHEN class_code IN (0,1) THEN roadsaspoints.value END),0) AS roads_hfreeways,
        COALESCE(SUM(CASE WHEN class_code IN (2,3,4)  THEN roadsaspoints.value END),0) AS roads_heavy,
        COALESCE(SUM(CASE WHEN class_code IN (5)  THEN roadsaspoints.value END),0) AS roads_local
FROM {2} 
LEFT JOIN {3}
ON ST_Intersects({3}.geom, {2}.geom)
WHERE {3}.hex = 
'''.format(roadLengths_table,points_id.lower(),roadPoints_table,sausage_buffer_table)

spatialQueryB = '''
  AND {0}.{1} NOT IN
  '''.format(sausage_buffer_table,points_id.lower())

spatialQueryC = '''
  GROUP BY {}) ON CONFLICT DO NOTHING;
  '''.format(points_id.lower())
 
 
 
 
parcel_count = int(arcpy.GetCount_management(points).getOutput(0))


# Connect to postgreSQL server
conn = psycopg2.connect(database=sqlDBName, user=sqlUserName, password=sqlPWD)
curs = conn.cursor()
 
# get list of already processed parcels 
# (for string match to work, had to select first item of returned tuple)
curs.execute("SELECT {} FROM {}".format(points_id.lower(),'parcelmb'))
raw_point_id_list = list(curs)
raw_point_id_list = [x[0] for x in raw_point_id_list]

curs.execute("SELECT {} FROM {}".format(points_id.lower(),roadLengths_table))
completed_points = list(curs)
completed_points = [x[0] for x in completed_points]
# print("{} parcels already processed".format(len(completed_points)))
completed_points = "'{}'".format("','".join(completed_points))


def unique_values(table, field):
  data = arcpy.da.TableToNumPyArray(table, [field])
  return np.unique(data[field])

def roadLengthInsert(hex):
  conn = psycopg2.connect(database=sqlDBName, user=sqlUserName, password=sqlPWD)
  curs = conn.cursor()

  curs.execute('{} {} {} ({}) {}'.format(spatialQueryA,hex,spatialQueryB,completed_points,spatialQueryC))
  conn.commit()  
  
  curs.execute("SELECT COUNT(*) FROM {}".format(roadLengths_table))
  numerator = list(curs)
  numerator = int(numerator[0][0])
  progressor(numerator,parcel_count,start,"{}/{}; last hex processed: {}, at {}".format(numerator,parcel_count,hex,time.strftime("%Y%m%d-%H%M%S")))

  return 0
    
nWorkers = 4
hex_list = unique_values(points, 'HEX_ID')
     
# MAIN PROCESS
if __name__ == '__main__':
  # Task name is now defined
  print("Commencing task: {} at {}".format(task,time.strftime("%Y%m%d-%H%M%S")))


  print("create table {}... ".format(roadLengths_table)),
  subTaskStart = time.time()
  curs.execute(createTable_roadLengths)
  conn.commit()
  print("{:4.2f} mins.".format((time.time() - start)/60))
	
  
  # Setup a pool of workers/child processes and split log output
  pool = multiprocessing.Pool(nWorkers)
  
  # Divide work by hexes
  progressor(0,parcel_count,start," ")
  pool.map(roadLengthInsert, hex_list, chunksize=1)
      
  # output to completion log    
  script_running_log(script, task, start)
  
  # clean up
  conn.close()
