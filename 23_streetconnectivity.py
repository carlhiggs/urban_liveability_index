# Purpose: This script calculates StreetConnectivity (3 plus leg intersections per km2)
#          It outputs PFI, 3 legIntersections, and street connectivity to an SQL database.
#          Buffer area is referenced in SQL table nh1600m
# Author:  Carl Higgs

import arcpy
import os
import sys
import time
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
task = "Calculate StreetConnectivity (3 plus leg intersections per  km2)"

def basename(filePath):
  '''strip a path to the basename of file, without the extension.  Requires OS '''
  try: 
    return os.path.basename(os.path.normpath(filePath)).split(".",1)[0]
  except:
    print('Return basename failed. Did you import os?')



# INPUT PARAMETERS
folderPath = parser.get('data', 'folderPath')
urbanGDB    = os.path.join(folderPath,parser.get('data', 'workspace'))  
arcpy.env.workspace = urbanGDB
arcpy.env.overwriteOutput = True 

sde_connection = parser.get('postgresql', 'sde_connection')

srid = int(parser.get('workspace', 'srid'))

## specify locations
points =  parser.get('parcels','parcel_dwellings')
denominator = int(arcpy.GetCount_management(points).getOutput(0))

# specify the unique location identifier 
pointsID = parser.get('parcels', 'parcel_id')

intersections = basename(os.path.join(folderPath,parser.get('roads', 'intersections')))

arcpy.MakeFeatureLayer_management(intersections, "intersections")
intersection_count = int(arcpy.GetCount_management("intersections").getOutput(0))

distance = int(parser.get('network', 'distance'))
sausage_buffer_table = "sausagebuffer_{}".format(distance)


fields = [pointsID]



# SQL Settings - storing passwords in plain text is obviously not ideal
sqlDBName   = parser.get('postgresql', 'database')
sqlUserName = parser.get('postgresql', 'user')
sqlPWD      = parser.get('postgresql', 'password')

# output tables
intersections_table = "intersections_3plus_way"
street_connectivity_table = "street_connectivity"

#  Size of tuple chunk sent to postgresql 
sqlChunkify = 1000

# Define query to create table
createTable_intersections     = '''
  CREATE TABLE IF NOT EXISTS {0}
  (OBJECTID bigint PRIMARY KEY,
   geom geometry NOT NULL); 
  '''.format(intersections_table)

queryPartA_intersections = '''
  INSERT INTO {} VALUES 
  '''.format(intersections_table)

createTable_sc = '''
  CREATE TABLE IF NOT EXISTS {0}
  ({1} varchar PRIMARY KEY,
   intersection_count integer NOT NULL,
   area_sqkm double precision NOT NULL,
   sc_nh1600m double precision NOT NULL 
  ); 
  '''.format(street_connectivity_table,pointsID.lower())

sc_query_A = '''
INSERT INTO {0} ({1},intersection_count,area_sqkm,sc_nh1600m)
(SELECT {1}, COALESCE(COUNT({2}),0) AS intersection_count,area_sqkm, COALESCE(COUNT({2}),0)/area_sqkm AS sc_nh1600mm
FROM {2} 
LEFT JOIN 
(SELECT {3}.{1},area_sqkm,geom FROM nh1600m LEFT JOIN {3} ON nh1600m.{1} = {3}.{1}) 
AS sp_temp
ON ST_Intersects(sp_temp.geom, {2}.geom)
WHERE {1} IN 
'''.format(street_connectivity_table,pointsID.lower(),intersections_table,sausage_buffer_table)

sc_query_C = '''
  GROUP BY {},area_sqkm) ON CONFLICT DO NOTHING;
  '''.format(pointsID.lower())

  


def unique_values(table, field):
  data = arcpy.da.TableToNumPyArray(table, [field])
  return np.unique(data[field])
    

# OUTPUT PROCESS
# Connect to postgreSQL server
try:
  conn = psycopg2.connect(database=sqlDBName, user=sqlUserName, password=sqlPWD)
  curs = conn.cursor()
  print("Connection to SQL success {}".format(time.strftime("%Y%m%d-%H%M%S")) )
  # drop table if it already exists
  
  print("create table {}... ".format(intersections_table)),
  subTaskStart = time.time()
  curs.execute(createTable_intersections)
  conn.commit()
  print("{:4.2f} mins.".format((time.time() - start)/60))	
  
except:
  print("SQL connection error {}".format(time.strftime("%Y%m%d-%H%M%S")) )
  print(sys.exc_info()[0])
  print(sys.exc_info()[1])
  raise

# export intersections to PostGIS feature
intersections_to_postgis = time.time()
count = 0
chunkedLines = list()
progressor(count,intersection_count,intersections_to_postgis,"Exporting intersections: {}".format(count))
with arcpy.da.SearchCursor("intersections", ["OBJECTID", "SHAPE@WKT"]) as cursor:
  for row in cursor:
      count += 1
      wkt = row[1].encode('utf-8').replace(' NAN','').replace(' M ','')
      chunkedLines.append("({0},ST_GeometryFromText('{1}', {2}))".format(row[0],wkt,srid)) 
      if (count % sqlChunkify == 0) :
        curs.execute(queryPartA_intersections + ','.join(rowOfChunk for rowOfChunk in chunkedLines)+' ON CONFLICT DO NOTHING')
        conn.commit()
        chunkedLines = list()
        progressor(count,intersection_count,intersections_to_postgis,"Exporting intersections: {}".format(count))
  if(count % sqlChunkify != 0):
     curs.execute(queryPartA_intersections + ','.join(rowOfChunk for rowOfChunk in chunkedLines)+' ON CONFLICT DO NOTHING')
     conn.commit()          
progressor(count,intersection_count,intersections_to_postgis,"Exporting intersections: {}".format(count))

# Create sausage buffer spatial index
print("Creating intersections spatial index... "),
curs.execute("CREATE INDEX IF NOT EXISTS {0}_gix ON {0} USING GIST (geom);".format(intersections_table))
conn.commit()
print("Done.")

print("Analyze the intersections table to improve performance.")
curs.execute("ANALYZE {};".format(intersections_table))
conn.commit()
print("Done.")



# Now calculate street connectivity (three way intersections w/ in nh1600m/area in  km2)

print("create table {}... ".format(street_connectivity_table)),
subTaskStart = time.time()
curs.execute(createTable_sc)
conn.commit()
print("{:4.2f} mins.".format((time.time() - start)/60))	

  
print("fetch list of processed parcels, if any..."), 
# (for string match to work, had to select first item of returned tuple)
curs.execute("SELECT {} FROM {}".format(pointsID.lower(),sausage_buffer_table))
raw_point_id_list = list(curs)
raw_point_id_list = [x[0] for x in raw_point_id_list]

curs.execute("SELECT {} FROM {}".format(pointsID.lower(),street_connectivity_table))
completed_points = list(curs)
completed_points = [x[0] for x in completed_points]

point_id_list = [x for x in raw_point_id_list if x not in completed_points]  
print("Done.")

denom = len(point_id_list)
count = 0
chunkedPoints = list()

print("Processing points...")
for point in point_id_list:
  count += 1
  chunkedPoints.append(point) 
  if (count % sqlChunkify == 0) :
      curs.execute('{} ({}) {}'.format(sc_query_A,','.join("'"+x+"'" for x in chunkedPoints),sc_query_C))
      conn.commit()
      chunkedPoints = list()
      progressor(count,denom,start,"{}/{} points processed".format(count,denom))
if(count % sqlChunkify != 0):
   curs.execute('{} ({}) {}'.format(sc_query_A,','.join("'"+x+"'" for x in chunkedPoints),sc_query_C))
   conn.commit()

progressor(count,denom,start,"{}/{} points processed".format(count,denom))

# output to completion log    
script_running_log(script, task, start)

# clean up
conn.close()

