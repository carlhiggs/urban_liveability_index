# Purpose: convert polyline segments of road network to a series of points
#          at intervals along the line.  This is used to calculate intersection
#          of road network with neighbourhood buffer.  The points retain the road
#          length of the segment they represent.  The implication is that processing 
#          time is reduced (relative to clipping road network), and that roads that
#          intersect at the edge of the neighbourhood buffer only overlap a minimal 
#          distance; less than if the original road segment were selected.
#
#          An SQL table is then created which tallies length of roads intersecting 
#          sausage buffer polygons.
#            -- Lengths are categorised by road code 
#                -- (0-1: high/freeway; 2-4: heavy; 5: local)
#            -- Results are indexed by Sausage Buffer parcel PFI
#            -- the script is parallelised.
# Author: Koen Simons, Carl Higgs

#import packages
import arcpy
import os
import time
import psycopg2 
from progressor import progressor
import math

from script_running_log import script_running_log
from ConfigParser import SafeConfigParser

parser = SafeConfigParser()
parser.read(os.path.join(sys.path[0],'config.ini'))

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'convert polyline segments of road network to a series of points at intervals along the line'
# INPUT PARAMETERS
folderPath = parser.get('data', 'folderPath')
urbanGDB    = os.path.join(folderPath,parser.get('data', 'workspace'))  

arcpy.env.workspace = urbanGDB
arcpy.env.scratchWorkspace = os.path.join(folderPath,'altScratch2')
arcpy.env.overwriteOutput = True 

srid = int(parser.get('workspace', 'srid'))

A_pointsID = parser.get('parcels', 'parcel_id')

roadLines = "roadsAny"

# SQL Settings - storing passwords in plain text is obviously not ideal
sqlDBName   = parser.get('postgresql', 'database')
sqlUserName = parser.get('postgresql', 'user')
sqlPWD      = parser.get('postgresql', 'password')
roadPoints_table = "roadsAsPoints"
roadLengths_table = "road_length"

#  Size of tuple chunk sent to postgresql 
sqlChunkify = 500

createTable_roadPoints = '''
  DROP TABLE IF EXISTS {0};
  CREATE TABLE {0}
  (objectid bigint PRIMARY KEY,
   class_code integer NOT NULL,
   value double precision NOT NULL,
   geom geometry NOT NULL); 
  '''.format(roadPoints_table)

queryInsert      = '''
  INSERT INTO {} VALUES
  '''.format(roadPoints_table)
  
# Maximum distance of a road segment
MaxDistance = 10.0
HalfMaxDistance = MaxDistance / 2

# Create output dataset in RAM
mem_roadsAsPoints = arcpy.CreateFeatureclass_management("in_memory", "roadsAsPoints", "POINT", "", "DISABLED", "DISABLED", spatial_reference=roadLines)
arcpy.AddField_management(mem_roadsAsPoints, "CLASS_CODE", "SHORT")
arcpy.AddField_management(mem_roadsAsPoints, "Value", "FLOAT")

polyLineCount = int(arcpy.GetCount_management(roadLines).getOutput(0))

count = 0
startCount = time.time()
progressor(count,polyLineCount,startCount, 'Creating Points on Lines {:,}'.format(count))

# iterate over polylines and create points
lines  = arcpy.da.SearchCursor(roadLines, ["SHAPE@","CLASS_CODE","Shape_Length"])
points = arcpy.da.InsertCursor(mem_roadsAsPoints, ["SHAPE@","CLASS_CODE","Value"])

for pline in lines :
  classCode = pline[1]
  totalLength = pline[2]
  nPoints = int(math.ceil(totalLength / MaxDistance))
  distance = totalLength / nPoints
  for i in range(nPoints) :
    newPoint = pline[0].positionAlongLine((i+0.5)*distance)
    points.insertRow((newPoint, classCode, distance))
  count += 1
  progressor(count,polyLineCount,startCount, 'Creating Points on Lines {:,}'.format(count))

# Copy RAM to disk
arcpy.CopyFeatures_management(mem_roadsAsPoints, os.path.join(urbanGDB, "roadsAsPoints"))


# Connect to postgreSQL server
try:
  conn = psycopg2.connect(database=sqlDBName, user=sqlUserName, password=sqlPWD)
  curs = conn.cursor()
  print("Connection to SQL success {}".format(time.strftime("%Y%m%d-%H%M%S")) )

  print("create table {}... ".format(roadPoints_table)),
  subTaskStart = time.time()
  curs.execute(createTable_roadPoints)
  conn.commit()
  print("{:4.2f} mins.".format((time.time() - start)/60))	
  
  # export intersections to PostGIS feature
  roadpoints_to_postgis = time.time()
  count = 0
  chunkedLines = list()
  road_point_count = int(arcpy.GetCount_management(mem_roadsAsPoints).getOutput(0))
  progressor(count,road_point_count,roadpoints_to_postgis,"Exporting road points to PostGIS: {}".format(count))
  with arcpy.da.SearchCursor(mem_roadsAsPoints, ["OID","CLASS_CODE","Value","SHAPE@WKT"]) as cursor:
    for row in cursor:
      count += 1
      wkt = row[3].encode('utf-8').replace(' NAN','').replace(' M ','')
      chunkedLines.append("({0},{1},{2},ST_GeometryFromText('{3}',{4}))".format(row[0],row[1],row[2],wkt,srid)) 
      if (count % sqlChunkify == 0) :
        curs.execute(queryInsert + ','.join(rowOfChunk for rowOfChunk in chunkedLines)+' ON CONFLICT DO NOTHING')
        conn.commit()
        chunkedLines = list()
        progressor(count,road_point_count,roadpoints_to_postgis,"Exporting road points to PostGIS: {}".format(count))
    if(count % sqlChunkify != 0):
       curs.execute(queryInsert + ','.join(rowOfChunk for rowOfChunk in chunkedLines)+' ON CONFLICT DO NOTHING')
       conn.commit()
     
  progressor(count,road_point_count,roadpoints_to_postgis,"Exporting road points to PostGIS: {}".format(count))  
  
  
  print("Creating spatial index... "),
  curs.execute("CREATE INDEX IF NOT EXISTS {0}_gix ON {0} USING GIST (geom);".format(roadPoints_table))
  conn.commit()
  print("Done.")
  
except:
  print("SQL connection error {}".format(time.strftime("%Y%m%d-%H%M%S")) )
  print(sys.exc_info()[0])
  print(sys.exc_info()[1])
  raise

finally:
  # output to completion log    
  script_running_log(script, task, start)
  
  # clean up
  conn.close()