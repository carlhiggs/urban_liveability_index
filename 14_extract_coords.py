# Purpose: Extract identifier and coordinates to individual tables for specified features
# Author: Carl Higgs
# Date of creation: 12 Jan 2017

#import packages
import arcpy
import os
import time
import psycopg2 
from progressor import progressor

from script_running_log import script_running_log
from ConfigParser import SafeConfigParser

parser = SafeConfigParser()
parser.read(os.path.join(sys.path[0],'config.ini'))

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Extract identifier and coordinates to individual tables for specified features'

# INPUT PARAMETERS
folderPath = parser.get('data', 'folderPath')
destGdb    = os.path.join(folderPath,parser.get('destinations', 'study_destinations'))  
urbanGdb    = os.path.join(folderPath,parser.get('data', 'workspace'))  

arcpy.env.workspace = folderPath
arcpy.env.qualifiedFieldNames = False  
arcpy.env.overwriteOutput = True 

A_points = os.path.join(urbanGdb,parser.get('parcels', 'parcel_dwellings'))
A_pointsID = parser.get('parcels', 'parcel_id')

destinations =  os.path.join(destGdb,parser.get('destinations', 'study_dest_combined'))

pos = os.path.join(urbanGdb,parser.get('pos', 'pos_entry_rdxd'))
pos_poly_id   = parser.get('pos', 'pos_poly_id')
# Specify features of interest (using output table name)
# Note - the POS section will need amendment for regions outside Melbourne!!
#        I have commented parks out since it depends on region if data is available
# features = ['parcel_xy','destination_xy','parks_xy']
features = ['parcel_xy','destination_xy']
featureLocation = [A_points,
                   destinations,
                   ]
featureID = [A_pointsID,'Dest_OID',pos_poly_id]
featureID_delimiter = ['',',',',']
featureID_type = ['varchar','integer','varchar']
feature_query = ['','','']

SpatialRef = arcpy.SpatialReference(parser.get('workspace', 'SpatialRef'))
srid = int(parser.get('workspace', 'srid'))

# SQL Settings - storing passwords in plain text is obviously not ideal
sqlDBName   = parser.get('postgresql', 'database')
sqlUserName = parser.get('postgresql', 'user')
sqlPWD      = parser.get('postgresql', 'password')

#  Size of tuple chunk sent to postgresql 
sqlChunkify = 500

# Define query to create table
createTableParcel     = '''
  CREATE TABLE {0}
  ({1} {2} PRIMARY KEY,
   x  double precision NOT NULL ,
   y double precision NOT NULL,
   geom geometry NOT NULL); 
  '''.format(features[0],featureID[0].lower(),featureID_type[0])

createTableDest     = '''
  CREATE TABLE {0}
  ({1} {2} NOT NULL,
   {3} {2} NOT NULL,
   x  double precision NOT NULL ,
   y double precision NOT NULL,
   geom geometry NOT NULL,
   CONSTRAINT {0}_key PRIMARY KEY({1},{3})
  ); 
  '''.format(features[1],featureID[1].split('_')[0].lower(),featureID_type[1],featureID[1].split('_')[1].lower())

# createTablePOS     = '''
  # CREATE TABLE {0}
  # ({1} {2} NOT NULL,
   # oid integer NOT NULL,
   # x  double precision NOT NULL ,
   # y double precision NOT NULL,
   # CONSTRAINT {0}_key PRIMARY KEY({1},oid)
  # ); 
  # '''.format(features[2],featureID[2].lower(),featureID_type[2])

# tables = [createTableParcel,createTableDest,createTablePOS]
tables = [createTableParcel,createTableDest]
  


  
def coordsToSQL(featureName, feature,featureID,featureID_type='num', featureID_delimiter='',feature_query='',SpatialRef='',SQLconnection=''):

  ''' Extracts coordinates from feature according to given spatial reference
      and attempts to insert these into a table of the same name as supplied 
      feature in a postgresql database.  Feature will be indexed using supplied
      identifier: if this has a compound identifier, specify the mode of delimitation
      (e.g. for comma use ',') else leave blank or specify  ''  .'''
  def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False  
  
  try:
    print("Extracting coordinates from {}".format(featureName))
    queryPartA = '''
      INSERT INTO {} VALUES 
      '''.format(featureName)
    denom = int(arcpy.GetCount_management(feature).getOutput(0))
    startCount = time.time()   
    with arcpy.da.SearchCursor(feature, [featureID,"SHAPE@XY","SHAPE@WKT"],where_clause = feature_query, spatial_reference=SpatialRef) as cursor:
      count = 0
      chunkedLines = list()
      if len(featureID_delimiter) > 0:    
        for row in cursor:
          count += 1
          id = row[0].split(featureID_delimiter)
          
          # make sure id is encased in quotes if string
          for el in id:
            if is_number(el) == False:
              id[id.index(el)] = "'{}'".format(el)
          x, y = row[1]
          wkt = row[2].encode('utf-8').replace(' NAN','').replace(' M ','')
          chunkedLines.append("({},{},{},{},ST_GeometryFromText('{}', {}))".format(id[0],id[1],x,y,wkt,srid)) 
          if (count % sqlChunkify == 0) :
            curs.execute(queryPartA + ','.join(rowOfChunk for rowOfChunk in chunkedLines)+' ON CONFLICT DO NOTHING')
            conn.commit()
            chunkedLines = list() 
      else:
        for row in cursor:
          id = row[0]
          
          # make sure id is encased in quotes if string
          if is_number(id) == False:
            id = "'{}'".format(id)          
          x, y = row[1]
          wkt = row[2].encode('utf-8').replace(' NAN','').replace(' M ','')
          chunkedLines.append("({},{},{},ST_GeometryFromText('{}', {}))".format(id, x, y, wkt, srid) )     
          if (count % sqlChunkify == 0) :
            curs.execute(queryPartA + ','.join(rowOfChunk for rowOfChunk in chunkedLines)+' ON CONFLICT DO NOTHING')
            conn.commit()
            chunkedLines = list()      
      if(count % sqlChunkify != 0):
       curs.execute(queryPartA + ','.join(rowOfChunk for rowOfChunk in chunkedLines)+' ON CONFLICT DO NOTHING')
       conn.commit()
    

  except:
    print("ERROR: "+str(sys.exc_info()[0]))
    raise
    


# OUTPUT PROCESS
# Connect to postgreSQL server
try:
  conn = psycopg2.connect(database=sqlDBName, user=sqlUserName, password=sqlPWD)
  curs = conn.cursor()
  print("Connection to SQL success {}".format(time.strftime("%Y%m%d-%H%M%S")) )
  
  for feature in features:
    idx = features.index(feature)

    print("drop table {} if exists... ".format(feature)),
    subTaskStart = time.time()
    curs.execute("DROP TABLE IF EXISTS %s;" % feature)
    conn.commit()
    print("{:4.2f} mins.".format((time.time() - start)/60))
    
    print("create table {}... ".format(feature)),
    subTaskStart = time.time()
    curs.execute(tables[idx])
    conn.commit()
    print("{:4.2f} mins.".format((time.time() - start)/60))	
    
    coordsToSQL(feature,featureLocation[idx],featureID[idx],featureID_type[idx],featureID_delimiter[idx],feature_query[idx],SpatialRef,conn)
    
except:
  print("Error {}".format(time.strftime("%Y%m%d-%H%M%S")) )
  print(sys.exc_info()[0])
  print(sys.exc_info()[1])
  raise
finally:
  conn.close()
  
# output to completion log    
script_running_log(script, task, start)