# Purpose: This script calculates dwelling density (dwellings per hectare)
#          summing dwellings in meshblocks intersecting sausage buffers
#
#          It creates an SQL table containing: 
#            parcel identifier, 
#            sausage buffer area in sqm 
#            sausage buffer area in ha 
#            dwelling count, and 
#            dwellings per hectare (dwelling density)
#
#          It requires that the sausagebuffer_1600 and abs_linkage scripts have been run
#          as it uses the intersection of these spatial database features 
#          to aggregate dwelling counts within meshblocks intersecting sausage buffers
#
# Author:  Carl Higgs 20/03/2017

import os
import sys
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
task = 'calculate dwelling density (dwellings per hectare)'

# SQL Settings - storing passwords in plain text is obviously not ideal
sqlDBName   = parser.get('postgresql', 'database')
sqlUserName = parser.get('postgresql', 'user')
sqlPWD      = parser.get('postgresql', 'password')
dd_table = 'dwelling_density'
#  Size of tuple chunk sent to postgresql 
sqlChunkify = 500


# specify the unique location identifier 
pointsID = parser.get('parcels', 'parcel_id')

# intersections tables
meshblock_table = "abs_linkage"
distance = int(parser.get('network', 'distance'))
buffer_table = "sausagebuffer_{}".format(distance)


createTable_dd = '''
  CREATE TABLE IF NOT EXISTS {0}
  ({1} varchar PRIMARY KEY,
   dwellings integer NOT NULL,
   area_ha double precision NOT NULL,
   dd_nh1600m double precision NOT NULL 
  ); 
  '''.format(dd_table,pointsID.lower())
  
  
query_A = '''
INSERT INTO {0} ({1},dwellings,area_ha,dd_nh1600m)
(SELECT {2}.{1},  
          coalesce(sum({3}.dwellings),0) AS dwellings,
          (ST_Area({2}.geom)/10000)::double precision AS area_ha,
           coalesce(sum({3}.dwellings),0)/(ST_Area({2}.geom)/10000)::double precision as dd_nh1600m
FROM {2}  
LEFT JOIN {3}
ON ST_intersects({2}.geom, {3}.geom)
WHERE {1} IN
'''.format(dd_table,pointsID.lower(),buffer_table,meshblock_table)
  
query_C = '''
  GROUP BY {}) ON CONFLICT DO NOTHING;
  '''.format(pointsID.lower())


def unique_values(table, field):
  data = arcpy.da.TableToNumPyArray(table, [field])
  return np.unique(data[field])
    
  
try:  
  # Connect to postgreSQL server
  conn = psycopg2.connect(database=sqlDBName, user=sqlUserName, password=sqlPWD)
  curs = conn.cursor()
  print("Connection to SQL success {}".format(time.strftime("%Y%m%d-%H%M%S")) )
  # drop table if it already exists
  
  
  # Create spatial indices if not already existing
  print("Creating sausage buffer spatial index if not exists... "),
  curs.execute("CREATE INDEX IF NOT EXISTS {0}_gix ON {0} USING GIST (geom);".format(buffer_table))
  conn.commit()
  print("Creating abs linkage (meshblock_table) spatial index if not exists... "),
  curs.execute("CREATE INDEX IF NOT EXISTS {0}_gix ON {0} USING GIST (geom);".format(meshblock_table))
  conn.commit()
  print("Done.")
  
  # create dwelling density table
  print("create table {}... ".format(dd_table)),
  subTaskStart = time.time()
  curs.execute(createTable_dd)
  conn.commit()
  print("{:4.2f} mins.".format((time.time() - start)/60))	
   
   
  print("fetch list of processed parcels, if any..."), 
  # (for string match to work, had to select first item of returned tuple)
  curs.execute("SELECT {} FROM {}".format(pointsID.lower(),buffer_table))
  raw_point_id_list = list(curs)
  raw_point_id_list = [x[0] for x in raw_point_id_list]
  
  curs.execute("SELECT {} FROM {}".format(pointsID.lower(),dd_table))
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
        curs.execute('{} ({}) {}'.format(query_A,','.join("'"+x+"'" for x in chunkedPoints),query_C))
        conn.commit()
        chunkedPoints = list()
        progressor(count,denom,start,"{}/{} points processed".format(count,denom))
  if(count % sqlChunkify != 0):
     curs.execute('{} ({}) {}'.format(query_A,','.join("'"+x+"'" for x in chunkedPoints),query_C))
     conn.commit()
  
  progressor(count,denom,start,"{}/{} points processed".format(count,denom))
  
except:
       print('''HEY, IT'S AN ERROR: {}'''.format(sys.exc_info()))
       
# output to completion log    
script_running_log(script, task, start)

# clean up
conn.close()