# Purpose: create a table for local vs heavy road indicators (trialling different versions)
#          - local_to_heavy_roads: local roads/heavy roads
#          - heavy_to_local_roads: local roads/heavy roads
#          - local_road_balance_density: road difference/sausagebuffer area
#          - local_road_diff_std: road difference/half sum of road lengths 
# Author:  Carl Higgs 
# Date:    20170216

import os
import sys
import time
import psycopg2 

from script_running_log import script_running_log
from ConfigParser import SafeConfigParser


parser = SafeConfigParser()
parser.read(os.path.join(sys.path[0],'config.ini'))

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'create a table for PT distance to closest data with various area linkage'

A_pointsID = parser.get('parcels', 'parcel_id')


# SQL Settings - storing passwords in plain text is obviously not ideal
sqlDBName   = parser.get('postgresql', 'database')
sqlUserName = parser.get('postgresql', 'user')
sqlPWD      = parser.get('postgresql', 'password')

createTable = '''
DROP TABLE IF EXISTS ind_roads;
CREATE TABLE ind_roads AS
SELECT DISTINCT parcelmb.{0},
                (area_sqm/10000) AS area_ha, 
                roads_heavy, 
                roads_local, 
                CASE WHEN roads_heavy = 0 
                          THEN (roads_local+1)/1  :: double precision
                     ELSE roads_local/roads_heavy  :: double precision END AS local_to_heavy_roads,
                CASE WHEN roads_local = 0 
                          THEN (roads_heavy+1)/1 :: double precision
                     ELSE roads_heavy/roads_local :: double precision END AS heavy_to_local_roads,
                (roads_local-roads_heavy)/(area_sqm/10000)::double precision AS local_road_balance_density,
                CASE WHEN roads_local+roads_heavy = 0 
                           THEN 0
                      ELSE ((roads_local-roads_heavy)/(0.5*(roads_local+roads_heavy)::double precision))::double precision
                      END AS local_road_diff_std                     
FROM parcelmb 
LEFT JOIN road_length ON parcelmb.{0} = road_length.{0}
LEFT JOIN nh1600m ON road_length.{0} = nh1600m.{0};
'''.format(A_pointsID.lower())
   
conn = psycopg2.connect(database=sqlDBName, user=sqlUserName, password=sqlPWD)
curs = conn.cursor()
curs.execute(createTable)
conn.commit()
conn.close()

# output to completion log    
script_running_log(script, task, start)