# Purpose: " create POS indicators"
# Author:  Carl Higgs 
# Date:    20170216

import os
import sys
import time
import psycopg2 

from script_running_log import script_running_log
from ConfigParser import SafeConfigParser

task = " create POS indicators"
parser = SafeConfigParser()
parser.read(os.path.join(sys.path[0],'config.ini'))

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])


# SQL Settings - storing passwords in plain text is obviously not ideal
sqlDBName   = parser.get('postgresql', 'database')
sqlUserName = parser.get('postgresql', 'user')
sqlPWD      = parser.get('postgresql', 'password')
pos_all  = 'dist_cl_od_parcel_pos_all'
pos_large   = 'dist_cl_od_parcel_pos_gr15km2'
out_table   = 'ind_pos'
A_pointsID = parser.get('parcels', 'parcel_id')
pos_poly_id = parser.get('pos', 'pos_poly_id')

# set decay parameters
cutoff = 400
slope  = 5
distance    = 'distance'

# formula for soft-cutoff access
formula = ''' 
  (1-1.0/(1+exp(-{0}*({3}.{1}-{2})/{2}::double precision)))
  '''.format(slope,distance,cutoff,pos_large)
  
createTable = '''
DROP TABLE IF EXISTS ind_pos;

CREATE TABLE ind_pos AS
SELECT parcelmb.{1},
       COALESCE({3}.distance,0) AS d_cl_pos_any, 
       CASE WHEN ({3}.distance <= 400) THEN 1
            ELSE 0 END  AS pos_le_eq_400m,
       COALESCE({4}.distance,0) AS d_cl_pos_greq15000m2,
       CASE WHEN ({4}.distance <= 400) THEN 1
            ELSE 0 END AS pos_greq15000m2_in_400m_hard,
       CASE WHEN ({4}.distance <= 800) THEN 1
            ELSE 0 END AS pos_greq15000m2_in_800m_hard,
       CASE WHEN ({4}.distance <= 1600) THEN 1
            ELSE 0 END AS pos_greq15000m2_in_1600m_hard,
       COALESCE({2},0)::double precision AS pos_greq15000m2_in_{6}m_soft
FROM parcelmb 
LEFT JOIN {3} ON parcelmb.{1} = {3}.{1}
LEFT JOIN {4} ON parcelmb.{1} = {4}.{1}
LEFT JOIN pos_attribute ON {3}.{5} = pos_attribute.{5};
'''.format(out_table,A_pointsID,formula,pos_all,pos_large,pos_poly_id.lower(),cutoff)
   
conn = psycopg2.connect(database=sqlDBName, user=sqlUserName, password=sqlPWD)
curs = conn.cursor()
curs.execute(createTable)
conn.commit()
conn.close()

# output to completion log    
script_running_log(script, task, start)