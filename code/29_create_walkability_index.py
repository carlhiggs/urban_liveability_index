# Purpose: calculate walkability index using config file for id variable
# Author:  Carl Higgs 
# Date:    20170418

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
task = 'calculate walkability index using config file for id variable'

A_pointsID = parser.get('parcels', 'parcel_id')


# SQL Settings - storing passwords in plain text is obviously not ideal
sqlDBName   = parser.get('postgresql', 'database')
sqlUserName = parser.get('postgresql', 'user')
sqlPWD      = parser.get('postgresql', 'password')

create_ind_walkability_hard = '''
DROP TABLE IF EXISTS ind_walkability_hard;
CREATE TABLE ind_walkability_hard AS
SELECT dl.{0}, z_dl, z_sc, z_dd, z_dl + z_sc + z_dd AS walkability
FROM (SELECT {0},(daily_living - AVG(daily_living) OVER())/stddev_pop(daily_living) OVER() as z_dl FROM ind_daily_living_hard) AS dl
LEFT JOIN
    (SELECT {0}, (sc_nh1600m - AVG(sc_nh1600m) OVER())/stddev_pop(sc_nh1600m) OVER() as z_sc FROM street_connectivity) AS sc
        ON sc.{0} = dl.{0}
LEFT JOIN
    (SELECT {0}, (dd_nh1600m - AVG(dd_nh1600m) OVER())/stddev_pop(dd_nh1600m) OVER() as z_dd FROM dwelling_density) AS dd
        ON dd.{0} = dl.{0};'''.format(A_pointsID.lower())

create_ind_walkability_soft = '''
DROP TABLE IF EXISTS ind_walkability_soft;
CREATE TABLE ind_walkability_soft AS
SELECT dl.{0}, z_dl, z_sc, z_dd, z_dl + z_sc + z_dd AS walkability
FROM (SELECT {0},(daily_living - AVG(daily_living) OVER())/stddev_pop(daily_living) OVER() as z_dl FROM ind_daily_living_soft) AS dl
LEFT JOIN
    (SELECT {0}, (sc_nh1600m - AVG(sc_nh1600m) OVER())/stddev_pop(sc_nh1600m) OVER() as z_sc FROM street_connectivity) AS sc
        ON sc.{0} = dl.{0}
LEFT JOIN
    (SELECT {0}, (dd_nh1600m - AVG(dd_nh1600m) OVER())/stddev_pop(dd_nh1600m) OVER() as z_dd FROM dwelling_density) AS dd
        ON dd.{0} = dl.{0};'''.format(A_pointsID.lower())
        
conn = psycopg2.connect(database=sqlDBName, user=sqlUserName, password=sqlPWD)
curs = conn.cursor()
curs.execute(create_ind_walkability_hard)
curs.execute(create_ind_walkability_soft)
conn.commit()
conn.close()

# output to completion log    
script_running_log(script, task, start)