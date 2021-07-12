# Purpose: create a table for ABS indicator variables
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
task = 'create a table for ABS indicator variables'

A_pointsID = parser.get('parcels', 'parcel_id')


# SQL Settings - storing passwords in plain text is obviously not ideal
sqlDBName   = parser.get('postgresql', 'database')
sqlUserName = parser.get('postgresql', 'user')
sqlPWD      = parser.get('postgresql', 'password')

createTable = '''
DROP TABLE IF EXISTS ind_abs;
CREATE TABLE ind_abs AS
SELECT DISTINCT parcelmb.{0}, 
                t2.sa1_7dig11, 
                t2.sa2_name11, 
                t3.sa2_prop_live_work_sa3, 
                t4.sa1_mean_cars, 
                t5.sa1_prop_rental,  
                t6.sa1_prop_affordablehous_30_40,
                t7.sa1_prop_mtwp_private, t7.sa1_prop_mtwp_public, t7.sa1_prop_mtwp_active, t7.sa1_prop_mtwp_public + t7.sa1_prop_mtwp_active AS sa1_prop_mtwp_activeORpublic
                FROM parcelmb 
LEFT JOIN (SELECT abs_linkage.mb_code11, abs_linkage.sa1_7dig11, abs_linkage.sa2_name11 FROM abs_linkage) AS t2 ON parcelmb.mb_code11 = t2.mb_code11
LEFT JOIN (SELECT liveworksamesa3.sa2_name11, sa3_prop_live_work AS sa2_prop_live_work_sa3 FROM liveworksamesa3) AS t3 ON t2.sa2_name11 = t3.sa2_name11
LEFT JOIN (SELECT sa1_7dig11, CASE WHEN COALESCE(one_mv, 0)        + 
                        COALESCE(two_mv, 0)*2           +
                        COALESCE(three_mv, 0)*3         +
                        COALESCE(fourup_mv, 0)*4 = 0 THEN 0 :: double precision
                        ELSE  (COALESCE(one_mv, 0)      + 
                               COALESCE(two_mv, 0)*2    +
                               COALESCE(three_mv, 0)*3  +
                               COALESCE(fourup_mv, 0)*4) 
                               / (COALESCE(no_mv, 0)    +
                                  COALESCE(one_mv, 0)   + 
                                  COALESCE(two_mv, 0)   +
                                  COALESCE(three_mv, 0) +
                                  COALESCE(fourup_mv, 0)) :: double precision END :: double precision AS sa1_mean_cars 
           FROM carownership)  AS t4 ON t2.sa1_7dig11 = t4.sa1_7dig11
LEFT JOIN (SELECT sa1_7dig11, CASE WHEN COALESCE(owner_occupied,0)+COALESCE(rental,0) = 0 THEN 0 :: double precision
                                   ELSE COALESCE(rental,0)/(COALESCE(owner_occupied,0)+COALESCE(rental,0)) :: double precision END :: double precision AS sa1_prop_rental 
                              FROM owneroccupiedtorentalhousing) AS t5 ON  t2.sa1_7dig11 = t5.sa1_7dig11
LEFT JOIN (SELECT sa1_7dig11, CASE WHEN COALESCE(validtot_1st2nd_hhinc_quint,0) = 0 THEN 0 :: double precision
                                   ELSE COALESCE(hous_cost_le30pct_hhinc,0)/COALESCE(validtot_1st2nd_hhinc_quint,0) :: double precision END :: double precision AS sa1_prop_affordablehous_30_40 
                              FROM affordablehousing) AS t6 ON  t2.sa1_7dig11 = t6.sa1_7dig11
LEFT JOIN (SELECT sa1_7dig11, CASE WHEN COALESCE(mtwp_total,0) = 0 THEN 0 :: double precision
                                   ELSE COALESCE(mtwp_private,0)/COALESCE(mtwp_total,0) :: double precision END :: double precision AS sa1_prop_mtwp_private, 
                              CASE WHEN COALESCE(mtwp_total,0) = 0 THEN 0 :: double precision
                                   ELSE COALESCE(mtwp_public,0)/COALESCE(mtwp_total,0) :: double precision END :: double precision AS sa1_prop_mtwp_public, 
                              CASE WHEN COALESCE(mtwp_total,0) = 0 THEN 0 :: double precision
                                   ELSE COALESCE(mtwp_active,0)/COALESCE(mtwp_total,0) :: double precision END :: double precision AS sa1_prop_mtwp_active
                              FROM methodoftraveltoworkplace) AS t7 ON  t2.sa1_7dig11 = t7.sa1_7dig11;
'''.format(A_pointsID.lower())
   
conn = psycopg2.connect(database=sqlDBName, user=sqlUserName, password=sqlPWD)
curs = conn.cursor()
curs.execute(createTable)
conn.commit()
conn.close()

# output to completion log    
script_running_log(script, task, start)