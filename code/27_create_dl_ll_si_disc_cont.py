# Purpose: create destination indicator tables:
#          In particular, discrete and continuous versions of:
#             daily living, local living and social infrastructure mix
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
task = 'create destination indicator tables'

A_pointsID = parser.get('parcels', 'parcel_id')


# SQL Settings - storing passwords in plain text is obviously not ideal
sqlDBName   = parser.get('postgresql', 'database')
sqlUserName = parser.get('postgresql', 'user')
sqlPWD      = parser.get('postgresql', 'password')

createTable = '''
-- Non necessary tables are commented out here, but code retained for future reference
--
-- CREATE TABLE IF NOT EXISTS ind_dest_sum AS
-- SELECT {0}, (COALESCE(communitycentre_1000m, 0) + 
--              COALESCE(museumartgallery_3200m, 0) + 
--              COALESCE(cinematheatre_3200m, 0) + 
--              COALESCE(libraries_2014_1000m, 0) + 
--              COALESCE(childcareoutofschool_1600m, 0) + 
--              COALESCE(childcare_800m, 0) + 
--              COALESCE(statesecondaryschools_1600m, 0) + 
--              COALESCE(stateprimaryschools_1600m, 0) + 
--              COALESCE(tafecampuses_3200m, 0) + 
--              COALESCE(u3a_3200m, 0) + 
--              COALESCE(universitymaincampuses_3200m, 0) + 
--              COALESCE(agedcare_1000m, 0) + 
--              COALESCE(communityhealthcentres_1000m, 0) + 
--              COALESCE(dentists_1000m, 0) + 
--              COALESCE(gp_clinics_1000m, 0) + 
--              COALESCE(maternalchildhealth_1000m, 0) + 
--              COALESCE(swimmingpools_1200m, 0) + 
--              COALESCE(sport_1200m, 0) + 
--              COALESCE(supermarkets_1000m, 0) + 
--              COALESCE(conveniencestores_1000m, 0) + 
--              COALESCE(petrolstations_1000m, 0) + 
--              COALESCE(newsagents_1000m, 0) + 
--              COALESCE(fishmeatpoultryshops_1600m, 0) + 
--              COALESCE(fruitvegeshops_1600m, 0) + 
--              COALESCE(pharmacy_1000m, 0) + 
--              COALESCE(postoffice_1600m, 0) + 
--              COALESCE(banksfinance_1600m, 0) + 
--              COALESCE(busstop2012_400m, 0) + 
--              COALESCE(tramstops2012_600m, 0) + 
--              COALESCE(trainstations2012_800m,0)) AS dest_sum 
-- FROM ind_dest_hard;


DROP TABLE ind_dest_pt_hard;
CREATE TABLE ind_dest_pt_hard AS
SELECT {0},  (CASE WHEN COALESCE(busstop2012_400m, 0) + 
                        COALESCE(tramstops2012_600m, 0) + 
                        COALESCE(trainstations2012_800m,0) > 0 THEN 1
                   ELSE 0 END) AS dest_pt 
FROM ind_dest_hard;

DROP TABLE ind_dest_pt_soft;
CREATE TABLE ind_dest_pt_soft AS
SELECT {0},  GREATEST(COALESCE(busstop2012_400m,0),COALESCE(tramstops2012_600m,0),COALESCE(trainstations2012_800m,0))::double precision AS dest_pt 
FROM ind_dest_soft;

DROP TABLE ind_daily_living_hard;
CREATE TABLE ind_daily_living_hard AS
SELECT {0},  (COALESCE(supermarkets_1000m, 0) + 
             (CASE WHEN COALESCE(conveniencestores_1000m, 0) + 
                        COALESCE(petrolstations_1000m, 0) + 
                        COALESCE(newsagents_1000m, 0) > 0 THEN 1
			  ELSE 0 END) +  
             (CASE WHEN COALESCE(busstop2012_400m, 0) + 
                        COALESCE(tramstops2012_600m, 0) + 
                        COALESCE(trainstations2012_800m,0) > 0 THEN 1
			  ELSE 0 END)) AS daily_living 
FROM ind_dest_hard;

DROP TABLE ind_daily_living_soft;
CREATE TABLE ind_daily_living_soft AS
SELECT {0},  (COALESCE(supermarkets_1000m,0) + 
             GREATEST(COALESCE(conveniencestores_1000m,0),COALESCE(petrolstations_1000m,0),COALESCE(newsagents_1000m,0)) +  
             GREATEST(COALESCE(busstop2012_400m,0),COALESCE(tramstops2012_600m,0),COALESCE(trainstations2012_800m,0)))::double precision AS daily_living
FROM ind_dest_soft;

DROP TABLE ind_local_living_hard;
CREATE TABLE ind_local_living_hard AS
SELECT {0}, (COALESCE(communitycentre_1000m, 0) + 
             COALESCE(libraries_2014_1000m, 0) + 
			 (CASE WHEN COALESCE(childcareoutofschool_1600m, 0) + 
                        COALESCE(childcare_800m, 0) > 0 THEN 1
			  ELSE 0 END) + 
             COALESCE(dentists_1000m, 0) + 
             COALESCE(gp_clinics_1000m, 0) + 
             COALESCE(supermarkets_1000m, 0) + 
             (CASE WHEN COALESCE(conveniencestores_1000m, 0) + 
                        COALESCE(petrolstations_1000m, 0) + 
                        COALESCE(newsagents_1000m, 0) > 0 THEN 1
			  ELSE 0 END) +  
             (CASE WHEN COALESCE(fishmeatpoultryshops_1600m, 0) + 
                        COALESCE(fruitvegeshops_1600m, 0) > 0 THEN 1
			  ELSE 0 END) + 
             COALESCE(pharmacy_1000m, 0) + 
             COALESCE(postoffice_1600m, 0) + 
             COALESCE(banksfinance_1600m, 0) + 
             (CASE WHEN COALESCE(busstop2012_400m, 0) + 
                        COALESCE(tramstops2012_600m, 0) + 
                        COALESCE(trainstations2012_800m,0) > 0 THEN 1
			  ELSE 0 END)) AS local_living 
FROM ind_dest_hard;

DROP TABLE ind_local_living_soft;
CREATE TABLE ind_local_living_soft AS
SELECT {0}, (COALESCE(communitycentre_1000m, 0) + 
             COALESCE(libraries_2014_1000m, 0) + 
			 GREATEST(COALESCE(childcareoutofschool_1600m,0), COALESCE(childcare_800m, 0)) + 
             COALESCE(dentists_1000m, 0) +
             COALESCE(gp_clinics_1000m, 0) + 
             COALESCE(supermarkets_1000m, 0) + 
             GREATEST(COALESCE(conveniencestores_1000m, 0),COALESCE(petrolstations_1000m, 0),COALESCE(newsagents_1000m, 0))+
			 GREATEST(COALESCE(fishmeatpoultryshops_1600m, 0) + 
                        COALESCE(fruitvegeshops_1600m, 0)) +
             COALESCE(pharmacy_1000m, 0) + 
             COALESCE(postoffice_1600m, 0) + 
             COALESCE(banksfinance_1600m, 0) + 
             GREATEST(COALESCE(busstop2012_400m, 0) + 
                        COALESCE(tramstops2012_600m, 0) + 
                        COALESCE(trainstations2012_800m,0))) AS local_living
FROM ind_dest_soft;

DROP TABLE ind_si_mix_hard;
CREATE TABLE ind_si_mix_hard AS
SELECT {0}, (COALESCE(communitycentre_1000m,0) + 
             COALESCE(museumartgallery_3200m,0) + 
             COALESCE(cinematheatre_3200m,0) + 
             COALESCE(libraries_2014_1000m,0) + 
             COALESCE(childcareoutofschool_1600m,0) + 
             COALESCE(childcare_800m,0) + 
             COALESCE(statesecondaryschools_1600m,0) + 
             COALESCE(stateprimaryschools_1600m,0) + 
             COALESCE(agedcare_2012_1000m,0) + 
             COALESCE(communityhealthcentres_1000m,0) + 
             COALESCE(dentists_1000m,0) + 
             COALESCE(gp_clinics_1000m,0) + 
             COALESCE(maternalchildhealth_1000m,0) + 
             COALESCE(swimmingpools_1200m,0) + 
             COALESCE(sport_1200m,0) +
             COALESCE(pharmacy_1000m,0)) AS si_mix
FROM ind_dest_hard;

DROP TABLE ind_si_mix_soft;
CREATE TABLE ind_si_mix_soft AS
SELECT {0}, (COALESCE(communitycentre_1000m,0) + 
             COALESCE(museumartgallery_3200m,0) + 
             COALESCE(cinematheatre_3200m,0) + 
             COALESCE(libraries_2014_1000m,0) + 
             COALESCE(childcareoutofschool_1600m,0) + 
             COALESCE(childcare_800m,0) + 
             COALESCE(statesecondaryschools_1600m,0) + 
             COALESCE(stateprimaryschools_1600m,0) + 
             COALESCE(agedcare_2012_1000m,0) + 
             COALESCE(communityhealthcentres_1000m,0) + 
             COALESCE(dentists_1000m,0) + 
             COALESCE(gp_clinics_1000m,0) + 
             COALESCE(maternalchildhealth_1000m,0) + 
             COALESCE(swimmingpools_1200m,0) + 
             COALESCE(sport_1200m,0) +
             COALESCE(pharmacy_1000m,0)) AS si_mix
FROM ind_dest_soft;
'''.format(A_pointsID.lower())



conn = psycopg2.connect(database=sqlDBName, user=sqlUserName, password=sqlPWD)
curs = conn.cursor()
curs.execute(createTable)
conn.commit()
conn.close()

# output to completion log    
script_running_log(script, task, start)