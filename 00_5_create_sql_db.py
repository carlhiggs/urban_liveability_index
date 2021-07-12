# Script:  0_create_sql_db.py
# Purpose: Facilitate creation of a postgre sql database 
# Context: Used to create Liveability Index database
# Authors: Carl Higgs, Koen Simons

from ConfigParser import SafeConfigParser
import psycopg2
import os
import time
import sys
import getpass
import arcpy
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from script_running_log import script_running_log
from ConfigParser import SafeConfigParser
parser = SafeConfigParser()
parser.read(os.path.join(sys.path[0],'config.ini'))

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Create Liveability Index database, users and ArcSDE connection file'

## INPUT PARAMETERS
# note: 
# new database
newdb        = parser.get('postgresql', 'database')
newdbComment = 'An SQL database for Liveability Index related data.'

# new user
newUser      = parser.get('postgresql', 'user')
newUserPword = parser.get('postgresql', 'password')
r_user       = parser.get('postgresql', 'r_user')

# default database
print("Please enter default PostgreSQL database details to procede with new database creation, or close terminal to abort.")
sqlDBName   = raw_input("Database: ")    
sqlUserName = raw_input("Username: ")
sqlPWD      = getpass.getpass("Password for user {} on database {}: ".format(sqlUserName, sqlDBName))

# data directory (to save ArcSDE config file)
sql_host =  parser.get('postgresql', 'host')
folderPath = parser.get('data', 'folderPath')
arc_sde_user = parser.get('postgresql', 'arc_sde_user')
sde_connection = parser.get('postgresql', 'sde_connection')

# SQL queries
createDB = '''
  CREATE DATABASE {} 
  WITH OWNER = {} 
  ENCODING = 'UTF8' 
  LC_COLLATE = 'English_Australia.1252' 
  LC_CTYPE = 'English_Australia.1252' 
  TABLESPACE = pg_default 
  CONNECTION LIMIT = -1
  TEMPLATE template0;
  '''.format(newdb,sqlUserName)  

commentDB = '''
  COMMENT ON DATABASE {} IS '{}';
  '''.format(newdb,newdbComment)


createUser = '''
  CREATE USER {} PASSWORD '{}' ;
  '''.format(newUser, newUserPword)  

createUser_R = '''
  CREATE USER {} WITH
  LOGIN
  NOSUPERUSER
  NOCREATEDB
  NOCREATEROLE
  INHERIT
  NOREPLICATION
  CONNECTION LIMIT -1
  PASSWORD '{}';
  GRANT {} TO {};
  '''.format(r_user, newUserPword, newUser, r_user)

createUser_ArcSDE = '''
  CREATE USER {} WITH
  LOGIN
  NOSUPERUSER
  NOCREATEDB
  NOCREATEROLE
  INHERIT
  NOREPLICATION
  CONNECTION LIMIT -1
  PASSWORD '{}';
  GRANT {} TO {};
  '''.format(arc_sde_user, newUserPword, newUser, arc_sde_user)  
  
createPostGIS = '''CREATE EXTENSION postgis;SELECT postgis_full_version();'''
  
## OUTPUT PROCESS
try:
  print("Connecting to default database to action queries.")
  conn = psycopg2.connect(dbname=sqlDBName, user=sqlUserName, password=sqlPWD)
  conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
  cur = conn.cursor()
  
  print('Creating database {}... '.format(newdb)),
  cur.execute(createDB) 
  print('Done.')
  
  print('Adding comment "{}"... '.format(newdbComment)),
  cur.execute(commentDB)
  print('Done.')
  
  print('Creating PostGIS extension ... '),
  cur.execute(createPostGIS)
  print('Done.')

  print('Creating user {}... '.format(newUser)),
  cur.execute(createUser)
  print('Done.')

  print('Creating R user {}... '.format(r_user)),
  cur.execute(createUser_R)
  print('Done.')

  print('Creating ArcSDE user {}... '.format(arc_sde_user)),
  cur.execute(createUser_ArcSDE)
  print('Done.')
  
  arcpy.CreateDatabaseConnection_management(out_folder_path = folderPath,
                                            out_name = sde_connection, 
                                            database_platform = "POSTGRESQL", 
                                            instance = sql_host, 
                                            account_authentication = "DATABASE_AUTH", 
                                            username = arc_sde_user, 
                                            password = newUserPword, 
                                            save_user_pass = "SAVE_USERNAME", 
                                            database = newdb)
  
  conn.close()
  
except (Exception, psycopg2.DatabaseError) as error:
  print(error) 
finally:
  print("Process successfully completed.")
  if conn is not None:
     conn.close()


# output to completion log					
script_running_log(script, task, start)


