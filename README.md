# Pilot Liveability Index project repository #

### Liveability Index indicator calculation ###

This repository contains the code used for creation of the pilot- and 15-indicator revised versions of the Urban Liveability Index for Melbourne, 2012.

The code was authored and run in 2016-18, and formed the basis of subsequent code used for analysis for the Australian National Liveability projects (capital cities in 2017; 21 cities in 2018-19).

This project assumes the following:

- You have postgresql installed w/ PostGIS extension.
  -- Get from: https://www.enterprisedb.com/downloads/postgres-postgresql-downloads
  -- Scripts were written with PostgreSQL 9.6, so recommend using this at least
        -- e.g. upsert functionality not available prior to v9.5

- You have Python 2.7 installed.  
- More specifically, use the 64-bit version with ArcGIS 10.5.x
  -- should still work on 10.3.x, however some network building functions will have to be done manually
       -- ie. dissolve and build cannot be scripted prior to 10.5.x
  -- the scripts use certain libraries (e.g. arcpy, pandas) that may otherwise have to be installed if not included in the Python release you are using where other than the above (e.g. Anaconda distribution will need to have arcpy library located in its library directory -- see online).   
  
- Make sure that the following folders are included in your Path environment settings.  The instructions here describe this process in the context of Java, but the principle is the same (https://www.java.com/en/download/help/path.xml):
  --  python scripts folder (e.g. C:\Python27\ArcGIS10.5\Scripts) 
        -- this allows you to use the python installer package 'pip' from commandline
  -- postgresql bin directory (e.g. C:\Program Files\PostgreSQL\9.6\bin)
        -- this contains programs and drivers req'd for some operations
        
- Install psycopg2 (the python library facilitating connection with PostgreSQL)
 -- open a console window and type 'pip install psycopg2'
 -- if you have completed the above, this should work

- Install config parser
 -- while you have the console window above still open, type 'pip install ConfigParser'

Carl Higgs


### Contributors ###

Carl Higgs, Koen Simons; 2016-18

The repository is maintained by Carl Higgs <carlhiggs@rmit.edu.au>, Healthy Liveable Cities Research Group in the Centre for Urban Research, RMIT.  
