# Urban Liveability Index

This repository contains the code used for creation of the pilot- and 15-indicator revised versions of the spatial Urban Liveability Index for Melbourne, 2012.

The code was authored and run in 2016-18, and formed the basis of subsequent code used for analysis for the Australian National Liveability projects (capital cities in 2017; 21 cities in 2018-19) and related projects.  The code is run as a sequential series of scripts, or manual steps (as per directions in plain text) using Python 2.7 in conjunction with the ArcGIS arcpy library, and PostgreSQL with PostGIS.  Most of the scripts relate to address level built environment analyses conducted in preparation of component indicators included in the Urban Liveability Index.

Code used for the calculation of the pilot Urban Liveability Index is contained in `.\code\34a_parcel-based_LiveabilityCI_pilot_ULI.py`.  This relates to the methods published as: Higgs, C., Badland, H., Simons, K. and Giles-Corti, B. The Urban Liveability Index: developing a policy-relevant urban liveability composite measure and evaluating associations with transport mode choice. Int J Health Geogr 18, 14 (2019). https://doi.org/10.1186/s12942-019-0178-8

The code for the revised Urban Liveability Index calculation is contained in `.\code\34b_parcel-based_LiveabilityCI_ULI_v2_i15.py`.  This was used to create the 15 indicator revised version as used in a linkage analysis of Victorian Population Health Survey participants, in a paper submitted to npj Urban Sustainability for review: Higgs, C., Badland, H., Simons, K. and Giles-Corti, B. Urban liveability and adult cardiometabolic health: policy-relevant evidence from a cross-sectional Australian built environment data linkage study. npj Urban Sustainability (2021; submitted for review).

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

In addition input source data are required; file locations may be configured as part of the code configuration process.

Analysis code are located at https://bitbucket.org/Koen_Simons/liveability_vista (pilot ULI), and https://bitbucket.org/Koen_Simons/liveability_vphs (revised ULI).

### Contributors ###

Carl Higgs, Koen Simons; 2016-18

The repository is maintained by Carl Higgs <carlhiggs@rmit.edu.au>, Healthy Liveable Cities Research Group in the Centre for Urban Research, RMIT.  
