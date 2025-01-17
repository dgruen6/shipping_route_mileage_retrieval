# What this script does
This script is retrieving shipping mileages / truck mileages for given origin-destination-pairs of a shipping route. First, it tries to find the given route in a historical routes destination database table and retrieve mileage from the destination database table. If route could not be found in destination database table, the PC Miler API will be called to retrieve mileage. If mileage is invalid (mileage < 1.0), the ZIP codes will be alternated in order to fix the invalid mileage. ZIP code alternation in this context means, that for the same (city, state, country)-pair an other valid ZIP code is utilised for the API call. If mileage is still invalid after ZIP code alternation, the logic ends.    

# Requirements
1. VPN
2. Python 3.8 
3. Python packages listed in main.py

# How to use this script
The file that needs to be executed with a Python compiler/interpreter is >main.py<. 
Following steps will guide you through the process of running the script:  
1. The folder 'SQL_Queries' contains all required SQL quries in separate files. If you make changes to the logic, please make sure the SQL query is still matching your requirements.  
2. The folder 'API_Templates' contains the API template for a PC Miler API call. To call the PC Miler API you need to have a valid PC Miler license.  
3. The credentials for the source and destination databases are located in a file called 'config.ini'. You need to recreate that file in order to provide database credentials. The 'config.ini' file is not on Github as it contains sensitive data.  
4. Run the file 'main.py' with Python 3.8  
5. Mileage results will be written back to the destination database table. If the zip code is altered it will be in origin zip modified or destination zip modified fields in the destination database table.
6. A 'processed_routes_statistics.csv' will be created to give an overview of how many routes have been processed, how many valid mileages, etc.

# Enhancements
1. SMTP Email notification of success, failure with 'processed_routes_statistics.csv' attached on sucess. 
2. GoogleMaps API call and get a rough mileage estimate and update Mileage Type in destination database table to GoogleAPI 
