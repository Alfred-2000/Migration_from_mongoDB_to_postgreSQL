
#To import json file to mongodb:
"""mongoimport --db testproject --collection trips --file trips.json"""

#To create database:
"""
CREATE DATABASE testproject;
CREATE USER testprojectuser WITH PASSWORD 'password';
ALTER ROLE testprojectuser SET client_encoding TO 'utf8';
ALTER DATABASE testproject SET timezone TO 'Asia/Kolkata';
ALTER ROLE testprojectuser SET default_transaction_isolation TO 'read committed';
ALTER ROLE testprojectuser SET timezone TO 'Asia/Kolkata';
GRANT ALL PRIVILEGES ON DATABASE testproject TO testprojectuser;
"""

#with foreign key
"""CREATE TABLE trip (
	trip_id varchar(24) NOT NULL PRIMARY KEY, 
	trip_duration integer NULL, 
	start_station_id integer NULL, 
	start_station_name text NULL, 
	end_station_id integer NULL, 
	end_station_name text NULL, 
	bike_id integer NULL, 
	user_type text NULL, 
	birth_year integer NULL, 
	gender smallint NULL, 
	start_time timestamp with time zone NULL, 
	stop_time timestamp with time zone NULL					
);"""
		
"""CREATE TABLE point (
	 trip_id varchar(24) NOT NULL PRIMARY KEY,
	 start_station_location text NULL, 
	 end_station_location text NULL, 
	 FOREIGN KEY (trip_id) REFERENCES trip(trip_id) ON DELETE CASCADE		 
)"""

#To change owner of table:
"""
alter table trip owner to testprojectuser;
alter table point owner to testprojectuser;
"""

import os, sys, psycopg2, json
from pymongo import MongoClient

"""Postgresql Database connections"""
conn = psycopg2.connect(
    host="localhost",
    database="testproject",
    user="testprojectuser",
    password="password",
    port=5432
    )

"""Mongo-DB database connectivity initialization"""
uri = "mongodb://127.0.0.1:27017/testproject"
client = MongoClient(uri)
mydatabase = client['testproject']
mycollection_trip = mydatabase['trips']

def migrate_records_from_mongodb_to_postgresql():
    try:
        
        list_of_trips = [datas for datas in mycollection_trip.find()]
        list_of_trips_values = []
        list_of_location_values = []
        for ele in list_of_trips:
            ele_id = str(ele['_id'])
            ele_tripduration = ele['tripduration'] if ele.get('tripduration') else None
            ele_start_station_id = ele['start station id'] if ele.get('start station id') else None
            ele_start_station_name = ele['start station name'] if ele.get('tart station name') else None
            ele_end_station_id = ele['end station id'] if ele.get('end station id') else None
            ele_end_station_name = ele['end station name'] if ele.get('end station name') else None
            ele_bikeid = ele['bikeid'] if ele.get('bikeid') else None
            ele_usertype = ele['usertype'] if ele.get('usertype') else None
            ele_birth_year = ele['birth year'] if ele.get('birth year') else None
            ele_gender = ele['gender'] if ele.get('gender') else None
            ele_start_time = ele['start time'] if ele.get('start time') else None
            ele_stop_time = ele['stop time'] if ele.get('stop time') else None
            start_location = convert_gps_coordinates(ele['start station location']) if ele.get('start station location') else None
            ele_start_station_location = start_location
            end_location = convert_gps_coordinates(ele['end station location']) if ele.get('end station location') else None
            ele_end_station_location = end_location
            
            trip_values = (ele_id, ele_tripduration, ele_start_station_id, ele_start_station_name,
                    ele_end_station_id, ele_end_station_name, ele_bikeid, ele_usertype,
                    ele_birth_year, ele_gender, ele_start_time, ele_stop_time)
            list_of_trips_values.append(trip_values)
            
            location_values = (ele_id, ele_start_station_location, ele_end_station_location)
            list_of_location_values.append(location_values)
        
        trip_insert_query = """INSERT INTO trip (
                                trip_id, trip_duration, start_station_id, 
                                start_station_name, end_station_id, end_station_name,
                                bike_id, user_type, birth_year, gender, 
                                start_time, stop_time) 
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            """
        
        location_insert_query = """INSERT INTO point (
                                    trip_id, start_station_location, end_station_location)
                                VALUES (%s,%s,%s)
                                """

        cur = conn.cursor()
        cur.executemany(trip_insert_query, list_of_trips_values)
        cur.executemany(location_insert_query, list_of_location_values)
        cur.close()
        conn.commit()
    except:
        print("Exception occured while fetching record's from mongodb")


def convert_gps_coordinates(dict_values):
    for key, value in dict_values.items():
        if key == 'coordinates':
            lattitude = value[0]['$numberDouble']
            longitude = value[1]['$numberDouble']
            iter_nav = lattitude + ', ' + longitude
    return iter_nav


if __name__ == '__main__':
    try:
        migrate_records_from_mongodb_to_postgresql()   #with foreign key relationship
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
     
     
