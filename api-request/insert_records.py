import psycopg2
from api_request import fetch_data
from datetime import datetime
import os
from dotenv import load_dotenv, find_dotenv

#Find the .env file(as it is not in the same dir as script)
load_dotenv("/home/usereugene/projects/Weatherstack/.env")
#Read the password
pwd = os.getenv("DBT_PASSWORD")

#Function to connect to our Database
def connect_to_db():
    print("Initializing Connection to PostgreSQL Database...")
    try:
        conn = psycopg2.connect(
            host="db",
            port=5432,
            dbname="weatherstack",
            user="eugene",
            password=pwd
        )
        print("Successfully Connected to the Postgres Database!")
        return conn
                
    except psycopg2.Error as e:
        print(f"Error connecting to Database: {e}")

#Function to create table
def create_table(conn):
    print("Creating Table ...")
    try:
        #Open a cursor to perform database operations
        cursor = conn.cursor()
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS data;
            CREATE TABLE IF NOT EXISTS data.nairobi_weather_data (
                id SERIAL PRIMARY KEY,
                temperature FLOAT,
                weather_description TEXT,
                wind_speed FLOAT,
                humidity FLOAT,
                pressure FLOAT,
                time TIMESTAMP,
                recorded_at TIMESTAMP DEFAULT NOW(),
                utc_offset TEXT,
                co FLOAT,
                so2 FLOAT,
                pm2_5 FLOAT,
                pm10 FLOAT
            );
        """)
        conn.commit()
        time = datetime.now().strftime("%c")
        print(f"Table 'data.nairobi_weather_data' was successfully created at: {time} ")
        
    except psycopg2.Error as e:
        print(f"Failed to create table: {e}")

def insert_records(conn, data):
    print("Inserting Nairobi Weather Data into the Database!!")
    try:
        cursor = conn.cursor()
        cursor.execute(""" 
            INSERT INTO data.nairobi_weather_data (
                temperature,
                weather_description,
                wind_speed,
                humidity,
                pressure,
                time,
                recorded_at,
                utc_offset,
                co,
                so2,
                pm2_5,
                pm10
            ) VALUES (%s, %s, %s, %s, %s, %s, NOW(), %s, %s, %s, %s, %s)
        """,(
            data["current"]["temperature"],
            data["current"]["weather_descriptions"][0],
            data["current"]["wind_speed"],
            data["current"]["humidity"],
            data["current"]["pressure"],
            data["location"]["localtime"],
            data["location"]["utc_offset"],
            data["current"]["air_quality"]["co"],
            data["current"]["air_quality"]["so2"],
            data["current"]["air_quality"]["pm2_5"],
            data["current"]["air_quality"]["pm10"],
        ))
        conn.commit()
        print("Data Successfully Inserted to Database in 'nairobi_weather_data' Table")
        
    except psycopg2.Error as e:
        print(f"Error inserting data into the Database: {e}")
def main():
    conn = None
    try:
        data = fetch_data()
        conn = connect_to_db()
        if conn is None:
            print("Failed to establish database connection. Exiting.")
            return 
        create_table(conn)
        insert_records(conn, data)
    except Exception as e:
        print(f"An error occurred during execution: {e}")    
    finally:
        if conn is not None:
            conn.close()
            print("Database Connection Closed!")