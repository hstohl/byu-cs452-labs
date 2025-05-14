## This script is used to create the tables in the database

import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

CONNECTION = os.getenv("DATABASE_URL")
# paste connection string here or read from .env file

# need to run this to enable vector data type
CREATE_EXTENSION = "CREATE EXTENSION vector"

# TODO: Add create table statement
CREATE_PODCAST_TABLE = """
    CREATE TABLE IF NOT EXISTS podcast (
        id INT PRIMARY KEY,
        title VARCHAR(255) NOT NULL
    );
"""
# TODO: Add create table statement
CREATE_SEGMENT_TABLE = """
    CREATE TABLE IF NOT EXISTS podcast_segment (
        id VARCHAR(8) PRIMARY KEY,
        start_time FLOAT NOT NULL,
        end_time FLOAT NOT NULL,
        content TEXT NOT NULL,
        embedding VECTOR(1536) NOT NULL,
        podcast_id INT FOREIGN KEY REFERENCES podcast(id)
    );

"""

conn = psycopg2.connect(CONNECTION)
cursor = conn.cursor()

cursor.execute(CREATE_EXTENSION)
cursor.execute(CREATE_PODCAST_TABLE)
cursor.execute(CREATE_SEGMENT_TABLE)

conn.commit()
cursor.close()
conn.close()
# TODO: Create tables with psycopg2 (example: https://www.geeksforgeeks.org/executing-sql-query-with-psycopg2-in-python/)


