## This script is used to query the database
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

CONNECTION = os.getenv("DATABASE_URL") # paste connection string here or read from .env file

query_1 = """
    SELECT p.title, ps.id, ps.content, ps.start_time, ps.end_time, ps.embedding <-> target.embedding AS embedding_distance
    FROM podcast_segment ps
    JOIN podcast p ON ps.podcast_id = p.id,
        (SELECT embedding FROM podcast_segment WHERE id = '267:476') AS target
    WHERE ps.id != '267:476'
    ORDER BY embedding_distance
    LIMIT 5;
"""

query_2 = """
    SELECT p.title, ps.id, ps.content, ps.start_time, ps.end_time, ps.embedding <-> target.embedding AS embedding_distance
    FROM podcast_segment ps
    JOIN podcast p ON ps.podcast_id = p.id,
        (SELECT embedding FROM podcast_segment WHERE id = '267:476') AS target
    WHERE ps.id != '267:476'
    ORDER BY embedding_distance DESC
    LIMIT 5;
"""

query_3 = """
    SELECT p.title, ps.id, ps.content, ps.start_time, ps.end_time, ps.embedding <-> target.embedding AS embedding_distance
    FROM podcast_segment ps
    JOIN podcast p ON ps.podcast_id = p.id,
        (SELECT embedding FROM podcast_segment WHERE id = '48:511') AS target
    WHERE ps.id != '48:511'
    ORDER BY embedding_distance
    LIMIT 5;
"""


