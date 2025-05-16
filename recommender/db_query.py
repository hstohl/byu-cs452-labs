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

query_4 = """
    SELECT p.title, ps.id, ps.content, ps.start_time, ps.end_time, ps.embedding <-> target.embedding AS embedding_distance
    FROM podcast_segment ps
    JOIN podcast p ON ps.podcast_id = p.id,
        (SELECT embedding FROM podcast_segment WHERE id = '51:56') AS target
    WHERE ps.id != '51:56'
    ORDER BY embedding_distance
    LIMIT 5;
"""

query_5 = """
    SELECT 
    p.title AS podcast_title,
    target.embedding <-> avg_embeddings.avg_embedding AS embedding_distance
    FROM (
        SELECT 
            podcast_id, 
            AVG(embedding) AS avg_embedding
        FROM podcast_segment
        GROUP BY podcast_id
    ) AS avg_embeddings
    JOIN podcast p ON p.id = avg_embeddings.podcast_id,
        (SELECT embedding FROM podcast_segment WHERE id = '267:476') AS target
    ORDER BY embedding_distance
    LIMIT 5;
"""

query_6 = """ 
    SELECT 
    p.title AS podcast_title,
    target.embedding <-> avg_embeddings.avg_embedding AS embedding_distance
    FROM (
        SELECT 
            podcast_id, 
            AVG(embedding) AS avg_embedding
        FROM podcast_segment
        GROUP BY podcast_id
    ) AS avg_embeddings
    JOIN podcast p ON p.id = avg_embeddings.podcast_id,
        (SELECT embedding FROM podcast_segment WHERE id = '48:511') AS target
    ORDER BY embedding_distance
    LIMIT 5;
"""

query_7 = """
    SELECT 
    p.title AS podcast_title,
    target.embedding <-> avg_embeddings.avg_embedding AS embedding_distance
    FROM (
        SELECT 
            podcast_id, 
            AVG(embedding) AS avg_embedding
        FROM podcast_segment
        GROUP BY podcast_id
    ) AS avg_embeddings
    JOIN podcast p ON p.id = avg_embeddings.podcast_id,
        (SELECT embedding FROM podcast_segment WHERE id = '51:56') AS target
    ORDER BY embedding_distance
    LIMIT 5;
"""

query_8 = """
    SELECT 
        p.title AS podcast_title,
        target.avg_embedding <-> avg_embeddings.avg_embedding AS embedding_distance
    FROM (
        SELECT 
            podcast_id, 
            AVG(embedding) AS avg_embedding
        FROM podcast_segment
        GROUP BY podcast_id
    ) AS avg_embeddings
    JOIN podcast p ON p.id = avg_embeddings.podcast_id,
        (
            SELECT AVG(embedding) AS avg_embedding
            FROM podcast_segment
            WHERE podcast_id = 205
        ) AS target
    WHERE avg_embeddings.podcast_id != 205
    ORDER BY embedding_distance
    LIMIT 5;
"""

queries = [query_1, query_2, query_3, query_4]

conn = psycopg2.connect(CONNECTION)
cursor = conn.cursor()
i = 1
# Execute the first four queries
for query in queries:
    print("Query ", i)
    cursor.execute(query)
    results = cursor.fetchall()
    for row in results:
        podcast_title, segment_id, content, start_time, end_time, distance = row
        print(f"[{segment_id}] {podcast_title} | {start_time:.2f}–{end_time:.2f} | distance={distance:.4f}")
        print(f"    {content[:100]}...\n")
    i += 1

# Execute the last three queries
for query in [query_5, query_6, query_7]:
    print("Query ", i)
    cursor.execute(query)
    results = cursor.fetchall()
    for row in results:
        podcast_title, distance = row
        print(f"{podcast_title} | distance={distance:.4f}")
    i += 1

# Execute the last query
print("Query ", i)
cursor.execute(query_8)
results = cursor.fetchall()
for row in results:
    podcast_title, distance = row
    print(f"{podcast_title} | distance={distance:.4f}")

cursor.close()
conn.close()

