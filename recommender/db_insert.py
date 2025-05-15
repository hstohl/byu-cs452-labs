## This script is used to insert data into the database
import os
import json
from dotenv import load_dotenv
from datasets import load_dataset
import pandas as pd

from utils import fast_pg_insert

load_dotenv()

CONNECTION = os.getenv("DATABASE_URL")

# TODO: Read the embedding files

# TODO: Read documents files
documents_dir = "documents"
doc_segments = {}
podcast_id_to_title = {}

for filename in os.listdir(documents_dir):
    if filename.endswith(".jsonl"):
        with open(os.path.join(documents_dir, filename), "r", encoding="utf-8") as f:
            for line in f:
                try:
                    obj = json.loads(line)
                    custom_id = obj["custom_id"]
                    podcast_id = int(custom_id.split(":")[0])
                    input_text = obj["body"]["input"]
                    metadata = obj["body"]["metadata"]
                    start_time = metadata["start_time"]
                    end_time = metadata["stop_time"]
                    title = metadata["title"]

                    doc_segments[custom_id] = {
                        "podcast_id": podcast_id,
                        "start_time": start_time,
                        "end_time": end_time,
                        "content": input_text
                    }

                    if podcast_id not in podcast_id_to_title:
                        podcast_id_to_title[podcast_id] = title

                except Exception as e:
                    print(f"[DOC ERROR] {e}")


embeddings_dir = "embedding"
segment_rows = []

for filename in os.listdir(embeddings_dir):
    if filename.endswith(".jsonl"):
        with open(os.path.join(embeddings_dir, filename), "r", encoding="utf-8") as f:
            for line in f:
                try:
                    obj = json.loads(line)
                    custom_id = obj["custom_id"]
                    embedding = obj["response"]["body"]["data"][0]["embedding"]

                    if custom_id in doc_segments:
                        doc_data = doc_segments[custom_id]
                        segment_rows.append({
                            "id": custom_id,
                            "start_time": doc_data["start_time"],
                            "end_time": doc_data["end_time"],
                            "content": doc_data["content"],
                            "embedding": embedding,
                            "podcast_id": doc_data["podcast_id"]
                        })
                except Exception as e:
                    print(f"[EMBEDDING ERROR] {e}")
# HINT: In addition to the embedding and document files you likely need to load the raw data via the hugging face datasets library
ds = load_dataset("Whispering-GPT/lex-fridman-podcast")


# TODO: Insert into postgres
podcast_df = pd.DataFrame([
    {"id": pid, "title": title}
    for pid, title in podcast_id_to_title.items()
])

df_segments = pd.DataFrame(segment_rows)

fast_pg_insert(df=podcast_df, connection=CONNECTION, table_name="podcast", columns=["id", "title"])

fast_pg_insert(
    df=df_segments,
    connection=CONNECTION,
    table_name="podcast_segment",
    columns=["id", "start_time", "end_time", "content", "embedding", "podcast_id"]
)


# HINT: use the recommender.utils.fast_pg_insert function to insert data into the database
# otherwise inserting the 800k documents will take a very, very long time