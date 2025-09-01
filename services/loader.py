import pandas as pd
from dal import es_instance as es, INDEX_NAME

mapping = {
    "mappings": {
        "properties": {
            "TweetID": {"type": "keyword"},
            "CreateDate": {"type": "date"},
            "Antisemitic": {"type": "boolean"},
            "text": {"type": "text"},
            "weapons_found": {"type": "keyword"},
            "sentiment": {"type": "keyword"},
            "weapons_count": {"type": "integer"}
           }
       }
    }

def reset_index():
    if es.indices.exists(index=INDEX_NAME):
        es.indices.delete(index=INDEX_NAME)
        print(f"Index '{INDEX_NAME}' deleted.")
    es.indices.create(index=INDEX_NAME, body=mapping)
    print(f"---Index '{INDEX_NAME}' created with mapping.---\n")

def load_data_to_elastic(file_path):
    df = pd.read_csv(file_path)
    inserted = 0
    for i, row in enumerate(df.to_dict(orient="records")):
        row["CreateDate"] = pd.to_datetime(row["CreateDate"]).isoformat()
        row["Antisemitic"] = row["Antisemitic"]==1
        es.index(index=INDEX_NAME, document=row)
        inserted += 1
        if inserted%1000==0:
            print(f"Inserted {inserted} documents")
    print(f"---Inserted {inserted} documents----\n")
    es.indices.refresh(index=INDEX_NAME)