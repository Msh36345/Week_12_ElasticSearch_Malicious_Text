import uvicorn
from fastapi import FastAPI
from services.dal import es_instance as es, INDEX_NAME
import logging

logger = logging.getLogger(__name__)

# This makes a FastAPI app
app = FastAPI()

# This shows the main page with two links
@app.get("/")
def main_page():
    return ["/antisemitic_with_weapons","/multiple_weapons"]


# This checks if all data has "sentiment"
def is_data_processed():
    query = {
        "query": {
            "bool": {
                "must_not": [
                    {"exists": {"field": "sentiment"}}
                ]
            }
        }
    }
    res = es.count(index=INDEX_NAME, body=query)
    return res["count"] == 0

# This shows documents that are antisemitic and have weapons
@app.get("/antisemitic_with_weapons")
def get_antisemitic_with_weapons():
    if not is_data_processed():
        return {"message": "Data not fully processed"}
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"Antisemitic": True}},
                    {"exists": {"field": "weapons_found"}}
                ]
            }
        }
    }
    res = es.search(index=INDEX_NAME, body=query, size=10000)
    hits = res["hits"]["hits"]
    return [doc["_source"] for doc in hits]

# This shows documents with two or more weapons
@app.get("/multiple_weapons")
def get_docs_with_multiple_weapons():
    if not is_data_processed():
        return {"message": "Data not fully processed"}
    query = {
        "query": {
            "range": {
                "weapons_count": {"gte": 2}
            }
        }
    }
    res = es.search(index=INDEX_NAME, body=query, size=10000)
    hits = res["hits"]["hits"]
    return [doc["_source"] for doc in hits]

# This runs the FastAPI app
def run():
    uvicorn.run(app, host="0.0.0.0", port=8000)