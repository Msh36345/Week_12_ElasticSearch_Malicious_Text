import uvicorn
from fastapi import FastAPI
from dal import es_instance as es, INDEX_NAME

app = FastAPI()

@app.get("/")
def main_page():
    return ["/antisemitic_with_weapons","/multiple_weapons"]


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

def run():
    uvicorn.run("fast_api:app", host="127.0.0.1", port=8000)
