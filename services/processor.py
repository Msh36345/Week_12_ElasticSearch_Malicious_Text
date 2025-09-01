import os
from elasticsearch import helpers
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from services.dal import es_instance as es, INDEX_NAME

# This function adds "sentiment" to documents
def process_sentiment_batch(batch_size=1000):
    count = 0
    query = {
        "size": batch_size,
        "query": {
            "bool": {
                "must_not": {
                    "exists": {
                        "field": "sentiment"
                    }
                }
            }
        }
    }

    # Get documents from Elasticsearch
    scroll = es.search(index=INDEX_NAME, body=query, scroll="1m")
    sid = scroll['_scroll_id']
    hits = scroll['hits']['hits']

    while hits:
        actions = []
        for doc in hits:
            doc_id = doc["_id"]
            text = doc["_source"]["text"]
            sentiment = _identify_sentiment(text)

            actions.append({
                "_op_type": "update",
                "_index": INDEX_NAME,
                "_id": doc_id,
                "doc": {
                    "sentiment": sentiment
                }
            })

        if actions:
            helpers.bulk(es, actions)
            print(f"Processed {len(actions)} docs.")
            count+=len(actions)

        scroll = es.scroll(scroll_id=sid, scroll="1m")
        sid = scroll['_scroll_id']
        hits = scroll['hits']['hits']
    print(f"---Processed {count} docs.---\n")


# This function adds "weapons_found" and "weapons_count" to documents
def process_weapons_batch(batch_size=1000):
    count = 0
    # Find documents without "weapons_found"
    query = {
        "size": batch_size,
        "query": {
            "bool": {
                "must_not": {
                    "exists": {
                        "field": "weapons_found"
                    }
                }
            }
        }
    }

    # Get documents from Elasticsearch
    scroll = es.search(index=INDEX_NAME, body=query, scroll="1m")
    sid = scroll['_scroll_id']
    hits = scroll['hits']['hits']

    while hits:
        actions = []
        for doc in hits:
            doc_id = doc["_id"]
            text = doc["_source"]["text"]
            found_weapons = _detect_weapons(text)
            if found_weapons:
                actions.append({
                    "_op_type": "update",
                    "_index": INDEX_NAME,
                    "_id": doc_id,
                    "doc": {
                        "weapons_found": found_weapons,
                        "weapons_count": len(found_weapons)
                    }
                })

        if actions:
            helpers.bulk(es, actions)
            print(f"Processed {len(actions)} documents with weapon detection.")
            count+=len(actions)

        scroll = es.scroll(scroll_id=sid, scroll="1m")
        sid = scroll['_scroll_id']
        hits = scroll['hits']['hits']
    print(f"---Processed {count} documents with weapon detection.---\n")


# This function deletes safe documents
def delete_safe_documents(batch_size=1000):
    count = 0
    # Find documents that are safe
    query = {
        "size": batch_size,
        "query": {
            "bool": {
                "must": [
                    {
                        "bool": {
                            "should": [
                                {"term": {"sentiment": "neutral"}},
                                {"term": {"sentiment": "positive"}}
                            ]
                        }
                    },
                    {
                        "bool": {
                            "should": [
                                {"bool": {"must_not": {"exists": {"field": "weapons_found"}}}},
                                {"term": {"weapons_count": 0}}
                            ]
                        }
                    },
                    {
                        "bool": {
                            "should": [
                                {"term": {"Antisemitic": False}},
                                {"bool": {"must_not": {"exists": {"field": "Antisemitic"}}}}
                            ]
                        }
                    }
                ]
            }
        }
    }

    # Get documents from Elasticsearch
    scroll = es.search(index=INDEX_NAME, body=query, scroll="1m")
    sid = scroll['_scroll_id']
    hits = scroll['hits']['hits']

    while hits:
        actions = [
            {
                "_op_type": "delete",
                "_index": INDEX_NAME,
                "_id": doc["_id"]
            }
            for doc in hits
        ]

        if actions:
            helpers.bulk(es, actions)
            print(f"Deleted {len(actions)} safe documents.")
            count+=len(actions)


        scroll = es.scroll(scroll_id=sid, scroll="1m")
        sid = scroll['_scroll_id']
        hits = scroll['hits']['hits']
    print(f"---Deleted {count} safe documents.---\n")




# This function finds if text is positive, negative, or neutral
def _identify_sentiment(text):
    score = SentimentIntensityAnalyzer().polarity_scores(text)
    num_score=score["compound"]
    if num_score >= 0.5:
        return "positive"
    elif num_score <= -0.5:
        return "negative"
    else:
        return "neutral"

# This function finds weapons in the text
def _detect_weapons(text):
    words = {word.strip().lower() for word in text.split()}
    found = words & weapons
    return list(found)

# This function loads the weapons from a file
def _load_weapons():
    weapons_file = os.environ.get("WEAPONS_FILE", "data/weapons.txt")
    with open(weapons_file, "r") as f:
        return set([w.strip().lower() for w in f.readlines()])

weapons = _load_weapons()