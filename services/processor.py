from elasticsearch import helpers
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from dal import es_instance as es, INDEX_NAME

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


def process_weapons_batch(batch_size=1000):
    count = 0
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


def delete_safe_documents(batch_size=1000):
    count = 0
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




def _identify_sentiment(text):
    score = SentimentIntensityAnalyzer().polarity_scores(text)
    num_score=score["compound"]
    if num_score>=0.5:
        return "positive"
    if num_score<=0.5:
        return "negative"
    else:
        return "neutral"

def _detect_weapons(text):
    words = {word.strip().lower() for word in text.split()}
    found = words & _load_weapons()
    return list(found)

def _load_weapons():
    weapons_file = "../data/weapons.txt"
    with open(weapons_file, "r") as f:
        return set([w.strip().lower() for w in f.readlines()])