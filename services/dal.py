from elasticsearch import Elasticsearch
import time

ES_HOST = "http://localhost:9200"
INDEX_NAME = "tweets"

class ElasticDAL:
    def __init__(self, host= ES_HOST):
        self.es = Elasticsearch(host)
        self._wait_until_ready()

    def _wait_until_ready(self):
        while not self.es.ping():
            print("Waiting for Elasticsearch...")
            time.sleep(1)
        print("---Elasticsearch is up!---\n")

    def get_client(self):
        return self.es

es_instance = ElasticDAL().get_client()