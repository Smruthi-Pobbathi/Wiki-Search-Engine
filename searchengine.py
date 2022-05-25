#!/usr/bin/env python3
import json
from elasticsearch import Elasticsearch, helpers

mapping = {
  "settings": {
    "index": {
      "number_of_shards": 2,
      "number_of_replicas": 2
    },
    "analysis": {
      "analyzer": "standard"
    }
  },
    "mappings": {
        "properties": {
          "id": { "type": "integer" },
          "description": {"type": "text"},
          "title" : { "type": "text" }
          }}
}

def bulk_json_data(file, _index):
  with open(file, 'r') as f:
    data = json.load(f)
    for obj in data:
      yield{
        "_index": _index,
        "_id": obj["id"],
        "_source": obj
      }

if __name__ == "__main__":
  ELASTIC_PASSWORD = "BN6DY0PAr*Igq_dPHNWI"
  index = "wiki_documents"
  es = Elasticsearch(
    "https://localhost:9200",
    ca_certs="/Users/smruthipobbathi/Documents/Spring22/Information Retrieval/Project/elasticsearch/config/certshttp_ca.crt",
    basic_auth=("elastic", ELASTIC_PASSWORD),
    verify_certs=False
    )

  # bulk api call for indexing
  response = helpers.bulk(es, bulk_json_data("test_data.json", index))

  # test call to check indexing
  getcall = es.get(index=index, id="17279752")
  print(getcall)

  