#!/usr/bin/env python3
import json
import os
import csv
from elasticsearch import Elasticsearch, helpers

mapping = {
  "settings": {
    "index": {
      "number_of_shards": 5,
      "number_of_replicas": 2
    },
    "analysis": {
        "filter": {},
        "analyzer": {
            "keyword_analyzer": {
                "filter" :[
                    "lowercase",
                    "asciifolding",
                    "trim"
                ],
                "char_filter": [],
                "type": "custom",
                "tokenizer": "keyword"
            },
            "edge_ngram_analyzer": {
                "filter":  "lowercase",
                "tokenizer": "edge_ngram_tokenizer"
            },
            "edge_ngram_search_analyzer": {
                "tokenizer": "lowercase"
            },
            "tokenizer": {
                "egde_ngram_tokenizer": {
                    "type": "edge_ngram",
                    "min_gram": 2,
                    "max_gram": 5,
                    "token_chars": ["letter"]
                }
            }
        }
    }
  },
    "mappings": {
        "properties": {
          "id": { "type": "integer" },
          "description": {"type": "text"},
          "title" : {
               "type": "text",
               "fields": {
                   "keywordstring": {
                       "type": "text",
                       "analyzer": "keyword_analyzer"
                   },
                   "edgengram": {
                       "type": "text",
                       "analyzer": "edge_ngram_analyzer",
                       "search_analyzer": "edge_n_gram_search_analyzer"
                   },
                   "completion": {
                       "type": "completition"
                   }
               },
               "analyzer": "standard"
               }
          }}
}

def bulk_json_data(dir, _index):
  for file in os.listdir(dir):
    f = os.path.join(dir, file)
    with open(f, 'r') as fi:
      data = json.load(fi)
      for obj in data:
        yield{
          "_index": _index,
          "_id": obj["id"],
          "_source": obj
        }

def get_query_list(file):
  with open(file, "r") as f:
    csv_reader = csv.reader(f)
    next(csv_reader)
    queries = list(csv_reader)
  return queries

def create_out_files(result, file, query):
    for i in range(len(result['hits']['hits'])):
        hit = result['hits']['hits'][i]
        query_id = query[0]
        title = hit['_source']['title']
        doc_id = hit['_id']
        rank = i
        score = hit['_score']
        file.write(str(query_id) + "\t" + str(doc_id) + "\t" + str(rank) + "\t" + str(score) +  "\t" +  title +"\n")

def pagerank_search(es, query, file, index):
    body = {
      "query": {
      "bool": {
        "must": {
          "match": { "title": query[1]}
        },
        "should": {
          "rank_feature": {
            "field": "pagerank", 
            "saturation": {
              "pivot": 10
            }
          }
        }}}
  }
  
    result = es.search(index = index, size=50, body = body)
    create_out_files(result, file, query)
  
def function_score_search(es, query, file, index):
    body = {
      "query":{
          "function_score": {
              "query": {
                  "match":{ "title": { "query": query[1]} },
                  "match":{ "title": { "query": query[1]}}
                  },
                  "boost": "5",
              "functions": [{
                  "filter": { "match": { "title": { "query": query[1]} } },
                  "random_score": {}, 
                  "weight": 40
                  },
                  {
                      "filter": { "match": { "description":  { "query": query[1]} } },
                      "weight": 20
                      }
                      ],
              "max_boost": 42,
              "score_mode": "multiply",
              "boost_mode": "multiply",
          }
        }
    }
    result = es.search(index = index, size=50, body = body)
    create_out_files(result, file, query)

def auto_complete_search(es, query, file, index ):
    body = {
        "query": {
            "prefix": {
                "title.keyword": "Wickl"
            }
        }
    }
    result = es.search(index = index, size=50, body = body)
    create_out_files(result, file, "Wickyl")

if __name__ == "__main__":
  ELASTIC_PASSWORD = "BN6DY0PAr*Igq_dPHNWI"
  index = "autocomplete1"
  es = Elasticsearch(
    "https://localhost:9200",
    ca_certs="/Users/smruthipobbathi/Documents/Spring22/Information Retrieval/Project/elasticsearch/config/certshttp_ca.crt",
    basic_auth=("elastic", ELASTIC_PASSWORD),
    verify_certs=False
    )

#   # create mapping for the index
#   res = es.create(index = index, document=mapping, id= 0)
  
  # bulk api call for indexing
#   response = helpers.bulk(es, bulk_json_data("enwiki20201020", index))

  # create queries list
  queries = get_query_list("query_res_last.csv")
  auto_complete = open('auto_complete_res.txt', 'w')

  page_rank_res = open('page_rank_res.txt', 'w') 
  func_score_res = open('func_score_res', 'w')
  auto_complete_search(es, queries[1], auto_complete, index)

  for query in queries:
    pagerank_search(es, query, page_rank_res, index)
    function_score_search(es, query, func_score_res, index)

  # test call to check indexing
  # getcall = es.get(index=index, id="1294428")
  # print(getcall)

#   trying rank_eval
# es.rank_eval()

