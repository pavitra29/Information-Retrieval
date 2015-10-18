from bs4 import BeautifulSoup as BS
import elasticsearch
from elasticsearch import helpers
import os

es = elasticsearch.Elasticsearch()

def index(indexname, typename, docs):
    doc_array = []
    count = 0
    for doc in docs:
        docno = doc.find("docno").get_text().strip()
        count += 1
        doc_array.append({
            "_index": indexname,
            "_type": typename,
            "_id": docno,
            "_source": {"docno": docno,
                         "text": appendalltexts(doc)}
        })
    helpers.bulk(es, doc_array)
    return count

def appendalltexts(element):
    text = ""
    for e in element.find_all("text"):
        text += e.get_text().strip()
    return text

def parsealldocs(directory, indexname, typename):
    total = 0
    for f in os.listdir(directory):
        location = os.path.join(directory,f)
        content = open(location, "r").read()
        soup = BS(content)
        total = total + index(indexname, typename, soup.find_all("doc"))
    print("No of docs in corpus: ",total)

def createindex(indexname, doctype):
    if es.indices.exists(indexname):
        print("deleting '%s' index..." % (indexname))
    print(es.indices.delete(index=indexname, ignore=[400, 404]))
    print("creating '%s' index..." % (indexname))
    print(es.indices.create(index=indexname,
                            body={
                                "settings": {
                                    "index": {
                                        "store": {
                                            "type": "default"
                                        },
                                        "number_of_shards": 5,
                                        "number_of_replicas": 1
                                    },
                                    "analysis": {
                                        "analyzer": {
                                            "my_analyzer": {
                                                "type": "english",
                                                "stopwords_path": "stoplist.txt"
                                            }
                                        }
                                    }
                                },
                                "mappings": {
                                    doctype:  {
                                        "properties": {
                                            "docno": {
                                                "type": "string",
                                                "store": True,
                                                "index": "not_analyzed"
                                            },
                                            "text": {
                                                "type": "string",
                                                "store": True,
                                                "term_vector": "with_positions_offsets_payloads",
                                                "analyzer": "my_analyzer"
                                            }
                                        }
                                    }

                                }

                            }))



def getparameters(indexname, doctype):
    result = es.search(index = indexname, doc_type = doctype, size = 1,
                             body = {
                                 "facets": {
                                     "text": {
                                         "statistical": {
                                             "script": "doc['text'].values.size()"
                                         }
                                     }
                                 }
                             })

    facet = result["facets"]["text"]
    totaldoc = facet["count"]
    avgdoclength = facet["total"]/totaldoc
    print("Total Doc Length: ", facet["total"])
    print("Average Doc Length: ", avgdoclength)
    return {"avgdoclength": avgdoclength, "totaldocLen": facet["total"]}

if __name__ == "__main__":
    createindex("pavitra", "appdata")
    parsealldocs("C:/Users/acer14/Desktop/AP89_DATA/AP_DATA/ap89_collection", "pavitra", "appdata")
    docinfo = getparameters("pavitra","appdata")