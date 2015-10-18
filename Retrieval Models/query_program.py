import elasticsearch
import time
import snowballstemmer
import math

es = elasticsearch.Elasticsearch(timeout=100)

def stopwordlist():
    stopwords = []
    f = open("C:/Users/acer14/Desktop/AP89_DATA/AP_DATA/stoplist.txt", "r")
    for word in f:
        word = word.strip()
        stopwords.append(word)
    return stopwords

def querylist():
    list = {}
    count = 0
    f = open("C:/Users/acer14/Desktop/AP89_DATA/AP_DATA/query_desc.51-100.short.txt", "r")
    for line in f:
        try:
            query = line.split(" ",1)
            list[query[0].strip()] = " ".join([i for i in query[1].strip().split(" ") if i not in stopwordlist()])
        except:
            count += 1
    return list

def writetofile(filename, queryno, result):
    rank = 0
    for docno, score in result:
        content = str(queryno) + " Q0 " + str(docno) + " " + str(rank) + " " + str(score) + " Exp \n"
        f = open(filename, "a")
        f.write(content)
        f.close()
        rank += 1
        if rank == 1000:
            break

def okapi_tf(tf, doclen, avgcorpuslength):
    return tf / (tf + 0.5 + (1.5 * (doclen/avgcorpuslength)))

def bm25(totaldoc, totaldocwithterm, tfdoc, doclen, avgdoclen, tfquery):
    k1 = 1.2
    k2 = 100
    b = 0.75
    return (math.log((totaldoc+0.5)/(totaldocwithterm+0.5))) * ((tfdoc + (k1 * tfdoc))/ (tfdoc + (k1 * ((1 - b) + (b * (doclen/avgdoclen)))))) * ((tfquery + (k2 * tfquery)) / (tfquery + k2))

def unigram_laplace(tf, doclen, vocabsize):
    return math.log((tf+1)/(doclen + vocabsize))

def unigram_jelinek(tf, doclen, tfrest, doclenrest, avgdoclen):
    jelineklambda = doclen / (doclen + avgdoclen)
    return math.log((jelineklambda * (tf / doclen)) + ((1 - jelineklambda) * (tfrest/doclenrest)))

if __name__ == "__main__":
    start = time.time()
    indexname = "pavitra"
    doctype = "appdata"
    queries = querylist()
    uniquetermdict = {}
    total_docs_in_corpus = 84679
    avg_doc_length = 164
    total_doc_length = 13933397
    for qNo, query in queries.items():
        searchresult = es.search(index=indexname, doc_type=doctype, size=100000,body={"query": {"query_string": {"query":query}}})
        okapi_tf_scores = {}
        tf_idf_scores = {}
        bm_25_scores = {}
        unigram_laplace_scores = {}
        unigram_jelinek_scores = {}
        stemmer = snowballstemmer.stemmer('english')
        for doc in searchresult["hits"]["hits"]:
            docid = doc["_id"]
            termvectors = es.termvector(doc["_index"], doc["_type"], doc["_id"], fields = "text", term_statistics  = True)
            okapi_tf_d_q = 0
            tf_idf_d_q = 0
            bm_25 = 0
            unigram_laplace_d_q = 0
            unigram_jelinek_d_q = 0
            for term in query.split(" "):
                term = stemmer.stemWord(term)
                tf = termvectors["term_vectors"]["text"]["terms"]
                if docid not in uniquetermdict:
                    doclength = 0
                    for t in tf:
                        doclength = doclength + tf[t]["term_freq"]
                    uniquetermdict[docid] = doclength
                if term in tf:
                    okapi_tf_d_q += okapi_tf(tf[term]["term_freq"], uniquetermdict[docid], avg_doc_length)
                    tf_idf_d_q = okapi_tf_d_q * math.log(total_docs_in_corpus/tf[term]["doc_freq"])
                    bm_25 += bm25(total_docs_in_corpus, tf[term]["doc_freq"],tf[term]["term_freq"], uniquetermdict[docid],
                                  avg_doc_length, query.count(term))
                    unigram_laplace_d_q += unigram_laplace(tf[term]["term_freq"], uniquetermdict[docid], 200000)
                    unigram_jelinek_d_q += unigram_jelinek(tf[term]["term_freq"], uniquetermdict[docid],
                                                            (tf[term]["ttf"] - tf[term]["term_freq"]),
                                                            (total_doc_length - uniquetermdict[docid]), avg_doc_length)
            okapi_tf_scores[docid] = okapi_tf_d_q
            tf_idf_scores[docid] = tf_idf_d_q
            bm_25_scores[docid] = bm_25
            unigram_laplace_scores[docid] = unigram_laplace_d_q
            unigram_jelinek_scores[docid] = unigram_jelinek_d_q
        writetofile("Okapi-tf", qNo, sorted(okapi_tf_scores.items(), key=lambda x:x[1], reverse=True))
        writetofile("tf-idf", qNo, sorted(tf_idf_scores.items(), key=lambda x:x[1], reverse=True))
        writetofile("bm25", qNo, sorted(bm_25_scores.items(), key=lambda x:x[1], reverse=True))
        writetofile("unigram-laplace", qNo, sorted(unigram_laplace_scores.items(), key=lambda x:x[1], reverse=True))
        writetofile("unigram-jelinek", qNo, sorted(unigram_jelinek_scores.items(), key=lambda x:x[1], reverse=True))
        print(query)

print("MAIN: " + str((time.time() - start)))
