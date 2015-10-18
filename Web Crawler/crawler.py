from urllib.parse import urlparse, urlunparse, urldefrag
from urllib.request import urlopen
import urllib.robotparser
from urllib.error import URLError
from bs4 import BeautifulSoup, Comment
import time
import logging
import heapq
from elasticsearch import Elasticsearch

logging.basicConfig(level=logging.ERROR)

class Crawler:
    def crawl(self):

        givenSeeds = ["http://en.wikipedia.org/wiki/American_Revolutionary_War",
                      "http://www.history.com/topics/american-revolution/american-revolution-history",
                      "http://www.revolutionary-war.net/causes-of-the-american-revolution.html",
                      "http://en.wikipedia.org/wiki/American_Revolution"]
        totalCrawled = 0
        frontier = Frontier()
        for i in range(0,len(givenSeeds)):
            obj = Link(i+1,givenSeeds[i])
            frontier.put(obj)
        no = 5
        counter = 0
        documentES = IndexES("crawler_index", "crawler_doc")
        documentES.create_index()
        while True:
            link = frontier.get()
            if link is not None:
                try:
                    isSuccess = robot(link.url)
                except:
                    isSuccess = False
                    print("robot url error"+link.url)
                if isSuccess is True:
                    if link is None:
                        break
                    try:
                        text,doc,page = cleanHTMl(link.url)
                        urls = getURL(link.url)
                        if "american" in str(page).lower() and "independence" in str(page).lower():
                            outlinks=[]
                            for canonizedURL in urls:
                                if canonizedURL != "" and not canonizedURL.startswith("#") and canonizedURL not in outlinks:
                                    url = canonizedURL
                                    newLink = Link(no, url, link.url)
                                    outlinks.append(url)
                                    if newLink.url == link.url:
                                        if newLink.url not in link.in_links:
                                            link.in_links.append(newLink.url)

                                    if not frontier.exists(newLink):
                                        if documentES.document_exists(newLink.url):
                                            documentES.update_inlinks(newLink.url, link.url)
                                            frontier.put(newLink)
                                            no += 1
                                        else:
                                            frontier.add_in_link_to_doc(newLink.url, link.url)

                            counter += 1
                            documentES.index_doc(link.url.lower(), link.url, link.in_links, outlinks, page, text)
                            totalCrawled += 1
                            print(str(totalCrawled) + ":" + str(link.seq_no) + "->" + link.url)

                            if counter > 12000:
                                break

                            time.sleep(1)

                    except URLError:
                        print("Error connecting to URL -> " + link.url)
                    except:
                        print("Error occurred while processing URL -> " + link.url + " ")

class Frontier:
    def __init__(self):
        self.frontier = []

    def put(self, obj):
        heapq.heappush(self.frontier, obj)

    def add_in_link_to_doc(self, url, in_link):
        for doc in self.frontier:
            if doc.url == url:
                if in_link not in doc.in_links:
                    doc.in_links.append(in_link)
                    heapq.heapify(self.frontier)
                    return None

    def exists(self, item):
        return item in (x for x in self.frontier)

    def get(self):
        if len(self.frontier) > 0:
            return heapq.heappop(self.frontier)
        else:
            return None

class Link:

    def __init__(self, seq_no, urlVal, new_in_link=None):
        self.__seq_no = seq_no
        self.__url = urlVal
        self.__in_links = []
        if new_in_link is not None:
            self.__in_links.append(new_in_link)
        self.__out_links = []

    @property
    def url(self):
        return self.__url

    @url.setter
    def url(self, urlVal):
        self.__url = urlVal

    @property
    def in_links(self):
        return self.__in_links

    @in_links.setter
    def in_links(self, in_link):
        self.__in_links.append(in_link)

    @property
    def out_links(self):
        return self.__out_links

    @out_links.setter
    def out_links(self, out_link):
        self.__out_links.append(out_link)

    @property
    def seq_no(self):
        return self.__seq_no

    @seq_no.setter
    def seq_no(self, num):
        self.__seq_no = num

    def __eq__(self, other):
        return self.url == other.url

    def __hash__(self):
        return hash(self.url)

    def __lt__(self, other):
        if len(self.in_links) == len(other.in_links):
            return self.seq_no < other.seq_no
        else:
            return len(self.in_links) > len(other.in_links)

    def __str__(self):
        return "_seq_no:" + str(self.seq_no) + ", _url:" + self.url + ", _in_links:" + str(self.in_links) + \
               ", out_links" + str(self.out_links)

    __repr__ = __str__

class IndexES:
    es = Elasticsearch()

    def __init__(self, indexName, docType):
        self.indexName = indexName
        self.docType = docType

    def create_index(self):
        IndexES.es.indices.delete(self.indexName, ignore=404)
        IndexES.es.indices.create(index=self.indexName,
                                        body={
                                            "settings": {
                                                "index": {
                                                    "store": {
                                                        "type": "default"
                                                    },
                                                    "number_of_shards": 5,
                                                    "number_of_replicas": 1
                                                }
                                            },
                                            "mappings": {
                                                self.docType: {
                                                    "properties": {
                                                        "id": {
                                                            "type": "string",
                                                            "store": True,
                                                            "index": "not_analyzed"
                                                        },
                                                        "url": {
                                                            "type": "string",
                                                            "store": True,
                                                            "index": "not_analyzed"
                                                        },
                                                        "in_links": {
                                                            "type": "string",
                                                            "store": True,
                                                            "index": "not_analyzed"
                                                        },
                                                        "out_links": {
                                                            "type" : "string",
                                                            "store": True,
                                                            "index": "not_analyzed"
                                                        },
                                                        "raw_html": {
                                                            "type": "string",
                                                            "store": True,
                                                            "index": "not_analyzed"
                                                        },
                                                        "clean_text": {
                                                            "type": "string",
                                                            "store": True,
                                                            "term_vector": "with_positions_offsets_payloads"
                                                        }
                                                    }
                                                }
                                            }
                                        })

    def index_doc(self, id, url, inlinks, outlinks, html, text):
        IndexES.es.index(index=self.indexName, doc_type=self.docType, id=id,
                             body={
                                "url": url,
                                "in_links": inlinks,
                                "out_links": outlinks,
                                "raw_html": html,
                                "clean_text": text
                             })

    def document_exists(self, docId):
        return IndexES.es.exists(index=self.indexName, doc_type=self.docType, id=docId)

    def update_inlinks(self, docId, url):
        doc = IndexES.es.get(index=self.indexName, doc_type=self.docType, id=docId)
        inlinks = doc["_source"]["in_links"]
        if url not in inlinks:
            inlinks.append(url)
            self.index_doc(docId, docId, inlinks, doc["_source"]["out_links"], doc["_source"]["raw_html"], doc["_source"]["clean_text"])

def cleanHTMl(url):
    html = urlopen(url).read()
    soup = BeautifulSoup(html)
    for script in soup(["script", "style","li"]):
        script.extract()
    comments = soup.findAll(text=lambda text:isinstance(text, Comment))
    [comment.extract() for comment in comments]
    return soup.find("body").text,soup,html

def getURL(url):
    page = urllib.request.urlopen(url)
    text = page.read()
    page.close()
    soup = BeautifulSoup(text)
    urls = []
    for tag in soup.findAll('a', href=True):
        urls.append(canonize(url,tag['href']))
    return urls

def robot(seedURL):
    rp = urllib.robotparser.RobotFileParser()
    h = urlparse(seedURL)
    if h.scheme is '':
        host = urlunparse(("http",h.netloc,"","","",""))
    else:
        host = urlunparse((h.scheme,h.netloc,"","","",""))
    roboturl = host + "/robots.txt"
    rp.set_url(roboturl)
    rp.read()
    return (rp.can_fetch("*", seedURL))

def canonize(main,url):
    u = urlparse(url)
    parse = urlparse(main)
    main = parse.netloc
    if u.netloc is '':
        url = parse.scheme + "://" + main + url
    u = urlparse(url)
    url = (urlunparse((str(u.scheme).lower(),str(u.netloc).lower(),u.path, u.params,u.query,u.fragment)))
    v = urlparse(url)
    if v.port != '':
        netloc = urlparse(url).netloc
        netloc = netloc.split(':')[0]
        url = urlunparse((v.scheme,netloc,v.path,v.params,v.query,v.fragment))
    url = urldefrag(url)[0]
    w = urlparse(url)
    url = urlunparse((w.scheme,w.netloc,urlparse(url).path.replace('//', '/'),w.params,w.query,w.fragment))
    return url

def main():
    start = time.time()
    crawler = Crawler()
    crawler.crawl()
    print(time.time() - start)

if __name__ == "__main__":
    main()
