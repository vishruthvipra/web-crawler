from elasticsearch import Elasticsearch
from elasticsearch import helpers
from bs4 import BeautifulSoup
import pickle

PICKLEPATH = "/Users/vishruthkrishnaprasad/Downloads/IR/pickle.txt"
OUTPATH1 = "/Users/vishruthkrishnaprasad/Downloads/IR/result1.txt"
OUTPATH2 = "/Users/vishruthkrishnaprasad/Downloads/IR/result2.txt"


INDEX = 'crawler'
DOC_TYPE = 'document'

unloadlinks = {}
with open(PICKLEPATH, 'rb') as handle:
    unloadlinks = pickle.load(handle)


def main():
    global unloadlinks

    es = Elasticsearch()
    f = open(OUTPATH1, "r")
    a = f.read()
    print("i read full file")
    soup = BeautifulSoup(a, 'html.parser')
    fdoc = soup.find_all("doc")
    print("i copied all docs")
    data = []
    for doc in fdoc:
        try:
            docno = doc.find("docno").get_text().replace("\n", "")
        except:
            docno = ""

        try:
            title = doc.find("title").get_text().replace("\n", "")
        except:
            title = ""

        try:
            url = doc.find("url").get_text().replace("\n", "")
        except:
            url = ""

        try:
            depth = doc.find("depth").get_text().replace("\n", "")
        except:
            depth = ""

        try:
            text = doc.find("text").get_text().replace("\n", "")
        except:
            text = ""

        try:
            httpheaders = " ".join(doc.find("httpheaders").get_text().replace("\n", "").replace("{", "").replace("}", "").replace("'","").split(","))
        except:
            httpheaders = ""

        try:
            htmlsource = doc.find("htmlsource").get_text().replace("\n", "")
        except:
            htmlsource = ""

        try:
            outlinks = " ".join(doc.find("outlinks").get_text().replace("\n", "").replace("{","").replace("}","").replace("'", "").split(","))
        except:
            outlinks = ""

        try:
            inlinks = "\n".join(unloadlinks[docno])
        except:
            inlinks = ""

        obj = {
            "DOCNO": docno,
            "TITLE": title,
            "URL": url,
            "DEPTH": depth,
            "AUTHOR": "Vishruth",
            "TEXT": text,
            "HTTPHEADERS": httpheaders,
            "OUTLINKS": outlinks,
            "INLINKS": inlinks,
            "HTMLSOURCE": htmlsource
        }

        data += [{
            "_op_type": 'update',
            "_index": INDEX,
            "_type": DOC_TYPE,
            "_id": docno,
            "doc": obj,
            "doc_as_upsert": True
        }]

    print("started to bulk")
    try:
        helpers.bulk(es, data, index=INDEX, doc_type=DOC_TYPE, request_timeout=30)
    except:
        print("timeout error... changed to infinity")
        helpers.bulk(es, data, index=INDEX, doc_type=DOC_TYPE, request_timeout=float("inf"))

    print("<---------------- Done ---------------->")

if __name__ == '__main__':
    main()
