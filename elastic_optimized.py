from elasticsearch import Elasticsearch
from elasticsearch import helpers
from bs4 import BeautifulSoup
import pickle

PICKLEPATH = "/Users/vishruthkrishnaprasad/Downloads/IR/ASSGN3/pickle.txt"
OUTPATH1 = "/Users/vishruthkrishnaprasad/Downloads/IR/ASSGN3/result1.1.txt"
OUTPATH2 = "/Users/vishruthkrishnaprasad/Downloads/IR/ASSGN3/result2.1.txt"


INDEX = 'crawler'
DOC_TYPE = 'document'

unloadlinks = {}
with open(PICKLEPATH, 'rb') as handle:
    unloadlinks = pickle.load(handle)


def main():
    es = Elasticsearch()
    f = open(OUTPATH2, "r")
    a = f.read()
    f.close()
    print("finshed reading file")
    bulking(a)
    f = open(OUTPATH1, "r")
    b = f.read()
    f.close()
    bulking(b)

def bulking(a):
    global unloadlinks

    output = a.replace("<DOC>", "").replace("</DOC>", "||||")
    documents = output.split("||||")

    data = []
    actions = []
    print(len(documents))
    count = 0
    datacount = 0
    for doc in documents[:len(documents)-1]:
        count += 1
        print(count)
        soup = BeautifulSoup(doc, 'html.parser')
        # fdoc = soup.find_all("doc")
        # print("i copied all docs")

        # print(len(fdoc))
        # for doc in fdoc:
        try:
            docno = soup.find("docno").get_text().replace("\n", "")
        except:
            docno = ""

        try:
            title = soup.find("title").get_text().replace("\n", "")
        except:
            title = ""

        try:
            url = soup.find("url").get_text().replace("\n", "")
        except:
            url = ""

        try:
            depth = soup.find("depth").get_text().replace("\n", "")
        except:
            depth = ""

        try:
            text = soup.find("text").get_text().replace("\n", "")
        except:
            text = ""

        try:
            httpheaders = " ".join(soup.find("httpheaders").get_text().replace("\n", "").replace("{", "").replace("}", "").replace("'","").split(","))
        except:
            httpheaders = ""

        try:
            htmlsource = str(soup.find("htmlsource")).replace("<htmlsource>\n", "").replace("</htmlsource>", "")
        except:
            htmlsource = ""

        try:
            outlinks = " ".join(soup.find("outlinks").get_text().replace("\n", "").replace("{","").replace("}","").replace("'", "").split(","))
        except:
            outlinks = ""

        try:
            inlinks = "\n".join(unloadlinks[docno])
        except:
            inlinks = ""

        insert = {
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

        update = {
            "DOCNO": docno,
            "TITLE": title,
            "URL": url,
            "DEPTH": depth,
            "AUTHOR": "Team",
            "TEXT": text,
            "HTTPHEADERS": httpheaders,
            "OUTLINKS": outlinks,
            "INLINKS": inlinks,
            "HTMLSOURCE": htmlsource
        }

        # data += [{
        #     "_op_type": 'update',
        #     "_index": INDEX,
        #     "_type": DOC_TYPE,
        #     "_id": docno,
        #     "doc": obj,
        #     "doc_as_upsert": True
        # }]"inline": "if (ctx._source.DOCNO == params.DOCNO) { ctx._source.DOCNO=params.DOCNO; } else { ctx._source.DOCNO=params.DOCNO; }",

        action = {
            "_op_type": "update",
            "_index": INDEX,
            "_type": DOC_TYPE,
            "_id": docno,
            "_source": {
                "script": {
                    "inline": "if (ctx._source.DOCNO == params.DOCNO) { ctx._source.DOCNO=params.DOCNO; ctx._source.TITLE=params.TITLE; ctx._source.URL=params.URL; ctx._source.DEPTH=params.DEPTH; ctx._source.AUTHOR=params.AUTHOR; ctx._source.TEXT=params.TEXT; ctx._source.HTTPHEADERS=params.HTTPHEADERS; ctx._source.OUTLINKS=params.OUTLINKS; ctx._source.INLINKS=params.INLINKS; ctx._source.HTMLSOURCE=params.HTMLSOURCE;} else { ctx._source.DOCNO=params.DOCNO; ctx._source.TITLE=params.TITLE; ctx._source.URL=params.URL; ctx._source.DEPTH=params.DEPTH; ctx._source.AUTHOR=params.AUTHOR; ctx._source.TEXT=params.TEXT; ctx._source.HTTPHEADERS=params.HTTPHEADERS; ctx._source.OUTLINKS=params.OUTLINKS; ctx._source.INLINKS=params.INLINKS; ctx._source.HTMLSOURCE=params.HTMLSOURCE;}",
                    "params": {"DOCNO": docno,
                               "TITLE": title,
                               "URL": url,
                               "DEPTH": depth,
                               "AUTHOR": "Team",
                               "TEXT": text,
                               "HTTPHEADERS": httpheaders,
                               "OUTLINKS": outlinks,
                               "INLINKS": inlinks,
                               "HTMLSOURCE": htmlsource}
                },
                "upsert": {"DOCNO": docno,
                           "TITLE": title,
                           "URL": url,
                           "DEPTH": depth,
                           "AUTHOR": "Vishruth",
                           "TEXT": text,
                           "HTTPHEADERS": httpheaders,
                           "OUTLINKS": outlinks,
                           "INLINKS": inlinks,
                           "HTMLSOURCE": htmlsource}
            }
        }
        actions.append(action)

        # data += [{
        #     "_op_type": 'update',
        #     "_index": INDEX,
        #     "_type": DOC_TYPE,
        #     "_id": docno,
        #     "doc": obj,
        #     "doc_as_upsert": True
        # }]
        #
        #
        #


        count += 1
        datacount += 1
        print(count)

        if datacount == 5:
            print("started to bulk")
            try:
                helpers.bulk(es, actions)
                # helpers.bulk(es, data, index=INDEX, doc_type=DOC_TYPE, request_timeout=30)
            except:
                print("timeout error... changed to infinity")
                helpers.bulk(es, actions)
                # helpers.bulk(es, data, index=INDEX, doc_type=DOC_TYPE, request_timeout=float("inf"))
            finally:
                datacount = 0
                data = []
                actions = []


                # try:
                #     helpers.bulk(es, data, index=INDEX, doc_type=DOC_TYPE, request_timeout=30)
                # except:
                #     print("timeout error... changed to infinity")
                #     helpers.bulk(es, data, index=INDEX, doc_type=DOC_TYPE, request_timeout=float("inf"))

    print("started to bulk")
    try:
        helpers.bulk(es, actions)
        # helpers.bulk(es, data, index=INDEX, doc_type=DOC_TYPE, request_timeout=30)
    except:
        print("timeout error... changed to infinity")
        helpers.bulk(es, actions)
        # helpers.bulk(es, data, index=INDEX, doc_type=DOC_TYPE, request_timeout=float("inf"))
    finally:
        datacount = 0
        data = []
    print("<---------------- Done ---------------->")


if __name__ == '__main__':
    main()
