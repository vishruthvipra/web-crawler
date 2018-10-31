from urllib.parse import urlparse
import urllib.robotparser
import subprocess
import time
import operator
import urllib.request
from bs4 import BeautifulSoup
import urllib.robotparser
import os
import pickle

# from subprocess import Popen, PIPE, STDOUT

PICKLEPATH = "/Users/vishruthkrishnaprasad/Downloads/IR/ASSGN3/pickle.txt"
PICKLEPATH2 = "/Users/vishruthkrishnaprasad/Downloads/IR/ASSGN3/crash.txt"
PICKLEPATH3 = "/Users/vishruthkrishnaprasad/Downloads/IR/ASSGN3/frontier.txt"
PICKLEPATH4 = "/Users/vishruthkrishnaprasad/Downloads/IR/ASSGN3/p.txt"
OUTPATH1 = "/Users/vishruthkrishnaprasad/Downloads/IR/ASSGN3/result1.1.txt"
OUTPATH2 = "/Users/vishruthkrishnaprasad/Downloads/IR/ASSGN3/result2.1.txt"
DURATIONPATH = "/Users/vishruthkrishnaprasad/Downloads/IR/ASSGN3/timetaken.txt"

linkfetch = {}
inlinks = {}
priority = {}
p = {}

visitedlinks = set()

urlinfo = []
nextwave = []

count = 0
basketball_count = 0
write_count = 0
wave = 0

seed1 = "http://www.basketball-reference.com/awards/nba_50_greatest.html"
seed2 = "http://www.basketball-reference.com/leaders/per_career.html"
seed3 = "http://en.wikipedia.org/wiki/Michael_Jordan"
seed4 = "http://www.biography.com/people/michael-jordan-9358066"

frontier = [seed3, seed4, seed1, seed2]
skip = {".jpg", ".jpeg", ".pdf", ".aspx", ".php", "cgi", "web.archive.org", "ticket", "ad.", "bit.ly",
        "http://www.basketball-reference.com/teams/"}
keywords = ["jordan", "bulls", "wizards",
            "cassesa01", "teaguje01", "stackje01", "rosede01", "stoudam01",
            "mcadobo01", "cowenda01", "lehmage01", "westbru01", "pricema01",
            "mutomdi01", "hardaan01", "woolror01", "smithal01", "laniebo01",
            "blackro01", "bellawa01", "marbust01", "jonesst01", "foustla01",
            "galemi01", "taylofa01", "howelba01", "barklch01", "evansty01",
            "barnema01", "freemdo01", "goodrga01", "johnsst01", "jonesla01",
            "isselda01", "millebr01", "robbire01", "mingya01", "architi01",
            "garneke01", "westda01", "mahafra01", "warrebo01", "silasja01",
            "youngth01", "reedwi01", "martike02", "willigu01", "hamiljo01",
            "drewjo01", "nattca01", "wadedw01", "calvima01", "mcdyean01",
            "busedo01", "grangda01", "hawkico01", "hardeja01", "boozeca01",
            "cousybo01", "wilkido01", "marshdo01", "moedo01", "englial01",
            "reddmi01", "leonaka01", "bayloel01", "shortpu01", "bryanko01",
            "okurme01", "leeda02", "rondora01", "powelci01", "beatyze01",
            "roundda01", "dandrbo01", "millspa01", "beasljo01", "dentora01",
            "robincl01", "johnsne01", "monrogr01", "lovelcl01", "kingbe01",
            "mullich01", "francst01", "hargeir01", "hornaje01", "willilo02",
            "johnsgu01", "briskjo01", "horfoal01", "wilkeja01", "hairsha01",
            "gilmoar01", "brownro01", "rochejo01", "derozde01", "aldrila01",
            "malonmo01", "dantlad01", "loveke01", "schaydo01", "chambto01",
            "howardw01", "colemde01", "stricro02", "paytoga01", "washitr01",
            "westppa01", "birdla01", "carrojo01", "greenjo01", "hammoju01",
            "neumajo01", "knighbi01", "jabalwa01", "hilarne01", "moorege01",
            "olajuha01", "kukocto01", "sharmbi01", "haywosp01", "adamsmi01",
            "abdulka01", "eakinji01", "parketo01", "owensto01", "millela01",
            "hamilri01", "ligongo01", "roberma01", "combsgl01", "haywago01",
            "gasolma01", "davisle01", "duncati01", "pettibo01", "lowryky01",
            "kenonla01", "monroea01", "hudsolo01", "couside01", "piercri01",
            "portete01", "paulch01", "wisewi01", "billuch01", "mcclate01",
            "anderke01", "jamisan01", "willich01", "jonesji01", "willich02",
            "pippesc01", "hayesel01", "robingl01", "lewisfr02", "webbech01",
            "richami01", "griffbl01", "sidledo01", "thurmna01", "walkeke02",
            "lawsoty01", "roberos01", "cunnibi01", "jonesbo01", "odomla01",
            "carrida01", "beckby01", "jordami01", "lewismi01", "ervinju01",
            "ewingpa01", "smitsri01", "barroda01", "natersw01", "millere01",
            "willide01", "govange01", "leaksma01", "thompge01", "tomjaru01",
            "johnske02", "brandte01", "freewo01", "riverdo01", "cambyma01",
            "stojape01", "smithra01", "leverfa01", "cartege01", "yardlge01",
            "caldwjo01", "nancela01", "thompkl01", "huntele01", "thompda01",
            "kiddja01", "ibakase01", "duranke01", "robinda01", "nashst01",
            "kempsh01", "theusre01", "simpsra01", "jeffeal01", "lewisra02",
            "hillmda01", "wilkele01", "tartle01", "divacvl01", "bingda01",
            "ladnewe01", "jonessa01", "mikkeve01", "netolbo01", "willira01",
            "jonesca01", "boonero01", "onealsh01", "okafoem01", "jonesed02",
            "keyeju01", "mannida01", "heinsto01", "davisba01", "mariosh01",
            "smithjo03", "barryri01", "havlijo01", "abdursh01", "hagancl01",
            "curryst01", "ginobma01", "lopezbr01", "aguirma01", "danieme01",
            "arizipa01", "walkech01", "howarjo01", "parisro01", "mitchmi01",
            "franzro01", "kerrre01", "vergabo01", "gilliar01", "maravpe01",
            "hightwa01", "stockjo01", "robisda01", "macaued01", "walljo01",
            "nowitdi01", "conlemi01", "littlge01", "mcgratr01", "millean02",
            "mcginge01", "simonwa01", "adamsal01", "roberal01", "patteru01",
            "brownfr01", "russebi01", "wrighlo01", "gortama01", "thomais01",
            "boshch01", "richmmi01", "noahjo01", "beckear01", "daviswa02",
            "cummite01", "kirilan01", "schrede01", "arenagi01", "melchbi01",
            "daviswa03", "mournal01", "murphca01", "iversal01", "fraziwa01",
            "ilgauzy01", "hillgr01", "daughbr01", "taylobr01", "piercpa01",
            "cartevi01", "laettch01", "blaylmo01", "joneswi02", "birdsot01",
            "maggeco01", "drexlcl01", "anthoca01", "kellebi01", "moncrsi01",
            "chambwi01", "malonka01", "gallaha01", "allenra02", "worthja01",
            "felixra01", "vanhoke01", "johnsmi01", "hardati01", "wallara01",
            "gervige01", "johnsma01", "randoza01", "dampilo01", "onealje01",
            "johnsma02", "paultbi01", "jordade01", "gayru01", "jonesri01",
            "dragigo01", "bogutan01", "twymaja01", "gueriri01", "cheekma01",
            "jamesle01", "mchalke01", "brownla01", "vandeki01", "brandel01",
            "lucasje01", "harride01", "westje01", "sikmaja01", "gasolpa01"]
important = ["jordan", "bulls", "wizards"]


def main():
    global inlinks
    global visitedlinks
    global frontier
    global p
    startTime = time.time()

    startCrawling()
    endTime = time.time()

    print(startTime, endTime)
    duration = endTime - startTime
    print(duration)

    f = open(DURATIONPATH, "w")
    f.write("Start: " + str(startTime) + "\nFinish: " + str(endTime) + "\nDuration: " + str(duration))
    f.close()

    with open(PICKLEPATH, 'wb') as handle:
        pickle.dump(inlinks, handle, protocol=pickle.HIGHEST_PROTOCOL)

    with open(PICKLEPATH2, 'wb') as handle:
        pickle.dump(visitedlinks, handle, protocol=pickle.HIGHEST_PROTOCOL)

    with open(PICKLEPATH3, 'wb') as handle:
        pickle.dump(set(frontier), handle, protocol=pickle.HIGHEST_PROTOCOL)

    with open(PICKLEPATH4, 'wb') as handle:
        list_key_value = [[k, v] for k, v in p.items()]
        pickle.dump(list_key_value, handle, protocol=pickle.HIGHEST_PROTOCOL)



        # unloadlinks = {}
        # with open(PICKLEPATH, 'rb') as handle:
        #     unloadlinks = pickle.load(handle)
        #
        # for link in unloadlinks:
        #     print(link)


def startCrawling():
    global count
    global frontier
    global nextwave
    global write_count
    global wave
    global skip

    while count <= 23000: #21500
        if wave >= 20000: #20000
            return
        for link in frontier:
            flag = 0
            if count >= 5000:
                skip.add("basketball-reference")
            for skiplink in skip:
                if skiplink in link:
                    flag = 1
                    break
            if flag is 1:
                continue
            else:
                if write_count >= 100: #100
                    writeToFile()
                    write_count = 0

                if count > 23000: #20200
                    return

                print(link)
                getUrlInfo(link)
                count += 1
                write_count += 1
                print("Wave: ", wave, " Count: ", count)

        updateNextWave()
        frontier = nextwave
        nextwave = []
        wave += 1
    return


def writeToFile():
    global urlinfo
    global count

    if count < 10000:
        if not os.path.exists(OUTPATH2):
            f = open(OUTPATH2, "w")
        else:
            f = open(OUTPATH2, "a")

        try:
            for item in urlinfo:
                f.write("<DOC>")
                f.write("\n")
                f.write("<DOCNO>\n" + str(item["doc_id"]) + "</DOCNO>")
                f.write("\n")
                f.write("<TITLE>\n" + str(item["title"]) + "</TITLE>")
                f.write("\n")
                f.write("<URL>\n" + str(item["doc_id"]) + "</URL>")
                f.write("\n")
                f.write("<DEPTH>\n" + str(item["depth"]) + "</DEPTH>")
                f.write("\n")
                f.write("<TEXT>\n" + str(item["text"]) + "</TEXT>")
                f.write("\n")
                f.write("<HTTPHEADERS>\n" + str(item["header"]).strip() + "</HTTPHEADERS>")
                f.write("\n")
                f.write("<OUTLINKS>\n" + str(item["outlinks"]).strip() + "</OUTLINKS>")
                f.write("<HTMLSOURCE>\n" + str(item["html"]).strip() + "</HTMLSOURCE>\n")
                f.write("\n")
                f.write("</DOC>\n")
        except:
            print("Some writing issue")
            f.close()
            urlinfo = []
        else:
            f.close()
            urlinfo = []
    else:
        if not os.path.exists(OUTPATH1):
            f = open(OUTPATH1, "w")
        else:
            f = open(OUTPATH1, "a")

        try:
            for item in urlinfo:
                f.write("<DOC>")
                f.write("\n")
                f.write("<DOCNO>\n" + str(item["doc_id"]) + "</DOCNO>")
                f.write("\n")
                f.write("<TITLE>\n" + str(item["title"]) + "</TITLE>")
                f.write("\n")
                f.write("<URL>\n" + str(item["doc_id"]) + "</URL>")
                f.write("\n")
                f.write("<DEPTH>\n" + str(item["depth"]) + "</DEPTH>")
                f.write("\n")
                f.write("<TEXT>\n" + str(item["text"]) + "</TEXT>")
                f.write("\n")
                f.write("<HTTPHEADERS>\n" + str(item["header"]).strip() + "</HTTPHEADERS>")
                f.write("\n")
                f.write("<OUTLINKS>\n" + str(item["outlinks"]).strip() + "</OUTLINKS>")
                f.write("<HTMLSOURCE>\n" + str(item["html"]).strip() + "</HTMLSOURCE>\n")
                f.write("\n")
                f.write("</DOC>\n")
        except:
            print("Some writing issue")
            f.close()
            urlinfo = []
        else:
            f.close()
            urlinfo = []


def updateNextWave():
    global nextwave
    global p

    try:
        sortedp = dict(sorted(p.items(), key=operator.itemgetter(1)))
        nextwave = sortedp.keys()
        p = {}
        sortedp = {}
    except:
        print("Could not empty p and load next wave")


def getUrlInfo(link):
    global visitedlinks
    global count
    global write_count

    httplink = link.replace("https", "http").lower()

    if httplink in visitedlinks:
        count -= 1
        write_count -= 1
        return
    elif canICrawl(link):
        crawl(link)
        visitedlinks.add(httplink)
        return
    else:
        visitedlinks.add(httplink)
        return


def canICrawl(link):
    global linkfetch
    global count
    global write_count

    if "ticket" in link:
        count -= 1
        write_count -= 1
        return False
    try:
        rp = urllib.robotparser.RobotFileParser()
        o = urlparse(link)
        domain = o.scheme + "://" + o.netloc
        robotlink = domain + "/robots.txt"
        rp.set_url(robotlink)
        rp.read()

        if (rp.can_fetch("*", robotlink)):
            if domain not in linkfetch:
                currenttime = time.time()
                linkfetch[domain] = currenttime
                return True
            else:
                delaytime = rp.crawl_delay(robotlink)
                if delaytime is None:
                    delaytime = 0.75
                if delaytime > 4:
                    return False
                currenttime = time.time()
                diff = currenttime - linkfetch[domain]
                if diff < delaytime:
                    time.sleep(delaytime - diff)
                    print("Slept: ", delaytime - diff)
                    linkfetch[domain] = currenttime
                    return True
                else:
                    print("Did not Sleep")
                    linkfetch[domain] = currenttime
                    return True
        else:
            count -= 1
            write_count -= 1
            print("Cannot crawl")
            return False
    except:
        count -= 1
        write_count -= 1
        print("Cannot crawl error")
        return False


def crawl(link):
    global urlinfo
    global count
    global write_count
    global wave

    try:
        u = urllib.request.urlopen(link)
        html_doc = u.read()
        headertag = dict(u.getheaders())

        if "text/html" not in headertag["Content-Type"]:
            return

        soup = BeautifulSoup(html_doc, 'html.parser')
        for script in soup(["script", "style"]):
            script.extract()

        clean_text = " ".join(soup.body.get_text().split())

        lang = ""

        for temp in soup.find_all('html'):
            lang = temp.get('lang')

        if lang is not None and "en" not in lang:
            count -= 1
            write_count -= 1
            return

        outlinks = set()
        for olink in soup.find_all('a'):
            alink = olink.get('href')
            if alink != None:
                outerlink = alink.lower()
                compareouterlink = alink
                flag = 0
                for skiplink in skip:
                    if skiplink in outerlink:
                        flag = 1

                if flag == 1 or "cite" in outerlink:
                    continue
                else:
                    outlinks.add(compareouterlink)
    except:
        count -= 1
        write_count -= 1
        print("Error url read")
        return
    try:
        urlinfo.append({
            "doc_id": link,
            "title": soup.title.string,
            "depth": wave,
            "header": headertag,
            "text": clean_text,
            "html": soup,
            "outlinks": set()
        })
    except:
        urlinfo.append({
            "doc_id": link,
            "title": link,
            "depth": wave,
            "header": headertag,
            "text": clean_text,
            "html": soup,
            "outlinks": set()
        })

    try:
        o = urlparse(link)
        domains = o.scheme + "://" + o.netloc
        domain = domains.lower()

        for compareouterlink in outlinks.copy():
            indicator = 0
            outlink = compareouterlink.lower()
            if "wikipedia" in domain:
                for word in important:
                    if word in domain or word in outlink:
                        indicator = 1
                        break
                if indicator == 0:
                    outlinks.discard(compareouterlink)

            else:
                for word in keywords:
                    if word in domain or word in outlink:
                        indicator = 1
                        break
                if indicator == 0:
                    outlinks.discard(compareouterlink)

        priortize(link, outlinks)
        outlinks = set()
    except:
        print("Prioritize issue")
        return


def priortize(link, outlinks):
    global basketball_count
    global skip
    global inlinks
    global p
    global priority
    global keywords
    global count

    o = urlparse(link)
    domains = o.scheme + "://" + o.netloc
    domain = domains.lower()
    tempoutlinks = [domain]
    tempoutlinks.extend(outlinks)
    output = list()

    try:
        testing = " ".join(tempoutlinks)
        output = canonicalize(testing)
    except Exception as e:
        print("error is:", e)

    for result in output:
        res = result.lower()
        try:
            priority[result] = 1
            indicator = 0
            for word in keywords:
                if "basketball-reference" in res:
                    basketball_count += 1
                if word in res:
                    indicator = 1
                    break

            if indicator >= 1:
                priority[result] += indicator

            if res in inlinks:
                priority[result] += len(inlinks[result])

            if priority[result] > 1:
                updateUrlInfo(link, result)
                if result in inlinks:
                    inlinks[result].append(link)
                    if result in p:
                        p[result].append(link)
                    else:
                        p[result] = [link]
                else:
                    inlinks[result] = [link]
                    p[result] = [link]

        except:
            print("Canon issue")
            continue

def canonicalize(outlinks):
    a = subprocess.check_output(['java', '-jar', 'canon.jar', outlinks])
    links = a.split()
    answer = []
    for link in links:
        try:
            answer.append(link.decode('ascii'))
        except:
            continue
    return answer


def updateUrlInfo(link, outlink):
    global urlinfo

    try:
        for obj in urlinfo:
            if obj["doc_id"] == link:
                obj["outlinks"].add(outlink)
                break
    except:
        print("Cannot update url outlinks")


if __name__ == '__main__':
    main()
