from urlparse import urlparse
from threading import Thread, Lock
import httplib, sys
from Queue import Queue
import time

concurrent = 100

lock = Lock()
counter = 0
alldata = {}

def doWork():
    global lock, counter
    while True:
        url = q.get()

        res, duration = getStatus(url)


            # Page URL, Server Status Code, Server Status Code -2 (if there is ,after redirect) ,
            # Server Status Code -3 (if there is ,after redirect) , Redirected Page (if redirected),
            # Load Time, How Many Redirect in There
        if res == "ERR":
            data = [url,"ERR","","","",duration,0]
        else:
            data = [url,res.status,"","","",duration,0]

            if (res.status >= 300) and (res.status <= 399):
                for k,v in res.getheaders():
                    if k.lower() == "location":
                        data[4] = v
                        data[6] = 1                             # 1 time redirected
                        if v[0] == "/":
                            up = urlparse(url)
                            res2,dur2 = getStatus(up.scheme+"://"+up.netloc+v)
                        else:
                            res2,dur2 = getStatus(v)

                        if res2 == "ERR":
                            data[2] = "ERR"
                        else:
                            data[2] = res2.status
                            data[5] = data[5] + dur2                # add time needed for first redirect
                            if (res2.status >= 300) and (res2.status <= 399):
                                for k,v in res2.getheaders():
                                    if k.lower() == "location":
                                        data[4] = v
                                        data[6] = 2                 # 2 times redirected
                                        if v[0] == "/":
                                            up = urlparse(v)
                                            res3,dur3 = getStatus(up.scheme+"://"+up.netloc+v)
                                        else:
                                            res3,dur3 = getStatus(v)


                                        if res3 == "ERR":
                                            data[3] = "ERR"
                                        else:
                                            data[3] = res3.status
                                            data[5] = data[5] + dur3    # add time needed for second redirect

        alldata[url] = data

        with lock:
            counter = counter + 1
            outfile.write(",".join(str(x) for x in data))
            outfile.write("\n")
        print counter, (time.time()-starttime)/counter, data

        q.task_done()



def getStatus(ourl):
    try:
        duration = time.time()
        url = urlparse(ourl)
        conn = httplib.HTTPConnection(url.netloc)
        conn.request("HEAD", url.path)
        res = conn.getresponse()
        duration = time.time() - duration

        return res, duration
    except:
        return "ERR", 0




outfile = open("son-sonuc.txt","w")

q = Queue(8)

starttime = time.time()
for i in range(1):
    t = Thread(target=doWork)
    t.daemon = True
    t.start()

try:
    for url in open('urllist.txt'):
        q.put(url.strip())
    q.join()
except KeyboardInterrupt:
    sys.exit(1)

