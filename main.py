# -*- coding: utf-8 -*-
import urllib
import lxml.html
from threading import Thread, Lock
from Queue import Queue
import re
import sqlite3 as lite
import sys
import csv
import codecs
import cStringIO


root = 'http://www.eniyihekim.com'
depth = 10


lock = Lock()
chunks = lambda lst, sz: [lst[z:z+sz] for z in range(0, len(lst), sz)]


class Spider():
    def __init__(self):
        pass

    @staticmethod
    def compare(list_1, list_2):
        check = set([(d['url']) for d in list_1])
        return [d for d in list_2 if (d['url']) not in check]

    @staticmethod
    def venom(url):
        web = []
        print("connecting url: ")
        print(url)
        connection = urllib.urlopen(url)
        dom = lxml.html.fromstring(connection.read())

        if dom.find(".//title") is None:
            title = ''
        elif dom.find(".//title").text is None:
            title = ''
        else:
            title = dom.find(".//title").text

        if len(dom.xpath(".//meta[@name='description']/@content")) != 0:
            description = dom.xpath(".//meta[@name='description']/@content")[0]
        else:
            description = ''

        p = re.compile('(http://\S+)|(eniyihekim.com(\d+))')
        if p.match(url) is None:
            print("DEBUG")
            print(url)
            return title, description, web

        for link in dom.xpath('//a/@href'):

            p = re.compile('(http://\S+)')
            if p.match(link) is None:
                p = re.compile('(https://\S+)')
                if p.match(link) is None:

                    p = re.compile('#')
                    if p.match(link) is None:
                        link = root + link
                        web.append(link)
                        continue
                    else:
                        continue

            #print(link)
            web.append(link)

        return title, description, web

    @staticmethod
    def get_unique(list1, list2):
        res = []
        for val in list2:
            if val not in list1:
                res.append(val)
        return res

    @staticmethod
    def create_db():
        con = None

        try:
            con = lite.connect('test.db')
            cur = con.cursor()
            #cur.execute("DROP TABLE IF EXISTS Result")
            #cur.execute("DROP TABLE IF EXISTS Urls")
            cur.execute("CREATE TABLE IF NOT EXISTS Result(Url TEXT PRIMARY KEY, Title TEXT, Description TEXT)")
            cur.execute("CREATE TABLE IF NOT EXISTS Urls(Url TEXT PRIMARY KEY, State INTEGER)")

        except lite.Error, e:
            print "Error %s:" % e.args[0]
            sys.exit(1)

        finally:
            if con:
                con.close()

    @staticmethod
    def insert_urls_db(urls_array):
        con = None

        try:
            #print(urls_array)
            con = lite.connect('test.db')
            cur = con.cursor()

            for item in urls_array:
                #print(item)
                cur.execute("INSERT OR IGNORE INTO Urls VALUES ('%s', NULL)" % item)

            cur.execute("SELECT * FROM Urls")
            rows = cur.fetchall()
            print("insert: ")
            for row in rows:
                print row

            con.commit()

        except lite.Error, e:
            print "Error %s:" % e.args[0]
            sys.exit(1)

        finally:
            if con:
                con.close()

    @staticmethod
    def insert_result_db(res_array):
        con = None

        try:
            #print(urls_array)
            con = lite.connect('test.db')
            cur = con.cursor()

            cur.execute("INSERT OR IGNORE INTO Result VALUES (?, ?, ?)", res_array)

            cur.execute("SELECT * FROM Urls")
            rows = cur.fetchall()
            print("insert: ")
            for row in rows:
                print row

            con.commit()

        except lite.Error, e:
            print "Error %s:" % e.args[0]
            sys.exit(1)

        finally:
            if con:
                con.close()

    @staticmethod
    def select_result_db():
        con = None
        result = []

        try:
            con = lite.connect('test.db')
            cur = con.cursor()

            cur.execute("SELECT * FROM Result")
            rows = cur.fetchall()
            for row in rows:
                #print row
                result.append(row)

        except lite.Error, e:
            print "Error %s:" % e.args[0]
            sys.exit(1)

        finally:
            if con:
                con.close()
            return result

    @staticmethod
    def select_urls_db():
        con = None
        result = []

        try:
            con = lite.connect('test.db')
            cur = con.cursor()

            cur.execute("SELECT Url FROM Urls")
            rows = cur.fetchall()
            for row in rows:
                #print row
                result.append(row)

        except lite.Error, e:
            print "Error %s:" % e.args[0]
            sys.exit(1)

        finally:
            if con:
                con.close()
            return result

    @staticmethod
    def select_unq_urls_db():
        con = None
        result = []

        try:
            con = lite.connect('test.db')
            cur = con.cursor()

            cur.execute("SELECT Url FROM Urls WHERE State IS NULL")
            rows = cur.fetchall()
            for row in rows:
                #print row
                result.append(row[0])

        except lite.Error, e:
            print "Error %s:" % e.args[0]
            sys.exit(1)

        finally:
            if con:
                con.close()
            print("result unq: ")
            print(result)
            return result

    @staticmethod
    def update_urls_db(url):
        con = None

        try:
            print("update: ")
            print(url)
            con = lite.connect('test.db')
            cur = con.cursor()

            cur.execute("UPDATE Urls SET State = 1 WHERE Url = '%s'" % url)

            cur.execute("SELECT * FROM Urls")
            rows = cur.fetchall()
            for row in rows:
                print row

            con.commit()

        except lite.Error, e:
            print "Error %s:" % e.args[0]
            sys.exit(1)

        finally:
            if con:
                con.close()

    @staticmethod
    def export():
        with open('output.csv', 'wb') as f:
            writer = UnicodeWriter(f, delimiter='\t')
            writer.writerow(['Url', 'Title', 'Description'])
            writer.writerows(Spider.select_result_db())


def test():
    global lock
    url = q.get()

    print(url)
    title, description, web = Spider.venom(url)

    with lock:
        Spider.insert_urls_db(web)

        result_arr = []
        result_arr.append(url)
        result_arr.append(title)
        result_arr.append(description)

        Spider.insert_result_db(result_arr)
        Spider.update_urls_db(url)

    q.task_done()


class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


def runner():
    k = 0
    Spider.create_db()
    Spider.insert_urls_db([root])
    print
    while True:
        if not Spider.select_unq_urls_db():
            print("export")
            Spider.export()
            sys.exit(1)

        else:
            for url in Spider.select_unq_urls_db():
                if k > depth:
                    print("export")
                    Spider.export()
                    sys.exit(1)

                title, description, web = Spider.venom(url)
                Spider.insert_urls_db(web)

                result_arr = []
                result_arr.append(url)
                result_arr.append(title)
                result_arr.append(description)

                Spider.insert_result_db(result_arr)
                Spider.update_urls_db(url)
                k += 1

runner()

'''
q = Queue(800)

Spider.create_db()
Spider.insert_urls_db([root])
print

k = 0
while True:
    if not Spider.select_unq_urls_db():
        print("export")
        Spider.export()
        sys.exit(1)

    elif k > 1:
        print("export")
        Spider.export()
        break

    else:
        while True:
            for i in range(100):
                t = Thread(target=test)
                t.daemon = True
                t.start()

            tmp_arr = []
            #chunk_arr = []
            for item in Spider.select_unq_urls_db():
                tmp_arr.append(item)

            chunk_arr = chunks(tmp_arr, 10)

            for ch in chunk_arr:

                print("chunk: ")
                #print(chunk_arr)
                try:
                    for url in ch:
                        q.put(url.strip())
                    q.join()

                except KeyboardInterrupt:
                    sys.exit(1)
            k += 1
            break
'''






