#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import bin.run as run
import config as c
from threading import Thread

db_file = c.db_file
# init run class object
master = run.Run


def main(args):
    # run init method
    run.Run(args)

    k = 0
    while True:
        # exit from loop if amount of parsed links equal amount of total links
        if not master.select_unq_urls_db(db_file):
            master.export(c.csv_headers, c.csv_file, master.select_result_db(db_file))
            sys.exit(1)

        # continue loop
        else:
            while True:
                print("run")
                # define range of threads
                for i in range(100):
                    t = Thread(target=master.spider)
                    t.daemon = True
                    t.start()

                # init array of 100 links
                tmp_arr = []
                for item in master.select_unq_urls_db(db_file):
                    tmp_arr.append(item)

                # divide array of links for 10 chunks
                chunks = lambda lst, sz: [lst[z:z+sz] for z in range(0, len(lst), sz)]
                chunk_arr = chunks(tmp_arr, 10)

                # push chunks in a queue
                for ch in chunk_arr:
                    try:
                        for url in ch:
                            master.q.put(url.strip())
                        master.q.join()
                    except Exception, err:
                        print "Error %s:" % err.args[0]
                        sys.exit(1)
                k += 1
                break

if __name__ == '__main__':
    try:
        main(sys.argv[1:])
    except Exception, e:
        print "Error %s:" % e.args[0]
