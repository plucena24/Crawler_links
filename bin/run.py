import manager_csv as mcsv
import manager_db as mdb
import manager_html as mhtml
import config as c
from threading import Lock
from Queue import Queue


class Run(mcsv.Manager, mdb.Manager, mhtml.Manager):
    # init global queue
    q = Queue(800)
    lock = Lock()
    db_file = c.db_file

    def __init__(self, arg):
        # pass domain name from args to other functions
        self.global_root(arg)
        # crate SQLite database
        self.create_db(self.db_file)
        # insert first url
        self.insert_urls_db(self.db_file, arg)

    @staticmethod
    def global_root(arg):
        global root
        root = arg
        return root

    # main worker, that will run in threads
    @staticmethod
    def spider():
        # init array for page information
        result_arr = []
        # pull queue
        target = Run.q.get()

        # get page information and array of urls
        title, description, web = Run.crawler(target, root)
        with Run.lock:
            # insert new urls
            Run.insert_urls_db(Run.db_file, web)
            result_arr.append(target)
            result_arr.append(title)
            result_arr.append(description)
            # insert page information
            Run.insert_result_db(Run.db_file, result_arr)
            # update url status
            Run.update_urls_db(Run.db_file, target)

        Run.q.task_done()









