import sqlite3 as lite
import sys


class Manager():
    def __init__(self):
        pass

    @staticmethod
    def create_db(db_file):
        con = None
        try:
            con = lite.connect(db_file)
            cur = con.cursor()
            cur.execute("""CREATE TABLE IF NOT EXISTS Result("""
                        """Url TEXT PRIMARY KEY, """
                        """Title TEXT, """
                        """Description TEXT)""")

            cur.execute("""CREATE TABLE IF NOT EXISTS Urls("""
                        """Url TEXT PRIMARY KEY, """
                        """State INTEGER)""")

        except lite.Error, e:
            print "Error %s:" % e.args[0]
            sys.exit(1)
        finally:
            if con:
                con.close()

    @staticmethod
    def drop_db(db_file):
        con = None
        try:
            con = lite.connect(db_file)
            cur = con.cursor()
            cur.execute("""DROP TABLE IF EXISTS Result""")
            cur.execute("""DROP TABLE IF EXISTS Urls""")

        except lite.Error, e:
            print "Error %s:" % e.args[0]
            sys.exit(1)
        finally:
            if con:
                con.close()

    @staticmethod
    def insert_urls_db(db_file, urls_array):
        con = None
        try:
            con = lite.connect(db_file)
            cur = con.cursor()
            for item in urls_array:
                cur.execute("""INSERT OR IGNORE INTO Urls VALUES ('%s', NULL)""" % item)
            con.commit()

        except lite.Error, e:
            print "Error %s:" % e.args[0]
            sys.exit(1)
        finally:
            if con:
                con.close()

    @staticmethod
    def insert_result_db(db_file, result_array):
        con = None
        try:
            con = lite.connect(db_file)
            cur = con.cursor()
            cur.execute("""INSERT OR IGNORE INTO Result VALUES (?, ?, ?)""", result_array)
            con.commit()

        except lite.Error, e:
            print "Error %s:" % e.args[0]
            sys.exit(1)
        finally:
            if con:
                con.close()

    @staticmethod
    def select_result_db(db_file):
        con = None
        result = []
        try:
            con = lite.connect(db_file)
            cur = con.cursor()
            cur.execute("""SELECT * FROM Result""")
            rows = cur.fetchall()
            for row in rows:
                result.append(row)

        except lite.Error, e:
            print "Error %s:" % e.args[0]
            sys.exit(1)
        finally:
            if con:
                con.close()
            return result

    @staticmethod
    def select_urls_db(db_file):
        con = None
        result = []
        try:
            con = lite.connect(db_file)
            cur = con.cursor()
            cur.execute("""SELECT Url FROM Urls""")
            rows = cur.fetchall()
            for row in rows:
                result.append(row)

        except lite.Error, e:
            print "Error %s:" % e.args[0]
            sys.exit(1)
        finally:
            if con:
                con.close()
            return result

    @staticmethod
    def select_unq_urls_db(db_file):
        con = None
        result = []
        try:
            con = lite.connect(db_file)
            cur = con.cursor()
            # select array of 100 urls, allows to lower amount of connections to the db
            cur.execute("""SELECT Url FROM Urls WHERE State IS NULL LIMIT 100""")
            rows = cur.fetchall()
            for row in rows:
                result.append(row[0])

        except lite.Error, e:
            print "Error %s:" % e.args[0]
            sys.exit(1)
        finally:
            if con:
                con.close()
            return result

    @staticmethod
    def update_urls_db(db_file, url):
        con = None
        try:
            con = lite.connect(db_file)
            cur = con.cursor()
            cur.execute("""UPDATE Urls SET State = 1 WHERE Url = '%s'""" % url)
            con.commit()

        except lite.Error, e:
            print "Error %s:" % e.args[0]
            sys.exit(1)
        finally:
            if con:
                con.close()