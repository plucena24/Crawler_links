import urllib
import lxml.html
from urlparse import urlparse


class Manager():
    def __init__(self):
        pass

    # html parser
    @staticmethod
    def crawler(url, root):
        web = []
        try:
            connection = urllib.urlopen(url)
            dom = lxml.html.fromstring(connection.read())
        except Exception, e:
            print "Error %s:" % e.args[0]
            title = ''
            description = ''
            # return empty result if page is wrong
            return title, description, web

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

        a_current = urlparse(url)
        a_root = urlparse(root[0])
        if not a_root.netloc[4:] in a_current.netloc:
            # skip page if it doesn't hosted on the same domain
            return title, description, web

        for link in dom.xpath('//a/@href'):
            a = urlparse(link)
            # skip javascript links
            if 'javascript' not in a.netloc:
                # append hostname to the internal links
                if a.netloc == '':
                    # skip jquery page links
                    if a.path != '':
                        link = root[0] + link
                        web.append(link)
                        continue

                if not a_root.netloc[4:] in a.netloc:
                    continue

                web.append(link)

        return title, description, web