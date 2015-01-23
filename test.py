from urlparse import urlparse

qq = 'http://www.milliyet.com.tr/kasik-fitigi-ameliyatinda-yeni-donem-kayseri-yerelhaber-80666/'

tt = 'http://www.eniyihekim.com/akustik-norinom-cerrahisi/kayseri'
pp = '#'
gg = '/doc'
sds = 'http://www.eniyihekim.comjavascript&#hbhj'


a = urlparse(sds)

if 'javascript' not in a.netloc:
    if 'eniyihekim' in a.netloc:
        print('site')

    if a.netloc == '':
        if a.path != '':
            print('ok')





print(a)