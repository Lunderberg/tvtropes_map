#!/usr/bin/env python3.3

from time import sleep
from queue import Queue
from threading import Thread
from collections import defaultdict

from lxml import html
import requests
# import requests_cache
# requests_cache.install_cache('tvtropes')



#A queue that won't check each item more than once.
#Used for the url_queue, so that each URL is only checked once.
class SetQueue(Queue):
    def _init(self,maxsize):
        Queue._init(self,maxsize)
        self.all_items = set()
    def _put(self,item):
        if item not in self.all_items:
            Queue._put(self,item)
            self.all_items.add(item)

class Indexer(dict):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.count = 0
    def __getitem__(self,key):
        try:
            return super().__getitem__(key)
        except KeyError:
            val = self.count
            self.count += 1
            self[key] = val
            return val

prefixes = ['http://tvtropes.org/pmwiki/pmwiki.php/',
            '/pmwiki/pmwiki.php/']
suffix = '?from'

def reduce_link(link):
    for prefix in prefixes:
        if link.startswith(prefix):
            link = link[len(prefix):]
            break
    else:
        if link.startswith('http'):
            return None

    if suffix in link:
        link = link[:link.index(suffix)]

    return link

def expand_link(link):
    return prefixes[0]+link

class TVTropes_Reader(Thread):
    def __init__(self,url_queue,link_queue,tries=10,*args,**kw):
        super().__init__(*args,**kw)
        self.url_queue = url_queue
        self.link_queue = link_queue
        self.tries = tries
        self.running = True

    @classmethod
    def find_links(cls,content):
        tree = html.fromstring(content)
        try:
            content = tree.get_element_by_id('wikitext')
        except KeyError:
            return []
        links = [link.get('href') for link in content.findall('.//a')
                 if link.get('class')=='twikilink']
        links = list(filter(bool,map(reduce_link,links)))
        return links

    def process_url(self,url):
        expanded = expand_link(url)
        for _ in range(self.tries):
            try:
                res = requests.get(expanded)
            except requests.exceptions.ConnectionError as e:
                sleep(10)
                continue
            if res.status_code==200:
                break
            sleep(10)
        else:
            print("Couldn't access {}, skipping".format(url))
            return
        #Check for redirects
        if res.url!=expanded:
            redirected = reduce_link(res.url)
            if redirected:
                self.url_queue.put(redirected)
                self.link_queue.put(('Redirect',(url,redirected)))
            return
        #Not a redirect, process links
        for link in self.find_links(res.content):
            self.url_queue.put(link)
            self.link_queue.put(('Link',(url,link)))

    def run(self):
        while True:
            if self.running:
                url = self.url_queue.get()
                self.process_url(url)
                self.url_queue.task_done()
            else:
                sleep(1)


class TVTropes_Counter(Thread):
    def __init__(self,link_queue,index=None,outfile=None,*args,**kw):
        super().__init__(*args,**kw)
        self.link_queue = link_queue
        self.index = Indexer() if index is None else index
        self.running = True
        self.links = defaultdict(list)
        self.redirects = []
        self.outfile = open(outfile,'w') if outfile else None

    @staticmethod
    def name_value(segment):
        segment = segment.lower()
        std = {'main':-1,
               'series':0,'film':0,'literature':0,'manga':0,'anime':0,'ymmv':0,
               'comicstrip':0,'franchise':0,'creator':0,'fanfic':0,'comicbook':0,
               'theatre':0,'videogame':0,'visualnovel':0,'webcomic':0,'webanimation':0,
               'roleplay':0,'westernanimation':0,'disney':0,'administrivia':0,
               'usefulnotes':0,'wrestling':0,'music':0,'website':0,'soyouwantto':0,
               'darthwiki':0,'analysis':0,'heavymetal':0,'tropers':0,'website':0}
        if segment in std:
            return std[segment]
        elif segment.startswith('tropes'):
            return 5
        else:
            return 10

    @classmethod
    def extract_main(cls,url):
        output = max(url.split('/'),key=cls.name_value)
        if output in []:
            print(url,output)
        return output

    def process_link(self,link):
        from_url,to_url = link
        from_index = self.index[self.extract_main(from_url)]
        to_index = self.index[self.extract_main(to_url)]
        self.links[from_index].append(to_index)
        if self.outfile:
            self.outfile.write('{} -> {}\n'.format(from_url,to_url))

    def process_redirect(self,redirect):
        from_url,to_url = redirect
        from_index = self.index[self.extract_main(from_url)]
        to_index = self.index[self.extract_main(to_url)]
        #Check for difference because e.g. 'Main/GameOfThrones' -> 'Series/GameOfThrones'
        if to_index!=from_index:
            self.redirects.append((to_index,from_index))
        if self.outfile:
            self.outfile.write('{} => {}\n'.format(from_url,to_url))

    def run(self):
        while True:
            if self.running:
                info_type,payload = self.link_queue.get()
                if info_type=='Link':
                    self.process_link(payload)
                elif info_type=='Redirect':
                    self.process_redirect(payload)
                self.link_queue.task_done()
            else:
                sleep(1)


class TVTropes_Scraper:
    def __init__(self,readers=10,start='Main/HomePage',outfile=None):
        self.url_queue = SetQueue()
        self.url_queue.put(start)
        self.link_queue = Queue()
        self.index = Indexer()
        self.readers = [TVTropes_Reader(self.url_queue,self.link_queue,daemon=True) for _ in range(readers)]
        self.counter = TVTropes_Counter(self.link_queue,self.index,outfile=outfile,daemon=True)

    def start(self):
        for thread in self.readers+[self.counter]:
            thread.start()

    def pause(self):
        for thread in self.readers+[self.counter]:
            thread.running = False

    def resume(self):
        for thread in self.readers+[self.counter]:
            thread.running = True

    @property
    def urls_known(self):
        return len(self.url_queue.all_items)

    @property
    def urls_checked(self):
        return self.urls_known - self.url_queue.qsize()

    @property
    def urls_remaining(self):
        return self.url_queue.qsize()

    @property
    def pages_known(self):
        return len(self.index)

    @property
    def links(self):
        return self.counter.links

    @property
    def most_linked(self):
        temp_index = list(self.index)
        output = [(page,len(self.links[self.index[page]])) for page in temp_index]
        output.sort(key = lambda t:t[1],reverse=True)
        return output

if __name__=='__main__':
    scraper = TVTropes_Scraper(readers=25,outfile='links.txt')
    scraper.start()
    import IPython; IPython.embed()
