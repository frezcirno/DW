import json
import random
from scrapy import signals, exceptions, Request
from amaspd import settings


class ProxyMiddleware:
    """Custom ProxyMiddleware."""

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        return cls()

    def __init__(self):
        self.proxies = []
        self.init_proxy_from_file()

    def process_request(self, request, spider):
        request.meta['proxy'] = 'http://127.0.0.1:8888'
        # if random.randint(1, 10) <= 6:
        #     request.meta['proxy'] = self.get_proxy()

    # def process_exception(self, request, exception, spider):
    #     if 'proxy' in request.meta:
    #         self.delete_proxy(request.meta['proxy'])
            # with open('proxy.txt', 'w', encoding='utf-8') as f:
            #     for proxy in self.proxyok:
            #         f.write(proxy + '\n')



    def init_proxy_from_file(self):
        with open('proxy.txt', encoding='utf-8') as f:
            for line in f:
                self.proxies.append(line.lower().strip())
        
    def init_proxy_from_jiangxianli(self):
        for page in range(1, 10):
            proxy_ips = requests.get(
                'https://ip.jiangxianli.com/api/proxy_ips', params={'page': page}).json()
            self.proxies += list(map(lambda ip: ip['ip']+":" +
                                    ip['port'], proxy_ips['data']['data']))
            for i in range(1, 100000):
                a = i * i * i

    def get_proxy(self):
        # if random.randint(1, 60) <= 59 and len(self.proxyok) > 0:
        #     return random.choice(list(self.proxyok))
        # ip_str = Request("https://ip.jiangxianli.com/api/proxy_ip")
        # ip = json.loads(ip_str)
        # return ip['ip']+":"+ip['port']
        return random.choice(self.proxies)

    def delete_proxy(self, p):
        # Request('http://127.0.0.1:5010/delete/?proxy=' + p)
        self.proxies.remove(p)
