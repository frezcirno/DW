from fake_useragent import UserAgent


class UserAgentMiddleware:
    """Custom UAMiddleware."""

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        return cls(crawler.settings)

    def __init__(self, settings):
        self.ua = UserAgent()

    def process_request(self, request, spider):
        request.headers['User-Agent'] = self.ua.random
