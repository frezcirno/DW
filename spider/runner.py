import os
from scrapy.cmdline import execute

try:
    execute("scrapy crawl spider1".split())
except SystemExit:
    pass
