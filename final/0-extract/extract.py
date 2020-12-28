import os
import re
import json
from lxml import etree

parser = etree.HTMLParser()


def parse(pid, path):
    pi = {}
    pi["pid"] = pid

    tree = etree.parse(path, parser)

    if "Prime Video" in tree.xpath("//title/text()")[0]:
        # Title
        pi["title"] = tree.xpath('//h1[@data-automation-id="title"]/text()')[0]

        # keywords
        # may contains category
        pi["keywords"] = [
            keyword.strip()
            for keyword in tree.xpath('//meta[@name="keywords"]/@content')[0].split(",")
        ]

        # runtime
        pi["runtime"] = tree.xpath(
            '//span[@data-automation-id="runtime-badge"]/text()'
        )[0]

        # meta-info
        dts = tree.xpath('//div[@id="meta-info"]/div/dl')
        for dt in dts:
            key = "".join(dt.xpath("./dt/span/text()"))
            value = "".join(dt.xpath("./dd//text()"))
            pi[key] = value

        # more details
        mdts = tree.xpath('//div[@id="btf-product-details"]/div/dl')
        for dt in mdts:
            key = "".join(dt.xpath("./dt/span/text()"))
            value = "".join(dt.xpath("./dd//text()")).replace("more…", "")
            pi[key] = value

        # otherFormats
        otherFormats = tree.xpath(
            '//div[@data-automation-id="other-formats"]/div/a/@href'
        )
        pi["otherFormat"] = [
            re.search("/dp/(\w+)/", otherFormat).group(1)
            for otherFormat in otherFormats
        ]

    else:
        # Title
        pi["title"] = tree.xpath('//span[@id="productTitle"]/text()')[0].strip()

        # keywords
        # may contains category
        pi["keywords"] = [
            keyword.strip()
            for keyword in tree.xpath('//meta[@name="keywords"]/@content')[0].split(",")
        ]

        # details
        mdts = tree.xpath(
            '//div[@id="detailBullets_feature_div"]/ul[@class="a-unordered-list a-nostyle a-vertical a-spacing-none detail-bullet-list"]//span[@class="a-list-item"]'
        )
        for dt in mdts:
            key = dt.xpath("./span[1]/text()")[0].strip(" :\r\n\t")
            value = dt.xpath("./span[2]/text()")[0].strip()
            pi[key] = value

        # format
        pi["format"] = tree.xpath('//div[@id="bylineInfo"]/span[last()]/text()')[0]

        # otherFormats
        otherFormats = tree.xpath(
            '//div[@id="tmmSwatches"]//li[starts-with(@class,"swatchElement unselected")]//span[@class="a-button-inner"]//a/@href'
        )
        pi["otherFormat"] = [
            re.search("/dp/(\w+)/", otherFormat).group(1)
            for otherFormat in otherFormats
        ]

        # additionalOptions
        # Attention: "top-level unselected-row"有两层
        additionalOptions = tree.xpath(
            '//div[@id="twister"]//div[@class="top-level unselected-row"]/span/@data-tmm-see-more-editions-click'
        )
        pi["additionalOptions"] = [
            re.search("/dp/(\w+)/", additionalOption).group(1)
            for additionalOption in additionalOptions
        ]
    return pi


def init():
    with open("task.txt", "w") as fp:
        for p, d, f in os.walk("D:/Code/etl/data/html/rest"):
            print(f"Walk in {p}")
            for _f in f:
                pid = _f[:-5]
                fp.write(pid + " " + os.path.join(p, _f) + "\n")

    tasks = []

    with open("task.txt") as task:
        for line in task:
            line = line.split()
            pid = line[0]
            p = line[1]
            tasks.append((pid, p))

    return tasks


if __name__ == '__main__':

    tasks = init()

    with open(f"rest.jl", "a") as out, open(f"rest.log", "a") as log, open(
        f"rest.err", "a"
    ) as err:

        for pid, path in tasks:
            print(f"Get {pid}")

            pi = parse(pid, path)
            out.write(json.dumps(pi) + "\n")
            log.write(pid + "\n")
