"""
从HTML里面解析数据
多线程版
"""

import os
import re
import json
import signal
import shutil
from lxml import etree
from multiprocessing import Process, Pool, Manager, Queue

parser = etree.HTMLParser()

count = os.cpu_count()


def parse(pid, path):
    pi = {}
    pi["pid"] = pid

    tree = etree.parse(path, parser)

    if len(tree.xpath('//a[@class="av-retail-m-nav-text-logo"]')) > 0:
        # 是Prime Video页面(黑色底)

        ## Title
        pi["title"] = tree.xpath('//h1[@data-automation-id="title"]/text()')[0]

        ## keywords
        # may contains category
        pi["keywords"] = [
            keyword.strip()
            for keyword in tree.xpath('//meta[@name="keywords"]/@content')[0].split(",")
        ]

        ## runtime
        pi["runtime"] = tree.xpath(
            '//span[@data-automation-id="runtime-badge"]/text()'
        )[0]

        ## meta-info
        dts = tree.xpath('//div[@id="meta-info"]/div/dl')
        for dt in dts:
            key = "".join(dt.xpath("./dt/span/text()"))
            value = "".join(dt.xpath("./dd//text()"))
            pi[key] = value

        ## more details
        mdts = tree.xpath('//div[@id="btf-product-details"]/div/dl')
        for dt in mdts:
            key = "".join(dt.xpath("./dt/span/text()"))
            value = "".join(dt.xpath("./dd//text()")).replace("more…", "")
            pi[key] = value

        ## otherFormats
        otherFormats = tree.xpath(
            '//div[@data-automation-id="other-formats"]/div/a/@href'
        )
        pi["otherFormat"] = [
            re.search("/dp/(\w+)/", otherFormat).group(1)
            for otherFormat in otherFormats
        ]

        ## point
        pi["Points"] = tree.xpath(
            '//a[@data-automation-id="customer-review-badge"]/@aria-label'
        )[0]

    else:
        # 是普通商品页面(白底)

        ## Title
        pi["title"] = tree.xpath('//span[@id="productTitle"]/text()')[0].strip()

        ## keywords
        # may contains category
        pi["keywords"] = [
            keyword.strip()
            for keyword in tree.xpath('//meta[@name="keywords"]/@content')[0].split(",")
        ]

        ## details
        mdts = tree.xpath(
            '//div[@id="detailBullets_feature_div"]/ul[@class="a-unordered-list a-nostyle a-vertical a-spacing-none detail-bullet-list"]//span[@class="a-list-item"]'
        )
        for dt in mdts:
            key = dt.xpath("./span[1]/text()")[0].strip(" :\r\n\t")
            value = dt.xpath("./span[2]/text()")[0].strip()
            pi[key] = value

        ## format
        pi["format"] = tree.xpath('//div[@id="bylineInfo"]/span[last()]/text()')[0]

        ## otherFormats
        otherFormats = tree.xpath(
            '//div[@id="tmmSwatches"]//li[@class="swatchElement unselected"]//a/@href'
        )
        pi["otherFormat"] = [
            re.search("/dp/(\w+)/", otherFormat).group(1)
            for otherFormat in otherFormats
        ]

        ## additionalOptions
        # Attention: "top-level unselected-row"有两层
        additionalOptions = tree.xpath(
            '//div[@id="twister"]//div[@class="top-level unselected-row"]/span/@data-tmm-see-more-editions-click'
        )
        pi["additionalOptions"] = [
            re.search("/dp/(\w+)/", additionalOption).group(1)
            for additionalOption in additionalOptions
        ]

        ## rating
        pi["Points"] = tree.xpath(
            '//div[@id="detailBullets_feature_div"]//i[contains(@class, "a-icon-star")]/span[@class="a-icon-alt"]/text()'
        )[0]

        ## PointPersonNumber
        pi["PointPersonNumber"] = tree.xpath(
            '//span[@id="acrCustomerReviewText"]/text()'
        )[0]

    # Common
    ## star
    starsTable = tree.xpath('//table[@id="histogramTable"]')
    if len(starsTable) > 1:
        starsComment = {}
        starsTable = starsTable[1]
        for i, stars in enumerate(
            starsTable.xpath('.//td[@class="a-text-right a-nowrap"]//a//text()')
        ):
            starsComment[f"{5-i} star"] = stars.strip()
        pi["Stars"] = starsComment

    return pi


def worker(index, queue: Queue):
    print(f"Worker {index} start.")

    with open(f"extract_logs/{index}.jl", "a") as out, open(
        f"extract_logs/{index}.log", "a"
    ) as log, open(f"extract_logs/{index}.err", "a") as err:
        while True:
            pid, path = queue.get(True)
            print(f"Get {pid}")
            try:
                pi = parse(pid, path)
                out.write(json.dumps(pi) + "\n")
                log.write(pid + "\n")
            except KeyboardInterrupt:
                print(f"Stoping worker {index}...")
                return
            except Exception as e:
                err.write(f"{pid} {e}\n")
            queue.task_done()


def init():
    if not os.path.exists("first_run"):
        if os.path.exists("extract_logs"):
            shutil.rmtree("extract_logs")
        os.mkdir("extract_logs")

        with open("task.txt", "w") as fp:
            for p, d, f in os.walk("D:/Code/etl/data/rest"):
                print(f"Walk in {p}")
                for _f in f:
                    pid = _f[:-5]
                    fp.write(pid + " " + os.path.join(p, _f) + "\n")
            for p, d, f in os.walk("D:/Code/etl/data/html"):
                print(f"Walk in {p}")
                for _f in f:
                    pid = _f[:-5]
                    fp.write(pid + " " + os.path.join(p, _f) + "\n")

        open("first_run", "w").close()

    finished = []
    tasks = []

    for i in range(count):
        if os.path.exists(f"extract_logs/{i}.log"):
            with open(f"extract_logs/{i}.log") as log:
                for line in log:
                    finished.append(line.strip())

    with open("task.txt") as task:
        for line in task:
            line = line.split()
            pid, p = line

            if pid not in finished:
                tasks.append((pid, p))

    return tasks


def combine():
    with open("final.jl", "w") as out, open("final.log", "w") as log, open(
        "final.err", "w"
    ) as err:
        for i in range(count):
            with open(f"extract_logs/{i}.jl", "a") as iout, open(
                f"extract_logs/{i}.log", "a"
            ) as ilog, open(f"extract_logs/{i}.err", "a") as ierr:
                out.write(iout.read())
                log.write(ilog.read())
                err.write(ierr.read())


if __name__ == "__main__":
    print(f"Process: {count}")

    tasks = init()
    print(f"Get tasks: {len(tasks)}")

    with Manager() as manager:
        queue = manager.Queue()
        pool = Pool(count)

        try:
            for i in range(count):
                pool.apply_async(worker, (i, queue))
            pool.close()

            for task in tasks:
                queue.put(task)
            print(f"Queue finished.")

            pool.join()
            print("All finished.")

            combine()
            print("Combined.")

        except KeyboardInterrupt:
            print("KeyboardInterrupt!")

