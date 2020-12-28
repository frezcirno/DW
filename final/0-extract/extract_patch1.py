import os
import re
import json
from lxml import etree

from multiprocessing import Process, Pool, Manager, Queue

parser = etree.HTMLParser()

count = os.cpu_count()


def parse(pid, path):
    pi = {}
    pi["pid"] = pid

    tree = etree.parse(path, parser)

    if len(tree.xpath('//a[@class="av-retail-m-nav-text-logo"]')) > 0:
        pi["star"] = tree.xpath(
            '//a[@data-automation-id="customer-review-badge"]/@aria-label'
        )[0]

    else:
        pi["star"] = tree.xpath(
            '//div[@id="detailBullets_feature_div"]//i[contains(@class, "a-icon-star")]/span[@class="a-icon-alt"]/text()'
        )[0]

    return pi


def worker(index, queue):
    print(f"Worker {index} start.")

    with open(f"{index}.jl", "a") as out, open(f"{index}.log", "a") as log, open(
        f"{index}.err", "a"
    ) as err:
        while True:
            pid, path = queue.get(True)
            # print(f"Get {pid}")
            try:
                pi = parse(pid, path)
                out.write(json.dumps(pi) + "\n")
                log.write(pid + "\n")
            except Exception as e:
                err.write(f"{pid} {e}\n")
            except KeyboardInterrupt as e:
                print(f"Stoping worker {index}...")
                raise e
            queue.task_done()


def init():
    if not os.path.exists("first_run"):
        for i in range(os.cpu_count()):
            if os.path.exists(f"{i}.log"):
                os.remove(f"{i}.log")
            if os.path.exists(f"{i}.err"):
                os.remove(f"{i}.err")
            if os.path.exists(f"{i}.jl"):
                os.remove(f"{i}.jl")

        with open("task.txt", "w") as fp:
            for p, d, f in os.walk("D:/Code/etl/data/html/"):
                print(f"Walk in {p}")
                for _f in f:
                    pid = _f[:-5]
                    fp.write(pid + " " + os.path.join(p, _f) + "\n")
        open("first_run", "w").close()

    finished = []
    tasks = []

    for i in range(count):
        if os.path.exists(f"{i}.log"):
            with open(f"{i}.log") as log:
                for line in log:
                    finished.append(line.strip())

    with open("task.txt") as task:
        for line in task:
            line = line.split()
            pid = line[0]
            p = line[1]

            if pid not in finished:
                tasks.append((pid, p))

    return tasks


def combine():
    with open("final.jl", "w") as out, open("final.log", "w") as log, open(
        "final.err", "w"
    ) as err:
        for i in range(count):
            with open(f"{i}.jl", "a") as iout, open(f"{i}.log", "a") as ilog, open(
                f"{i}.err", "a"
            ) as ierr:
                out.write(iout.read())
                log.write(ilog.read())
                err.write(ierr.read())

            os.remove(f"{i}.jl")
            os.remove(f"{i}.log")
            os.remove(f"{i}.err")


if __name__ == "__main__":
    print(f"Process: {count}")

    tasks = init()
    print(f"Get tasks: {len(tasks)}")

    with Manager() as manager:
        queue = manager.Queue()
        pool = Pool(count)

        for task in tasks:
            queue.put(task)
        print(f"Queue finished.")

        for i in range(count):
            pool.apply_async(worker, (i, queue))
        pool.close()

        pool.join()
        print("All finished.")

        combine()
        print("Combined.")
