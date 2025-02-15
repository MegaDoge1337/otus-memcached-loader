#!/usr/bin/env python
# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
# pip install python-memcached

import collections
import glob
import gzip
import logging
import os
import sys
import threading
from optparse import OptionParser
from queue import Queue

import memcache

import appsinstalled_pb2

MEMC_TIMEOUT = 1
MEMC_RETRIES = 3
WORKERS = 10

NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple(
    "AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"]
)


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def insert_appsinstalled(memc_addr, appsinstalled, dry_run=False):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = "{}:{}".format(appsinstalled.dev_type, appsinstalled.dev_id)
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()
    # @TODO persistent connection
    # @TODO retry and timeouts!
    try:
        if dry_run:
            logging.debug(
                "{} - {} -> {}".format(memc_addr, key, str(ua).replace("\n", " "))
            )
        else:
            memc = memcache.Client(
                [memc_addr], socket_timeout=MEMC_TIMEOUT, dead_retry=MEMC_RETRIES
            )
            result = memc.set(key, packed)
            if result:
                return True
            else:
                logging.error(f"Cannot write to memc {memc_addr}: connection error")
                return False
    except Exception as e:
        logging.exception("Cannot write to memc {}: {}".format(memc_addr, e))
        return False
    return True


def parse_appsinstalled(line):
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logging.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def worker(device_memc, options, queue, counters):
    while not queue.empty():
        if queue.empty():
            break

        line = queue.get()
        appsinstalled = parse_appsinstalled(line)

        if not appsinstalled:
            counters["errors"] += 1
            return None

        memc_addr = device_memc.get(appsinstalled.dev_type)
        if not memc_addr:
            counters["errors"] += 1
            logging.error("Unknow device type: %s" % appsinstalled.dev_type)
            return None

        ok = insert_appsinstalled(memc_addr, appsinstalled, options.dry)

        if ok:
            counters["processed"] += 1
        else:
            counters["errors"] += 1


def main(options):
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }
    for fn in glob.iglob(options.pattern):
        queue = Queue()
        counters = {"processed": 0, "errors": 0}
        logging.info("Processing %s" % fn)
        fd = gzip.open(fn, "rt")
        for line in fd:
            line = line.strip()
            if not line:
                continue
            queue.put(line)

        threads = []
        for _ in range(WORKERS):
            thread = threading.Thread(
                target=worker,
                args=(
                    device_memc,
                    options,
                    queue,
                    counters,
                ),
            )
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        if not counters["processed"]:
            fd.close()
            dot_rename(fn)
            continue

        err_rate = float(counters["errors"]) / counters["processed"]
        if err_rate < NORMAL_ERR_RATE:
            logging.info("Acceptable error rate (%s). Successfull load" % err_rate)
        else:
            logging.error(
                "High error rate ({} > {}). Failed load".format(
                    err_rate, NORMAL_ERR_RATE
                )
            )
        fd.close()
        dot_rename(fn)


def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == "__main__":
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="./data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()
    logging.basicConfig(
        filename=opts.log,
        level=logging.INFO if not opts.dry else logging.DEBUG,
        format="[%(asctime)s] %(levelname).1s %(message)s",
        datefmt="%Y.%m.%d %H:%M:%S",
    )
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        main(opts)
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
