import collections
import glob
import gzip
import logging
import multiprocessing
import os
import sys
from optparse import OptionParser

import memcache

import appsinstalled_pb2

MEMC_BATCHES = 10
MEMC_SOCKET_TIMEOUT = 60
MEMC_BATCHES_TIMEOUT = 60

NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple(
    "AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"]
)


def _configure_logging(options):
    logging.basicConfig(
        filename=options.log,
        level=logging.INFO if not options.dry else logging.DEBUG,
        format="[%(asctime)s] %(levelname).1s %(message)s",
        datefmt="%Y.%m.%d %H:%M:%S",
    )


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def insert_appsinstalled(memc_conn: memcache.Client, batch, dry_run=False):
    try:
        if dry_run:
            logging.debug("{} -> {}".format(memc_conn.servers[0].address, batch))
        else:
            failed_count = memc_conn.set_multi(batch, time=MEMC_BATCHES_TIMEOUT)
            return len(failed_count)
    except Exception as e:
        logging.exception(
            "Cannot write to memc {}: {}".format(memc_conn.servers[0].address, e)
        )
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


def worker(options, fn):
    device_memc = {
        "idfa": memcache.Client([options.idfa], socket_timeout=MEMC_SOCKET_TIMEOUT),
        "gaid": memcache.Client([options.gaid], socket_timeout=MEMC_SOCKET_TIMEOUT),
        "adid": memcache.Client([options.adid], socket_timeout=MEMC_SOCKET_TIMEOUT),
        "dvid": memcache.Client([options.dvid], socket_timeout=MEMC_SOCKET_TIMEOUT),
    }

    batches = {"idfa": {}, "gaid": {}, "adid": {}, "dvid": {}}

    _configure_logging(options)

    processed = errors = 0
    fd = gzip.open(fn, "rt")

    for line in fd:
        if not line:
            continue

        appsinstalled = parse_appsinstalled(line)

        if not appsinstalled:
            errors += 1
            return None

        memc_conn = device_memc.get(appsinstalled.dev_type)
        if not memc_conn:
            errors += 1
            logging.error("Unknow device type: %s" % appsinstalled.dev_type)
            return None

        dev_type = appsinstalled.dev_type

        if len(batches[dev_type]) >= MEMC_BATCHES:
            failed_count = insert_appsinstalled(
                memc_conn, batches[dev_type], options.dry
            )
            processed += len(batches[dev_type]) - failed_count
            errors += failed_count
            batches[dev_type] = {}
        else:
            ua = appsinstalled_pb2.UserApps()
            ua.lat = appsinstalled.lat
            ua.lon = appsinstalled.lon
            key = "{}:{}".format(appsinstalled.dev_type, appsinstalled.dev_id)
            ua.apps.extend(appsinstalled.apps)
            packed = ua.SerializeToString()
            batches[dev_type][key] = packed

    for dev_type, batch in batches.items():
        if not batch:
            continue
        memc_conn = device_memc.get(dev_type)
        failed_count = insert_appsinstalled(memc_conn, batch, options.dry)
        processed += len(batch) - failed_count
        errors += failed_count

    if not processed:
        fd.close()
        dot_rename(fn)

    err_rate = float(errors) / processed
    if err_rate < NORMAL_ERR_RATE:
        logging.info("Acceptable error rate (%s). Successfull load" % err_rate)
    else:
        logging.error(
            "High error rate ({} > {}). Failed load".format(err_rate, NORMAL_ERR_RATE)
        )
    fd.close()
    dot_rename(fn)


def main(options):
    pool = []

    for fn in glob.iglob(options.pattern):
        logging.info("Processing %s" % fn)
        process = multiprocessing.Process(
            target=worker,
            args=(
                options,
                fn,
            ),
        )
        process.start()
        pool.append(process)

    for process in pool:
        process.join()


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
    _configure_logging(opts)
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        import time

        start = time.time()
        main(opts)
        end = time.time()
        print(f"--- {end - start}")
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
