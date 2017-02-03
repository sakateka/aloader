#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Two step async/await loader example

Usage:
    loader.py <dir> -t HOST -p PARAMS [-b N] [-g GLOB] [--headers HEADERS]
    loader.py (-h | --help | --version)

Options:
    -h, --help                  Show this screen and exit.
    -t HOST, --target HOST      Upload endpoint.
    -p PARAMS, --params PARAMS  Query target GET params (json string)
    -b N, --batch N             Query batch size [default: 4].
    -g GLOB, --glob GLOB        Filter files to upload [default: *].
    --headers HEADRES           Custom headers dict (json string)

Author: Sergey Kacheev (sakateka)
"""

import sys
import glob
import logging
import time
import json
import os
import shutil
import asyncio
import aiohttp
from os.path import exists, basename, join, isfile
from docopt import docopt

logging.basicConfig(
    level=logging.INFO,
    format="{o}%(asctime)s{w} [%(levelname)-6s] {g}%(funcName)s{w}: %(message)s".format(
        w="\x1b[0m", o="\x1b[33m", g="\x1b[32m"
    )
)
log = logging.getLogger()


async def query_target(loop, args, file_path):
    "Query target urls for post and pool"

    fname = basename(file_path)
    fname_target = file_path + ".target"
    url = "{}/upload-url".format(args["--target"], fname)
    params = json.loads(args["--params"])
    params["path"] = fname
    headers = args["--headers"]
    if not headers:
        headers = '{}'
    headers = json.loads(headers)
    headers["User-Agent"] = "Async/Await loader example"
    try:
        now = time.time()
        urls = {}
        if exists(fname_target):
            with open(fname_target) as fd:
                urls = json.load(fd)
        if not urls.get("uploaded") and urls.get("acquire_time", now - 1200) <= now - 1200:
            log.info("Query target for: %s", fname)
            async with aiohttp.ClientSession(
                    connector=aiohttp.TCPConnector(verify_ssl=False)) as session:
                async with session.post(url, params=params, headers=headers) as target:
                    if target.status != 200:
                        raise Exception(url, target, target.reason)
                    log.info("Query target response OK.")
                    resp = await target.json()
                    resp["acquire_time"] = now
                    with open(fname_target, "w") as status:
                        status.write(json.dumps(resp, indent=2))
                    log.debug("Query target response data: %s", json.dumps(resp))
        else:
            log.info("Target for '%s' alredy exists and fresh, skip query.", fname)

        # try upload
        await loop.create_task(upload_file(file_path, args['junk']))
    except:
        log.exception("{0[0].__name__}: {0[1]}".format(sys.exc_info()))


async def upload_file(fname, junk):
    "Upload files in specified directory"

    status_file = fname + ".target"

    if not exists(status_file):
        log.info("Skip uploading for file %s, target not exists", fname)
        return

    try:
        with open(status_file, "r+") as fsf:
            urls = json.load(fsf)
            post_uri = urls["post-target"]
            stat_uri = urls["poll-result"]
            data = {"file": open(fname, "rb")}
            if not urls.get("uploaded"):
                log.info("Upload file: %s", fname)
                async with aiohttp.ClientSession(
                        connector=aiohttp.TCPConnector(verify_ssl=False)) as session:
                    async with session.post(post_uri, data=data, expect100=True) as resp:
                        if resp.status < 200 or resp.status > 299:  # on error move to failed subdir
                            for f2move in glob.glob(fname + "*"):
                                shutil.move(f2move, junk)
                            raise Exception(post_uri, resp, resp.reason)
                        urls["uploaded"] = True
                        fsf.seek(0)
                        fsf.write(json.dumps(urls, indent=2))

                        data = await resp.json()
                        log.info("Upload response: %s", data)
            else:
                log.info("File '%s' alredy uploaded, skip.", fname)
            await get_status(stat_uri, fname)
    except:
        log.exception("{0[0].__name__}: {0[1]}".format(sys.exc_info()))


async def get_status(url, fname):
    "Query status of upload"

    try:
        stat = None
        log.info("Query status for: %s", fname)
        async with aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(verify_ssl=False)) as session:
            async with session.get(url) as sresp:
                stat = await sresp.json()
        status = open(fname + ".status", "a+")
        log.info("Write status: %s", fname)
        stat["=>> QueryTime <<="] = time.ctime()
        status.write(json.dumps(stat, indent=2))
        status.write("\n")
    except:
        log.exception("{0[0].__name__}: {0[1]}".format(sys.exc_info()))


async def loader(loop, args):
    "Do all stuff"

    log.info("Given arguments %s", json.dumps(args))
    dir_name = args['<dir>']
    batch_size = int(args['--batch'])
    batch = []

    for fname in glob.glob(join(dir_name, args["--glob"])):
        if not isfile(fname):
            continue

        log.info("Add job for: %s", fname)
        batch.append(query_target(loop, args, fname))
        if batch_size <= len(batch):
            await asyncio.wait(batch)
            batch = []
    if batch:
        await asyncio.wait(batch)
        batch = []


if __name__ == "__main__":
    arguments = docopt(__doc__, version="0.0.1")
    arguments["junk"] = join(arguments['<dir>'], "failed-to-upload")

    if not exists(arguments["junk"]):
        os.makedirs(arguments["junk"])

    loop = asyncio.get_event_loop()
    loop.run_until_complete(loader(loop, arguments))
    loop.set_debug(True)
    loop.close()
