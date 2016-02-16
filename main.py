#!/usr/bin/env python3
import argparse
import asyncio
import logging
import os
import signal
import sys
from functools import partial
from typing import Callable, TypeVar

from control_client import ControlClient
from control_manager import ControlManager
from control_server import ControlServer
from models import TorrentInfo


DOWNLOAD_DIR = 'downloads'

STATE_FILENAME = 'state.bin'


logging.basicConfig(format='%(levelname)s %(asctime)s %(name)-23s %(message)s', datefmt='%H:%M:%S')

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def run_daemon(args):
    loop = asyncio.get_event_loop()

    control = ControlManager()
    loop.run_until_complete(control.start())

    if os.path.isfile(STATE_FILENAME):
        with open(STATE_FILENAME, 'rb') as f:
            control.load(f)

    control_server = ControlServer(control)
    loop.run_until_complete(control_server.start())

    stopping = False

    def stop_handler():
        nonlocal stopping
        if stopping:
            return
        stopping = True

        stop_task = asyncio.ensure_future(asyncio.wait([control_server.stop(), control.stop()]))
        stop_task.add_done_callback(lambda fut: loop.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_handler)

    try:
        loop.run_forever()
    finally:
        with open(STATE_FILENAME, 'wb') as f:
            control.dump(f)


T = TypeVar('T')


async def delegate_to_control(action: Callable[[ControlManager], T]) -> T:
    client = ControlClient()
    await client.connect()
    try:
        return await client.execute(action)
    finally:
        client.close()


async def add_handler(args):
    torrent_info = TorrentInfo.from_file(args.filename, download_dir=DOWNLOAD_DIR)
    await delegate_to_control(partial(ControlManager.add, torrent_info=torrent_info))


async def pause_handler(args):
    torrent_info = TorrentInfo.from_file(args.filename, download_dir=DOWNLOAD_DIR)
    await delegate_to_control(partial(ControlManager.pause, info_hash=torrent_info.download_info.info_hash))


async def resume_handler(args):
    torrent_info = TorrentInfo.from_file(args.filename, download_dir=DOWNLOAD_DIR)
    await delegate_to_control(partial(ControlManager.resume, info_hash=torrent_info.download_info.info_hash))


async def remove_handler(args):
    torrent_info = TorrentInfo.from_file(args.filename, download_dir=DOWNLOAD_DIR)
    await delegate_to_control(partial(ControlManager.remove, info_hash=torrent_info.download_info.info_hash))


async def status_handler(args):
    torrent_list = await delegate_to_control(ControlManager.get_torrents)
    torrent_list.sort(key=lambda torrent_info: torrent_info.download_info.suggested_name)
    for torrent_info in torrent_list:
        download_info = torrent_info.download_info
        selected_piece_count = download_info.piece_selected.count()
        progress = download_info.downloaded_piece_count / selected_piece_count
        print('{} - {:.1f}%'.format(torrent_info.download_info.suggested_name, progress * 100))


def main():
    parser = argparse.ArgumentParser(description='A prototype of BitTorrent client')
    subparsers = parser.add_subparsers(help='action')

    loop = asyncio.get_event_loop()

    parser_start = subparsers.add_parser('start', help='start a daemon')
    parser_start.set_defaults(func=run_daemon)

    subparser = subparsers.add_parser('add', help='add new torrent')
    subparser.add_argument('filename', help='Torrent filename')
    subparser.set_defaults(func=lambda args: loop.run_until_complete(add_handler(args)))

    subparser = subparsers.add_parser('pause', help='pause torrent')
    subparser.add_argument('filename', help='Torrent filename')
    subparser.set_defaults(func=lambda args: loop.run_until_complete(pause_handler(args)))

    subparser = subparsers.add_parser('resume', help='resume torrent')
    subparser.add_argument('filename', help='Torrent filename')
    subparser.set_defaults(func=lambda args: loop.run_until_complete(resume_handler(args)))

    subparser = subparsers.add_parser('remove', help='remove torrent')
    subparser.add_argument('filename', help='Torrent filename')
    subparser.set_defaults(func=lambda args: loop.run_until_complete(remove_handler(args)))

    subparser = subparsers.add_parser('status', help='show status')
    subparser.set_defaults(func=lambda args: loop.run_until_complete(status_handler(args)))

    try:
        arguments = parser.parse_args()
        arguments.func(arguments)
    finally:
        loop.close()


if __name__ == '__main__':
    sys.exit(main())
