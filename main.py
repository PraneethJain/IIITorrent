#!/usr/bin/env python3

import asyncio
import logging
import signal
import sys

from models import TorrentInfo, generate_peer_id
from torrent_manager import TorrentManager


DOWNLOAD_DIR = 'downloads'


logging.basicConfig(format='%(levelname)s %(asctime)s %(name)-23s %(message)s', datefmt='%H:%M:%S')


async def execute_download(manager: TorrentManager):
    try:
        await manager.download()
    finally:
        await manager.stop()


def main():
    torrent_filename = sys.argv[1]

    torrent_info = TorrentInfo.from_file(torrent_filename)
    our_peer_id = generate_peer_id()

    torrent_manager = TorrentManager(torrent_info, our_peer_id, DOWNLOAD_DIR)

    loop = asyncio.get_event_loop()
    manager_task = asyncio.ensure_future(execute_download(torrent_manager))

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, manager_task.cancel)

    try:
        loop.run_until_complete(manager_task)
    except asyncio.CancelledError:
        pass
    finally:
        loop.close()


if __name__ == '__main__':
    sys.exit(main())
