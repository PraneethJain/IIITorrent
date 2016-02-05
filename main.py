#!/usr/bin/env python3

import asyncio
import logging
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
    try:
        loop.run_until_complete(manager_task)
    except KeyboardInterrupt:
        manager_task.cancel()
        loop.run_forever()
        # KeyboardInterrupt stops an event loop.
        # For the task to be cancelled, we need to start the loop back up again.
        # run_forever() will actually exit as soon as `task` gets cancelled because the interrupted
        # loop.run_until_complete call added a done_callback to the task that stops the loop.
        # Source: http://stackoverflow.com/a/30766124
    finally:
        loop.close()


if __name__ == '__main__':
    sys.exit(main())
