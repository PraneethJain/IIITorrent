#!/usr/bin/env python3

import asyncio
import logging
import os
import signal
import sys

from control_manager import ControlManager
from models import TorrentInfo


DOWNLOAD_DIR = 'downloads'

STATE_FILENAME = 'state.bin'


logging.basicConfig(format='%(levelname)s %(asctime)s %(name)-23s %(message)s', datefmt='%H:%M:%S')

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def main():
    loop = asyncio.get_event_loop()
    control = ControlManager()
    loop.run_until_complete(control.start())

    if os.path.isfile(STATE_FILENAME):
        with open(STATE_FILENAME, 'rb') as f:
            control.load(f)
    else:
        for arg in sys.argv[1:]:
            torrent_info = TorrentInfo.from_file(arg, download_dir=DOWNLOAD_DIR)
            control.add(torrent_info)

    stopping = False

    def stop_handler():
        nonlocal stopping
        if stopping:
            return
        stopping = True

        stop_task = asyncio.ensure_future(control.stop())
        stop_task.add_done_callback(lambda fut: loop.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_handler)

    try:
        loop.run_forever()
    finally:
        with open(STATE_FILENAME, 'wb') as f:
            control.dump(f)

        loop.close()


if __name__ == '__main__':
    sys.exit(main())
