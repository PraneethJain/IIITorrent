#!/usr/bin/env python3

import asyncio
import logging
import os
import pickle
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
            torrent_info = pickle.load(f)
        logger.info('state recovered')
    else:
        torrent_filename = sys.argv[1]
        torrent_info = TorrentInfo.from_file(torrent_filename)
        logger.info('new torrent loaded')
    control.add(torrent_info, DOWNLOAD_DIR)

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
        torrent_info.download_info.reset_run_state()
        with open(STATE_FILENAME, 'wb') as f:
            pickle.dump(torrent_info, f)
        logger.info('state saved')

        loop.close()


if __name__ == '__main__':
    sys.exit(main())
