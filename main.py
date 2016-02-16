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
from utils import humanize_size, humanize_speed

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


PROGRESS_BAR_WIDTH = 50


def format_torrent_info(torrent_info: TorrentInfo):
    download_info = torrent_info.download_info
    result = 'Name: {}\n'.format(download_info.suggested_name)
    result += 'ID: {}\n'.format(download_info.info_hash.hex())

    if torrent_info.paused:
        state = 'Paused'
    elif download_info.is_complete():
        state = 'Uploading'
    else:
        state = 'Downloading'
    result += 'State: {}\n'.format(state)

    result += 'Download from: {}/{} peers\t'.format(download_info.downloading_peer_count, download_info.peer_count)
    result += 'Upload to: {}/{} peers\n'.format(download_info.uploading_peer_count, download_info.peer_count)

    result += 'Download speed: {}\t'.format(
        humanize_speed(download_info.download_speed) if download_info.download_speed is not None else 'unknown')
    result += 'Upload speed: {}\n'.format(
        humanize_speed(download_info.upload_speed) if download_info.download_speed is not None else 'unknown')

    last_piece_length = download_info.get_real_piece_length(download_info.piece_count - 1)
    downloaded_size = download_info.downloaded_piece_count * download_info.piece_length
    if download_info.piece_downloaded[-1]:
        downloaded_size += last_piece_length - download_info.piece_length
    selected_size = download_info.piece_selected.count() * download_info.piece_length
    if download_info.piece_selected[-1]:
        selected_size += last_piece_length - download_info.piece_length
    result += 'Size: {}/{}\t'.format(humanize_size(downloaded_size), humanize_size(selected_size))

    if download_info.total_downloaded:
        ratio = download_info.total_uploaded / download_info.total_downloaded
    else:
        ratio = 0
    result += 'Ratio: {:.1f}\n'.format(ratio)

    progress = downloaded_size / selected_size
    progress_bar = ('#' * round(progress * PROGRESS_BAR_WIDTH)).ljust(PROGRESS_BAR_WIDTH)
    result += 'Progress: {:5.1f}% [{}]\n'.format(progress * 100, progress_bar)

    return result


async def status_handler(args):
    torrent_list = await delegate_to_control(ControlManager.get_torrents)
    torrent_list.sort(key=lambda item: item.download_info.suggested_name)
    print('\n'.join(map(format_torrent_info, torrent_list)), end='')


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
    except Exception as e:
        print('Critical error: {}'.format(e), file=sys.stderr)
    finally:
        loop.close()


if __name__ == '__main__':
    sys.exit(main())
