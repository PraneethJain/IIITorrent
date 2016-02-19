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
from models import DownloadInfo, TorrentInfo
from utils import humanize_size, humanize_speed


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
    torrent_info = TorrentInfo.from_file(args.filename, download_dir=args.download_dir)
    await delegate_to_control(partial(ControlManager.add, torrent_info=torrent_info))


async def control_action_handler(action, args):
    torrent_info = TorrentInfo.from_file(args.filename, download_dir=None)
    await delegate_to_control(partial(action, info_hash=torrent_info.download_info.info_hash))


COLUMN_WIDTH = 30
PROGRESS_BAR_WIDTH = 50


def format_torrent_info(torrent_info: TorrentInfo):
    download_info = torrent_info.download_info  # type: DownloadInfo
    statistics = download_info.session_statistics
    lines = ['Name: {}\n'.format(download_info.suggested_name),
             'ID: {}\n'.format(download_info.info_hash.hex())]

    if torrent_info.paused:
        state = 'Paused'
    elif download_info.complete:
        state = 'Uploading'
    else:
        state = 'Downloading'
    lines.append('State: {}\n'.format(state))

    lines.append('Download from: {}/{} peers\t'.format(statistics.downloading_peer_count, statistics.peer_count))
    lines.append('Upload to: {}/{} peers\n'.format(statistics.uploading_peer_count, statistics.peer_count))

    lines.append('Download speed: {}\t'.format(
        humanize_speed(statistics.download_speed) if statistics.download_speed is not None else 'unknown'))
    lines.append('Upload speed: {}\n'.format(
        humanize_speed(statistics.upload_speed) if statistics.upload_speed is not None else 'unknown'))

    last_piece_info = download_info.pieces[-1]
    downloaded_size = download_info.downloaded_piece_count * download_info.piece_length
    if last_piece_info.downloaded:
        downloaded_size += last_piece_info.length - download_info.piece_length
    selected_piece_count = sum(1 for info in download_info.pieces if info.selected)
    selected_size = selected_piece_count * download_info.piece_length
    if last_piece_info.selected:
        selected_size += last_piece_info.length - download_info.piece_length
    lines.append('Size: {}/{}\t'.format(humanize_size(downloaded_size), humanize_size(selected_size)))

    ratio = statistics.total_uploaded / statistics.total_downloaded if statistics.total_downloaded else 0
    lines.append('Ratio: {:.1f}\n'.format(ratio))

    progress = downloaded_size / selected_size
    progress_bar = ('#' * round(progress * PROGRESS_BAR_WIDTH)).ljust(PROGRESS_BAR_WIDTH)
    lines.append('Progress: {:5.1f}% [{}]\n'.format(progress * 100, progress_bar))

    return ''.join(line[:-1].ljust(COLUMN_WIDTH) if line.endswith('\t') else line for line in lines)


async def status_handler(args):
    torrent_list = await delegate_to_control(ControlManager.get_torrents)
    torrent_list.sort(key=lambda item: item.download_info.suggested_name)
    print('\n'.join(map(format_torrent_info, torrent_list)), end='')


DEFAULT_DOWNLOAD_DIR = 'downloads'


def main():
    parser = argparse.ArgumentParser(description='A prototype of BitTorrent client')
    subparsers = parser.add_subparsers(help='action')

    loop = asyncio.get_event_loop()

    parser_start = subparsers.add_parser('start', help='start a daemon')
    parser_start.set_defaults(func=run_daemon)

    subparser = subparsers.add_parser('add', help='add new torrent')
    subparser.add_argument('filename', help='Torrent filename')
    subparser.add_argument('-d', '--download-dir', default=DEFAULT_DOWNLOAD_DIR,
                           help='download directory')
    subparser.set_defaults(func=lambda args: loop.run_until_complete(add_handler(args)))

    control_commands = ['pause', 'resume', 'remove']
    for command_name in control_commands:
        subparser = subparsers.add_parser(command_name, help='{} torrent'.format(command_name))
        subparser.add_argument('filename', help='Torrent filename')
        subparser.set_defaults(func=lambda args, action=getattr(ControlManager, command_name):
                               loop.run_until_complete(control_action_handler(action, args)))

    subparser = subparsers.add_parser('status', help='show status')
    subparser.set_defaults(func=lambda args: loop.run_until_complete(status_handler(args)))

    try:
        arguments = parser.parse_args()
        arguments.func(arguments)
    except (ValueError, RuntimeError) as e:
        print('Error: {}'.format(e), file=sys.stderr)
    finally:
        loop.close()


if __name__ == '__main__':
    sys.exit(main())
