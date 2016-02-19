#!/usr/bin/env python3
import argparse
import asyncio
import logging
import os
import re
import signal
import sys
from contextlib import closing
from functools import partial

import torrent_formatters
from control_client import ControlClient
from control_manager import ControlManager
from control_server import ControlServer
from models import TorrentInfo


logging.basicConfig(format='%(levelname)s %(asctime)s %(name)-23s %(message)s', datefmt='%H:%M:%S')


STATE_FILENAME = 'state.bin'


def run_daemon(args):
    with closing(asyncio.get_event_loop()) as loop:
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


def show_handler(args):
    torrent_info = TorrentInfo.from_file(args.filename, download_dir=None)
    content_description = torrent_formatters.join_lines(
        torrent_formatters.format_title(torrent_info) + torrent_formatters.format_content(torrent_info))
    print(content_description, end='')


PATH_SPLIT_RE = re.compile(r'/|{}'.format(os.path.sep))


async def add_handler(args):
    torrents = [TorrentInfo.from_file(filename, download_dir=args.download_dir) for filename in args.filenames]

    if args.include:
        paths = args.include
        mode = 'whitelist'
    elif args.exclude:
        paths = args.exclude
        mode = 'blacklist'
    else:
        paths = None
        mode = None
    if mode is not None:
        if len(torrents) > 1:
            raise ValueError('Can\'t handle "--include" and "--exclude" when several files are added')
        torrent_info = torrents[0]

        paths = [PATH_SPLIT_RE.split(path) for path in paths]
        torrent_info.download_info.select_files(paths, mode)

    async with ControlClient() as client:
        for info in torrents:
            await client.execute(partial(ControlManager.add, torrent_info=info))


async def control_action_handler(args):
    action = getattr(ControlManager, args.action)
    torrents = [TorrentInfo.from_file(filename, download_dir=None) for filename in args.filenames]
    # FIXME: Execute action with all torrents if torrents == []

    async with ControlClient() as client:
        for info in torrents:
            await client.execute(partial(action, info_hash=info.download_info.info_hash))


def status_server_handler(manager: ControlManager) -> str:
    torrents = list(manager.get_torrents())
    if not torrents:
        return 'No torrents added'

    torrents.sort(key=lambda info: info.download_info.suggested_name)
    return '\n'.join(
        torrent_formatters.join_lines(
            torrent_formatters.format_title(info) + torrent_formatters.format_status(info))
        for info in torrents).rstrip()


async def status_handler(args):
    async with ControlClient() as client:
        status_text = await client.execute(status_server_handler)

    print(status_text)


DEFAULT_DOWNLOAD_DIR = 'downloads'


def run_in_event_loop(coro_function, args):
    with closing(asyncio.get_event_loop()) as loop:
        loop.run_until_complete(coro_function(args))


def main():
    parser = argparse.ArgumentParser(description='A prototype of BitTorrent client (console management tool)')
    parser.add_argument('--debug', action='store_true',
                        help='Show debug messages')
    parser.set_defaults(func=lambda args: print('Use option "--help" to show usage.', file=sys.stderr))
    subparsers = parser.add_subparsers(description='Specify an action before "--help" to show parameters for it.',
                                       metavar='ACTION', dest='action')

    subparser = subparsers.add_parser('start', help='Start a daemon')
    subparser.set_defaults(func=run_daemon)

    subparser = subparsers.add_parser('show', help="Show torrent content (no daemon required)")
    subparser.add_argument('filename', help='Torrent file name')
    subparser.set_defaults(func=show_handler)

    subparser = subparsers.add_parser('add', help='Add a new torrent')
    subparser.add_argument('filenames', nargs='+',
                           help='Torrent file names')
    subparser.add_argument('-d', '--download-dir', default=DEFAULT_DOWNLOAD_DIR,
                           help='Download directory')
    group = subparser.add_mutually_exclusive_group()
    group.add_argument('--include', action='append',
                       help='Download only files and directories specified in "--include" options')
    group.add_argument('--exclude', action='append',
                       help='Download all files and directories except those that specified in "--exclude" options')
    subparser.set_defaults(func=partial(run_in_event_loop, add_handler))

    control_commands = ['pause', 'resume', 'remove']
    for command_name in control_commands:
        subparser = subparsers.add_parser(command_name, help='{} torrent'.format(command_name.capitalize()))
        subparser.add_argument('filenames', nargs='*' if command_name != 'remove' else '+',
                               help='Torrent file names')
        subparser.set_defaults(func=partial(run_in_event_loop, control_action_handler))

    subparser = subparsers.add_parser('status', help='Show status')
    subparser.set_defaults(func=partial(run_in_event_loop, status_handler))

    arguments = parser.parse_args()
    if not arguments.debug:
        logging.disable(logging.INFO)
    try:
        arguments.func(arguments)
    except (ValueError, RuntimeError) as e:
        print('Error: {}'.format(e), file=sys.stderr)


if __name__ == '__main__':
    sys.exit(main())
