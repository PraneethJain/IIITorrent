import asyncio
import copy
import logging
import pickle
from typing import Dict

from models import generate_peer_id, TorrentInfo
from peer_tcp_server import PeerTCPServer
from torrent_manager import TorrentManager


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class ControlManager:
    def __init__(self):
        self._our_peer_id = generate_peer_id()
        self._torrent_managers = {}  # type: Dict[bytes, TorrentManager]
        self._server = PeerTCPServer(self._our_peer_id, self._torrent_managers)

        self._torrent_manager_executors = {}  # type: Dict[bytes, asyncio.Task]

    async def start(self):
        await self._server.start()

    def add(self, torrent_info: TorrentInfo):
        info_hash = torrent_info.download_info.info_hash
        if info_hash in self._torrent_managers:
            raise ValueError('This torrent is already added')

        manager = TorrentManager(torrent_info, self._our_peer_id, self._server.port)
        self._torrent_managers[info_hash] = manager
        self._torrent_manager_executors[info_hash] = asyncio.ensure_future(manager.run())

        logger.info('"%s" added', torrent_info.download_info.suggested_name)

    def dump(self, f):
        torrents = []
        for manager in self._torrent_managers.values():
            torrent_info = copy.copy(manager.torrent_info)
            torrent_info.download_info = copy.copy(torrent_info.download_info)
            torrent_info.download_info.reset_run_state()
            torrents.append(torrent_info)

        pickle.dump(torrents, f)

        logger.info('state saved (%s torrents)', len(torrents))

    def load(self, f):
        torrents = pickle.load(f)

        for torrent_info in torrents:
            self.add(torrent_info)

        logger.info('state recovered (%s torrents)', len(torrents))

    async def stop(self):
        for task in self._torrent_manager_executors.values():
            task.cancel()
        if self._torrent_manager_executors:
            await asyncio.wait(self._torrent_manager_executors.values())

        if self._torrent_managers:
            await asyncio.wait([manager.stop() for manager in self._torrent_managers.values()])

        await self._server.stop()
