import asyncio
import copy
import logging
import pickle
from typing import Dict, List

from models import generate_peer_id, TorrentInfo
from peer_tcp_server import PeerTCPServer
from torrent_manager import TorrentManager
from utils import import_signals


QObject, pyqtSignal = import_signals()


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class ControlManager(QObject):
    def __init__(self):
        super().__init__()

        self._our_peer_id = generate_peer_id()

        self._torrents = {}          # type: Dict[bytes, TorrentInfo]
        self._torrent_managers = {}  # type: Dict[bytes, TorrentManager]

        self._server = PeerTCPServer(self._our_peer_id, self._torrent_managers)

        self._torrent_manager_executors = {}  # type: Dict[bytes, asyncio.Task]

    def get_torrents(self) -> List[TorrentInfo]:
        return list(self._torrents.values())

    async def start(self):
        await self._server.start()

    def _start_torrent_manager(self, torrent_info: TorrentInfo):
        info_hash = torrent_info.download_info.info_hash

        manager = TorrentManager(torrent_info, self._our_peer_id, self._server.port)
        self._torrent_managers[info_hash] = manager
        self._torrent_manager_executors[info_hash] = asyncio.ensure_future(manager.run())

    if pyqtSignal:
        torrents_changed = pyqtSignal()
        # torrent_added, torrent_changed, torrent_removed
        # Information message should be formed in ControlManager thread and then
        # transferred as strings to avoid lock problems

    def add(self, torrent_info: TorrentInfo):
        info_hash = torrent_info.download_info.info_hash
        if info_hash in self._torrents:
            raise ValueError('This torrent is already added')

        if not torrent_info.paused:
            self._start_torrent_manager(torrent_info)
        self._torrents[info_hash] = torrent_info

        if pyqtSignal:
            self.torrents_changed.emit()

    def resume(self, info_hash: bytes):
        if info_hash not in self._torrents:
            raise ValueError('Torrent not found')
        torrent_info = self._torrents[info_hash]
        if not torrent_info.paused:
            raise ValueError('The torrent is already running')

        self._start_torrent_manager(torrent_info)

        torrent_info.paused = False

    async def _stop_torrent_manager(self, info_hash: bytes):
        manager_executor = self._torrent_manager_executors[info_hash]
        manager_executor.cancel()
        try:
            await manager_executor
        except asyncio.CancelledError:
            pass
        del self._torrent_manager_executors[info_hash]

        manager = self._torrent_managers[info_hash]
        await manager.stop()
        del self._torrent_managers[info_hash]

    async def remove(self, info_hash: bytes):
        if info_hash not in self._torrents:
            raise ValueError('Torrent not found')
        torrent_info = self._torrents[info_hash]

        del self._torrents[info_hash]
        if not torrent_info.paused:
            await self._stop_torrent_manager(info_hash)

    async def pause(self, info_hash: bytes):
        if info_hash not in self._torrents:
            raise ValueError('Torrent not found')
        torrent_info = self._torrents[info_hash]
        if torrent_info.paused:
            raise ValueError('The torrent is already paused')

        await self._stop_torrent_manager(info_hash)

        torrent_info.paused = True

    def dump(self, f):
        torrent_list = []
        for manager, torrent_info in self._torrents.items():
            torrent_info = copy.copy(torrent_info)
            torrent_info.download_info = copy.copy(torrent_info.download_info)
            torrent_info.download_info.reset_run_state()
            torrent_list.append(torrent_info)

        pickle.dump(torrent_list, f)

        logger.info('state saved (%s torrents)', len(torrent_list))

    def load(self, f):
        torrent_list = pickle.load(f)

        for torrent_info in torrent_list:
            self.add(torrent_info)

        logger.info('state recovered (%s torrents)', len(torrent_list))

    async def stop(self):
        await self._server.stop()

        for task in self._torrent_manager_executors.values():
            task.cancel()
        if self._torrent_manager_executors:
            await asyncio.wait(self._torrent_manager_executors.values())

        if self._torrent_managers:
            await asyncio.wait([manager.stop() for manager in self._torrent_managers.values()])
