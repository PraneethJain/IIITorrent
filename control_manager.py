import asyncio
from typing import Dict

from models import generate_peer_id, TorrentInfo
from peer_tcp_server import PeerTCPServer
from torrent_manager import TorrentManager


class ControlManager:
    def __init__(self):
        self._our_peer_id = generate_peer_id()
        self._torrent_managers = {}           # type: Dict[bytes, TorrentManager]
        self._server = PeerTCPServer(self._our_peer_id, self._torrent_managers)
        self._torrent_manager_executors = {}  # type: Dict[bytes, asyncio.Task]

    async def start(self):
        await self._server.start()

    def add(self, torrent_info: TorrentInfo, download_dir: str):
        info_hash = torrent_info.download_info.info_hash
        manager = TorrentManager(torrent_info, self._our_peer_id, self._server.port, download_dir)
        self._torrent_managers[info_hash] = manager
        self._torrent_manager_executors[info_hash] = asyncio.ensure_future(manager.run())

    async def stop(self):
        for task in self._torrent_manager_executors.values():
            task.cancel()
        if self._torrent_manager_executors:
            await asyncio.wait(self._torrent_manager_executors.values())
        self._torrent_manager_executors.clear()

        if self._torrent_managers:
            await asyncio.wait([manager.stop() for manager in self._torrent_managers.values()])
        self._torrent_managers.clear()

        await self._server.stop()
