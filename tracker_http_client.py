import logging
import re
from collections import OrderedDict
from typing import List, Optional, cast, Sequence

import aiohttp
import bencodepy

from models import TorrentInfo, Peer
from utils import grouper, humanize_size

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class TrackerError(Exception):
    pass


class TrackerHTTPClient:
    def __init__(self, torrent_info: TorrentInfo, our_peer_id: bytes):
        if re.match(r'https?://', torrent_info.announce_url) is None:
            raise ValueError('TrackerHTTPClient expects announce_url with HTTP and HTTPS protocol')

        self._torrent_info = torrent_info
        self._download_info = torrent_info.download_info
        self._our_peer_id = our_peer_id

        self._session = aiohttp.ClientSession()

        self._tracker_id = None   # type: Optional[bytes]
        self.interval = None      # type: int
        self.min_interval = None  # type: Optional[int]
        self.seed_count = None    # type: Optional[int]
        self.leech_count = None   # type: Optional[int]

        self._peers = set()

    @staticmethod
    def _parse_compact_peers_list(data: bytes) -> List[Peer]:
        if len(data) % 6 != 0:
            raise ValueError('Invalid length of a compact representation of peers')
        return list(map(Peer.from_compact_form, grouper(data, 6)))

    def _handle_primary_response_fields(self, response: OrderedDict):
        if b'failure reason' in response:
            raise TrackerError(response[b'failure reason'].decode())

        self.interval = response[b'interval']
        if b'min interval' in response:
            self.min_interval = response[b'min interval']
            if self.min_interval > self.interval:
                raise ValueError('Tracker returned min_interval that is greater than a default interval')

        peers = response[b'peers']
        if isinstance(peers, bytes):
            self._peers = TrackerHTTPClient._parse_compact_peers_list(peers)
        else:
            self._peers = list(map(Peer.from_dict, peers))

    def _handle_optional_response_fields(self, response: OrderedDict):
        if b'warning message' in response:
            logger.warning('Tracker returned warning message: %s', response[b'warning message'].decode())

        if b'tracker id' in response:
            self._tracker_id = response[b'tracker id']
        if b'complete' in response:
            self.seed_count = response[b'complete']
        if b'incomplete' in response:
            self.leech_count = response[b'incomplete']

    REQUEST_TIMEOUT = 5

    async def announce(self, server_port: int, event: Optional[str]):
        logger.debug('announce %s (uploaded = %s, downloaded = %s, left = %s)', event,
                     humanize_size(self._download_info.uploaded_per_session),
                     humanize_size(self._download_info.downloaded_per_session),
                     humanize_size(self._download_info.bytes_left))

        params = {
            'info_hash': self._download_info.info_hash,
            'peer_id': self._our_peer_id,
            'port': server_port,
            'uploaded': self._download_info.total_uploaded,
            'downloaded': self._download_info.total_downloaded,
            'left': self._download_info.bytes_left,
            'compact': 1,
        }
        if event is not None:
            params['event'] = event
        if self._tracker_id is not None:
            params['trackerid'] = self._tracker_id

        with aiohttp.Timeout(TrackerHTTPClient.REQUEST_TIMEOUT):
            async with self._session.get(self._torrent_info.announce_url, params=params) as conn:
                response = await conn.read()

        response = bencodepy.decode(response)
        if not response:
            if event == 'started':
                raise ValueError('Tracker returned an empty answer on start announcement')
            return
        response = cast(OrderedDict, response)

        self._handle_primary_response_fields(response)
        self._handle_optional_response_fields(response)

        logger.debug('%s peers, interval = %s, min_interval = %s', len(self._peers), self.interval, self.min_interval)

    @property
    def peers(self) -> Sequence[Peer]:
        return self._peers

    def close(self):
        self._session.close()
