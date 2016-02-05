import logging
import re
from collections import OrderedDict
from typing import List, Optional, cast
from urllib.parse import urlencode
from urllib.request import urlopen

import bencodepy

from models import TorrentInfo, Peer
from utils import grouper


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class TrackerError(Exception):
    pass


class TrackerHTTPClient:
    def __init__(self, torrent_info: TorrentInfo, our_peer_id: bytes):
        if re.match(r'https?://', torrent_info.announce_url) is None:
            raise ValueError('TrackerHTTPClient expects announce_url with HTTP and HTTPS protocol')

        self._torrent_info = torrent_info
        self._our_peer_id = our_peer_id

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

    def _handle_optional_response_fields(self, response: OrderedDict):
        if b'warning message' in response:
            logger.warning('Tracker returned warning message: %s', response[b'warning message'].decode())

        if b'tracker id' in response:
            self._tracker_id = response[b'tracker id']
        if b'complete' in response:
            self.seed_count = response[b'complete']
        if b'incomplete' in response:
            self.leech_count = response[b'incomplete']

    def announce(self, uploaded: int, downloaded: int, left: int, event: str):
        params = {
            'info_hash': self._torrent_info.download_info.info_hash,
            'peer_id': self._our_peer_id,
            'port': 6881,  # FIXME:
            'uploaded': uploaded,
            'downloaded': downloaded,
            'left': left,
            'event': event,
            'compact': 1,
        }
        if self._tracker_id is not None:
            params['trackerid'] = self._tracker_id

        url = self._torrent_info.announce_url + '?' + urlencode(params)
        conn = urlopen(url)
        response = cast(OrderedDict, bencodepy.decode(conn.read()))
        conn.close()

        if b'failure reason' in response:
            raise TrackerError(response[b'failure reason'].decode())

        self.interval = response[b'interval']
        if b'min interval' in response:
            self.min_interval = response[b'min interval']

        self._handle_optional_response_fields(response)

        peers = response[b'peers']
        if isinstance(peers, bytes):
            peers = TrackerHTTPClient._parse_compact_peers_list(peers)
        else:
            peers = list(map(Peer.from_dict, peers))
        self._peers = self._peers.union(peers)

    @property
    def peers(self) -> set:
        return self._peers
