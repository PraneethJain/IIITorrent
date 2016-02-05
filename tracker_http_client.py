import logging
import re
from collections import OrderedDict
from typing import List, Optional, cast, Sequence
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

    BYTES_PER_MIB = 2 ** 20

    def announce(self, event: Optional[str]):
        download_info = self._torrent_info.download_info

        logger.debug('announce %s (uploaded = %.1f MiB, downloaded = %.1f MiB, left = %.1f MiB)', event,
                     download_info.total_uploaded / TrackerHTTPClient.BYTES_PER_MIB,
                     download_info.total_downloaded / TrackerHTTPClient.BYTES_PER_MIB,
                     download_info.bytes_left / TrackerHTTPClient.BYTES_PER_MIB)

        params = {
            'info_hash': download_info.info_hash,
            'peer_id': self._our_peer_id,
            'port': 6881,  # FIXME:
            'uploaded': download_info.total_uploaded,
            'downloaded': download_info.total_downloaded,
            'left': download_info.bytes_left,
            'compact': 1,
        }
        if event is not None:
            params['event'] = event
        if self._tracker_id is not None:
            params['trackerid'] = self._tracker_id

        url = self._torrent_info.announce_url + '?' + urlencode(params)
        conn = urlopen(url)
        response = cast(OrderedDict, bencodepy.decode(conn.read()))
        conn.close()
        # FIXME: handle exceptions, use aiohttp

        if b'failure reason' in response:
            raise TrackerError(response[b'failure reason'].decode())

        self.interval = response[b'interval']
        if b'min interval' in response:
            self.min_interval = response[b'min interval']

        self._handle_optional_response_fields(response)

        peers = response[b'peers']
        if isinstance(peers, bytes):
            self._peers = TrackerHTTPClient._parse_compact_peers_list(peers)
        else:
            self._peers = list(map(Peer.from_dict, peers))

        logger.debug('%s peers, interval = %s, min_interval = %s', len(self._peers), self.interval, self.min_interval)

    @property
    def peers(self) -> Sequence[Peer]:
        return self._peers
