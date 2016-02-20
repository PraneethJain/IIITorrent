import logging
import urllib.parse
from collections import OrderedDict
from typing import cast, Optional

import aiohttp
import bencodepy

from models import Peer, TorrentInfo
from tracker_clients.base import BaseTrackerClient, TrackerError, parse_compact_peers_list, EventType
from utils import humanize_size


__all__ = ['HTTPTrackerClient']


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class HTTPTrackerClient(BaseTrackerClient):
    def __init__(self, torrent_info: TorrentInfo, parsed_announce_url: urllib.parse.ParseResult, our_peer_id: bytes):
        super().__init__(torrent_info, our_peer_id)
        if parsed_announce_url.scheme not in ('http', 'https'):
            raise ValueError('TrackerHTTPClient expects announce_url with HTTP and HTTPS protocol')

        self._tracker_id = None   # type: Optional[bytes]

        self._session = aiohttp.ClientSession()

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
            self._peers = parse_compact_peers_list(peers)
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

    async def announce(self, server_port: int, event: EventType):
        logger.debug('announce %s (uploaded = %s, downloaded = %s, left = %s)', event.name,
                     humanize_size(self._statistics.uploaded_per_session),
                     humanize_size(self._statistics.downloaded_per_session),
                     humanize_size(self._download_info.bytes_left))

        params = {
            'info_hash': self._download_info.info_hash,
            'peer_id': self._our_peer_id,
            'port': server_port,
            'uploaded': self._statistics.total_uploaded,
            'downloaded': self._statistics.total_downloaded,
            'left': self._download_info.bytes_left,
            'compact': 1,
        }
        if event != EventType.none:
            params['event'] = event.name
        if self._tracker_id is not None:
            params['trackerid'] = self._tracker_id

        with aiohttp.Timeout(HTTPTrackerClient.REQUEST_TIMEOUT):
            async with self._session.get(self._torrent_info.announce_url, params=params) as conn:
                response = await conn.read()

        response = bencodepy.decode(response)
        if not response:
            if event == EventType.started:
                raise ValueError('Tracker returned an empty answer on start announcement')
            return
        response = cast(OrderedDict, response)

        self._handle_primary_response_fields(response)
        self._handle_optional_response_fields(response)

        logger.debug('%s peers, interval = %s, min_interval = %s', len(self._peers), self.interval, self.min_interval)

    def close(self):
        self._session.close()
