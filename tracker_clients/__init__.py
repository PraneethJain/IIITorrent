from urllib.parse import urlparse

from models import TorrentInfo
from tracker_clients.base import *
from tracker_clients.http import *
from tracker_clients.udp import *


def create_tracker_client(torrent_info: TorrentInfo, our_peer_id: bytes) -> BaseTrackerClient:
    parsed_announce_url = urlparse(torrent_info.announce_url)
    scheme = parsed_announce_url.scheme
    protocols = {
        'http': HTTPTrackerClient,
        'https': HTTPTrackerClient,
        'udp': UDPTrackerClient,
    }
    if scheme not in protocols:
        raise ValueError('announce_url uses unknown protocol "{}"'.format(scheme))
    client_class = protocols[scheme]

    return client_class(torrent_info, parsed_announce_url, our_peer_id)
