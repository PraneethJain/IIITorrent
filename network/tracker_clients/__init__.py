from urllib.parse import urlparse

from network.tracker_clients.http import *
from network.tracker_clients.udp import *

from models import DownloadInfo
from network.tracker_clients.base import *


def create_tracker_client(announce_url: str, download_info: DownloadInfo, our_peer_id: bytes) -> BaseTrackerClient:
    parsed_announce_url = urlparse(announce_url)
    scheme = parsed_announce_url.scheme
    protocols = {
        'http': HTTPTrackerClient,
        'https': HTTPTrackerClient,
        'udp': UDPTrackerClient,
    }
    if scheme not in protocols:
        raise ValueError('announce_url uses unknown protocol "{}"'.format(scheme))
    client_class = protocols[scheme]

    return client_class(parsed_announce_url, download_info, our_peer_id)
