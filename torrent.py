#!/usr/bin/env python3

import asyncio
import hashlib
import logging
import os
import random
import re
import socket
import struct
import sys
from bisect import bisect_right
from collections import OrderedDict
from enum import Enum
from functools import partial
from math import ceil
from urllib.parse import urlencode
from urllib.request import urlopen
from typing import cast, Optional, List, BinaryIO, Iterable, Tuple, MutableSet, MutableSequence

import bencodepy
from bitarray import bitarray

from utils import grouper


logging.basicConfig(format='%(levelname)s %(asctime)s %(name)-23s %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


DOWNLOAD_DIR = 'downloads'


def generate_peer_id():
    return bytes(random.randint(0, 255) for i in range(20))


class Peer:
    def __init__(self, host: str, port: int, peer_id: bytes=None):
        # FIXME: Need we typecheck for the case of malicious data?

        self.host = host
        self.port = port
        self.peer_id = peer_id

    def __eq__(self, other):
        if not isinstance(other, Peer):
            return False
        return self.host == other.host and self.port == other.port

    def __hash__(self):
        return hash((self.host, self.port))

    @classmethod
    def from_dict(cls, dictionary: OrderedDict):
        return cls(dictionary[b'ip'], dictionary[b'port'], dictionary.get(b'peer id'))

    @classmethod
    def from_compact_form(cls, data: bytes):
        ip, port = struct.unpack('!4sH', data)
        host = socket.inet_ntoa(ip)
        return cls(host, port)

    def __repr__(self):
        return '{}:{}'.format(self.host, self.port)


class FileInfo:
    def __init__(self, length: int, path: List[str], *, md5sum: Optional[str]=None):
        self.length = length
        self.path = path
        self.md5sum = md5sum

    @classmethod
    def from_dict(cls, dictionary: OrderedDict):
        if b'path' in dictionary:
            path = list(map(bytes.decode, dictionary[b'path']))
        else:
            path = []

        return cls(dictionary[b'length'], path, md5sum=dictionary.get(b'md5sum'))


SHA1_DIGEST_LEN = 20


class DownloadInfo:
    MARKED_BLOCK_SIZE = 2 ** 10

    def __init__(self, info_hash: bytes,
                 piece_length: int, piece_hashes: List[bytes], suggested_name: str, files: List[FileInfo], *,
                 private: bool=False):
        self.info_hash = info_hash
        self.piece_length = piece_length
        self.piece_hashes = piece_hashes
        self.suggested_name = suggested_name
        self.files = files
        self.private = private

        piece_count = len(piece_hashes)
        self._piece_owners = [set() for _ in range(piece_count)]
        self._piece_sources = [set() for _ in range(piece_count)]
        self._piece_downloaded = bitarray(piece_count)
        self._piece_downloaded.setall(False)

        blocks_per_piece = ceil(piece_length / DownloadInfo.MARKED_BLOCK_SIZE)
        self._piece_block_downloaded = [None] * piece_count * blocks_per_piece  # type: List[Optional[bitarray]]
        self._piece_blocks_expected = [set() for _ in range(piece_count)]

    @classmethod
    def from_dict(cls, dictionary: OrderedDict):
        info_hash = hashlib.sha1(bencodepy.encode(dictionary)).digest()

        if len(dictionary[b'pieces']) % SHA1_DIGEST_LEN != 0:
            raise ValueError('Invalid length of "pieces" string')
        piece_hashes = grouper(dictionary[b'pieces'], SHA1_DIGEST_LEN)

        if b'files' in dictionary:
            files = list(map(FileInfo.from_dict, dictionary[b'files']))
        else:
            files = [FileInfo.from_dict(dictionary)]

        return cls(info_hash,
                   dictionary[b'piece length'], piece_hashes, dictionary[b'name'].decode(), files,
                   private=dictionary.get('private', False))

    @property
    def piece_count(self) -> int:
        return len(self.piece_hashes)

    def get_real_piece_length(self, index: int) -> int:
        if index == self.piece_count - 1:
            return self.total_size - self.piece_length * (self.piece_count - 1)
        else:
            return self.piece_length

    @property
    def total_size(self) -> int:
        return sum(file.length for file in self.files)

    @property
    def piece_owners(self) -> List[MutableSet[Peer]]:
        return self._piece_owners

    @property
    def piece_sources(self) -> Optional[List[MutableSet[Peer]]]:
        return self._piece_sources

    @property
    def piece_downloaded(self) -> MutableSequence[bool]:
        return self._piece_downloaded

    def reset_piece(self, index: int):
        self._piece_downloaded[index] = False

        self._piece_sources[index] = set()
        self._piece_block_downloaded[index] = None
        self._piece_blocks_expected[index] = set()

    @property
    def piece_blocks_expected(self) -> List[Optional[MutableSet[Tuple[int, int, asyncio.Future]]]]:
        return self._piece_blocks_expected

    def mark_downloaded_blocks(self, piece_index: int, begin: int, length: int):
        if self._piece_downloaded[piece_index]:
            raise ValueError('The piece is already downloaded')

        arr = self._piece_block_downloaded[piece_index]
        if arr is None:
            arr = bitarray(ceil(self.get_real_piece_length(piece_index) / DownloadInfo.MARKED_BLOCK_SIZE))
            arr.setall(False)
            self._piece_block_downloaded[piece_index] = arr

        mark_begin = ceil(begin / DownloadInfo.MARKED_BLOCK_SIZE)
        mark_end = (begin + length) // DownloadInfo.MARKED_BLOCK_SIZE
        arr[mark_begin:mark_end] = True

        cur_piece_blocks_expected = self.piece_blocks_expected[piece_index]
        realized_requests = []
        for request in cur_piece_blocks_expected:
            begin, length, future = request
            query_begin = begin // DownloadInfo.MARKED_BLOCK_SIZE
            query_end = ceil((begin + length) / DownloadInfo.MARKED_BLOCK_SIZE)
            if arr[query_begin:query_end].all():
                realized_requests.append(request)
                future.set_result(True)
        for request in realized_requests:
            cur_piece_blocks_expected.remove(request)

    def is_all_piece_blocks_downloaded(self, index: int):
        arr = self._piece_block_downloaded[index]
        return arr is not None and arr.all()

    def mark_piece_downloaded(self, index: int):
        if self._piece_downloaded[index]:
            raise ValueError('The piece is already validated')

        self._piece_downloaded[index] = True

        # Delete data structures for this piece to save memory
        self._piece_sources[index] = None
        self._piece_block_downloaded[index] = None
        self._piece_blocks_expected[index] = None


class TorrentInfo:
    def __init__(self, download_info: DownloadInfo, announce_url: str):
        # TODO: maybe implement optional fields

        self.download_info = download_info
        self.announce_url = announce_url

    @classmethod
    def from_file(cls, filename):
        dictionary = cast(OrderedDict, bencodepy.decode_from_file(filename))
        download_info = DownloadInfo.from_dict(dictionary[b'info'])
        return cls(download_info, dictionary[b'announce'].decode())


class MessageType(Enum):
    choke = 0
    unchoke = 1
    interested = 2
    not_interested = 3
    have = 4
    bitfield = 5
    request = 6
    piece = 7
    cancel = 8
    port = 9


class PeerTCPClient:
    def __init__(self, download_info: DownloadInfo, file_structure: 'FileStructure', our_peer_id: bytes, peer: Peer):
        self._download_info = download_info
        self._file_structure = file_structure
        self._our_peer_id = our_peer_id
        self._peer = peer

        self._logger = logging.getLogger('[{}]'.format(peer))
        self._logger.setLevel(logging.DEBUG)

        self._am_choking = True
        self._am_interested = False
        self._peer_choking = True
        self._peer_interested = False

        self._downloaded = 0
        self._uploaded = 0
        self._distrust_rate = 0

        self._reader = None  # type: asyncio.StreamReader
        self._writer = None  # type: asyncio.StreamWriter
        self._connected = False

    PEER_HANDSHAKE_MESSAGE = b'BitTorrent protocol'

    @asyncio.coroutine
    def _perform_handshake(self):
        info_hash = self._download_info.info_hash

        message = PeerTCPClient.PEER_HANDSHAKE_MESSAGE
        message_len = len(message)
        handshake_data = (bytes([message_len]) + message + b'\0' * 8 +
                          info_hash + self._our_peer_id)
        self._writer.write(handshake_data)
        self._logger.debug('handshake sent')

        response = yield from self._reader.readexactly(len(handshake_data))
        # FIXME: timeouts?

        if response[:message_len + 1] != handshake_data[:message_len + 1]:
            raise ValueError('Unknown protocol')
        offset = message_len + 1 + 8

        if response[offset:offset + SHA1_DIGEST_LEN] != info_hash:
            raise ValueError("info_hashes don't match")
        offset += SHA1_DIGEST_LEN

        actual_peer_id = response[offset:offset + len(self._our_peer_id)]
        if self._peer.peer_id is not None and self._peer.peer_id != actual_peer_id:
            raise ValueError('Unexpected peer_id')
        elif self._peer.peer_id is None:
            self._peer.peer_id = actual_peer_id

        self._logger.debug('handshake performed')

    CONNECT_TIMEOUT = 5.0

    @asyncio.coroutine
    def connect(self):
        self._logger.debug('trying to connect')

        self._reader, self._writer = yield from asyncio.wait_for(
            asyncio.open_connection(self._peer.host, self._peer.port), PeerTCPClient.CONNECT_TIMEOUT)
        self._logger.debug('connected')

        try:
            yield from self._perform_handshake()
        except:
            self.close()
            raise

        self._connected = True

    @property
    def am_choking(self):
        return self._am_choking

    @property
    def am_interested(self):
        return self._am_interested

    def _check_connect(self):
        if not self._connected:
            raise RuntimeError("Can't change state when the client isn't connected")

    @am_choking.setter
    def am_choking(self, value: bool):
        self._check_connect()
        if self._am_choking != value:
            self._am_choking = value
            self._send_message(MessageType.choke if value else MessageType.unchoke)

    @am_interested.setter
    def am_interested(self, value: bool):
        self._check_connect()
        if self._am_interested != value:
            self._am_interested = value
            self._send_message(MessageType.interested if value else MessageType.not_interested)

    @property
    def peer_choking(self):
        return self._peer_choking

    @property
    def peer_interested(self):
        return self._peer_interested

    @property
    def downloaded(self):
        return self._downloaded

    @property
    def uploaded(self):
        return self._uploaded

    @property
    def distrust_rate(self):
        return self._distrust_rate

    def increase_distrust(self):
        self._distrust_rate += 1

    @asyncio.coroutine
    def _receive_message(self) -> Tuple[MessageType, bytes]:
        data = yield from self._reader.readexactly(4)
        (length,) = struct.unpack('!I', data)
        if length == 0:  # keep-alive
            return b''

        # FIXME: Don't receive too much stuff
        data = yield from self._reader.readexactly(length)
        message_id = MessageType(data[0])
        payload = memoryview(data)[1:]

        self._logger.debug('incoming message %s %s', message_id.name, repr(payload[:12].tobytes()))

        return message_id, payload

    def _send_message(self, message_id: MessageType=None, *payload: List[bytes]):
        if message_id is None:  # keep-alive
            self._writer.write('\0' * 4)

        size = sum(len(portion) for portion in payload) + 1

        self._logger.debug('outcoming message %s size=%s', message_id.name, size)

        self._writer.write(struct.pack('!IB', size, message_id.value))
        for portion in payload:
            self._writer.write(portion)

    @staticmethod
    def _check_payload_len(message_id: MessageType, payload: bytes, expected_len: int):
        if len(payload) != expected_len:
            raise ValueError('Invalid payload length on message_id = {} '
                             '(expected {}, got {})'.format(message_id.name, expected_len, len(payload)))

    def _handle_setting_states(self, message_id: MessageType, payload: bytes):
        PeerTCPClient._check_payload_len(message_id, payload, 0)

        if message_id == MessageType.choke:
            self._peer_choking = True
        elif message_id == MessageType.unchoke:
            self._peer_choking = False
        elif message_id == MessageType.interested:
            self._peer_interested = True
        elif message_id == MessageType.not_interested:
            self._peer_interested = False

    def _handle_haves(self, message_id: MessageType, payload: bytes):
        if message_id == MessageType.have:
            (index,) = struct.unpack('!I', payload)
            self._download_info.piece_owners[index].add(self._peer)
        elif message_id == MessageType.bitfield:
            piece_count = self._download_info.piece_count
            PeerTCPClient._check_payload_len(message_id, payload, int(ceil(piece_count / 8)))

            arr = bitarray(endian='big')
            arr.frombytes(payload.tobytes())  # FIXME: Fix types
            for i in range(piece_count):
                if arr[i]:
                    self._download_info.piece_owners[i].add(self._peer)
            for i in range(piece_count, len(arr)):
                if arr[i]:
                    raise ValueError('Spare bits in "bitfield" message must be zero')

    MAX_REQUEST_LENGTH = 2 ** 17

    def _check_position_range(self, piece_index: int, begin: int, length: int):
        if piece_index < 0 or piece_index >= self._download_info.piece_count:
            raise IndexError('Piece index out of range')
        if (begin < 0 or begin + length > self._download_info.piece_length or
                piece_index * self._download_info.piece_length + begin + length > self._download_info.total_size):
            raise IndexError('Position in piece out of range')

    def _send_block(self, piece_index: int, begin: int, length: int):
        block = self._file_structure.read(piece_index * self._download_info.piece_length + begin, length)

        self._send_message(MessageType.piece, struct.pack('!2I', piece_index, begin), block)

        self._uploaded += length

    @asyncio.coroutine
    def _process_requests(self, message_id: MessageType, payload: bytes):
        piece_index, begin, length = struct.unpack('!3I', payload)
        self._check_position_range(piece_index, begin, length)

        if message_id == MessageType.request:
            if length > PeerTCPClient.MAX_REQUEST_LENGTH:
                raise ValueError('Requested {} bytes, but the current policy allows to accept requests '
                                 'of not more than {} bytes'.format(length, PeerTCPClient.MAX_REQUEST_LENGTH))

            if (self._am_choking or not self._peer_interested or
                    not self._download_info.piece_downloaded[piece_index]):
                # We shouldn't disconnect in case of request of unready piece
                # also because its download flag can be removed for some reasons
                return
            # FIXME: Check that one block isn't requested for many times?

            yield from self.drain()
            # FIXME: Check here if block hasn't been cancelled. We need sure that cancel message can be received
            # FIXME: (run this as a task? avoid DoS in implementing; we should can receive and send "simultaneously")
            self._send_block(piece_index, begin, length)
        elif message_id == MessageType.cancel:
            pass

    def _handle_block(self, message_id: MessageType, payload: bytes):
        if not self._am_interested:
            return

        fmt = '!2I'
        piece_index, begin = struct.unpack_from(fmt, payload)
        block = memoryview(payload)[struct.calcsize(fmt):]
        length = len(block)
        self._check_position_range(piece_index, begin, length)

        if self._download_info.piece_downloaded[piece_index] or not length:
            return

        self._downloaded += length

        self._file_structure.write(piece_index * self._download_info.piece_length + begin, block)

        self._download_info.mark_downloaded_blocks(piece_index, begin, length)
        self._download_info.piece_sources[piece_index].add(self._peer)


    @asyncio.coroutine
    def run(self):
        while True:
            message_id, payload = yield from self._receive_message()
            # FIXME: send keep-alives (or do it in another Task)

            if message_id in (MessageType.choke, MessageType.unchoke,
                              MessageType.interested, MessageType.not_interested):
                self._handle_setting_states(message_id, payload)
            elif message_id in (MessageType.have, MessageType.bitfield):
                self._handle_haves(message_id, payload)
            elif message_id in (MessageType.request, MessageType.cancel):
                yield from self._process_requests(message_id, payload)
            elif message_id == MessageType.piece:
                self._handle_block(message_id, payload)
            elif message_id == MessageType.port:
                PeerTCPClient._check_payload_len(message_id, payload, 2)
                # TODO (?): Ignore or implement DHT
            else:
                # TODO: Unknown message, we can log it
                pass

    def send_request(self, piece_index: int, begin: int, length: int):  # FIXME:
        self._check_position_range(piece_index, begin, length)
        if self._peer not in self._download_info.piece_owners[piece_index]:
            raise ValueError("Peer doesn't have this piece")

        self._send_message(MessageType.request, struct.pack('!3I', piece_index, begin, length))

    @asyncio.coroutine
    def drain(self):
        yield from self._writer.drain()

    def close(self):
        self._writer.close()

        self._connected = False


class FileStructure:
    def __init__(self, download_dir: str, download_info: DownloadInfo):
        self._download_info = download_info

        self._descriptors = []
        self._offsets = []
        offset = 0

        try:
            for file in download_info.files:
                path = os.path.join(download_dir, download_info.suggested_name, *file.path)
                directory = os.path.dirname(path)
                if not os.path.isdir(directory):
                    os.makedirs(os.path.normpath(directory))

                f = open(path, 'r+b')
                f.truncate(file.length)

                self._descriptors.append(f)
                self._offsets.append(offset)
                offset += file.length
        except (OSError, IOError):
            for f in self._descriptors:
                f.close()
            raise

        self._offsets.append(offset)  # Fake entry for convenience

    def _iter_files(self, offset: int, data_length: int) -> Iterable[Tuple[BinaryIO, int, int]]:
        if offset < 0 or offset + data_length > self._download_info.total_size:
            raise IndexError('Data position out of range')

        # Find rightmost file which start offset less than or equal to `offset`
        index = bisect_right(self._offsets, offset) - 1

        while data_length != 0:
            file_start_offset = self._offsets[index]
            file_end_offset = self._offsets[index + 1]
            file_pos = offset - file_start_offset
            bytes_to_operate = min(file_end_offset - offset, data_length)

            descriptor = self._descriptors[index]
            yield descriptor, file_pos, bytes_to_operate

            offset += bytes_to_operate
            data_length -= bytes_to_operate
            index += 1

    def read(self, offset: int, length: int) -> bytes:
        result = []
        for f, file_pos, bytes_to_operate in self._iter_files(offset, length):
            f.seek(file_pos)
            result.append(f.read(bytes_to_operate))
        return b''.join(result)

    def write(self, offset: int, data: memoryview):
        for f, file_pos, bytes_to_operate in self._iter_files(offset, len(data)):
            f.seek(file_pos)
            f.write(data[:bytes_to_operate])
            f.flush()

            data = data[bytes_to_operate:]

    def close(self):
        for f in self._descriptors:
            f.close()


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
            # FIXME: Can do something
            pass

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


class TorrentManager:
    def __init__(self, torrent_info: TorrentInfo, our_peer_id: bytes):
        self._torrent_info = torrent_info
        self._our_peer_id = our_peer_id

        self._tracker_client = TrackerHTTPClient(self._torrent_info, self._our_peer_id)

        self._peer_clients = {}
        self._peer_tasks = {}
        self._peers_interested_to_me = set()

        self._file_structure = FileStructure(DOWNLOAD_DIR, torrent_info.download_info)

    @staticmethod
    @asyncio.coroutine
    def _handle_peer(client: PeerTCPClient):
        yield from client.connect()
        try:
            yield from client.run()
        finally:
            client.close()

    def _add_peer_client(self, peer: Peer):
        client = PeerTCPClient(self._torrent_info.download_info, self._file_structure,
                               self._our_peer_id, peer)
        task = asyncio.async(TorrentManager._handle_peer(client))
        task.add_done_callback(partial(self._remove_peer_client, peer))
        self._peer_clients[peer] = client
        self._peer_tasks[peer] = task

    def _remove_peer_client(self, peer: Peer, task: asyncio.Task):
        if not task.cancelled():
            logger.debug('%s disconnected because %s', peer, repr(task.exception()))
        del self._peer_clients[peer]
        del self._peer_tasks[peer]

    REQUEST_LENGTH = 2 ** 14

    def _validate_piece(self, index: int) -> bool:
        download_info = self._torrent_info.download_info

        piece_offset = index * download_info.piece_length
        data = self._file_structure.read(piece_offset, download_info.get_real_piece_length(index))
        actual_digest = hashlib.sha1(data).digest()
        return actual_digest == download_info.piece_hashes[index]

    _INF = float('inf')

    def get_peer_rate(self, peer: Peer):
        """Manager will download from peers with maximal rate first."""

        if peer not in self._peer_clients:
            return -TorrentManager._INF
        client = self._peer_clients[peer]

        rate = client.downloaded  # To reach maximal download speed
        # TODO: Try to measure speed for last minute + "optimistic interest"
        rate += client.uploaded  # They owe us for our uploading
        rate -= 2 ** client.distrust_rate
        return rate

    @asyncio.coroutine
    def _download_piece(self, piece_index: int):
        download_info = self._torrent_info.download_info

        for peer in self._peers_interested_to_me - download_info.piece_owners[piece_index]:
            if peer in self._peer_clients:
                self._peer_clients[peer].am_interested = False
            self._peers_interested_to_me.remove(peer)

        while True:
            cur_piece_length = download_info.get_real_piece_length(piece_index)
            cur_piece_owners = download_info.piece_owners[piece_index]
            cur_piece_blocks_expected = download_info.piece_blocks_expected[piece_index]
            for block_begin in range(0, cur_piece_length, TorrentManager.REQUEST_LENGTH):
                block_end = min(block_begin + TorrentManager.REQUEST_LENGTH, cur_piece_length)
                block_length = block_end - block_begin

                block_downloaded = asyncio.Future()
                request = (block_begin, block_length, block_downloaded)
                cur_piece_blocks_expected.add(request)

                progress = block_begin / cur_piece_length
                logger.debug('downloading block - %.1lf%% (%s/%s)', progress * 100,
                             block_begin, cur_piece_length)

                # TODO: refactor, separate to _download_block
                while True:
                    logger.debug('piece owned by %s of %s connected peers',
                                 len(cur_piece_owners & set(self._peer_clients)), len(self._peer_clients))

                    for peer in sorted(cur_piece_owners, key=self.get_peer_rate, reverse=True):
                        if peer not in self._peer_clients:
                            continue
                        logger.debug('try to download from peer with rate %s', self.get_peer_rate(peer))
                        client = self._peer_clients[peer]

                        # TODO: check whether we choked?
                        client.am_interested = True
                        self._peers_interested_to_me.add(peer)
                        client.send_request(piece_index, block_begin, block_length)
                        # FIXME: Don't request twice?
                        # FIXME: If request failed, will we got an exception here? It's inadmissible

                        try:
                            yield from asyncio.wait_for(asyncio.shield(block_downloaded), 7.0)  # FIXME: timeout
                            break
                        except asyncio.TimeoutError:
                            pass
                    if block_downloaded.done():
                        break
                    logger.debug('iteration failed')

                    yield from asyncio.sleep(7.0)
                    # TODO: Request more peers here if we can

            if not download_info.is_all_piece_blocks_downloaded(piece_index):
                raise RuntimeError("Some piece blocks aren't downloaded")
            if self._validate_piece(piece_index):
                download_info.mark_piece_downloaded(piece_index)
                logger.debug('piece %s downloaded and valid', piece_index)
                return
            else:
                for peer in download_info.piece_sources[piece_index]:
                    if peer not in self._peer_clients:
                        continue
                    self._peer_clients[peer].increase_distrust()

                download_info.reset_piece(piece_index)
                logger.debug('piece %s not valid, redownloading', piece_index)

    def get_piece_order_rate(self, index: int):
        owners = self._torrent_info.download_info.piece_owners[index]
        return len(owners) if owners else TorrentManager._INF

    @asyncio.coroutine
    def download(self):
        download_info = self._torrent_info.download_info

        self._tracker_client.announce(0, 0, download_info.total_size, 'started')
        peers_to_connect = list(self._tracker_client.peers)[:25]
        # FIXME: handle exceptions, use aiohttp

        for peer in peers_to_connect:
            # FIXME: don't connect to our own server (compare peer_ids?)
            self._add_peer_client(peer)

        remaining_piece_indexes = list(range(download_info.piece_count))
        random.shuffle(remaining_piece_indexes)
        while remaining_piece_indexes:
            cur_piece = min(remaining_piece_indexes, key=self.get_piece_order_rate)

            logger.debug('downloading piece #%s', cur_piece)
            downloaded_piece_count = download_info.piece_count - len(remaining_piece_indexes)
            progress = downloaded_piece_count / download_info.piece_count
            logger.info('progress %.1lf%% (%s / %s pieces)', progress * 100,
                         downloaded_piece_count, download_info.piece_count)

            yield from self._download_piece(cur_piece)

            remaining_piece_indexes.remove(cur_piece)
        logger.info('file download complete')

        # TODO: announces, upload

    @asyncio.coroutine
    def stop(self):
        self._file_structure.close()

        for task in self._peer_tasks.values():
            task.cancel()
        if self._peer_tasks:
            yield from asyncio.wait(self._peer_tasks.values())


@asyncio.coroutine
def handle_download(manager: TorrentManager):
    try:
        yield from manager.download()
    finally:
        yield from manager.stop()


def main():
    torrent_filename = sys.argv[1]

    torrent_info = TorrentInfo.from_file(torrent_filename)
    our_peer_id = generate_peer_id()

    torrent_manager = TorrentManager(torrent_info, our_peer_id)

    loop = asyncio.get_event_loop()
    manager_task = asyncio.async(handle_download(torrent_manager))
    try:
        loop.run_until_complete(manager_task)
    except KeyboardInterrupt:
        # KeyboardInterrupt is not a subclass of Exception and need special handling
        manager_task.cancel()
        loop.run_forever()
        # For the task to be cancelled, we need to start the loop back up again.
        # run_forever() will actually exit as soon as `task` gets cancelled because the interrupted
        # loop.run_until_complete call added a done_callback to the task that stops the loop.
        # Source: http://stackoverflow.com/a/30766124
    finally:
        loop.close()


if __name__ == '__main__':
    sys.exit(main())
