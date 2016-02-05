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
from collections import deque, OrderedDict
from enum import Enum
from math import ceil
from typing import cast, Dict, Optional, List, BinaryIO, Iterable, Tuple, MutableSet, MutableSequence
from urllib.parse import urlencode
from urllib.request import urlopen

import bencodepy
import contexttimer
import time
from bitarray import bitarray

from utils import grouper

TORRENT_MANAGER_LOGGING_LEVEL = logging.DEBUG
PEER_CLIENT_LOGGING_LEVEL = logging.INFO

TIMER_WARNING_THRESHOLD_MS = 50


logging.basicConfig(format='%(levelname)s %(asctime)s %(name)-23s %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)
logger.setLevel(TORRENT_MANAGER_LOGGING_LEVEL)


DOWNLOAD_DIR = 'downloads'


def generate_peer_id():
    return bytes(random.randint(0, 255) for _ in range(20))


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
        return cls(dictionary[b'ip'].decode(), dictionary[b'port'], dictionary.get(b'peer id'))

    @classmethod
    def from_compact_form(cls, data: bytes):
        ip, port = struct.unpack('!4sH', data)
        host = socket.inet_ntoa(ip)
        return cls(host, port)

    def __repr__(self):
        return '{}:{}'.format(self.host, self.port)


class FileInfo:
    def __init__(self, length: int, path: List[str], *, md5sum: str=None):
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
        if ceil(self.total_size / piece_length) != piece_count:
            raise ValueError('Invalid count of piece hashes')

        self._piece_owners = [set() for _ in range(piece_count)]
        self._piece_sources = [set() for _ in range(piece_count)]
        self._piece_downloaded = bitarray(piece_count)
        self._piece_downloaded.setall(False)
        self._downloaded_piece_count = 0

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

    @property
    def downloaded_piece_count(self):
        return self._downloaded_piece_count

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
        else:
            arr = cast(bitarray, arr)

        mark_begin = ceil(begin / DownloadInfo.MARKED_BLOCK_SIZE)
        mark_end = (begin + length) // DownloadInfo.MARKED_BLOCK_SIZE
        arr[mark_begin:mark_end] = True

        cur_piece_blocks_expected = self._piece_blocks_expected[piece_index]
        downloaded_blocks = []
        for block_info in cur_piece_blocks_expected:
            begin, length, future = block_info
            query_begin = begin // DownloadInfo.MARKED_BLOCK_SIZE
            query_end = ceil((begin + length) / DownloadInfo.MARKED_BLOCK_SIZE)
            if arr[query_begin:query_end].all():
                downloaded_blocks.append(block_info)
                future.set_result(True)
        for block_info in downloaded_blocks:
            cur_piece_blocks_expected.remove(block_info)

    def mark_piece_downloaded(self, index: int):
        if self._piece_downloaded[index]:
            raise ValueError('The piece is already downloaded')

        self._piece_downloaded[index] = True
        self._downloaded_piece_count += 1

        # Delete data structures for this piece to save memory
        self._piece_sources[index] = None
        self._piece_block_downloaded[index] = None
        self._piece_blocks_expected[index] = None

    def is_all_piece_blocks_downloaded(self, index: int):
        if self._piece_downloaded[index]:
            raise ValueError('The whole piece is already downloaded')

        return cast(bitarray, self._piece_block_downloaded[index]).all()


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
        self._logger.setLevel(PEER_CLIENT_LOGGING_LEVEL)

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

    async def _perform_handshake(self):
        info_hash = self._download_info.info_hash

        message = PeerTCPClient.PEER_HANDSHAKE_MESSAGE
        message_len = len(message)
        handshake_data = (bytes([message_len]) + message + b'\0' * 8 +
                          info_hash + self._our_peer_id)
        self._writer.write(handshake_data)
        self._logger.debug('handshake sent')

        response = await self._reader.readexactly(len(handshake_data))
        # FIXME: timeouts?

        if response[:message_len + 1] != handshake_data[:message_len + 1]:
            raise ValueError('Unknown protocol')
        offset = message_len + 1 + 8

        if response[offset:offset + SHA1_DIGEST_LEN] != info_hash:
            raise ValueError("info_hashes don't match")
        offset += SHA1_DIGEST_LEN

        actual_peer_id = response[offset:offset + len(self._our_peer_id)]
        if self._our_peer_id == actual_peer_id:
            raise ValueError('Connection to ourselves')
        if self._peer.peer_id is not None and self._peer.peer_id != actual_peer_id:
            raise ValueError('Unexpected peer_id')
        self._peer.peer_id = actual_peer_id

        self._logger.debug('handshake performed')

    CONNECT_TIMEOUT = 3

    async def connect(self):
        self._logger.debug('trying to connect')

        self._reader, self._writer = await asyncio.wait_for(
            asyncio.open_connection(self._peer.host, self._peer.port), PeerTCPClient.CONNECT_TIMEOUT)
        self._logger.debug('connected')

        try:
            await self._perform_handshake()
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

    async def _receive_message(self) -> Optional[Tuple[MessageType, memoryview]]:
        data = await self._reader.readexactly(4)
        (length,) = struct.unpack('!I', data)
        if length == 0:  # keep-alive
            return None

        # FIXME: Don't receive too much stuff
        # TODO: timeouts
        data = await self._reader.readexactly(length)
        try:
            message_id = MessageType(data[0])
        except ValueError:
            logger.debug('Unknown message type %s', data[0])
            return None
        payload = memoryview(data)[1:]

        self._logger.debug('incoming message %s length=%s', message_id.name, length)

        return message_id, payload

    def _send_message(self, message_id: MessageType=None, *payload: List[bytes]):
        if message_id is None:  # keep-alive
            self._writer.write('\0' * 4)

        length = sum(len(portion) for portion in payload) + 1

        self._logger.debug('outcoming message %s length=%s', message_id.name, length)

        self._writer.write(struct.pack('!IB', length, message_id.value))
        for portion in payload:
            self._writer.write(portion)

    @staticmethod
    def _check_payload_len(message_id: MessageType, payload: memoryview, expected_len: int):
        if len(payload) != expected_len:
            raise ValueError('Invalid payload length on message_id = {} '
                             '(expected {}, got {})'.format(message_id.name, expected_len, len(payload)))

    def _handle_setting_states(self, message_id: MessageType, payload: memoryview):
        PeerTCPClient._check_payload_len(message_id, payload, 0)

        if message_id == MessageType.choke:
            self._peer_choking = True
        elif message_id == MessageType.unchoke:
            self._peer_choking = False
        elif message_id == MessageType.interested:
            self._peer_interested = True
        elif message_id == MessageType.not_interested:
            self._peer_interested = False

    def _handle_haves(self, message_id: MessageType, payload: memoryview):
        if message_id == MessageType.have:
            (index,) = struct.unpack('!I', cast(bytes, payload))
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

    async def _process_requests(self, message_id: MessageType, payload: memoryview):
        piece_index, begin, length = struct.unpack('!3I', cast(bytes, payload))
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
            # or that there's not requested too many blocks?
            # FIXME: Check here if block hasn't been cancelled. We need sure that cancel message can be received
            # FIXME: (run this as a task? avoid DoS in implementing; we should can receive and send "simultaneously")
            self._send_block(piece_index, begin, length)
        elif message_id == MessageType.cancel:
            pass

    def _handle_block(self, payload: memoryview):
        if not self._am_interested:
            # For example, we can be not interested in pieces from peers with big distrust rate
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

    async def run(self):
        while True:
            message = await self._receive_message()
            if message is None:
                continue
            message_id, payload = message
            # FIXME: send keep-alives (or do it in another Task)

            if message_id in (MessageType.choke, MessageType.unchoke,
                              MessageType.interested, MessageType.not_interested):
                self._handle_setting_states(message_id, payload)
            elif message_id in (MessageType.have, MessageType.bitfield):
                self._handle_haves(message_id, payload)
            elif message_id in (MessageType.request, MessageType.cancel):
                await self._process_requests(message_id, payload)
            elif message_id == MessageType.piece:
                self._handle_block(payload)
            elif message_id == MessageType.port:
                PeerTCPClient._check_payload_len(message_id, payload, 2)
                # TODO (?): Ignore or implement DHT

    def send_request(self, piece_index: int, begin: int, length: int):  # FIXME:
        self._check_position_range(piece_index, begin, length)
        if self._peer not in self._download_info.piece_owners[piece_index]:
            raise ValueError("Peer doesn't have this piece")

        self._send_message(MessageType.request, struct.pack('!3I', piece_index, begin, length))

    async def drain(self):
        await self._writer.drain()

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
                if not os.path.isfile(path):
                    f = open(path, 'w')
                    f.close()

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

            data = data[bytes_to_operate:]

    def flush(self, offset: int, length: int):
        for f, _, _ in self._iter_files(offset, length):
            f.flush()

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


class BlockRequest:
    def __init__(self, piece_index: int, block_begin: int, block_length: int, downloaded: asyncio.Future):
        self.piece_index = piece_index
        self.block_begin = block_begin
        self.block_length = block_length
        self.downloaded = downloaded

    @property
    def block_info(self) -> Tuple[int, int, asyncio.Future]:
        return self.block_begin, self.block_length, self.downloaded

    @property
    def client_request(self) -> Tuple[int, int, int]:
        return self.piece_index, self.block_begin, self.block_length


class NotEnoughPeersError(RuntimeError):
    pass


class TorrentManager:
    def __init__(self, torrent_info: TorrentInfo, our_peer_id: bytes):
        self._torrent_info = torrent_info
        self._our_peer_id = our_peer_id

        self._tracker_client = TrackerHTTPClient(self._torrent_info, self._our_peer_id)
        self._peer_clients = {}                  # type: Dict[Peer, PeerTCPClient]
        self._peer_hanged_time = {}              # type: Dict[Peer, int]
        self._client_executors = []              # type: List[asyncio.Task]
        self._request_executors = []             # type: List[asyncio.Task]
        self._executors_processed_requests = []  # type: List[List[BlockRequest]]

        self._non_started_pieces = None   # type: List[int]
        self._interesting_pieces = set()  # type: MutableSet[int]
        self._request_deque = deque()
        self._peers_busy = set()          # type: MutableSet[Peer]
        self._endgame_mode = False  # TODO: Send cancels in endgame mode

        self._file_structure = FileStructure(DOWNLOAD_DIR, torrent_info.download_info)

    async def _execute_peer_client(self, peer: Peer, client: PeerTCPClient):
        try:
            await client.connect()

            self._peer_clients[peer] = client

            try:
                await client.run()
            finally:
                client.close()

                del self._peer_clients[peer]
                if peer in self._peer_hanged_time:
                    del self._peer_hanged_time[peer]
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.debug('%s disconnected because of %s', peer, repr(e))

    REQUEST_LENGTH = 2 ** 14

    def _get_piece_position(self, index: int) -> Tuple[int, int]:
        download_info = self._torrent_info.download_info
        piece_offset = index * download_info.piece_length
        cur_piece_length = download_info.get_real_piece_length(index)
        return piece_offset, cur_piece_length

    def _flush_piece(self, index: int):
        piece_offset, cur_piece_length = self._get_piece_position(index)
        with contexttimer.Timer() as timer:
            self._file_structure.flush(piece_offset, cur_piece_length)
        if timer.elapsed >= TIMER_WARNING_THRESHOLD_MS:
            logger.warning('Too long flush (%s ms)', timer.elapsed)

    async def _select_piece_to_download(self) -> bool:
        if not self._non_started_pieces:
            return False

        index = min(self._non_started_pieces, key=self.get_piece_order_rate)
        self._non_started_pieces.remove(index)

        await self._start_downloading_piece(index)
        return True

    async def _start_downloading_piece(self, piece_index: int):
        download_info = self._torrent_info.download_info
        cur_piece_length = download_info.get_real_piece_length(piece_index)

        blocks_expected = download_info.piece_blocks_expected[piece_index]
        for block_begin in range(0, cur_piece_length, TorrentManager.REQUEST_LENGTH):
            block_end = min(block_begin + TorrentManager.REQUEST_LENGTH, cur_piece_length)
            block_length = block_end - block_begin
            request = BlockRequest(piece_index, block_begin, block_length, asyncio.Future())

            blocks_expected.add(request.block_info)
            self._request_deque.append(request)

        self._interesting_pieces.add(piece_index)
        alive_piece_owners = self._peer_clients.keys() & download_info.piece_owners[piece_index]
        for peer in alive_piece_owners:
            self._peer_clients[peer].am_interested = True
        if alive_piece_owners:
            # await asyncio.wait([self._peer_clients[peer].drain() for peer in alive_piece_owners], timeout=2)  # FIXME:
            # await asyncio.sleep(2)  # FIXME:
            pass
        # TODO: What if there's no alive_piece_owners?

        logger.debug('piece %s started (owned by %s alive peers, concurrency: %s pieces, %s peers)',
                     piece_index, len(alive_piece_owners), len(self._interesting_pieces), len(self._peers_busy) + 1)
        # Count busy peers and a peer for a task launched _start_downloading_piece

    async def _validate_piece(self, piece_index: int):
        download_info = self._torrent_info.download_info

        assert download_info.is_all_piece_blocks_downloaded(piece_index)

        piece_offset, cur_piece_length = self._get_piece_position(piece_index)
        with contexttimer.Timer() as timer:
            data = self._file_structure.read(piece_offset, cur_piece_length)
            actual_digest = hashlib.sha1(data).digest()
        if timer.elapsed >= TIMER_WARNING_THRESHOLD_MS:
            logger.warning('Too long hash comparison (%s ms)', timer.elapsed)
        if actual_digest == download_info.piece_hashes[piece_index]:
            await self._finish_downloading_piece(piece_index)
            return

        for peer in download_info.piece_sources[piece_index]:
            if peer not in self._peer_clients:
                continue
            self._peer_clients[peer].increase_distrust()
        download_info.reset_piece(piece_index)

        logger.debug('piece %s not valid, redownloading', piece_index)

    async def _finish_downloading_piece(self, piece_index: int):
        download_info = self._torrent_info.download_info

        self._flush_piece(piece_index)
        download_info.mark_piece_downloaded(piece_index)

        await self._prepare_requests()
        # We started downloading of next blocks to avoid fibrillation of "am_interested" flag
        self._interesting_pieces.remove(piece_index)
        alive_piece_owners = self._peer_clients.keys() & download_info.piece_owners[piece_index]
        for peer in alive_piece_owners:
            for index in self._interesting_pieces:
                if peer in download_info.piece_owners[index]:
                    break
            else:
                self._peer_clients[peer].am_interested = False

        logger.debug('piece %s finished', piece_index)

        progress = download_info.downloaded_piece_count / download_info.piece_count
        logger.info('progress %.1lf%% (%s / %s pieces)', progress * 100,
                    download_info.downloaded_piece_count, download_info.piece_count)

    _INF = float('inf')

    PEER_LEAVING_ALONE_TIME = 20
    PEER_LEAVING_ALONE_TIME_ENDGAME = 40

    def get_peer_rate(self, peer: Peer):
        # Manager will download from peers with maximal rate first

        if self._endgame_mode:
            leaving_alone_time = TorrentManager.PEER_LEAVING_ALONE_TIME_ENDGAME
        else:
            leaving_alone_time = TorrentManager.PEER_LEAVING_ALONE_TIME
        if peer not in self._peer_clients or \
                (peer in self._peer_hanged_time and time.time() - self._peer_hanged_time[peer] <= leaving_alone_time):
            return -TorrentManager._INF
        client = self._peer_clients[peer]

        rate = client.downloaded  # To reach maximal download speed
        # TODO: Try to measure speed for last minute
        rate += client.uploaded  # They owe us for our uploading
        rate -= 2 ** client.distrust_rate
        rate += random.randint(1, 100)  # Helps to shuffle clients in the beginning
        return rate

    def get_piece_order_rate(self, index: int):
        owners = self._torrent_info.download_info.piece_owners[index]
        return len(owners) if owners else TorrentManager._INF

    async def _prepare_requests(self) -> bool:
        while self._request_deque:
            if not self._request_deque[0].downloaded.done():
                return True
            self._request_deque.popleft()

        return await self._select_piece_to_download()

    DOWNLOAD_PEER_COUNT = 20
    DOWNLOAD_REQUEST_QUEUE_SIZE = 10

    async def _append_requests(self, processed_requests: List[BlockRequest],
                               future_to_request: Dict[asyncio.Future, BlockRequest]) -> Optional[Peer]:
        download_info = self._torrent_info.download_info

        if not await self._prepare_requests():
            return None

        first_request = self._request_deque[0]
        available_peers = download_info.piece_owners[first_request.piece_index] & self._peer_clients.keys()
        # available_peers = {peer for peer in available_peers if not self._peer_clients[peer].peer_choking}  # FIXME:
        available_peers -= self._peers_busy
        if not available_peers:
            raise NotEnoughPeersError('No peers to perform a request')

        self._request_deque.popleft()
        performing_peer = max(available_peers, key=self.get_peer_rate)
        client = self._peer_clients[performing_peer]
        client.am_interested = True
        # FIXME: It should be done here, it's a hack for a case when client connected
        #        after sending am_interested

        request = first_request
        while True:
            processed_requests.append(request)
            future_to_request[request.downloaded] = request

            client.send_request(*request.client_request)
            # FIXME: If the request failed, will we got an exception here? It's inadmissible

            if (len(processed_requests) == TorrentManager.DOWNLOAD_REQUEST_QUEUE_SIZE or
                    not await self._prepare_requests()):
                break
            request = self._request_deque[0]
            if performing_peer not in download_info.piece_owners[request.piece_index]:
                break

            self._request_deque.popleft()
        return performing_peer

    NO_REQUESTS_SLEEP_TIME = 2
    NO_PEERS_SLEEP_TIME = 5

    REQUEST_TIMEOUT = 6
    REQUEST_TIMEOUT_ENDGAME = 1

    async def _execute_block_requests(self, processed_requests: List[BlockRequest]):
        download_info = self._torrent_info.download_info

        future_to_request = {}
        performer = None
        while True:
            try:
                new_performer = await self._append_requests(processed_requests, future_to_request)

                if not processed_requests:
                    if not any(self._executors_processed_requests):
                        return

                    if not self._endgame_mode:
                        non_finished_pieces = [i for i in range(download_info.piece_count)
                                               if not download_info.piece_downloaded[i]]
                        logger.info('entering endgame mode (remaining pieces: %s)',
                                    ', '.join(map(str, non_finished_pieces)))
                    self._endgame_mode = True

                    # TODO: maybe use some signals instead of sleeping
                    logger.debug('no requests to process, sleeping')
                    await asyncio.sleep(TorrentManager.NO_REQUESTS_SLEEP_TIME)
                    continue

                if new_performer is not None:
                    performer = new_performer
            except NotEnoughPeersError:
                if not processed_requests:
                    # TODO: Request more peers in some cases (but not too often)
                    #       It should be implemented in announce task, that must wake up this task on case of new peers
                    # TODO: Maybe start another piece?
                    await asyncio.sleep(TorrentManager.NO_PEERS_SLEEP_TIME)
                    continue

            if performer is not None:
                self._peers_busy.add(performer)

            expected_futures = [request.downloaded for request in processed_requests]
            if self._endgame_mode:
                request_timeout = TorrentManager.REQUEST_TIMEOUT_ENDGAME
            else:
                request_timeout = TorrentManager.REQUEST_TIMEOUT
            futures_done, futures_pending = await asyncio.wait(expected_futures, return_when=asyncio.FIRST_COMPLETED,
                                                               timeout=request_timeout)

            if performer is not None:
                self._peers_busy.remove(performer)

            if len(futures_pending) < len(expected_futures):
                for fut in futures_done:
                    piece_index = future_to_request[fut].piece_index
                    if (not download_info.piece_downloaded[piece_index] and
                            not download_info.piece_blocks_expected[piece_index]):
                        await self._validate_piece(piece_index)

                    del future_to_request[fut]
                processed_requests.clear()
                processed_requests += [future_to_request[fut] for fut in futures_pending]
            else:
                logger.debug('peer %s hanged, leaving it alone for a while', performer)
                self._peer_hanged_time[performer] = time.time()
                # Sometimes here a new performer can be marked as hanged instead of an old performer.
                # It's normal, because in following requests the old performer will be marked as hanged too
                # (and the new performer will be unlocked soon).

                for fut in futures_pending:
                    self._request_deque.appendleft(future_to_request[fut])
                    del future_to_request[fut]
                processed_requests.clear()

    async def download(self):
        download_info = self._torrent_info.download_info

        self._non_started_pieces = list(range(download_info.piece_count))
        random.shuffle(self._non_started_pieces)

        logger.debug('announce start')
        self._tracker_client.announce(0, 0, download_info.total_size, 'started')
        peers_to_connect = list(self._tracker_client.peers)
        # FIXME: handle exceptions, use aiohttp

        logger.debug('starting client executors')
        self._client_executors = []
        for peer in peers_to_connect:
            client = PeerTCPClient(self._torrent_info.download_info, self._file_structure,
                                   self._our_peer_id, peer)
            self._client_executors.append(asyncio.ensure_future(self._execute_peer_client(peer, client)))

        logger.debug('starting request executors')
        for _ in range(TorrentManager.DOWNLOAD_PEER_COUNT):
            processed_requests = []
            self._executors_processed_requests.append(processed_requests)
            self._request_executors.append(asyncio.ensure_future(self._execute_block_requests(processed_requests)))
        await asyncio.wait(self._request_executors)

        # TODO: announces, upload

        logger.info('file download complete')

    async def stop(self):
        self._file_structure.close()

        for task in self._request_executors:
            task.cancel()
        if self._request_executors:
            await asyncio.wait(self._request_executors)
        self._request_executors.clear()
        self._executors_processed_requests.clear()

        for task in self._client_executors:
            task.cancel()
        if self._client_executors:
            await asyncio.wait(self._client_executors)
        self._client_executors.clear()


async def handle_download(manager: TorrentManager):
    try:
        await manager.download()
    finally:
        await manager.stop()


def main():
    torrent_filename = sys.argv[1]

    torrent_info = TorrentInfo.from_file(torrent_filename)
    our_peer_id = generate_peer_id()

    torrent_manager = TorrentManager(torrent_info, our_peer_id)

    loop = asyncio.get_event_loop()
    manager_task = asyncio.ensure_future(handle_download(torrent_manager))
    try:
        loop.run_until_complete(manager_task)
    except KeyboardInterrupt:
        manager_task.cancel()
        loop.run_forever()
        # KeyboardInterrupt stops an event loop.
        # For the task to be cancelled, we need to start the loop back up again.
        # run_forever() will actually exit as soon as `task` gets cancelled because the interrupted
        # loop.run_until_complete call added a done_callback to the task that stops the loop.
        # Source: http://stackoverflow.com/a/30766124
    finally:
        loop.close()


if __name__ == '__main__':
    sys.exit(main())
