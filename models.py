import asyncio
import hashlib
import random
import socket
import struct
from collections import OrderedDict
from math import ceil
from typing import List, Set, cast, Optional, Dict

import bencodepy
from bitarray import bitarray

from utils import grouper


def generate_peer_id():
    return bytes(random.randint(0, 255) for _ in range(20))


class Peer:
    def __init__(self, host: str, port: int, peer_id: bytes=None):
        # FIXME: Need we typecheck for the case of malicious data?

        self._host = host
        self._port = port
        self.peer_id = peer_id

        self._hash = hash((host, port))  # Important for performance

    @property
    def host(self) -> str:
        return self._host

    @property
    def port(self) -> int:
        return self._port

    def __eq__(self, other):
        if not isinstance(other, Peer):
            return False
        return self._host == other._host and self._port == other._port

    def __hash__(self):
        return self._hash

    @classmethod
    def from_dict(cls, dictionary: OrderedDict):
        return cls(dictionary[b'ip'].decode(), dictionary[b'port'], dictionary.get(b'peer id'))

    @classmethod
    def from_compact_form(cls, data: bytes):
        ip, port = struct.unpack('!4sH', data)
        host = socket.inet_ntoa(ip)
        return cls(host, port)

    def __repr__(self):
        return '{}:{}'.format(self._host, self._port)


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


class BlockRequest:
    def __init__(self, piece_index: int, block_begin: int, block_length: int):
        self.piece_index = piece_index
        self.block_begin = block_begin
        self.block_length = block_length

    def __eq__(self, other):
        if not isinstance(other, BlockRequest):
            return False
        return self.__dict__ == other.__dict__

    def __hash__(self):
        return hash((self.piece_index, self.block_begin, self.block_length))


class BlockRequestFuture(asyncio.Future, BlockRequest):
    def __init__(self, piece_index: int, block_begin: int, block_length: int):
        asyncio.Future.__init__(self)
        BlockRequest.__init__(self, piece_index, block_begin, block_length)

        self.prev_performers = set()
        self.performer = None

    __eq__ = asyncio.Future.__eq__
    __hash__ = asyncio.Future.__hash__


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
        self._piece_validating = bitarray(piece_count)
        self._piece_validating.setall(False)
        self._piece_downloaded = bitarray(piece_count, endian='big')
        self._piece_downloaded.setall(False)
        self._downloaded_piece_count = 0
        self._piece_selected = bitarray(piece_count)
        self._piece_selected.setall(True)
        # TODO: Download only some files

        blocks_per_piece = ceil(piece_length / DownloadInfo.MARKED_BLOCK_SIZE)
        self._piece_block_downloaded = [None] * piece_count * blocks_per_piece  # type: List[Optional[bitarray]]

        self._interesting_pieces = set()
        self._piece_blocks_expected = [set() for _ in range(self.piece_count)]
        self.total_uploaded = 0
        self.total_downloaded = 0

        self._host_distrust_rates = {}

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

    def reset_run_state(self):
        for owners in self._piece_owners:
            owners.clear()

        self._interesting_pieces.clear()
        for requests in self._piece_blocks_expected:
            if requests is not None:
                requests.clear()
        self.total_uploaded = 0
        self.total_downloaded = 0

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
    def bytes_left(self) -> int:
        result = (self.piece_count - self.downloaded_piece_count) * self.piece_length
        last_piece_index = self.piece_count - 1
        if not self.piece_downloaded[last_piece_index]:
            result += self.get_real_piece_length(last_piece_index) - self.piece_length
        return result

    @property
    def piece_owners(self) -> List[Set[Peer]]:
        return self._piece_owners

    @property
    def piece_sources(self) -> Optional[List[Set[Peer]]]:
        return self._piece_sources

    @property
    def piece_validating(self) -> bitarray:
        return self._piece_validating

    @property
    def piece_downloaded(self) -> bitarray:
        return self._piece_downloaded

    @property
    def downloaded_piece_count(self) -> int:
        return self._downloaded_piece_count

    @property
    def piece_selected(self) -> bitarray:
        return self._piece_selected

    def is_complete(self) -> bool:
        return self._piece_downloaded & self._piece_selected == self._piece_selected

    @property
    def interesting_pieces(self) -> Set[int]:
        return self._interesting_pieces

    def reset_piece(self, index: int):
        self._piece_downloaded[index] = False

        self._piece_sources[index] = set()
        self._piece_block_downloaded[index] = None
        self._piece_blocks_expected[index] = set()

    @property
    def piece_blocks_expected(self) -> List[Optional[Set[BlockRequestFuture]]]:
        return self._piece_blocks_expected

    def mark_downloaded_blocks(self, source: Peer, request: BlockRequest):
        if self._piece_downloaded[request.piece_index]:
            return

        real_piece_length = self.get_real_piece_length(request.piece_index)
        arr = self._piece_block_downloaded[request.piece_index]
        if arr is None:
            arr = bitarray(ceil(real_piece_length / DownloadInfo.MARKED_BLOCK_SIZE))
            arr.setall(False)
            self._piece_block_downloaded[request.piece_index] = arr
        else:
            arr = cast(bitarray, arr)

        mark_begin = ceil(request.block_begin / DownloadInfo.MARKED_BLOCK_SIZE)
        if request.block_begin + request.block_length == real_piece_length:
            mark_end = len(arr)
        else:
            mark_end = (request.block_begin + request.block_length) // DownloadInfo.MARKED_BLOCK_SIZE
        arr[mark_begin:mark_end] = True

        blocks_expected = self._piece_blocks_expected[request.piece_index]
        downloaded_blocks = []
        for fut in blocks_expected:
            query_begin = fut.block_begin // DownloadInfo.MARKED_BLOCK_SIZE
            query_end = ceil((fut.block_begin + fut.block_length) / DownloadInfo.MARKED_BLOCK_SIZE)
            if arr[query_begin:query_end].all():
                downloaded_blocks.append(fut)
                fut.set_result(source)
        for fut in downloaded_blocks:
            blocks_expected.remove(fut)

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
            raise ValueError('The piece is already marked as downloaded')

        return self._piece_block_downloaded is not None and self._piece_block_downloaded[index].all()

    DISTRUST_RATE_TO_BAN = 5

    def increase_distrust(self, peer: Peer):
        self._host_distrust_rates[peer.host] = self._host_distrust_rates.get(peer.host, 0) + 1

    def is_banned(self, peer: Peer) -> bool:
        return (peer.host in self._host_distrust_rates and
                self._host_distrust_rates[peer.host] >= DownloadInfo.DISTRUST_RATE_TO_BAN)


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
