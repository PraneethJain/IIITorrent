import asyncio
import hashlib
import logging
import random
import time
from collections import deque
from typing import Dict, List, MutableSet, Optional, Tuple, Sequence

import contexttimer

from file_structure import FileStructure
from models import Peer, TorrentInfo
from peer_tcp_client import PeerTCPClient
from tracker_http_client import TrackerHTTPClient


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

TIMER_WARNING_THRESHOLD_MS = 50


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


class NoRequestsError(RuntimeError):
    pass


class TorrentManager:
    def __init__(self, torrent_info: TorrentInfo, our_peer_id: bytes, download_dir: str):
        self._torrent_info = torrent_info
        self._our_peer_id = our_peer_id

        self._tracker_client = TrackerHTTPClient(self._torrent_info, self._our_peer_id)
        self._peer_clients = {}                  # type: Dict[Peer, PeerTCPClient]
        self._peer_hanged_time = {}              # type: Dict[Peer, int]
        self._client_executors = []              # type: List[asyncio.Task]
        self._request_executors = []             # type: List[asyncio.Task]
        self._executors_processed_requests = []  # type: List[List[BlockRequest]]
        self._announcement_executor = None       # type: Optional[asyncio.Task]

        self._non_started_pieces = None   # type: List[int]
        self._request_deque = deque()
        self._peers_busy = set()          # type: MutableSet[Peer]
        self._request_consumption_lock = asyncio.Lock()
        self._endgame_mode = False
        # TODO: Send cancels in endgame mode

        self._file_structure = FileStructure(download_dir, torrent_info.download_info)

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

    FLAG_TRANSMISSION_TIMEOUT = 0.1

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

        download_info.interesting_pieces.add(piece_index)
        alive_piece_owners = set(self._peer_clients.keys()) & download_info.piece_owners[piece_index]
        for peer in alive_piece_owners:
            self._peer_clients[peer].am_interested = True

        choking_owners = [peer for peer in alive_piece_owners if self._peer_clients[peer].peer_choking]
        if len(choking_owners) == len(alive_piece_owners) and choking_owners:
            logger.debug('all piece owners are choking us yet, waiting for an answer for am_interested')
            _, pending = await asyncio.wait([self._peer_clients[peer].drain() for peer in choking_owners],
                                            timeout=TorrentManager.FLAG_TRANSMISSION_TIMEOUT)
            # FIXME: iterate "done" to handle exceptions that can be raised by drain()
            await asyncio.sleep(TorrentManager.FLAG_TRANSMISSION_TIMEOUT)
        # TODO: What if there's no alive_piece_owners?

        logger.debug('piece %s started (owned by %s alive peers, concurrency: %s pieces, %s peers)',
                     piece_index,
                     len(alive_piece_owners), len(download_info.interesting_pieces), len(self._peers_busy) + 1)
        # Count busy peers and a peer for a task launched _start_downloading_piece

    async def _select_piece_to_download(self) -> bool:
        if not self._non_started_pieces:
            return False

        index = min(self._non_started_pieces, key=self.get_piece_order_rate)
        self._non_started_pieces.remove(index)

        await self._start_downloading_piece(index)
        return True

    async def _finish_downloading_piece(self, piece_index: int):
        download_info = self._torrent_info.download_info

        self._flush_piece(piece_index)
        download_info.mark_piece_downloaded(piece_index)

        await self._prepare_requests()
        # We started downloading of next blocks to avoid fibrillation of "am_interested" flag

        download_info.interesting_pieces.remove(piece_index)
        alive_piece_owners = set(self._peer_clients.keys()) & download_info.piece_owners[piece_index]
        for peer in alive_piece_owners:
            for index in download_info.interesting_pieces:
                if peer in download_info.piece_owners[index]:
                    break
            else:
                self._peer_clients[peer].am_interested = False

        for client in self._peer_clients.values():
            client.send_have(piece_index)

        logger.debug('piece %s finished', piece_index)
        progress = download_info.downloaded_piece_count / download_info.piece_count
        logger.info('progress %.1lf%% (%s / %s pieces)', progress * 100,
                    download_info.downloaded_piece_count, download_info.piece_count)

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

    _INF = float('inf')

    PEER_LEAVING_ALONE_TIME = 15

    def get_peer_rate(self, peer: Peer):
        # Manager will download from peers with maximal rate first

        if peer not in self._peer_clients or \
                (peer in self._peer_hanged_time and
                 time.time() - self._peer_hanged_time[peer] <= TorrentManager.PEER_LEAVING_ALONE_TIME):
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

    async def _consume_requests(self, processed_requests: List[BlockRequest],
                                future_to_request: Dict[asyncio.Future, BlockRequest]) -> Peer:
        download_info = self._torrent_info.download_info

        if not await self._prepare_requests():
            raise NoRequestsError('No more undistributed requests')

        first_request = self._request_deque[0]
        available_peers = download_info.piece_owners[first_request.piece_index] & self._peer_clients.keys()
        available_peers = {peer for peer in available_peers if not self._peer_clients[peer].peer_choking}
        available_peers -= self._peers_busy
        if not available_peers:
            raise NotEnoughPeersError('No peers to perform a request')

        self._request_deque.popleft()
        performer = max(available_peers, key=self.get_peer_rate)
        client = self._peer_clients[performer]

        request = first_request
        while True:
            processed_requests.append(request)
            future_to_request[request.downloaded] = request

            client.send_request(*request.client_request)

            if (len(processed_requests) == TorrentManager.DOWNLOAD_REQUEST_QUEUE_SIZE or
                    not await self._prepare_requests()):
                break
            request = self._request_deque[0]
            if performer not in download_info.piece_owners[request.piece_index]:
                break

            self._request_deque.popleft()
        return performer

    NO_REQUESTS_SLEEP_TIME = 2
    NOT_ENOUGH_PEERS_SLEEP_TIME = 5

    REQUEST_TIMEOUT = 6
    REQUEST_TIMEOUT_ENDGAME = 1

    async def _execute_block_requests(self, processed_requests: List[BlockRequest]):
        download_info = self._torrent_info.download_info

        future_to_request = {}
        prev_performer = None
        while True:
            if not processed_requests:
                prev_performer = None
            try:
                with await self._request_consumption_lock:
                    cur_performer = await self._consume_requests(processed_requests, future_to_request)
            except NotEnoughPeersError:
                cur_performer = None
                if not processed_requests:
                    # TODO: Request more peers in some cases (but not too often)
                    #       It should be implemented in announce task, that must wake up this task on case of new peers
                    # TODO: Maybe start another piece?
                    await asyncio.sleep(TorrentManager.NOT_ENOUGH_PEERS_SLEEP_TIME)
                    continue
            except NoRequestsError:
                cur_performer = None
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

            assert prev_performer is not None or cur_performer is not None  # Because processed_requests != []
            if cur_performer is None:  # No new requests added
                cur_performer = prev_performer
            self._peers_busy.add(cur_performer)

            expected_futures = [request.downloaded for request in processed_requests]
            if self._endgame_mode:
                request_timeout = TorrentManager.REQUEST_TIMEOUT_ENDGAME
            else:
                request_timeout = TorrentManager.REQUEST_TIMEOUT
            futures_done, futures_pending = await asyncio.wait(expected_futures, return_when=asyncio.FIRST_COMPLETED,
                                                               timeout=request_timeout)

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
                logger.debug('peer %s hanged, leaving it alone for a while', cur_performer)
                self._peer_hanged_time[cur_performer] = time.time()
                # Sometimes here a new performer can be marked as failed because of problems with an old performer.
                # It's acceptable, because in following requests the old performer will be marked as hanged too
                # (and the new performer will be unlocked soon).

                for fut in futures_pending:
                    self._request_deque.appendleft(future_to_request[fut])
                    del future_to_request[fut]
                processed_requests.clear()

            self._peers_busy.remove(cur_performer)
            prev_performer = cur_performer

    MAX_PEERS_TO_CONNECT = 30
    MAX_PEERS_TO_ACCEPT = 55

    def _connect_to_peers(self, peers: Sequence[Peer]):
        peers = list({peer for peer in peers if peer not in self._peer_clients})
        peers_to_connect_count = max(TorrentManager.MAX_PEERS_TO_CONNECT - len(self._peer_clients), 0)
        logger.debug('connecting to %s new peers', min(len(peers), peers_to_connect_count))

        for peer in peers[:peers_to_connect_count]:
            client = PeerTCPClient(self._torrent_info.download_info, self._file_structure,
                                   self._our_peer_id, peer)
            self._client_executors.append(asyncio.ensure_future(self._execute_peer_client(peer, client)))

    async def _execute_regular_announcements(self):
        download_info = self._torrent_info.download_info

        download_complete = download_info.complete
        try:
            while True:
                await asyncio.sleep(self._tracker_client.interval)

                if not download_complete and download_info.complete:
                    download_complete = True
                    event = 'completed'
                else:
                    event = None
                await self._tracker_client.announce(event)
                self._connect_to_peers(self._tracker_client.peers)
        finally:
            await self._tracker_client.announce('stopped')

    async def download(self):
        download_info = self._torrent_info.download_info

        self._non_started_pieces = list(range(download_info.piece_count))
        random.shuffle(self._non_started_pieces)

        await self._tracker_client.announce('started')
        self._connect_to_peers(self._tracker_client.peers)

        for _ in range(TorrentManager.DOWNLOAD_PEER_COUNT):
            processed_requests = []
            self._executors_processed_requests.append(processed_requests)
            self._request_executors.append(asyncio.ensure_future(self._execute_block_requests(processed_requests)))
        await asyncio.wait(self._request_executors)

        self._announcement_executor = asyncio.ensure_future(self._execute_regular_announcements())

        # TODO: upload

        logger.info('file download complete')

    async def stop(self):
        self._announcement_executor.cancel()
        await asyncio.wait([self._announcement_executor])
        self._announcement_executor = None

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

        self._file_structure.close()
