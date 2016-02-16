import asyncio
import hashlib
import itertools
import logging
import random
import time
from collections import deque
from typing import Dict, List, MutableSet, Optional, Tuple, Sequence, Iterable, cast

import contexttimer

from file_structure import FileStructure
from models import BlockRequest, Peer, TorrentInfo
from peer_tcp_client import PeerTCPClient
from tracker_http_client import TrackerHTTPClient


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

TIMER_WARNING_THRESHOLD_MS = 50


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
        self._peer_connected_time = {}           # type: Dict[Peer, float]
        self._peer_hanged_time = {}              # type: Dict[Peer, float]
        self._client_executors = []              # type: List[asyncio.Task]
        self._announcement_executor = None       # type: Optional[asyncio.Task]
        self._uploading_executor = None          # type: Optional[asyncio.Task]
        self._request_executors = []             # type: List[asyncio.Task]
        self._executors_processed_requests = []  # type: List[List[BlockRequest]]

        self._pieces_to_download = None   # type: Sequence[int]
        self._non_started_pieces = None   # type: List[int]
        self._request_deque = deque()
        self._peers_busy = set()          # type: MutableSet[Peer]
        self._request_consumption_lock = asyncio.Lock()
        self._endgame_mode = False
        # TODO: Send cancels in endgame mode

        self._tasks_waiting_for_more_peers = 0
        self._more_peers_requested = asyncio.Event()
        self._request_deque_relevant = asyncio.Event()

        self._file_structure = FileStructure(download_dir, torrent_info.download_info)

    async def _execute_peer_client(self, peer: Peer, client: PeerTCPClient):
        try:
            await client.connect()

            self._peer_clients[peer] = client
            self._peer_connected_time[peer] = time.time()

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

    FLAG_TRANSMISSION_TIMEOUT = 0.5

    async def _start_downloading_piece(self, piece_index: int):
        download_info = self._torrent_info.download_info
        cur_piece_length = download_info.get_real_piece_length(piece_index)

        blocks_expected = download_info.piece_blocks_expected[piece_index]
        for block_begin in range(0, cur_piece_length, TorrentManager.REQUEST_LENGTH):
            block_end = min(block_begin + TorrentManager.REQUEST_LENGTH, cur_piece_length)
            block_length = block_end - block_begin
            request = BlockRequest(piece_index, block_begin, block_length, asyncio.Future())

            blocks_expected.add(request)
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
        progress = download_info.downloaded_piece_count / len(self._pieces_to_download)
        logger.info('progress %.1lf%% (%s / %s pieces)', progress * 100,
                    download_info.downloaded_piece_count, len(self._pieces_to_download))

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

    HANG_PENALTY_DURATION = 15
    HANG_PENALTY_COEFF = 100

    def get_peer_download_rate(self, peer: Peer) -> int:
        client = self._peer_clients[peer]

        rate = client.downloaded  # To reach maximal download speed
        rate -= 2 ** client.distrust_rate
        rate += random.randint(1, 100)  # Helps to shuffle clients in the beginning

        if peer in self._peer_hanged_time and \
            time.time() - self._peer_hanged_time[peer] <= TorrentManager.HANG_PENALTY_DURATION:
            rate //= TorrentManager.HANG_PENALTY_COEFF

        return rate

    def get_peer_upload_rate(self, peer: Peer) -> int:
        client = self._peer_clients[peer]

        rate = client.downloaded  # We owe them for downloading
        if self.download_complete:
            rate += client.uploaded  # To reach maximal upload speed
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
        performer = max(available_peers, key=self.get_peer_download_rate)
        client = self._peer_clients[performer]

        request = first_request
        while True:
            processed_requests.append(request)
            future_to_request[request.downloaded] = request

            client.send_request(request)

            if (len(processed_requests) == TorrentManager.DOWNLOAD_REQUEST_QUEUE_SIZE or
                    not await self._prepare_requests()):
                break
            request = self._request_deque[0]
            if performer not in download_info.piece_owners[request.piece_index]:
                break

            self._request_deque.popleft()
        return performer

    DOWNLOAD_PEER_COUNT = 20
    DOWNLOAD_PEERS_ACTIVE_TO_REQUEST_MORE_PEERS = 2
    NOT_ENOUGH_PEERS_SLEEP_TIME = 5

    async def _wait_more_peers(self):
        self._tasks_waiting_for_more_peers += 1
        download_peers_active = TorrentManager.DOWNLOAD_PEER_COUNT - self._tasks_waiting_for_more_peers
        if download_peers_active <= TorrentManager.DOWNLOAD_PEERS_ACTIVE_TO_REQUEST_MORE_PEERS and \
                len(self._peer_clients) < TorrentManager.MAX_PEERS_TO_ACTIVELY_CONNECT:
            self._more_peers_requested.set()

        # TODO: Maybe start another piece?
        await asyncio.sleep(TorrentManager.NOT_ENOUGH_PEERS_SLEEP_TIME)
        self._tasks_waiting_for_more_peers -= 1

    async def _wait_more_requests(self):
        download_info = self._torrent_info.download_info

        if not self._endgame_mode:
            non_finished_pieces = [i for i in self._pieces_to_download
                                   if not download_info.piece_downloaded[i]]
            logger.info('entering endgame mode (remaining pieces: %s)',
                        ', '.join(map(str, non_finished_pieces)))
            self._endgame_mode = True

        await self._request_deque_relevant.wait()

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
                    await self._wait_more_peers()
                    continue
            except NoRequestsError:
                cur_performer = None
                if not processed_requests:
                    if not any(self._executors_processed_requests):
                        self._request_deque_relevant.set()
                        return
                    await self._wait_more_requests()
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
                self._request_deque_relevant.set()
                self._request_deque_relevant.clear()

            self._peers_busy.remove(cur_performer)
            prev_performer = cur_performer

    MAX_PEERS_TO_ACTIVELY_CONNECT = 30
    MAX_PEERS_TO_ACCEPT = 55

    def _connect_to_peers(self, peers: Sequence[Peer], force: bool):
        peers = list({peer for peer in peers if peer not in self._peer_clients})
        if force:
            max_peers_count = TorrentManager.MAX_PEERS_TO_ACCEPT
        else:
            max_peers_count = TorrentManager.MAX_PEERS_TO_ACTIVELY_CONNECT
        peers_to_connect_count = max(max_peers_count - len(self._peer_clients), 0)
        logger.debug('connecting to up to %s new peers', min(len(peers), peers_to_connect_count))

        for peer in peers[:peers_to_connect_count]:
            client = PeerTCPClient(self._torrent_info.download_info, self._file_structure,
                                   self._our_peer_id, peer)
            self._client_executors.append(asyncio.ensure_future(self._execute_peer_client(peer, client)))

    DEFAULT_MIN_INTERVAL = 45

    async def _try_to_announce(self, event: Optional[str]) -> bool:
        try:
            await self._tracker_client.announce(event)
            return True
        except Exception as e:
            logger.warning('exception on announce: %s', repr(e))
            return False

    async def _execute_regular_announcements(self):
        try:
            while True:
                if self._tracker_client.min_interval is not None:
                    min_interval = self._tracker_client.min_interval
                else:
                    min_interval = min(TorrentManager.DEFAULT_MIN_INTERVAL, self._tracker_client.interval)
                await asyncio.sleep(min_interval)

                default_interval = self._tracker_client.interval
                try:
                    await asyncio.wait_for(self._more_peers_requested.wait(), default_interval - min_interval)
                    more_peers = True
                    self._more_peers_requested.clear()
                except asyncio.TimeoutError:
                    more_peers = False

                await self._try_to_announce(None)

                self._connect_to_peers(self._tracker_client.peers, more_peers)
        finally:
            await self._try_to_announce('stopped')

    async def _download(self):
        download_info = self._torrent_info.download_info

        self._non_started_pieces = [index for index in self._pieces_to_download
                                    if not download_info.piece_downloaded[index]]
        if not self._non_started_pieces:
            return

        random.shuffle(self._non_started_pieces)

        for _ in range(TorrentManager.DOWNLOAD_PEER_COUNT):
            processed_requests = []
            self._executors_processed_requests.append(processed_requests)
            self._request_executors.append(asyncio.ensure_future(self._execute_block_requests(processed_requests)))

        await asyncio.wait(self._request_executors)

        assert self.download_complete
        await self._try_to_announce('completed')
        logger.info('file download complete')
        # TODO: disconnect from seeders (maybe), connect to new peers, upload

    CHOKING_CHANGING_TIME = 10
    UPLOAD_PEER_COUNT = 4

    ITERS_PER_OPTIMISTIC_UNCHOKING = 3
    CONNECTED_RECENTLY_THRESHOLD = 60
    CONNECTED_RECENTLY_COEFF = 3

    def _select_optimistically_unchoked(self, peers: Iterable[Peer]) -> Peer:
        cur_time = time.time()
        connected_recently = []
        remaining_peers = []
        for peer in peers:
            if cur_time - self._peer_connected_time[peer] <= TorrentManager.CONNECTED_RECENTLY_THRESHOLD:
                connected_recently.append(peer)
            else:
                remaining_peers.append(peer)

        max_index = len(remaining_peers) + TorrentManager.CONNECTED_RECENTLY_COEFF * len(connected_recently) - 1
        index = random.randint(0, max_index)
        if index < len(remaining_peers):
            return remaining_peers[index]
        return connected_recently[(index - len(remaining_peers)) // len(connected_recently)]

    async def _execute_uploading(self):
        prev_unchoked_peers = set()
        optimistically_unchoked = None
        for i in itertools.count():
            alive_peers = list(sorted(self._peer_clients.keys(), key=self.get_peer_upload_rate, reverse=True))
            cur_unchoked_peers = set()
            interested_count = 0

            if TorrentManager.UPLOAD_PEER_COUNT:
                if i % TorrentManager.ITERS_PER_OPTIMISTIC_UNCHOKING == 0:
                    if alive_peers:
                        optimistically_unchoked = self._select_optimistically_unchoked(alive_peers)
                    else:
                        optimistically_unchoked = None

                if optimistically_unchoked is not None and optimistically_unchoked in self._peer_clients:
                    cur_unchoked_peers.add(optimistically_unchoked)
                    if self._peer_clients[optimistically_unchoked].peer_interested:
                        interested_count += 1

            for peer in cast(List[Peer], alive_peers):
                if interested_count == TorrentManager.UPLOAD_PEER_COUNT:
                    break
                if self._peer_clients[peer].peer_interested:
                    interested_count += 1

                cur_unchoked_peers.add(peer)

            for peer in prev_unchoked_peers - cur_unchoked_peers:
                if peer in self._peer_clients:
                    self._peer_clients[peer].am_choking = True
            for peer in cur_unchoked_peers:
                self._peer_clients[peer].am_choking = False
            logger.debug('now %s peers are unchoked', len(cur_unchoked_peers))

            await asyncio.sleep(TorrentManager.CHOKING_CHANGING_TIME)

            prev_unchoked_peers = cur_unchoked_peers

    ANNOUNCE_FAILED_SLEEP_TIME = 3

    async def run(self, pieces_to_download: Sequence[int]=None):
        download_info = self._torrent_info.download_info

        if pieces_to_download is None:
            pieces_to_download = range(download_info.piece_count)
        self._pieces_to_download = pieces_to_download

        while not await self._try_to_announce('started'):
            await asyncio.sleep(TorrentManager.ANNOUNCE_FAILED_SLEEP_TIME)

        self._connect_to_peers(self._tracker_client.peers, False)

        self._announcement_executor = asyncio.ensure_future(self._execute_regular_announcements())
        self._uploading_executor = asyncio.ensure_future(self._execute_uploading())

        await self._download()

    @property
    def download_complete(self):
        download_info = self._torrent_info.download_info

        return download_info.downloaded_piece_count == len(self._pieces_to_download)

    async def stop(self):
        executors = self._request_executors
        if self._announcement_executor is not None:
            executors.append(self._announcement_executor)
        if self._uploading_executor:
            executors.append(self._uploading_executor)
        executors += self._client_executors

        for task in executors:
            task.cancel()
        if executors:
            logger.debug('all tasks done or cancelled, awaiting finalizers')
            await asyncio.wait(executors)
            logger.debug('finalizers done')

        self._request_executors.clear()
        self._executors_processed_requests.clear()
        self._announcement_executor = None
        self._client_executors.clear()

        self._tracker_client.close()

        self._file_structure.close()
