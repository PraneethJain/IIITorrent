import asyncio
import hashlib
import itertools
import logging
import random
import time
from collections import deque, OrderedDict
from typing import Dict, List, Optional, Tuple, Sequence, Iterable, cast, Iterator

import contexttimer

from file_structure import FileStructure
from models import BlockRequestFuture, Peer, TorrentInfo
from peer_tcp_client import PeerTCPClient
from tracker_http_client import TrackerHTTPClient


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

TIMER_WARNING_THRESHOLD = 0.1


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

        self._server = None
        self._client_executors = []              # type: List[asyncio.Task]
        self._keeping_alive_executor = None      # type: Optional[asyncio.Task]
        self._announcement_executor = None       # type: Optional[asyncio.Task]
        self._uploading_executor = None          # type: Optional[asyncio.Task]
        self._request_executors = []             # type: List[asyncio.Task]
        self._executors_processed_requests = []  # type: List[List[BlockRequestFuture]]

        self._pieces_to_download = None  # type: Sequence[int]
        self._non_started_pieces = None  # type: List[int]

        self._piece_block_queue = OrderedDict()
        self._peer_queue_size = {}  # type: Dict[Peer, int]

        self._request_consumption_lock = asyncio.Lock()
        self._endgame_mode = False
        # TODO: Send cancels in endgame mode

        self._tasks_waiting_for_more_peers = 0
        self._more_peers_requested = asyncio.Event()
        self._request_deque_relevant = asyncio.Event()

        self._file_structure = FileStructure(download_dir, torrent_info.download_info)

    async def _execute_peer_client(self, peer: Peer, client: PeerTCPClient,
                                   streams: Tuple[asyncio.StreamReader, asyncio.StreamWriter]=None):
        download_info = self._torrent_info.download_info

        try:
            await client.connect(streams)

            self._peer_clients[peer] = client
            self._peer_connected_time[peer] = time.time()
            self._peer_queue_size[peer] = 0

            try:
                await client.run()
            finally:
                client.close()

                for owners in download_info.piece_owners:
                    if peer in owners:
                        owners.remove(peer)

                del self._peer_clients[peer]
                del self._peer_connected_time[peer]
                if peer in self._peer_hanged_time:
                    del self._peer_hanged_time[peer]
                del self._peer_queue_size[peer]
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.debug('%s disconnected because of %s', peer, repr(e))

    KEEP_ALIVE_TIMEOUT = 2 * 60

    async def _execute_keeping_alive(self):
        while True:
            await asyncio.sleep(TorrentManager.KEEP_ALIVE_TIMEOUT)

            logger.debug('broadcasting keep-alives to %s alive peers', len(self._peer_clients))
            for client in self._peer_clients.values():
                client.send_keep_alive()

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
        if timer.elapsed >= TIMER_WARNING_THRESHOLD:
            logger.warning('Too long flush (%.1f s)', timer.elapsed)

    FLAG_TRANSMISSION_TIMEOUT = 0.5

    async def _start_downloading_piece(self, piece_index: int):
        download_info = self._torrent_info.download_info
        cur_piece_length = download_info.get_real_piece_length(piece_index)

        blocks_expected = download_info.piece_blocks_expected[piece_index]
        request_deque = deque()
        for block_begin in range(0, cur_piece_length, TorrentManager.REQUEST_LENGTH):
            block_end = min(block_begin + TorrentManager.REQUEST_LENGTH, cur_piece_length)
            block_length = block_end - block_begin
            request = BlockRequestFuture(piece_index, block_begin, block_length)

            blocks_expected.add(request)
            request_deque.append(request)
        self._piece_block_queue[piece_index] = request_deque

        download_info.interesting_pieces.add(piece_index)
        piece_owners = download_info.piece_owners[piece_index]
        for peer in piece_owners:
            self._peer_clients[peer].am_interested = True

        choking_owners = [peer for peer in piece_owners if self._peer_clients[peer].peer_choking]
        if len(choking_owners) == len(piece_owners) and choking_owners:
            logger.debug('all piece owners are choking us yet, waiting for an answer for am_interested')
            _, pending = await asyncio.wait([self._peer_clients[peer].drain() for peer in choking_owners],
                                            timeout=TorrentManager.FLAG_TRANSMISSION_TIMEOUT)
            # FIXME: iterate "done" to handle exceptions that can be raised by drain()
            await asyncio.sleep(TorrentManager.FLAG_TRANSMISSION_TIMEOUT)
        # TODO: What if there's no alive_piece_owners?

        logger.debug('piece %s started (owned by %s alive peers, concurrency: %s pieces, %s peers)',
                     piece_index, len(piece_owners), len(download_info.interesting_pieces),
                     sum(1 for peer, size in self._peer_queue_size.items() if size))

    def _finish_downloading_piece(self, piece_index: int):
        download_info = self._torrent_info.download_info

        self._flush_piece(piece_index)
        download_info.mark_piece_downloaded(piece_index)

        download_info.interesting_pieces.remove(piece_index)
        for peer in download_info.piece_owners[piece_index]:
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
        if timer.elapsed >= TIMER_WARNING_THRESHOLD:
            logger.warning('Too long hash comparison (%.1f s)', timer.elapsed)
        if actual_digest == download_info.piece_hashes[piece_index]:
            self._finish_downloading_piece(piece_index)
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

    DOWNLOAD_REQUEST_QUEUE_SIZE = 10

    def _is_peer_free(self, peer: Peer):
        return self._peer_queue_size[peer] < TorrentManager.DOWNLOAD_REQUEST_QUEUE_SIZE

    def _request_piece_blocks(self, count: int, piece_index: int) -> Iterator[BlockRequestFuture]:
        download_info = self._torrent_info.download_info

        if not count:
            return

        request_deque = self._piece_block_queue[piece_index]
        performer = None
        yielded_count = 0
        while request_deque:
            request = request_deque[0]
            if request.done():
                request_deque.popleft()
                continue

            if performer is None or not self._is_peer_free(performer):
                available_peers = {peer for peer in download_info.piece_owners[piece_index]
                                   if not self._peer_clients[peer].peer_choking and self._is_peer_free(peer)}
                if not available_peers:
                    return
                performer = max(available_peers, key=self.get_peer_download_rate)
            request_deque.popleft()
            self._peer_queue_size[performer] += 1

            request.performer = performer
            self._peer_clients[performer].send_request(request)
            yield request

            yielded_count += 1
            if yielded_count == count:
                return

    RAREST_PIECE_COUNT_TO_SELECT = 10

    def _select_new_piece(self) -> Optional[int]:
        download_info = self._torrent_info.download_info

        available_pieces = []
        for piece_index in self._non_started_pieces:
            available = False
            for peer in download_info.piece_owners[piece_index]:
                if self._is_peer_free(peer):
                    available = True
                    break
            if available:
                available_pieces.append(piece_index)
        if not available_pieces:
            return None

        available_pieces.sort(key=lambda index: len(download_info.piece_owners[index]))
        piece_count_to_select = min(len(available_pieces), TorrentManager.RAREST_PIECE_COUNT_TO_SELECT)
        return available_pieces[random.randint(0, piece_count_to_select - 1)]

    async def _request_blocks(self, count: int) -> List[BlockRequestFuture]:
        result = []
        consumed_pieces = []
        for piece_index, request_deque in self._piece_block_queue.items():
            result += list(self._request_piece_blocks(count - len(result), piece_index))
            if not request_deque:
                consumed_pieces.append(piece_index)
        for piece_index in consumed_pieces:
            del self._piece_block_queue[piece_index]

        if len(result) == count:
            return result

        with contexttimer.Timer() as timer:
            new_piece_index = self._select_new_piece()
        if timer.elapsed >= TIMER_WARNING_THRESHOLD:
            logger.warning('Too long select_new_piece (%.1f s)', timer.elapsed)
        if new_piece_index is not None:
            self._non_started_pieces.remove(new_piece_index)
            await self._start_downloading_piece(new_piece_index)

            result += list(self._request_piece_blocks(count - len(result), new_piece_index))
            if not self._piece_block_queue[new_piece_index]:
                del self._piece_block_queue[new_piece_index]

        if not result:
            if not self._piece_block_queue and not self._non_started_pieces:
                raise NoRequestsError('No more undistributed requests')
            raise NotEnoughPeersError('No peers to perform a request')
        return result

    DOWNLOAD_PEER_COUNT = 20
    DOWNLOAD_PEERS_ACTIVE_TO_REQUEST_MORE_PEERS = 2
    NOT_ENOUGH_PEERS_SLEEP_TIME = 5

    async def _wait_more_peers(self):
        self._tasks_waiting_for_more_peers += 1
        download_peers_active = TorrentManager.DOWNLOAD_PEER_COUNT - self._tasks_waiting_for_more_peers
        if download_peers_active <= TorrentManager.DOWNLOAD_PEERS_ACTIVE_TO_REQUEST_MORE_PEERS and \
                len(self._peer_clients) < TorrentManager.MAX_PEERS_TO_ACTIVELY_CONNECT:
            self._more_peers_requested.set()

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
    REQUEST_TIMEOUT_ENDGAME = 1.5

    async def _execute_block_requests(self, processed_requests: List[BlockRequestFuture]):
        download_info = self._torrent_info.download_info

        while True:
            try:
                free_place_count = TorrentManager.DOWNLOAD_REQUEST_QUEUE_SIZE - len(processed_requests)
                async with self._request_consumption_lock:
                    processed_requests += await self._request_blocks(free_place_count)
            except NotEnoughPeersError:
                if not processed_requests:
                    await self._wait_more_peers()
                    continue
            except NoRequestsError:
                if not processed_requests:
                    if not any(self._executors_processed_requests):
                        self._request_deque_relevant.set()
                        return
                    await self._wait_more_requests()
                    continue

            if self._endgame_mode:
                request_timeout = TorrentManager.REQUEST_TIMEOUT_ENDGAME
            else:
                request_timeout = TorrentManager.REQUEST_TIMEOUT
            requests_done, requests_pending = await asyncio.wait(
                processed_requests, return_when=asyncio.FIRST_COMPLETED, timeout=request_timeout)

            if len(requests_pending) < len(processed_requests):
                for request in requests_done:
                    if request.performer in self._peer_clients:
                        self._peer_queue_size[request.performer] -= 1

                    if not download_info.piece_downloaded[request.piece_index] and \
                            not download_info.piece_blocks_expected[request.piece_index]:
                        await self._validate_piece(request.piece_index)
                processed_requests.clear()
                processed_requests += list(requests_pending)
            else:
                hanged_peers = {request.performer for request in requests_pending} & set(self._peer_clients.keys())
                if hanged_peers:
                    logger.debug('peers %s hanged, leaving them alone for a while', ', '.join(map(str, hanged_peers)))
                for peer in hanged_peers:
                    self._peer_hanged_time[peer] = time.time()

                for request in requests_pending:
                    if request.performer in self._peer_clients:
                        self._peer_queue_size[request.performer] -= 1

                    request.performer = None
                    self._piece_block_queue.setdefault(request.piece_index, deque()).append(request)
                processed_requests.clear()
                self._request_deque_relevant.set()
                self._request_deque_relevant.clear()

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

    async def _accept_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        if len(self._peer_clients) >= TorrentManager.MAX_PEERS_TO_ACCEPT:
            # FIXME: reject connection earlier?
            writer.close()
            return
        addr = writer.get_extra_info('peername')
        peer = Peer(addr[0], addr[1])
        logger.debug('accepted connection from %s', peer)

        client = PeerTCPClient(self._torrent_info.download_info, self._file_structure, self._our_peer_id, peer)
        self._client_executors.append(asyncio.ensure_future(self._execute_peer_client(peer, client,
                                                                                      (reader, writer))))

    DEFAULT_MIN_INTERVAL = 30

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
        return connected_recently[(index - len(remaining_peers)) % len(connected_recently)]

    async def _execute_uploading(self):
        download_info = self._torrent_info.download_info

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
            logger.debug('now %s peers are unchoked (total_uploaded = %.1f MiB)',
                         len(cur_unchoked_peers), download_info.total_uploaded / TrackerHTTPClient.BYTES_PER_MIB)

            await asyncio.sleep(TorrentManager.CHOKING_CHANGING_TIME)

            prev_unchoked_peers = cur_unchoked_peers

    SERVER_PORT_RANGE = range(6881, 6889 + 1)

    ANNOUNCE_FAILED_SLEEP_TIME = 3

    async def run(self, pieces_to_download: Sequence[int]=None):
        download_info = self._torrent_info.download_info

        if pieces_to_download is None:
            pieces_to_download = range(download_info.piece_count)
        self._pieces_to_download = pieces_to_download

        logger.debug('starting server')
        for port in TorrentManager.SERVER_PORT_RANGE:
            try:
                self._server = await asyncio.start_server(self._accept_client, port=port)
            except Exception as e:
                logger.debug('exception on server starting on port %s: %s', port, repr(e))
            else:
                logger.debug('server started on port %s', port)
                break
        else:
            logger.warning('failed to start server, giving up')

        while not await self._try_to_announce('started'):
            await asyncio.sleep(TorrentManager.ANNOUNCE_FAILED_SLEEP_TIME)

        self._connect_to_peers(self._tracker_client.peers, False)

        self._keeping_alive_executor = asyncio.ensure_future(self._execute_keeping_alive())
        self._announcement_executor = asyncio.ensure_future(self._execute_regular_announcements())
        self._uploading_executor = asyncio.ensure_future(self._execute_uploading())

        await self._download()

    @property
    def download_complete(self):
        download_info = self._torrent_info.download_info

        return download_info.downloaded_piece_count == len(self._pieces_to_download)

    async def stop(self):
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

        executors = (self._request_executors +
                     [self._uploading_executor, self._announcement_executor, self._keeping_alive_executor] +
                     self._client_executors)
        executors = [task for task in executors if task is not None]

        for task in executors:
            task.cancel()
        if executors:
            logger.debug('all tasks done or cancelled, awaiting finalizers')
            await asyncio.wait(executors)
            logger.debug('finalizers done')

        self._request_executors.clear()
        self._executors_processed_requests.clear()
        self._uploading_executor = None
        self._announcement_executor = None
        self._keeping_alive_executor = None
        self._client_executors.clear()

        self._tracker_client.close()

        self._file_structure.close()
