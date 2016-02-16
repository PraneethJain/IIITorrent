import asyncio
import hashlib
import itertools
import logging
import random
import time
from collections import deque, OrderedDict
from math import ceil
from typing import Dict, List, Optional, Tuple, Sequence, Iterable, cast, Iterator, Set

from file_structure import FileStructure
from models import BlockRequestFuture, Peer, TorrentInfo
from peer_tcp_client import PeerTCPClient, SeedError
from tracker_http_client import TrackerHTTPClient


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class NotEnoughPeersError(RuntimeError):
    pass


class NoRequestsError(RuntimeError):
    pass


DOWNLOAD_REQUEST_QUEUE_SIZE = 10


class PeerData:
    def __init__(self, client: PeerTCPClient, connected_time: float):
        self._client = client
        self._connected_time = connected_time
        self.hanged_time = None  # type: Optional[float]
        self.queue_size = 0

    @property
    def client(self) -> PeerTCPClient:
        return self._client

    @property
    def connected_time(self) -> float:
        return self._connected_time

    def is_free(self) -> bool:
        return self.queue_size < DOWNLOAD_REQUEST_QUEUE_SIZE

    def is_available(self) -> bool:
        return self.is_free() and not self._client.peer_choking


class TorrentManager:
    def __init__(self, torrent_info: TorrentInfo, our_peer_id: bytes, download_dir: str):
        self._torrent_info = torrent_info
        self._download_info = torrent_info.download_info
        self._our_peer_id = our_peer_id

        self._tracker_client = TrackerHTTPClient(self._torrent_info, self._our_peer_id)
        self._peer_data = {}  # type: Dict[Peer, PeerData]

        self._server = None
        self._server_port = None                 # type: Optional[int]
        self._client_executors = {}              # type: Dict[Peer, asyncio.Task]
        self._keeping_alive_executor = None      # type: Optional[asyncio.Task]
        self._announcement_executor = None       # type: Optional[asyncio.Task]
        self._uploading_executor = None          # type: Optional[asyncio.Task]
        self._request_executors = []             # type: List[asyncio.Task]
        self._executors_processed_requests = []  # type: List[List[BlockRequestFuture]]

        self._non_started_pieces = None   # type: List[int]
        self._download_start_time = None  # type: float
        self._validating_pieces = set()   # type: Set[int]

        self._piece_block_queue = OrderedDict()

        self._endgame_mode = False
        self._tasks_waiting_for_more_peers = 0
        self._more_peers_requested = asyncio.Event()
        self._request_deque_relevant = asyncio.Event()

        self._file_structure = FileStructure(download_dir, torrent_info.download_info)

    async def _execute_peer_client(self, peer: Peer, client: PeerTCPClient,
                                   streams: Tuple[asyncio.StreamReader, asyncio.StreamWriter]=None):
        try:
            await client.connect(streams)

            self._peer_data[peer] = PeerData(client, time.time())

            try:
                await client.run()
            finally:
                client.close()

                for owners in self._download_info.piece_owners:
                    if peer in owners:
                        owners.remove(peer)
                del self._peer_data[peer]
        except asyncio.CancelledError:
            raise
        except SeedError:
            pass
        except Exception as e:
            logger.debug('%s disconnected because of %s', peer, repr(e))

    KEEP_ALIVE_TIMEOUT = 2 * 60

    async def _execute_keeping_alive(self):
        while True:
            await asyncio.sleep(TorrentManager.KEEP_ALIVE_TIMEOUT)

            logger.debug('broadcasting keep-alives to %s alive peers', len(self._peer_data))
            for data in self._peer_data.values():
                data.client.send_keep_alive()

    REQUEST_LENGTH = 2 ** 14

    def _get_piece_position(self, index: int) -> Tuple[int, int]:
        piece_offset = index * self._download_info.piece_length
        cur_piece_length = self._download_info.get_real_piece_length(index)
        return piece_offset, cur_piece_length

    async def _flush_piece(self, index: int):
        piece_offset, cur_piece_length = self._get_piece_position(index)
        await self._file_structure.flush(piece_offset, cur_piece_length)

    FLAG_TRANSMISSION_TIMEOUT = 0.5

    def _send_cancels(self, request: BlockRequestFuture):
        performers = request.prev_performers
        if request.performer is not None:
            performers.add(request.performer)
        source = request.result()
        for peer in performers - {source}:
            if peer in self._peer_data:
                self._peer_data[peer].client.send_request(request, cancel=True)

    def _start_downloading_piece(self, piece_index: int):
        cur_piece_length = self._download_info.get_real_piece_length(piece_index)
        blocks_expected = self._download_info.piece_blocks_expected[piece_index]
        request_deque = deque()
        for block_begin in range(0, cur_piece_length, TorrentManager.REQUEST_LENGTH):
            block_end = min(block_begin + TorrentManager.REQUEST_LENGTH, cur_piece_length)
            block_length = block_end - block_begin
            request = BlockRequestFuture(piece_index, block_begin, block_length)
            request.add_done_callback(self._send_cancels)

            blocks_expected.add(request)
            request_deque.append(request)
        self._piece_block_queue[piece_index] = request_deque

        self._download_info.interesting_pieces.add(piece_index)
        piece_owners = self._download_info.piece_owners[piece_index]
        for peer in piece_owners:
            self._peer_data[peer].client.am_interested = True

        concurrent_peers_count = sum(1 for peer, data in self._peer_data.items() if data.queue_size)
        logger.debug('piece %s started (owned by %s alive peers, concurrency: %s peers)',
                     piece_index, len(piece_owners), concurrent_peers_count)

    def _finish_downloading_piece(self, piece_index: int):
        self._download_info.mark_piece_downloaded(piece_index)

        self._download_info.interesting_pieces.remove(piece_index)
        for peer in self._download_info.piece_owners[piece_index]:
            client = self._peer_data[peer].client
            for index in self._download_info.interesting_pieces:
                if client.piece_owned[index]:
                    break
            else:
                client.am_interested = False

        for data in self._peer_data.values():
            data.client.send_have(piece_index)

        logger.debug('piece %s finished', piece_index)
        selected_piece_count = self._download_info.piece_selected.count()
        progress = self._download_info.downloaded_piece_count / selected_piece_count
        logger.info('progress %.1lf%% (%s / %s pieces)', progress * 100,
                    self._download_info.downloaded_piece_count, selected_piece_count)

    async def _validate_piece(self, piece_index: int):
        assert self._download_info.is_all_piece_blocks_downloaded(piece_index)

        piece_offset, cur_piece_length = self._get_piece_position(piece_index)
        data = await self._file_structure.read(piece_offset, cur_piece_length)
        actual_digest = hashlib.sha1(data).digest()
        if actual_digest == self._download_info.piece_hashes[piece_index]:
            await self._flush_piece(piece_index)
            self._finish_downloading_piece(piece_index)
            return

        for peer in self._download_info.piece_sources[piece_index]:
            self._download_info.increase_distrust(peer)
            if self._download_info.is_banned(peer):
                logger.info('Host %s banned', peer.host)
                self._client_executors[peer].cancel()

        self._download_info.reset_piece(piece_index)
        self._start_downloading_piece(piece_index)

        logger.debug('piece %s not valid, redownloading', piece_index)

    _INF = float('inf')

    HANG_PENALTY_DURATION = 10
    HANG_PENALTY_COEFF = 100

    def get_peer_download_rate(self, peer: Peer) -> int:
        data = self._peer_data[peer]

        rate = data.client.downloaded  # To reach maximal download speed
        rate += random.randint(1, 100)  # Helps to shuffle clients in the beginning

        if data.hanged_time is not None and time.time() - data.hanged_time <= TorrentManager.HANG_PENALTY_DURATION:
            rate //= TorrentManager.HANG_PENALTY_COEFF

        return rate

    def get_peer_upload_rate(self, peer: Peer) -> int:
        data = self._peer_data[peer]

        rate = data.client.downloaded  # We owe them for downloading
        if self._download_info.is_complete():
            rate += data.client.uploaded  # To reach maximal upload speed
        rate += random.randint(1, 100)  # Helps to shuffle clients in the beginning

        return rate

    DOWNLOAD_PEER_COUNT = 15

    def _request_piece_blocks(self, count: int, piece_index: int) -> Iterator[BlockRequestFuture]:
        if not count:
            return

        request_deque = self._piece_block_queue[piece_index]
        performer = None
        performer_data = None
        yielded_count = 0
        while request_deque:
            request = request_deque[0]
            if request.done():
                request_deque.popleft()
                continue

            if performer is None or not performer_data.is_free():
                available_peers = {peer for peer in self._download_info.piece_owners[piece_index]
                                   if self._peer_data[peer].is_available()}
                if not available_peers:
                    return
                performer = max(available_peers, key=self.get_peer_download_rate)
                performer_data = self._peer_data[performer]
            request_deque.popleft()
            performer_data.queue_size += 1

            request.performer = performer
            performer_data.client.send_request(request)
            yield request

            yielded_count += 1
            if yielded_count == count:
                return

    RAREST_PIECE_COUNT_TO_SELECT = 10

    def _select_new_piece(self, *, force: bool) -> Optional[int]:
        is_appropriate = PeerData.is_free if force else PeerData.is_available
        appropriate_peers = {peer for peer, data in self._peer_data.items() if is_appropriate(data)}
        if not appropriate_peers:
            return None

        piece_owners = self._download_info.piece_owners
        available_pieces = [index for index in self._non_started_pieces
                            if appropriate_peers & piece_owners[index]]
        if not available_pieces:
            return None

        available_pieces.sort(key=lambda index: len(piece_owners[index]))
        piece_count_to_select = min(len(available_pieces), TorrentManager.RAREST_PIECE_COUNT_TO_SELECT)
        return available_pieces[random.randint(0, piece_count_to_select - 1)]

    _typical_piece_length = 2 ** 20
    _requests_per_piece = ceil(_typical_piece_length / REQUEST_LENGTH)
    _desired_request_stock = DOWNLOAD_PEER_COUNT * DOWNLOAD_REQUEST_QUEUE_SIZE
    DESIRED_PIECE_STOCK = ceil(_desired_request_stock / _requests_per_piece)

    def _request_blocks(self, count: int) -> List[BlockRequestFuture]:
        result = []
        consumed_pieces = []
        try:
            for piece_index, request_deque in self._piece_block_queue.items():
                result += list(self._request_piece_blocks(count - len(result), piece_index))
                if not request_deque:
                    consumed_pieces.append(piece_index)
                if len(result) == count:
                    return result

            piece_stock = len(self._piece_block_queue) - len(consumed_pieces)
            piece_stock_small = (piece_stock < TorrentManager.DESIRED_PIECE_STOCK)
            new_piece_index = self._select_new_piece(force=piece_stock_small)
            if new_piece_index is not None:
                self._non_started_pieces.remove(new_piece_index)
                self._start_downloading_piece(new_piece_index)

                result += list(self._request_piece_blocks(count - len(result), new_piece_index))
                if not self._piece_block_queue[new_piece_index]:
                    consumed_pieces.append(new_piece_index)
        finally:
            for piece_index in consumed_pieces:
                del self._piece_block_queue[piece_index]

        if not result:
            if not self._piece_block_queue and not self._non_started_pieces:
                raise NoRequestsError('No more undistributed requests')
            raise NotEnoughPeersError('No peers to perform a request')
        return result

    DOWNLOAD_PEERS_ACTIVE_TO_REQUEST_MORE_PEERS = 2

    NO_PEERS_SLEEP_TIME = 3
    STARTING_DURATION = 5
    NO_PEERS_SLEEP_TIME_ON_STARTING = 1

    async def _wait_more_peers(self):
        self._tasks_waiting_for_more_peers += 1
        download_peers_active = TorrentManager.DOWNLOAD_PEER_COUNT - self._tasks_waiting_for_more_peers
        if download_peers_active <= TorrentManager.DOWNLOAD_PEERS_ACTIVE_TO_REQUEST_MORE_PEERS and \
                len(self._peer_data) < TorrentManager.MAX_PEERS_TO_ACTIVELY_CONNECT:
            self._more_peers_requested.set()

        if time.time() - self._download_start_time <= TorrentManager.STARTING_DURATION:
            sleep_time = TorrentManager.NO_PEERS_SLEEP_TIME_ON_STARTING
        else:
            sleep_time = TorrentManager.NO_PEERS_SLEEP_TIME
        await asyncio.sleep(sleep_time)
        self._tasks_waiting_for_more_peers -= 1

    def _get_non_finished_pieces(self) -> List[int]:
        return [i for i in range(self._download_info.piece_count)
                if self._download_info.piece_selected[i] and not self._download_info.piece_downloaded[i]]

    async def _wait_more_requests(self):
        if not self._endgame_mode:
            logger.info('entering endgame mode (remaining pieces: %s)',
                        ', '.join(map(str, self._get_non_finished_pieces())))
            self._endgame_mode = True

        await self._request_deque_relevant.wait()

    REQUEST_TIMEOUT = 6
    REQUEST_TIMEOUT_ENDGAME = 1

    async def _execute_block_requests(self, processed_requests: List[BlockRequestFuture]):
        while True:
            try:
                free_place_count = DOWNLOAD_REQUEST_QUEUE_SIZE - len(processed_requests)
                processed_requests += self._request_blocks(free_place_count)
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
                    if request.performer in self._peer_data:
                        self._peer_data[request.performer].queue_size -= 1

                    piece_index = request.piece_index
                    if piece_index not in self._validating_pieces and \
                            not self._download_info.piece_downloaded[piece_index] and \
                            not self._download_info.piece_blocks_expected[piece_index]:
                        self._validating_pieces.add(piece_index)
                        await self._validate_piece(request.piece_index)
                        self._validating_pieces.remove(piece_index)
                processed_requests.clear()
                processed_requests += list(requests_pending)
            else:
                hanged_peers = {request.performer for request in requests_pending} & set(self._peer_data.keys())
                cur_time = time.time()
                for peer in hanged_peers:
                    self._peer_data[peer].hanged_time = cur_time
                if hanged_peers:
                    logger.debug('peers %s hanged', ', '.join(map(str, hanged_peers)))

                for request in requests_pending:
                    if request.performer in self._peer_data:
                        self._peer_data[request.performer].queue_size -= 1
                        request.prev_performers.add(request.performer)
                    request.performer = None

                    self._piece_block_queue.setdefault(request.piece_index, deque()).append(request)
                processed_requests.clear()
                self._request_deque_relevant.set()
                self._request_deque_relevant.clear()

    MAX_PEERS_TO_ACTIVELY_CONNECT = 30
    MAX_PEERS_TO_ACCEPT = 55

    def _connect_to_peers(self, peers: Sequence[Peer], force: bool):
        peers = list({peer for peer in peers
                      if peer not in self._peer_data and not self._download_info.is_banned(peer)})
        if force:
            max_peers_count = TorrentManager.MAX_PEERS_TO_ACCEPT
        else:
            max_peers_count = TorrentManager.MAX_PEERS_TO_ACTIVELY_CONNECT
        peers_to_connect_count = max(max_peers_count - len(self._peer_data), 0)
        logger.debug('connecting to up to %s new peers', min(len(peers), peers_to_connect_count))

        for peer in peers[:peers_to_connect_count]:
            client = PeerTCPClient(self._torrent_info.download_info, self._file_structure,
                                   self._our_peer_id, peer)
            self._client_executors[peer] = asyncio.ensure_future(self._execute_peer_client(peer, client))

    async def _accept_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        if len(self._peer_data) > TorrentManager.MAX_PEERS_TO_ACCEPT:
            writer.close()
            return
        addr = writer.get_extra_info('peername')
        peer = Peer(addr[0], addr[1])
        if self._download_info.is_banned(peer):
            writer.close()
            return
        logger.debug('accepted connection from %s', peer)

        client = PeerTCPClient(self._torrent_info.download_info, self._file_structure, self._our_peer_id, peer)
        self._client_executors[peer] = asyncio.ensure_future(self._execute_peer_client(peer, client, (reader, writer)))

    FAKE_SERVER_PORT = 6881
    DEFAULT_MIN_INTERVAL = 30

    async def _try_to_announce(self, event: Optional[str]) -> bool:
        try:
            server_port = self._server_port if self._server_port is not None else TorrentManager.FAKE_SERVER_PORT
            await self._tracker_client.announce(server_port, event)
            return True
        except asyncio.CancelledError:
            raise
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
        self._non_started_pieces = self._get_non_finished_pieces()
        self._download_start_time = time.time()
        if not self._non_started_pieces:
            return

        random.shuffle(self._non_started_pieces)

        for _ in range(TorrentManager.DOWNLOAD_PEER_COUNT):
            processed_requests = []
            self._executors_processed_requests.append(processed_requests)
            self._request_executors.append(asyncio.ensure_future(self._execute_block_requests(processed_requests)))

        await asyncio.wait(self._request_executors)

        assert self._download_info.is_complete()
        await self._try_to_announce('completed')
        logger.info('file download complete')

        for peer, data in self._peer_data.items():
            if data.client.is_seed():
                self._client_executors[peer].cancel()

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
            if cur_time - self._peer_data[peer].connected_time <= TorrentManager.CONNECTED_RECENTLY_THRESHOLD:
                connected_recently.append(peer)
            else:
                remaining_peers.append(peer)

        max_index = len(remaining_peers) + TorrentManager.CONNECTED_RECENTLY_COEFF * len(connected_recently) - 1
        index = random.randint(0, max_index)
        if index < len(remaining_peers):
            return remaining_peers[index]
        return connected_recently[(index - len(remaining_peers)) % len(connected_recently)]

    async def _execute_uploading(self):
        prev_unchoked_peers = set()
        optimistically_unchoked = None
        for i in itertools.count():
            alive_peers = list(sorted(self._peer_data.keys(), key=self.get_peer_upload_rate, reverse=True))
            cur_unchoked_peers = set()
            interested_count = 0

            if TorrentManager.UPLOAD_PEER_COUNT:
                if i % TorrentManager.ITERS_PER_OPTIMISTIC_UNCHOKING == 0:
                    if alive_peers:
                        optimistically_unchoked = self._select_optimistically_unchoked(alive_peers)
                    else:
                        optimistically_unchoked = None

                if optimistically_unchoked is not None and optimistically_unchoked in self._peer_data:
                    cur_unchoked_peers.add(optimistically_unchoked)
                    if self._peer_data[optimistically_unchoked].client.peer_interested:
                        interested_count += 1

            for peer in cast(List[Peer], alive_peers):
                if interested_count == TorrentManager.UPLOAD_PEER_COUNT:
                    break
                if self._peer_data[peer].client.peer_interested:
                    interested_count += 1

                cur_unchoked_peers.add(peer)

            for peer in prev_unchoked_peers - cur_unchoked_peers:
                if peer in self._peer_data:
                    self._peer_data[peer].client.am_choking = True
            for peer in cur_unchoked_peers:
                self._peer_data[peer].client.am_choking = False
            logger.debug('now %s peers are unchoked (total_uploaded = %.1f MiB)',
                         len(cur_unchoked_peers), self._download_info.total_uploaded / TrackerHTTPClient.BYTES_PER_MIB)

            await asyncio.sleep(TorrentManager.CHOKING_CHANGING_TIME)

            prev_unchoked_peers = cur_unchoked_peers

    SERVER_PORT_RANGE = range(6881, 6889 + 1)

    ANNOUNCE_FAILED_SLEEP_TIME = 3

    async def run(self):
        logger.debug('starting server')
        for port in TorrentManager.SERVER_PORT_RANGE:
            try:
                self._server = await asyncio.start_server(self._accept_client, port=port)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug('exception on server starting on port %s: %s', port, repr(e))
            else:
                self._server_port = port
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

    async def stop(self):
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

        executors = (self._request_executors +
                     [self._uploading_executor, self._announcement_executor, self._keeping_alive_executor] +
                     list(self._client_executors.values()))
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
