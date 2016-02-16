import asyncio
import hashlib
import itertools
import logging
import random
import time
from collections import deque, OrderedDict
from math import ceil
from typing import Dict, List, Optional, Tuple, Sequence, Iterable, cast, Iterator

from file_structure import FileStructure
from models import BlockRequestFuture, DownloadInfo, Peer, TorrentInfo
from peer_tcp_client import PeerTCPClient
from tracker_http_client import TrackerHTTPClient
from utils import humanize_size


class NotEnoughPeersError(RuntimeError):
    pass


class NoRequestsError(RuntimeError):
    pass


DOWNLOAD_REQUEST_QUEUE_SIZE = 12


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
    LOGGER_LEVEL = logging.DEBUG
    SHORT_NAME_LEN = 19

    def __init__(self, torrent_info: TorrentInfo, our_peer_id: bytes, server_port: Optional[int]):
        self._torrent_info = torrent_info
        self._download_info = torrent_info.download_info  # type: DownloadInfo
        self._download_info.reset_run_state()
        self._download_info.reset_stats()
        self._statistics = self._download_info.session_statistics

        self._our_peer_id = our_peer_id
        self._server_port = server_port

        short_name = self._download_info.suggested_name
        if len(short_name) > TorrentManager.SHORT_NAME_LEN:
            short_name = short_name[:TorrentManager.SHORT_NAME_LEN] + '..'
        self._logger = logging.getLogger('"{}"'.format(short_name))
        self._logger.setLevel(TorrentManager.LOGGER_LEVEL)

        self._tracker_client = TrackerHTTPClient(self._torrent_info, self._our_peer_id)
        self._peer_data = {}  # type: Dict[Peer, PeerData]

        self._client_executors = {}   # type: Dict[Peer, asyncio.Task]
        self._tasks = []              # type: List[asyncio.Task]
        self._request_executors = []  # type: List[asyncio.Task]

        self._executors_processed_requests = []  # type: List[List[BlockRequestFuture]]

        self._non_started_pieces = None   # type: List[int]
        self._download_start_time = None  # type: float

        self._piece_block_queue = OrderedDict()

        self._endgame_mode = False
        self._tasks_waiting_for_more_peers = 0
        self._more_peers_requested = asyncio.Event()
        self._request_deque_relevant = asyncio.Event()

        self._file_structure = FileStructure(torrent_info.download_dir, torrent_info.download_info)

    async def _execute_peer_client(self, peer: Peer, client: PeerTCPClient, *, need_connect: bool):
        try:
            if need_connect:
                await client.connect(self._download_info, self._file_structure)
            else:
                client.confirm_info_hash(self._download_info, self._file_structure)

            self._peer_data[peer] = PeerData(client, time.time())
            self._statistics.peer_count += 1

            await client.run()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self._logger.debug('%s disconnected because of %s', peer, repr(e))
        finally:
            if peer in self._peer_data:
                self._statistics.peer_count -= 1
                del self._peer_data[peer]

                for info in self._download_info.pieces:
                    if peer in info.owners:
                        info.owners.remove(peer)
                if peer in self._statistics.peer_last_download:
                    del self._statistics.peer_last_download[peer]
                if peer in self._statistics.peer_last_upload:
                    del self._statistics.peer_last_upload[peer]

            client.close()

    KEEP_ALIVE_TIMEOUT = 2 * 60

    async def _execute_keeping_alive(self):
        while True:
            await asyncio.sleep(TorrentManager.KEEP_ALIVE_TIMEOUT)

            self._logger.debug('broadcasting keep-alives to %s alive peers', len(self._peer_data))
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
        piece_info = self._download_info.pieces[piece_index]

        blocks_expected = piece_info.blocks_expected
        request_deque = deque()
        for block_begin in range(0, piece_info.length, TorrentManager.REQUEST_LENGTH):
            block_end = min(block_begin + TorrentManager.REQUEST_LENGTH, piece_info.length)
            block_length = block_end - block_begin
            request = BlockRequestFuture(piece_index, block_begin, block_length)
            request.add_done_callback(self._send_cancels)

            blocks_expected.add(request)
            request_deque.append(request)
        self._piece_block_queue[piece_index] = request_deque

        self._download_info.interesting_pieces.add(piece_index)
        for peer in piece_info.owners:
            self._peer_data[peer].client.am_interested = True

        concurrent_peers_count = sum(1 for peer, data in self._peer_data.items() if data.queue_size)
        self._logger.debug('piece %s started (owned by %s alive peers, concurrency: %s peers)',
                           piece_index, len(piece_info.owners), concurrent_peers_count)

    def _finish_downloading_piece(self, piece_index: int):
        piece_info = self._download_info.pieces[piece_index]

        piece_info.mark_as_downloaded()
        self._download_info.downloaded_piece_count += 1

        self._download_info.interesting_pieces.remove(piece_index)
        for peer in piece_info.owners:
            client = self._peer_data[peer].client
            for index in self._download_info.interesting_pieces:
                if client.piece_owned[index]:
                    break
            else:
                client.am_interested = False

        for data in self._peer_data.values():
            data.client.send_have(piece_index)

        self._logger.debug('piece %s finished', piece_index)
        selected_piece_count = sum(1 for info in self._download_info.pieces if info.selected)
        progress = self._download_info.downloaded_piece_count / selected_piece_count
        self._logger.info('progress %.1lf%% (%s / %s pieces)', progress * 100,
                          self._download_info.downloaded_piece_count, selected_piece_count)

    async def _validate_piece(self, piece_index: int):
        piece_info = self._download_info.pieces[piece_index]

        assert piece_info.are_all_blocks_downloaded()

        piece_offset, cur_piece_length = self._get_piece_position(piece_index)
        data = await self._file_structure.read(piece_offset, cur_piece_length)
        actual_digest = hashlib.sha1(data).digest()
        if actual_digest == piece_info.piece_hash:
            await self._flush_piece(piece_index)
            self._finish_downloading_piece(piece_index)
            return

        for peer in piece_info.sources:
            self._download_info.increase_distrust(peer)
            if self._download_info.is_banned(peer):
                self._logger.info('Host %s banned', peer.host)
                self._client_executors[peer].cancel()

        piece_info.reset_content()
        self._start_downloading_piece(piece_index)

        self._logger.debug('piece %s not valid, redownloading', piece_index)

    _INF = float('inf')

    HANG_PENALTY_DURATION = 10
    HANG_PENALTY_COEFF = 100

    def get_peer_download_rate(self, peer: Peer) -> int:
        data = self._peer_data[peer]

        rate = data.client.downloaded  # To reach maximal download speed
        if data.hanged_time is not None and time.time() - data.hanged_time <= TorrentManager.HANG_PENALTY_DURATION:
            rate //= TorrentManager.HANG_PENALTY_COEFF
        return rate

    def get_peer_upload_rate(self, peer: Peer) -> int:
        data = self._peer_data[peer]

        rate = data.client.downloaded  # We owe them for downloading
        if self._download_info.complete:
            rate += data.client.uploaded  # To reach maximal upload speed
        return rate

    DOWNLOAD_PEER_COUNT = 15

    def _request_piece_blocks(self, count: int, piece_index: int) -> Iterator[BlockRequestFuture]:
        if not count:
            return
        piece_info = self._download_info.pieces[piece_index]

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
                available_peers = {peer for peer in piece_info.owners
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

        pieces = self._download_info.pieces
        available_pieces = [index for index in self._non_started_pieces
                            if appropriate_peers & pieces[index].owners]
        if not available_pieces:
            return None

        available_pieces.sort(key=lambda index: len(pieces[index].owners))
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
        pieces = self._download_info.pieces
        return [i for i in range(self._download_info.piece_count)
                if pieces[i].selected and not pieces[i].downloaded]

    async def _wait_more_requests(self):
        if not self._endgame_mode:
            self._logger.info('entering endgame mode (remaining pieces: %s)',
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
                pieces = self._download_info.pieces
                for request in requests_done:
                    if request.performer in self._peer_data:
                        self._peer_data[request.performer].queue_size -= 1

                    piece_info = pieces[request.piece_index]
                    if not piece_info.validating and not piece_info.downloaded and not piece_info.blocks_expected:
                        piece_info.validating = True
                        await self._validate_piece(request.piece_index)
                        piece_info.validating = False
                processed_requests.clear()
                processed_requests += list(requests_pending)
            else:
                hanged_peers = {request.performer for request in requests_pending} & set(self._peer_data.keys())
                cur_time = time.time()
                for peer in hanged_peers:
                    self._peer_data[peer].hanged_time = cur_time
                if hanged_peers:
                    self._logger.debug('peers %s hanged', ', '.join(map(str, hanged_peers)))

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
        self._logger.debug('connecting to up to %s new peers', min(len(peers), peers_to_connect_count))

        for peer in peers[:peers_to_connect_count]:
            client = PeerTCPClient(self._our_peer_id, peer)
            self._client_executors[peer] = asyncio.ensure_future(
                self._execute_peer_client(peer, client, need_connect=True))

    def accept_client(self, peer: Peer, client: PeerTCPClient):
        if len(self._peer_data) > TorrentManager.MAX_PEERS_TO_ACCEPT or self._download_info.is_banned(peer) or \
                peer in self._peer_data:
            client.close()
            return
        self._logger.debug('accepted connection from %s', peer)

        self._client_executors[peer] = asyncio.ensure_future(
            self._execute_peer_client(peer, client, need_connect=False))

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
            self._logger.warning('exception on announce: %s', repr(e))
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
            self._download_info.complete = True
            return

        random.shuffle(self._non_started_pieces)

        for _ in range(TorrentManager.DOWNLOAD_PEER_COUNT):
            processed_requests = []
            self._executors_processed_requests.append(processed_requests)
            self._request_executors.append(asyncio.ensure_future(self._execute_block_requests(processed_requests)))

        await asyncio.wait(self._request_executors)

        self._download_info.complete = True
        await self._try_to_announce('completed')
        self._logger.info('file download complete')

        # for peer, data in self._peer_data.items():
        #     if data.client.is_seed():
        #         self._client_executors[peer].cancel()

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
            self._logger.debug('now %s peers are unchoked (total_uploaded = %s)', len(cur_unchoked_peers),
                               humanize_size(self._statistics.total_uploaded))

            await asyncio.sleep(TorrentManager.CHOKING_CHANGING_TIME)

            prev_unchoked_peers = cur_unchoked_peers

    SPEED_MEASUREMENT_PERIOD = 10
    SPEED_UPDATE_TIMEOUT = 2

    assert SPEED_MEASUREMENT_PERIOD % SPEED_UPDATE_TIMEOUT == 0

    async def _execute_speed_measurement(self):
        max_queue_length = TorrentManager.SPEED_MEASUREMENT_PERIOD // TorrentManager.SPEED_UPDATE_TIMEOUT

        downloaded_queue = deque()
        uploaded_queue = deque()
        while True:
            downloaded_queue.append(self._statistics.downloaded_per_session)
            uploaded_queue.append(self._statistics.uploaded_per_session)

            if len(downloaded_queue) > 1:
                period_in_seconds = (len(downloaded_queue) - 1) * TorrentManager.SPEED_UPDATE_TIMEOUT
                downloaded_per_period = downloaded_queue[-1] - downloaded_queue[0]
                uploaded_per_period = uploaded_queue[-1] - uploaded_queue[0]
                self._statistics.download_speed = downloaded_per_period / period_in_seconds
                self._statistics.upload_speed = uploaded_per_period / period_in_seconds

            if len(downloaded_queue) > max_queue_length:
                downloaded_queue.popleft()
                uploaded_queue.popleft()

            await asyncio.sleep(TorrentManager.SPEED_UPDATE_TIMEOUT)
        return downloaded_queue

    ANNOUNCE_FAILED_SLEEP_TIME = 3

    async def run(self):
        while not await self._try_to_announce('started'):
            await asyncio.sleep(TorrentManager.ANNOUNCE_FAILED_SLEEP_TIME)

        self._connect_to_peers(self._tracker_client.peers, False)

        self._tasks += [asyncio.ensure_future(coro) for coro in [
            self._execute_keeping_alive(),
            self._execute_regular_announcements(),
            self._execute_uploading(),
            self._execute_speed_measurement()
        ]]

        await self._download()

    async def stop(self):
        executors = self._request_executors + self._tasks + list(self._client_executors.values())
        executors = [task for task in executors if task is not None]

        for task in executors:
            task.cancel()
        if executors:
            await asyncio.wait(executors)

        self._request_executors.clear()
        self._tasks.clear()
        self._client_executors.clear()

        self._tracker_client.close()

        self._file_structure.close()
