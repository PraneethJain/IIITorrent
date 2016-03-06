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
from models import BlockRequestFuture, DownloadInfo, Peer, TorrentInfo, TorrentState, SessionStatistics
from peer_tcp_client import PeerTCPClient
from tracker_clients import BaseTrackerClient, create_tracker_client, EventType
from utils import humanize_size, floor_to, import_signals


QObject, pyqtSignal = import_signals()


DOWNLOAD_REQUEST_QUEUE_SIZE = 12


class PeerData:
    def __init__(self, client: PeerTCPClient, client_task: asyncio.Task, connected_time: float):
        self._client = client
        self._client_task = client_task
        self._connected_time = connected_time
        self.hanged_time = None  # type: Optional[float]
        self.queue_size = 0

    @property
    def client(self) -> PeerTCPClient:
        return self._client

    @property
    def client_task(self) -> asyncio.Task:
        return self._client_task

    @property
    def connected_time(self) -> float:
        return self._connected_time

    def is_free(self) -> bool:
        return self.queue_size < DOWNLOAD_REQUEST_QUEUE_SIZE

    def is_available(self) -> bool:
        return self.is_free() and not self._client.peer_choking


class PeerManager:
    def __init__(self, torrent_info: TorrentInfo, our_peer_id: bytes,
                 logger: logging.Logger, file_structure: FileStructure):
        # self._torrent_info = torrent_info
        self._download_info = torrent_info.download_info
        self._statistics = self._download_info.session_statistics
        self._our_peer_id = our_peer_id

        self._logger = logger
        self._file_structure = file_structure

        self._peer_data = {}
        self._client_executors = {}          # type: Dict[Peer, asyncio.Task]
        self._keeping_alive_executor = None  # type: Optional[asyncio.Task]
        self._last_connecting_time = None    # type: Optional[float]

    @property
    def peer_data(self) -> Dict[Peer, PeerData]:
        return self._peer_data

    @property
    def last_connecting_time(self) -> int:
        return self._last_connecting_time

    async def _execute_peer_client(self, peer: Peer, client: PeerTCPClient, *, need_connect: bool):
        try:
            if need_connect:
                await client.connect(self._download_info, self._file_structure)
            else:
                client.confirm_info_hash(self._download_info, self._file_structure)

            self._peer_data[peer] = PeerData(client, asyncio.Task.current_task(), time.time())
            self._statistics.peer_count += 1

            await client.run()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self._logger.debug('%s disconnected because of %r', peer, e)
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

            del self._client_executors[peer]

    KEEP_ALIVE_TIMEOUT = 2 * 60

    async def _execute_keeping_alive(self):
        while True:
            await asyncio.sleep(PeerManager.KEEP_ALIVE_TIMEOUT)

            self._logger.debug('broadcasting keep-alives to %s alive peers', len(self._peer_data))
            for data in self._peer_data.values():
                data.client.send_keep_alive()

    MAX_PEERS_TO_ACTIVELY_CONNECT = 30
    MAX_PEERS_TO_ACCEPT = 55

    def connect_to_peers(self, peers: Sequence[Peer], force: bool):
        peers = list({peer for peer in peers
                      if peer not in self._client_executors and not self._download_info.is_banned(peer)})
        if force:
            max_peers_count = PeerManager.MAX_PEERS_TO_ACCEPT
        else:
            max_peers_count = PeerManager.MAX_PEERS_TO_ACTIVELY_CONNECT
        peers_to_connect_count = max(max_peers_count - len(self._peer_data), 0)
        self._logger.debug('trying to connect to %s new peers', min(len(peers), peers_to_connect_count))

        for peer in peers[:peers_to_connect_count]:
            client = PeerTCPClient(self._our_peer_id, peer)
            self._client_executors[peer] = asyncio.ensure_future(
                self._execute_peer_client(peer, client, need_connect=True))

        self._last_connecting_time = time.time()

    def accept_client(self, peer: Peer, client: PeerTCPClient):
        if len(self._peer_data) > PeerManager.MAX_PEERS_TO_ACCEPT or self._download_info.is_banned(peer) or \
                peer in self._client_executors:
            client.close()
            return
        self._logger.debug('accepted connection from %s', peer)

        self._client_executors[peer] = asyncio.ensure_future(
            self._execute_peer_client(peer, client, need_connect=False))

    def invoke(self):
        self._keeping_alive_executor = asyncio.ensure_future(self._execute_keeping_alive())

    async def stop(self):
        tasks = []
        if self._keeping_alive_executor is not None:
            tasks.append(self._keeping_alive_executor)
        tasks += list(self._client_executors.values())

        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.wait(tasks)


class Announcer:
    def __init__(self, torrent_info: TorrentInfo, our_peer_id: bytes, server_port: int, logger: logging.Logger,
                 peer_manager: PeerManager):
        self._torrent_info = torrent_info
        self._download_info = torrent_info.download_info
        self._our_peer_id = our_peer_id
        self._server_port = server_port

        self._logger = logger
        self._peer_manager = peer_manager

        self._last_tracker_client = None
        self._more_peers_requested = asyncio.Event()
        self._task = None  # type: Optional[asyncio.Task]

    @property
    def last_tracker_client(self) -> BaseTrackerClient:
        return self._last_tracker_client

    @property
    def more_peers_requested(self) -> asyncio.Event:
        return self._more_peers_requested

    FAKE_SERVER_PORT = 6881
    DEFAULT_MIN_INTERVAL = 90

    async def try_to_announce(self, event: EventType) -> bool:
        server_port = self._server_port if self._server_port is not None else Announcer.FAKE_SERVER_PORT

        tier = None
        url = None
        lift_url = False
        try:
            for tier in self._torrent_info.announce_list:
                for url in tier:
                    try:
                        client = create_tracker_client(url, self._download_info, self._our_peer_id)
                        await client.announce(server_port, event)
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        self._logger.info('announce to "%s" failed: %r', url, e)
                    else:
                        peer_count = len(client.peers) if client.peers else 'no'
                        self._logger.debug('announce to "%s" succeed (%s peers, interval = %s, min_interval = %s)',
                                           url, peer_count, client.interval, client.min_interval)

                        self._last_tracker_client = client
                        lift_url = True
                        return True
            return False
        finally:
            if lift_url:
                tier.remove(url)
                tier.insert(0, url)

    async def execute(self):
        try:
            while True:
                if self._last_tracker_client.min_interval is not None:
                    min_interval = self._last_tracker_client.min_interval
                else:
                    min_interval = min(Announcer.DEFAULT_MIN_INTERVAL, self._last_tracker_client.interval)
                await asyncio.sleep(min_interval)

                default_interval = self._last_tracker_client.interval
                try:
                    await asyncio.wait_for(self._more_peers_requested.wait(), default_interval - min_interval)
                    more_peers = True
                    self._more_peers_requested.clear()
                except asyncio.TimeoutError:
                    more_peers = False

                await self.try_to_announce(EventType.none)
                # TODO: if more_peers, maybe rerequest in case of exception

                self._peer_manager.connect_to_peers(self._last_tracker_client.peers, more_peers)
        finally:
            await self.try_to_announce(EventType.stopped)


class NotEnoughPeersError(RuntimeError):
    pass


class NoRequestsError(RuntimeError):
    pass


class Downloader(QObject):
    if pyqtSignal:
        progress = pyqtSignal()

    def __init__(self, torrent_info: TorrentInfo, our_peer_id: bytes,
                 logger: logging.Logger, file_structure: FileStructure,
                 peer_manager: PeerManager, announcer: Announcer):
        super().__init__()

        self._torrent_info = torrent_info
        self._download_info = torrent_info.download_info
        self._our_peer_id = our_peer_id

        self._logger = logger
        self._file_structure = file_structure
        self._peer_manager = peer_manager
        self._announcer = announcer

        self._request_executors = []  # type: List[asyncio.Task]

        self._executors_processed_requests = []  # type: List[List[BlockRequestFuture]]

        self._non_started_pieces = None   # type: List[int]
        self._download_start_time = None  # type: float

        self._piece_block_queue = OrderedDict()

        self._endgame_mode = False
        self._tasks_waiting_for_more_peers = 0
        self._request_deque_relevant = asyncio.Event()

        self._last_piece_finish_signal_time = None  # type: Optional[float]

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
        peer_data = self._peer_manager.peer_data
        for peer in performers - {source}:
            if peer in peer_data:
                peer_data[peer].client.send_request(request, cancel=True)

    def _start_downloading_piece(self, piece_index: int):
        piece_info = self._download_info.pieces[piece_index]

        blocks_expected = piece_info.blocks_expected
        request_deque = deque()
        for block_begin in range(0, piece_info.length, Downloader.REQUEST_LENGTH):
            block_end = min(block_begin + Downloader.REQUEST_LENGTH, piece_info.length)
            block_length = block_end - block_begin
            request = BlockRequestFuture(piece_index, block_begin, block_length)
            request.add_done_callback(self._send_cancels)

            blocks_expected.add(request)
            request_deque.append(request)
        self._piece_block_queue[piece_index] = request_deque

        self._download_info.interesting_pieces.add(piece_index)
        peer_data = self._peer_manager.peer_data
        for peer in piece_info.owners:
            peer_data[peer].client.am_interested = True

        concurrent_peers_count = sum(1 for peer, data in peer_data.items() if data.queue_size)
        self._logger.debug('piece %s started (owned by %s alive peers, concurrency: %s peers)',
                           piece_index, len(piece_info.owners), concurrent_peers_count)

    PIECE_FINISH_SIGNAL_MIN_INTERVAL = 1

    def _finish_downloading_piece(self, piece_index: int):
        piece_info = self._download_info.pieces[piece_index]

        piece_info.mark_as_downloaded()
        self._download_info.downloaded_piece_count += 1

        self._download_info.interesting_pieces.remove(piece_index)
        peer_data = self._peer_manager.peer_data
        for peer in piece_info.owners:
            client = peer_data[peer].client
            for index in self._download_info.interesting_pieces:
                if client.piece_owned[index]:
                    break
            else:
                client.am_interested = False

        for data in peer_data.values():
            data.client.send_have(piece_index)

        self._logger.debug('piece %s finished', piece_index)

        torrent_state = TorrentState(self._torrent_info)
        self._logger.info('progress %.1lf%% (%s / %s pieces)', floor_to(torrent_state.progress * 100, 1),
                          self._download_info.downloaded_piece_count, torrent_state.selected_piece_count)

        if pyqtSignal and self._download_info.downloaded_piece_count < torrent_state.selected_piece_count:
            cur_time = time.time()
            if self._last_piece_finish_signal_time is None or \
                    cur_time - self._last_piece_finish_signal_time >= Downloader.PIECE_FINISH_SIGNAL_MIN_INTERVAL:
                self.progress.emit()
                self._last_piece_finish_signal_time = time.time()
            # If the signal isn't emitted, the GUI will be updated after the next speed measurement anyway

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

        peer_data = self._peer_manager.peer_data
        for peer in piece_info.sources:
            self._download_info.increase_distrust(peer)
            if self._download_info.is_banned(peer):
                self._logger.info('Host %s banned', peer.host)
                peer_data[peer].client_task.cancel()

        piece_info.reset_content()
        self._start_downloading_piece(piece_index)

        self._logger.debug('piece %s not valid, redownloading', piece_index)

    _INF = float('inf')

    HANG_PENALTY_DURATION = 10
    HANG_PENALTY_COEFF = 100

    def get_peer_download_rate(self, peer: Peer) -> int:
        data = self._peer_manager.peer_data[peer]

        rate = data.client.downloaded  # To reach maximal download speed
        if data.hanged_time is not None and time.time() - data.hanged_time <= Downloader.HANG_PENALTY_DURATION:
            rate //= Downloader.HANG_PENALTY_COEFF
        return rate

    DOWNLOAD_PEER_COUNT = 15

    def _request_piece_blocks(self, max_pending_count: int, piece_index: int) -> Iterator[BlockRequestFuture]:
        if not max_pending_count:
            return
        piece_info = self._download_info.pieces[piece_index]
        peer_data = self._peer_manager.peer_data

        request_deque = self._piece_block_queue[piece_index]
        performer = None
        performer_data = None
        pending_count = 0
        while request_deque:
            request = request_deque[0]
            if request.done():
                request_deque.popleft()

                yield request
                continue

            if performer is None or not performer_data.is_free():
                available_peers = {peer for peer in piece_info.owners
                                   if peer_data[peer].is_available()}
                if not available_peers:
                    return
                performer = max(available_peers, key=self.get_peer_download_rate)
                performer_data = peer_data[performer]
            request_deque.popleft()
            performer_data.queue_size += 1

            request.performer = performer
            performer_data.client.send_request(request)
            yield request

            pending_count += 1
            if pending_count == max_pending_count:
                return

    RAREST_PIECE_COUNT_TO_SELECT = 10

    def _select_new_piece(self, *, force: bool) -> Optional[int]:
        is_appropriate = PeerData.is_free if force else PeerData.is_available
        appropriate_peers = {peer for peer, data in self._peer_manager.peer_data.items() if is_appropriate(data)}
        if not appropriate_peers:
            return None

        pieces = self._download_info.pieces
        available_pieces = [index for index in self._non_started_pieces
                            if appropriate_peers & pieces[index].owners]
        if not available_pieces:
            return None

        available_pieces.sort(key=lambda index: len(pieces[index].owners))
        piece_count_to_select = min(len(available_pieces), Downloader.RAREST_PIECE_COUNT_TO_SELECT)
        return available_pieces[random.randint(0, piece_count_to_select - 1)]

    _typical_piece_length = 2 ** 20
    _requests_per_piece = ceil(_typical_piece_length / REQUEST_LENGTH)
    _desired_request_stock = DOWNLOAD_PEER_COUNT * DOWNLOAD_REQUEST_QUEUE_SIZE
    DESIRED_PIECE_STOCK = ceil(_desired_request_stock / _requests_per_piece)

    def _request_blocks(self, max_pending_count: int) -> List[BlockRequestFuture]:
        result = []
        pending_count = 0
        consumed_pieces = []
        try:
            for piece_index, request_deque in self._piece_block_queue.items():
                piece_requests = list(self._request_piece_blocks(max_pending_count - pending_count, piece_index))
                result += piece_requests
                pending_count += sum(1 for request in piece_requests if not request.done())
                if not request_deque:
                    consumed_pieces.append(piece_index)
                if pending_count == max_pending_count:
                    return result

            piece_stock = len(self._piece_block_queue) - len(consumed_pieces)
            piece_stock_small = (piece_stock < Downloader.DESIRED_PIECE_STOCK)
            new_piece_index = self._select_new_piece(force=piece_stock_small)
            if new_piece_index is not None:
                self._non_started_pieces.remove(new_piece_index)
                self._start_downloading_piece(new_piece_index)

                result += list(self._request_piece_blocks(max_pending_count - pending_count, new_piece_index))
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

    RECONNECT_TIMEOUT = 50

    async def _wait_more_peers(self):
        self._tasks_waiting_for_more_peers += 1
        download_peers_active = Downloader.DOWNLOAD_PEER_COUNT - self._tasks_waiting_for_more_peers
        if download_peers_active <= Downloader.DOWNLOAD_PEERS_ACTIVE_TO_REQUEST_MORE_PEERS and \
                len(self._peer_manager.peer_data) < PeerManager.MAX_PEERS_TO_ACTIVELY_CONNECT:
            cur_time = time.time()
            if self._peer_manager.last_connecting_time is None or \
                    cur_time - self._peer_manager.last_connecting_time >= Downloader.RECONNECT_TIMEOUT:
                # This can recover connections to peers after temporary loss of Internet connection
                self._logger.info('trying to reconnect to peers')
                self._peer_manager.connect_to_peers(self._announcer.last_tracker_client.peers, True)

            self._announcer.more_peers_requested.set()

        if time.time() - self._download_start_time <= Downloader.STARTING_DURATION:
            sleep_time = Downloader.NO_PEERS_SLEEP_TIME_ON_STARTING
        else:
            sleep_time = Downloader.NO_PEERS_SLEEP_TIME
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
                max_pending_count = DOWNLOAD_REQUEST_QUEUE_SIZE - len(processed_requests)
                if max_pending_count > 0:
                    processed_requests += self._request_blocks(max_pending_count)
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
                request_timeout = Downloader.REQUEST_TIMEOUT_ENDGAME
            else:
                request_timeout = Downloader.REQUEST_TIMEOUT
            requests_done, requests_pending = await asyncio.wait(
                processed_requests, return_when=asyncio.FIRST_COMPLETED, timeout=request_timeout)

            peer_data = self._peer_manager.peer_data
            if len(requests_pending) < len(processed_requests):
                pieces = self._download_info.pieces
                for request in requests_done:
                    if request.performer in peer_data:
                        peer_data[request.performer].queue_size -= 1

                    piece_info = pieces[request.piece_index]
                    if not piece_info.validating and not piece_info.downloaded and not piece_info.blocks_expected:
                        piece_info.validating = True
                        await self._validate_piece(request.piece_index)
                        piece_info.validating = False
                processed_requests.clear()
                processed_requests += list(requests_pending)
            else:
                hanged_peers = {request.performer for request in requests_pending} & set(peer_data.keys())
                cur_time = time.time()
                for peer in hanged_peers:
                    peer_data[peer].hanged_time = cur_time
                if hanged_peers:
                    self._logger.debug('peers %s hanged', ', '.join(map(str, hanged_peers)))

                for request in requests_pending:
                    if request.performer in peer_data:
                        peer_data[request.performer].queue_size -= 1
                        request.prev_performers.add(request.performer)
                    request.performer = None

                    self._piece_block_queue.setdefault(request.piece_index, deque()).append(request)
                processed_requests.clear()
                self._request_deque_relevant.set()
                self._request_deque_relevant.clear()

    async def run(self):
        self._non_started_pieces = self._get_non_finished_pieces()
        self._download_start_time = time.time()
        if not self._non_started_pieces:
            self._download_info.complete = True
            return

        random.shuffle(self._non_started_pieces)

        for _ in range(Downloader.DOWNLOAD_PEER_COUNT):
            processed_requests = []
            self._executors_processed_requests.append(processed_requests)
            self._request_executors.append(asyncio.ensure_future(self._execute_block_requests(processed_requests)))

        await asyncio.wait(self._request_executors)

        self._download_info.complete = True
        await self._announcer.try_to_announce(EventType.completed)
        self._logger.info('file download complete')

        if pyqtSignal:
            self.progress.emit()

        # for peer, data in self._peer_manager.peer_data.items():
        #     if data.client.is_seed():
        #         data.client_task.cancel()

    async def stop(self):
        for task in self._request_executors:
            task.cancel()
        if self._request_executors:
            await asyncio.wait(self._request_executors)


class Uploader:
    def __init__(self, torrent_info: TorrentInfo, logger: logging.Logger, peer_manager: PeerManager):
        self._download_info = torrent_info.download_info
        self._statistics = self._download_info.session_statistics

        self._logger = logger
        self._peer_manager = peer_manager

    CHOKING_CHANGING_TIME = 10
    UPLOAD_PEER_COUNT = 4

    ITERS_PER_OPTIMISTIC_UNCHOKING = 3
    CONNECTED_RECENTLY_THRESHOLD = 60
    CONNECTED_RECENTLY_COEFF = 3

    def _select_optimistically_unchoked(self, peers: Iterable[Peer]) -> Peer:
        cur_time = time.time()
        connected_recently = []
        remaining_peers = []
        peer_data = self._peer_manager.peer_data
        for peer in peers:
            if cur_time - peer_data[peer].connected_time <= Uploader.CONNECTED_RECENTLY_THRESHOLD:
                connected_recently.append(peer)
            else:
                remaining_peers.append(peer)

        max_index = len(remaining_peers) + Uploader.CONNECTED_RECENTLY_COEFF * len(connected_recently) - 1
        index = random.randint(0, max_index)
        if index < len(remaining_peers):
            return remaining_peers[index]
        return connected_recently[(index - len(remaining_peers)) % len(connected_recently)]

    def get_peer_upload_rate(self, peer: Peer) -> int:
        data = self._peer_manager.peer_data[peer]

        rate = data.client.downloaded  # We owe them for downloading
        if self._download_info.complete:
            rate += data.client.uploaded  # To reach maximal upload speed
        return rate

    async def execute(self):
        prev_unchoked_peers = set()
        optimistically_unchoked = None
        for i in itertools.count():
            peer_data = self._peer_manager.peer_data
            alive_peers = list(sorted(peer_data.keys(), key=self.get_peer_upload_rate, reverse=True))
            cur_unchoked_peers = set()
            interested_count = 0

            if Uploader.UPLOAD_PEER_COUNT:
                if i % Uploader.ITERS_PER_OPTIMISTIC_UNCHOKING == 0:
                    if alive_peers:
                        optimistically_unchoked = self._select_optimistically_unchoked(alive_peers)
                    else:
                        optimistically_unchoked = None

                if optimistically_unchoked is not None and optimistically_unchoked in peer_data:
                    cur_unchoked_peers.add(optimistically_unchoked)
                    if peer_data[optimistically_unchoked].client.peer_interested:
                        interested_count += 1

            for peer in cast(List[Peer], alive_peers):
                if interested_count == Uploader.UPLOAD_PEER_COUNT:
                    break
                if peer_data[peer].client.peer_interested:
                    interested_count += 1

                cur_unchoked_peers.add(peer)

            for peer in prev_unchoked_peers - cur_unchoked_peers:
                if peer in peer_data:
                    peer_data[peer].client.am_choking = True
            for peer in cur_unchoked_peers:
                peer_data[peer].client.am_choking = False
            self._logger.debug('now %s peers are unchoked (total_uploaded = %s)', len(cur_unchoked_peers),
                               humanize_size(self._statistics.total_uploaded))

            await asyncio.sleep(Uploader.CHOKING_CHANGING_TIME)

            prev_unchoked_peers = cur_unchoked_peers


class SpeedMeasurer(QObject):
    if pyqtSignal:
        updated = pyqtSignal()

    def __init__(self, statistics: SessionStatistics):
        super().__init__()

        self._statistics = statistics

    SPEED_MEASUREMENT_PERIOD = 60
    SPEED_UPDATE_TIMEOUT = 2

    assert SPEED_MEASUREMENT_PERIOD % SPEED_UPDATE_TIMEOUT == 0

    async def execute(self):
        max_queue_length = SpeedMeasurer.SPEED_MEASUREMENT_PERIOD // SpeedMeasurer.SPEED_UPDATE_TIMEOUT

        downloaded_queue = deque()
        uploaded_queue = deque()
        while True:
            downloaded_queue.append(self._statistics.downloaded_per_session)
            uploaded_queue.append(self._statistics.uploaded_per_session)

            if len(downloaded_queue) > 1:
                period_in_seconds = (len(downloaded_queue) - 1) * SpeedMeasurer.SPEED_UPDATE_TIMEOUT
                downloaded_per_period = downloaded_queue[-1] - downloaded_queue[0]
                uploaded_per_period = uploaded_queue[-1] - uploaded_queue[0]
                self._statistics.download_speed = downloaded_per_period / period_in_seconds
                self._statistics.upload_speed = uploaded_per_period / period_in_seconds

            if len(downloaded_queue) > max_queue_length:
                downloaded_queue.popleft()
                uploaded_queue.popleft()

            if pyqtSignal:
                self.updated.emit()

            await asyncio.sleep(SpeedMeasurer.SPEED_UPDATE_TIMEOUT)


class TorrentManager(QObject):
    if pyqtSignal:
        state_changed = pyqtSignal()

    LOGGER_LEVEL = logging.DEBUG
    SHORT_NAME_LEN = 19

    def __init__(self, torrent_info: TorrentInfo, our_peer_id: bytes, server_port: Optional[int]):
        super().__init__()

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

        self._executors = []  # type: List[asyncio.Task]

        self._file_structure = FileStructure(torrent_info.download_dir, torrent_info.download_info)

        self._peer_manager = PeerManager(torrent_info, our_peer_id, self._logger, self._file_structure)
        self._announcer = Announcer(torrent_info, our_peer_id, server_port, self._logger, self._peer_manager)
        self._downloader = Downloader(torrent_info, our_peer_id, self._logger, self._file_structure,
                                      self._peer_manager, self._announcer)
        self._uploader = Uploader(torrent_info, self._logger, self._peer_manager)
        self._speed_measurer = SpeedMeasurer(torrent_info.download_info.session_statistics)
        if pyqtSignal:
            self._downloader.progress.connect(self.state_changed)
            self._speed_measurer.updated.connect(self.state_changed)

    ANNOUNCE_FAILED_SLEEP_TIME = 3

    def _shuffle_announce_tiers(self):
        for tier in self._torrent_info.announce_list:
            random.shuffle(tier)

    async def run(self):
        self._shuffle_announce_tiers()
        while not await self._announcer.try_to_announce(EventType.started):
            await asyncio.sleep(TorrentManager.ANNOUNCE_FAILED_SLEEP_TIME)

        self._peer_manager.connect_to_peers(self._announcer.last_tracker_client.peers, True)

        self._executors += [asyncio.ensure_future(coro) for coro in [
            self._announcer.execute(),
            self._uploader.execute(),
            self._speed_measurer.execute(),
        ]]

        self._peer_manager.invoke()
        await self._downloader.run()

    def accept_client(self, peer: Peer, client: PeerTCPClient):
        self._peer_manager.accept_client(peer, client)

    async def stop(self):
        await self._downloader.stop()
        await self._peer_manager.stop()

        executors = [task for task in self._executors if task is not None]
        for task in executors:
            task.cancel()
        if executors:
            await asyncio.wait(executors)

        self._file_structure.close()
