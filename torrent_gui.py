#!/usr/bin/python3
# -*- coding: utf-8 -*-
import argparse
import asyncio
import logging
import os
import sys
from contextlib import closing
from math import floor
from typing import Dict

# noinspection PyUnresolvedReferences
from PyQt5.QtCore import Qt, QThread
# noinspection PyUnresolvedReferences
from PyQt5.QtGui import QIcon, QFont
# noinspection PyUnresolvedReferences
from PyQt5.QtWidgets import QWidget, QListWidget, QAbstractItemView, QLabel, QVBoxLayout, QProgressBar, \
    QListWidgetItem, QMainWindow, QApplication

from control_manager import ControlManager
from models import TorrentState
from utils import humanize_speed, humanize_time, humanize_size


logging.basicConfig(format='%(levelname)s %(asctime)s %(name)-23s %(message)s', datefmt='%H:%M:%S')


STATE_FILENAME = 'state.bin'


class TorrentWidgetItem(QWidget):
    _name_font = QFont()
    _name_font.setBold(True)

    _stats_font = QFont()
    _stats_font.setPointSize(10)

    def __init__(self):
        super().__init__()

        self._name_label = QLabel()
        self._name_label.setFont(TorrentWidgetItem._name_font)

        self._upper_status_label = QLabel()
        self._upper_status_label.setFont(TorrentWidgetItem._stats_font)

        self._progress_bar = QProgressBar()
        self._progress_bar.setFixedHeight(15)
        self._progress_bar.setMaximum(1000)

        self._lower_status_label = QLabel()
        self._lower_status_label.setFont(TorrentWidgetItem._stats_font)

        vbox = QVBoxLayout()
        vbox.addWidget(self._name_label)
        vbox.addWidget(self._upper_status_label)
        vbox.addWidget(self._progress_bar)
        vbox.addWidget(self._lower_status_label)

        self.setLayout(vbox)

    def set_state(self, state: TorrentState):
        self._name_label.setText(state.suggested_name)  # FIXME: XSS

        if state.downloaded_size < state.selected_size:
            status_text = '{} of {}'.format(humanize_size(state.downloaded_size), humanize_size(state.selected_size))
        else:
            status_text = '{} (complete)'.format(humanize_size(state.selected_size))
        status_text += ', Ratio: {:.1f}'.format(state.ratio)
        self._upper_status_label.setText(status_text)

        self._progress_bar.setValue(floor(state.progress * 1000))

        if state.paused:
            status_text = 'Paused'
        elif state.complete:
            status_text = 'Uploading to {} of {} peers'.format(state.uploading_peer_count, state.total_peer_count)
            if state.upload_speed:
                status_text += ' on {}'.format(humanize_speed(state.upload_speed))
        else:
            status_text = 'Downloading from {} of {} peers'.format(
                state.downloading_peer_count, state.total_peer_count)
            if state.download_speed:
                status_text += ' on {}'.format(humanize_speed(state.download_speed))
            eta_seconds = state.eta_seconds
            if eta_seconds is not None:
                status_text += ', {} remaining'.format(humanize_time(eta_seconds) if eta_seconds is not None else None)
        self._lower_status_label.setText(status_text)


class MainWindow(QMainWindow):
    def __init__(self, control: ControlManager):
        super().__init__()

        self._control = control

        self._icon = QIcon('exit24x24.png')

        toolbar = self.addToolBar('Exits')
        toolbar.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
        toolbar.setMovable(False)
        toolbar.addAction(self._icon, 'Open')
        toolbar.addAction(self._icon, 'Pause')
        toolbar.addAction(self._icon, 'Resume')
        toolbar.addAction(self._icon, 'Remove')

        self._list_widget = QListWidget()
        self._list_widget.setVerticalScrollMode(QAbstractItemView.ScrollPerPixel)
        self._torrent_to_item = {}  # type: Dict[bytes, QListWidgetItem]

        self.setCentralWidget(self._list_widget)

        self.setMinimumSize(600, 500)
        self.setWindowTitle('BitTorrent Client')

        self._control.torrent_added.connect(self._add_torrent_item)
        self._control.torrent_changed.connect(self._update_torrent_item)
        self._control.torrent_removed.connect(self._remove_torrent_item)

        self.show()

    def _add_torrent_item(self, state: TorrentState):
        widget = TorrentWidgetItem()
        widget.set_state(state)

        item = QListWidgetItem()
        item.setIcon(self._icon)
        item.setSizeHint(widget.sizeHint())
        self._list_widget.addItem(item)
        self._list_widget.setItemWidget(item, widget)
        self._torrent_to_item[state.info_hash] = item

    def _update_torrent_item(self, state: TorrentState):
        widget = self._list_widget.itemWidget(self._torrent_to_item[state.info_hash])
        widget.set_state(state)

    def _remove_torrent_item(self, info_hash: bytes):
        row = self._list_widget.row(self._torrent_to_item[info_hash])
        self._list_widget.takeItem(row)
        del self._torrent_to_item[info_hash]


class ControlManagerThread(QThread):
    def __init__(self, control: ControlManager):
        super().__init__()

        self._loop = None  # type: asyncio.AbstractEventLoop
        self._control = control
        self._stopping = False

    def _load_state(self):
        if os.path.isfile(STATE_FILENAME):
            with open(STATE_FILENAME, 'rb') as f:
                self._control.load(f)

    def _save_state(self):
        with open(STATE_FILENAME, 'wb') as f:
            self._control.dump(f)

    def run(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        with closing(self._loop):
            self._loop.run_until_complete(self._control.start())

            self._load_state()

            try:
                self._loop.run_forever()
            finally:
                self._save_state()

    def stop(self):
        if self._stopping:
            return
        self._stopping = True

        stop_fut = asyncio.run_coroutine_threadsafe(self._control.stop(), self._loop)
        stop_fut.add_done_callback(lambda fut: self._loop.stop())

        self.wait()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='A prototype of BitTorrent client (GUI)')
    parser.add_argument('--debug', action='store_true', help='Show debug messages')
    args = parser.parse_args()

    if not args.debug:
        logging.disable(logging.INFO)

    app = QApplication(sys.argv)

    control = ControlManager()
    control_thread = ControlManagerThread(control)
    control_thread.start()

    app.lastWindowClosed.connect(control_thread.stop)
    main_window = MainWindow(control)
    sys.exit(app.exec_())
