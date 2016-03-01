#!/usr/bin/python3
# -*- coding: utf-8 -*-
import argparse
import asyncio
import logging
import os
import sys
from contextlib import closing
from functools import partial
from math import floor
from typing import Dict

# noinspection PyUnresolvedReferences
from PyQt5.QtCore import Qt, QThread
# noinspection PyUnresolvedReferences
from PyQt5.QtGui import QIcon, QFont
# noinspection PyUnresolvedReferences
from PyQt5.QtWidgets import QWidget, QListWidget, QAbstractItemView, QLabel, QVBoxLayout, QProgressBar, \
    QListWidgetItem, QMainWindow, QApplication, QFileDialog, QMessageBox

from control_manager import ControlManager
from models import TorrentState, TorrentInfo
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

        self._state = None

    @property
    def state(self) -> TorrentState:
        return self._state

    @state.setter
    def state(self, state: TorrentState):
        self._state = state

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
    def __init__(self, control_thread: 'ControlManagerThread'):
        super().__init__()

        self._control_thread = control_thread
        self._control = control_thread.control

        self._icon = QIcon('exit24x24.png')

        toolbar = self.addToolBar('Exits')
        toolbar.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
        toolbar.setMovable(False)
        self._add_action = toolbar.addAction(self._icon, 'Add')
        self._add_action.triggered.connect(self._add_torrent_triggered)
        self._pause_action = toolbar.addAction(self._icon, 'Pause')
        self._pause_action.setEnabled(False)
        self._pause_action.triggered.connect(partial(self._control_action_triggered, self._control.pause))
        self._resume_action = toolbar.addAction(self._icon, 'Resume')
        self._resume_action.setEnabled(False)
        self._resume_action.triggered.connect(partial(self._control_action_triggered, self._control.resume))
        self._remove_action = toolbar.addAction(self._icon, 'Remove')
        self._remove_action.setEnabled(False)
        self._remove_action.triggered.connect(partial(self._control_action_triggered, self._control.remove))

        self._list_widget = QListWidget()
        self._list_widget.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self._list_widget.setVerticalScrollMode(QAbstractItemView.ScrollPerPixel)
        self._list_widget.itemSelectionChanged.connect(self._update_control_action_state)
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
        widget.state = state

        item = QListWidgetItem()
        item.setIcon(self._icon)
        item.setSizeHint(widget.sizeHint())
        item.setData(Qt.UserRole, state.info_hash)

        items_upper = 0
        for i in range(self._list_widget.count()):
            prev_item = self._list_widget.item(i)
            if self._list_widget.itemWidget(prev_item).state.suggested_name > state.suggested_name:
                break
            items_upper += 1
        self._list_widget.insertItem(items_upper, item)

        self._list_widget.setItemWidget(item, widget)
        self._torrent_to_item[state.info_hash] = item

    def _update_torrent_item(self, state: TorrentState):
        widget = self._list_widget.itemWidget(self._torrent_to_item[state.info_hash])
        widget.state = state

        self._update_control_action_state()

    def _remove_torrent_item(self, info_hash: bytes):
        item = self._torrent_to_item[info_hash]
        self._list_widget.takeItem(self._list_widget.row(item))
        del self._torrent_to_item[info_hash]

        self._update_control_action_state()

    def _update_control_action_state(self):
        self._pause_action.setEnabled(False)
        self._resume_action.setEnabled(False)
        self._remove_action.setEnabled(False)
        for item in self._list_widget.selectedItems():
            state = self._list_widget.itemWidget(item).state
            if state.paused:
                self._resume_action.setEnabled(True)
            else:
                self._pause_action.setEnabled(True)
            self._remove_action.setEnabled(True)

    def _add_torrent_triggered(self):
        filename, _ = QFileDialog.getOpenFileName(self, 'Add torrent', filter='Torrent file (*.torrent)')
        if not filename:
            return

        try:
            torrent_info = TorrentInfo.from_file(filename, download_dir=os.path.curdir)
        except Exception as err:
            QMessageBox.critical(self, 'Failed to add torrent', str(err))
            return

        self._control_thread.loop.call_soon_threadsafe(self._control.add, torrent_info)

    async def _invoke_control_action(self, action, info_hash: bytes):
        try:
            result = action(info_hash)
            if asyncio.iscoroutine(result):
                await result
        except ValueError:
            pass

    def _control_action_triggered(self, action):
        for item in self._list_widget.selectedItems():
            info_hash = item.data(Qt.UserRole)
            asyncio.run_coroutine_threadsafe(self._invoke_control_action(action, info_hash), self._control_thread.loop)


class ControlManagerThread(QThread):
    def __init__(self, control: ControlManager):
        super().__init__()

        self._loop = None  # type: asyncio.AbstractEventLoop
        self._control = control
        self._stopping = False

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    @property
    def control(self) -> ControlManager:
        return self._control

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


def main():
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
    main_window = MainWindow(control_thread)
    return app.exec_()


if __name__ == '__main__':
    sys.exit(main())
