#!/usr/bin/python3
# -*- coding: utf-8 -*-
import asyncio
import logging
import os
import sys
from contextlib import closing

# noinspection PyUnresolvedReferences
from PyQt5.QtCore import Qt, QThread
# noinspection PyUnresolvedReferences
from PyQt5.QtGui import QIcon, QFont
# noinspection PyUnresolvedReferences
from PyQt5.QtWidgets import QWidget, QListWidget, QAbstractItemView, QLabel, QVBoxLayout, QProgressBar, \
    QListWidgetItem, QMainWindow, QApplication

from control_manager import ControlManager
from models import TorrentInfo


logging.basicConfig(format='%(levelname)s %(asctime)s %(name)-23s %(message)s', datefmt='%H:%M:%S')


STATE_FILENAME = 'state.bin'


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

        self._name_font = QFont()
        self._name_font.setBold(True)
        self._stats_font = QFont()
        self._stats_font.setPointSize(10)

        self.setCentralWidget(self._list_widget)

        self.setGeometry(300, 300, 500, 450)
        self.setWindowTitle('BitTorrent Client')

        self._control.torrents_changed.connect(self._update_torrents)

        self.show()

    def _add_torrent(self, info: TorrentInfo):
        download_info = info.download_info

        name_label = QLabel()
        name_label.setFont(self._name_font)
        name_label.setText(download_info.suggested_name)  # FIXME: XSS

        progress_bar = QProgressBar()
        progress_bar.setFixedHeight(15)

        stats_label = QLabel()
        stats_label.setFont(self._stats_font)
        stats_label.setText('Download speed 1.9 MiB/s')

        vbox = QVBoxLayout()
        vbox.addWidget(name_label)
        vbox.addWidget(progress_bar)
        vbox.addWidget(stats_label)

        widget = QWidget()
        widget.setLayout(vbox)

        item = QListWidgetItem()
        item.setIcon(self._icon)
        item.setSizeHint(widget.sizeHint())
        self._list_widget.addItem(item)
        self._list_widget.setItemWidget(item, widget)

    def _update_torrents(self):
        self._list_widget.clear()

        # FIXME: locks
        torrents = self._control.get_torrents()
        torrents.sort(key=lambda info: info.download_info.suggested_name)
        for info in torrents:
            self._add_torrent(info)


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
    app = QApplication(sys.argv)

    control = ControlManager()
    control_thread = ControlManagerThread(control)
    control_thread.start()

    app.lastWindowClosed.connect(control_thread.stop)
    main_window = MainWindow(control)
    sys.exit(app.exec_())
