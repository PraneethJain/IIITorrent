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


logging.basicConfig(format='%(levelname)s %(asctime)s %(name)-23s %(message)s', datefmt='%H:%M:%S')


STATE_FILENAME = 'state.bin'


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        icon = QIcon('exit24x24.png')

        self.toolbar = self.addToolBar('Exits')
        self.toolbar.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
        self.toolbar.setMovable(False)
        self.toolbar.addAction(icon, 'Open')
        self.toolbar.addAction(icon, 'Pause')
        self.toolbar.addAction(icon, 'Resume')
        self.toolbar.addAction(icon, 'Remove')

        listWidget = QListWidget()
        listWidget.setVerticalScrollMode(QAbstractItemView.ScrollPerPixel)

        name_font = QFont()
        name_font.setBold(True)

        stats_font = QFont()
        stats_font.setPointSize(10)

        for i in range(10):
            label_name = QLabel()
            label_name.setFont(name_font)
            label_name.setText('Torrent {}'.format(i))  # FIXME: XSS

            label_stats = QLabel()
            label_stats.setFont(stats_font)
            label_stats.setText('Download speed 1.9 MiB/s')

            vbox = QVBoxLayout()
            vbox.addWidget(label_name)
            bar = QProgressBar()
            bar.setFixedHeight(15)
            vbox.addWidget(bar)
            vbox.addWidget(label_stats)

            widget = QWidget()
            widget.setLayout(vbox)

            item = QListWidgetItem()
            item.setIcon(icon)
            item.setSizeHint(widget.sizeHint())
            listWidget.addItem(item)
            listWidget.setItemWidget(item, widget)

        self.setCentralWidget(listWidget)

        self.setGeometry(300, 300, 500, 450)
        self.setWindowTitle('BitTorrent Client')
        self.show()


class ControlManagerThread(QThread):
    def __init__(self):
        super().__init__()

        self._loop = None     # type: asyncio.AbstractEventLoop
        self._control = None  # type: ControlManager
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
            self._control = ControlManager()
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
        stop_fut.add_done_callback(lambda fut: self._loop.call_soon_threadsafe(self._loop.stop))

        self.wait()


if __name__ == '__main__':
    app = QApplication(sys.argv)

    control_manager_thread = ControlManagerThread()
    control_manager_thread.start()

    app.lastWindowClosed.connect(control_manager_thread.stop)
    main_window = MainWindow()
    sys.exit(app.exec_())
