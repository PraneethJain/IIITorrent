bit-torrent
===========

A prototype of BitTorrent client

Requirements
------------

* Python 3.5
* Modules `aiohttp` and `bitarray`
* Module `bencodepy` (will not be required in the future)
* PyQt5 (only for GUI)

To install binary packages with the required Python version on Ubuntu you can use
[fkrull/deadsnakes](https://launchpad.net/~fkrull/+archive/ubuntu/deadsnakes) repository that contains various
versions of Python:

    $ sudo apt-add-repository ppa:fkrull/deadsnakes
    $ sudo apt-get update
    $ sudo apt-get install python3.5 python3.5-dev

To install necessary modules run:

    $ sudo python3.5 -m pip install aiohttp bitarray bencodepy

GUI Usage
---------

Run:

    $ python3.5 torrent_gui.py

Add `--debug` to enable debug mode.

CLI Usage
---------

1. Run a daemon in a separate terminal:

        $ python3.5 main.py start

    You also can enable debug mode (this slows down downloading):

        $ PYTHONASYNCIODEBUG=1 python3.5 -Wdefault main.py --debug start

2. *(Optional)* Look at a list of files in a torrent you want to download:

        $ python3.5 main.py show samples/debian-8.3.0-i386-netinst.iso.torrent

3. Specify a download directory and add the torrent to the daemon:

        $ python3.5 main.py add samples/debian-8.3.0-i386-netinst.iso.torrent -d ~/Downloads

    If the torrent contains more than one file, you also can select which files you want to download
    using `--include` and `--exclude` options. For more information run:

        $ python3.5 main.py add --help

4. Watch torrent status:

        $ watch python3.5 main.py status

    You also can add more torrents, pause, resume, and remove them. For more information run:

        $ python3.5 main.py --help

5. You can stop the daemon by hitting `Ctrl+C` in its terminal.
The daemon will resume downloading and uploading of torrents after the next start.
