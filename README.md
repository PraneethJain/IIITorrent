bit-torrent
===========

A prototype of BitTorrent client

Requirements
------------

* Python 3.5
* Modules `aiohttp` and `bitarray`
* Modules `bencodepy` and `contexttimer` (will not be required in the future)

To install these modules run:

    $ sudo python3.5 -m pip install aiohttp bitarray bencodepy contexttimer

Usage
-----

    $ python3.5 main.py <torrent_file>
