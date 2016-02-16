import asyncio
import logging

from control_manager import ControlManager

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class ControlServer:
    def __init__(self, control: ControlManager):
        self._control = control

        self._server = None

        self._flag = False

    HANDSHAKE_MESSAGE = b'bit-torrent:ControlServer\n'

    async def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr_repr = ':'.join(map(str, writer.get_extra_info('peername')))
        logger.info('accepted connection from %s', addr_repr)

        writer.write(ControlServer.HANDSHAKE_MESSAGE)

        try:
            for info_hash in self._control.torrents:
                if not self._flag:
                    await self._control.pause(info_hash)
                else:
                    self._control.resume(info_hash)
            self._flag = not self._flag
            writer.write(b'success\n')
        except Exception as e:
            writer.write(repr(e).encode() + b'\n')

        writer.close()

    PORT_RANGE = range(6991, 6999 + 1)

    async def start(self):
        for port in ControlServer.PORT_RANGE:
            try:
                self._server = await asyncio.start_server(self._accept, host='127.0.0.1', port=port)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug('exception on starting server on port %s: %s', port, repr(e))
            else:
                logger.info('server started on port %s', port)
                return
        else:
            raise RuntimeError('Failed to start a control server')

    async def stop(self):
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            logger.info('server stopped')
