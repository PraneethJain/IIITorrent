import asyncio
import logging
import pickle
import struct
from typing import Any, cast, Callable

from control_manager import ControlManager

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class ControlServer:
    def __init__(self, control: ControlManager):
        self._control = control

        self._server = None

        self._flag = False

    HANDSHAKE_MESSAGE = b'bit-torrent:ControlServer\n'

    LENGTH_FMT = '!I'

    @staticmethod
    async def receive_object(reader: asyncio.StreamReader) -> Any:
        length_data = await reader.readexactly(struct.calcsize(ControlServer.LENGTH_FMT))
        (length,) = struct.unpack(ControlServer.LENGTH_FMT, length_data)
        data = await reader.readexactly(length)
        return pickle.loads(data)

    @staticmethod
    def send_object(obj: Any, writer: asyncio.StreamWriter):
        data = pickle.dumps(obj)
        length_data = struct.pack(ControlServer.LENGTH_FMT, len(data))
        writer.write(length_data)
        writer.write(data)

    async def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr_repr = ':'.join(map(str, writer.get_extra_info('peername')))
        logger.info('accepted connection from %s', addr_repr)

        try:
            writer.write(ControlServer.HANDSHAKE_MESSAGE)

            while True:
                # FIXME: maybe do not allow to execute arbitrary object
                action = cast(Callable[[ControlManager], Any], await ControlServer.receive_object(reader))

                try:
                    result = action(self._control)
                    if asyncio.iscoroutine(result):
                        result = await result
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    result = e

                ControlServer.send_object(result, writer)
        except asyncio.IncompleteReadError:
            pass
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning('%s disconnected because of %s', addr_repr, repr(e))
        finally:
            writer.close()

    HOST = '127.0.0.1'
    PORT_RANGE = range(6991, 6999 + 1)

    async def start(self):
        for port in ControlServer.PORT_RANGE:
            try:
                self._server = await asyncio.start_server(self._accept, host=ControlServer.HOST, port=port)
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
