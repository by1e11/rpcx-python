from abc import ABC, abstractmethod
import abc
import inspect
import logging
import math
import signal
import stat
import sys
import traceback
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, Dict, Optional, Tuple, get_type_hints
from urllib import response

import anyio
from anyio import TASK_STATUS_IGNORED, open_signal_receiver, run
from anyio.abc import AnyByteStream, TaskStatus, SocketStream
from anyio import create_tcp_listener

from .message import (
    Message,
    MessageStatusType,
    MessageType
    # RequestCancel,
    # RequestStreamChunk,
    # RequestStreamEnd,
    # ResponseStatus,
    # ResponseStreamChunk,
    # ResponseStreamEnd,
)

if sys.version_info >= (3, 9):  # pragma: nocover
    from collections.abc import AsyncIterator
else:  # pragma: nocover
    from typing import AsyncIterator


LOG = logging.getLogger(__name__)


@dataclass
class RPCMethod:
    name: str
    func: Callable[..., Any]

    @property
    def signature(self) -> inspect.Signature:
        return inspect.signature(self.func)

    @property
    def stream_arg(self) -> Optional[str]:
        hints = get_type_hints(self.func)
        for name in self.signature.parameters:
            if hints.get(name) is Stream:
                return name
        return None
    
class RPCService(ABC):
    """
    Base class for RPC services. A Rpc service must be a subclass of this class.
    Now, only direct subclass of this class is allowed.
    """
    pass

class RPCManager:
    def __init__(self) -> None:
        self.services: Dict[str, RPCService] = {}

        # find all the derived classes of RPCService
        # Use path (like "TestService") as the key to store the instance of the class
        for subclass in RPCService.__subclasses__():
            path = subclass.__name__
            self.services[path] = subclass()

    def get_method(self, service_path: str, service_method: str) -> Optional[RPCMethod]:
        """
        Get the RPC method for the given service path and method name.
        """
        service = self.services.get(service_path)
        if service is None:
            return None
        method = getattr(service, service_method, None)
        if method is None or not callable(method):
            return None
        return RPCMethod(service_method, method)

@dataclass
class Stream(AsyncIterator["Stream"]):
    """
    An async-iterable stream object connected with callbacks to send stream messages.

    This object is passed to RPC methods which specify it as an argument:

        async def method_foo(stream: Stream):
            async for data in stream:
                await stream.send(data + 1)
    """

    send: Callable[[Any], Coroutine[None, None, None]]

    def __post_init__(self) -> None:
        self.stream_producer, self.stream_consumer = anyio.create_memory_object_stream[Any](math.inf)

    def __aiter__(self) -> "Stream":
        return self

    async def __anext__(self) -> Any:
        try:
            return await self.stream_consumer.receive()
        except anyio.EndOfStream:
            raise StopAsyncIteration()

class Task:
    def __init__(self) -> None:
        self.cancel_scope = anyio.CancelScope()
        self.stream: Optional[Stream] = None

@dataclass
class Dispatcher:
    manager: RPCManager
    tasks: Dict[int, Task] = field(default_factory=dict)

    async def aclose(self) -> None:
        for task in self.tasks.values():
            task.cancel_scope.cancel()

    def task_exists(self, request_id: int) -> bool:
        return request_id in self.tasks

    async def request(
        self,
        req: Message,
        send_stream_chunk: Callable[[Any], Coroutine[None, None, None]] = None,
        task_status: TaskStatus[None] = TASK_STATUS_IGNORED,
    ) -> Any:
        """
        Calls the method.
        """
        task_status.started()
        service_path = req.service_path
        method_name = req.service_method
        method = self.manager.get_method(service_path, method_name)
        if method is None:
            raise ValueError(f"Invalid method: '{method_name}'")

        task = self.tasks[req.message_id] = Task()
        stream = task.stream = Stream(send_stream_chunk)

        try:
            with task.cancel_scope, stream.stream_producer, stream.stream_consumer:
                # if method.stream_arg is not None:
                #     kwargs[method.stream_arg] = stream
                kwargs = req.payload

                # import pdb; pdb.set_trace()

                LOG.debug("Dispatch: %s %s %s", method_name, kwargs)
                return await method.func(**kwargs)
        finally:
            del self.tasks[req.message_id]

    async def stream_chunk(self, request_id: int, value: Any) -> None:
        task = self.tasks[request_id]
        if task.stream is not None:
            await task.stream.stream_producer.send(value)

    async def stream_end(self, request_id: int) -> None:
        task = self.tasks[request_id]
        if task.stream is not None:
            await task.stream.stream_producer.aclose()

    def cancel(self, request_id: int) -> None:
        task = self.tasks[request_id]
        task.cancel_scope.cancel()

class RPCServer:
    def __init__(self, manager: RPCManager = None) -> None:
        # self.stream = stream
        self.dispatcher = Dispatcher(manager)

    async def handle_request(self, client: SocketStream) -> None:
        """
        Handle a request call.
        """
        sent_stream_chunk = False

        async def send_stream_chunk_wrapper(value: Any) -> None:
            nonlocal sent_stream_chunk
            sent_stream_chunk = True
            await self.send_stream_chunk(request.id, value)

        async with client:
            try:
                request = Message()
                complete: bool = False
                while not complete:
                    msg = await client.receive(1024)
                    complete = request.decode(msg)
                
                LOG.debug("Receive %s", request)
                result = await self.dispatcher.request(
                    request, 
                    send_stream_chunk_wrapper,
                )
                # import pdb; pdb.set_trace()
                if sent_stream_chunk:
                    await self.send_stream_end(request.id)

                if not request.isoneway:
                    await self.send_response(client, request, MessageStatusType.Normal, result)

            except (TypeError, ValueError):
                LOG.exception("Invalid request")
                await self.send_response(client, request, MessageStatusType.Error, traceback.format_exc())
            except Exception as exc:
                LOG.warning("rpc error: %s", request, exc_info=exc)
                await self.send_response(client, request, MessageStatusType.Error, traceback.format_exc())

    async def handle_event(self, msg: Message) -> None:
        """
        Handle an event for an in-progress request.  This includes:
          - Cancellation
          - Stream chunks
          - Stream end
        """
        if not self.dispatcher.task_exists(msg.id):
            LOG.warning("Requested non-existing task: %s", msg)
        # elif isinstance(msg, RequestCancel):
        #     self.dispatcher.cancel(msg.id)
        # elif isinstance(msg, RequestStreamChunk):
        #     await self.dispatcher.stream_chunk(msg.id, msg.value)
        # elif isinstance(msg, RequestStreamEnd):
        #     await self.dispatcher.stream_end(msg.id)
        else:
            LOG.warning("Received unhandled message: %s", msg)

    # async def send_msg(self, message: Message) -> None:
    #     await self.stream.send(message_to_bytes(message))

    async def send_response(self, client: SocketStream, request: Message, status: MessageStatusType, value: Any) -> None:
        response = Message(
            service_path=request.service_path,
            service_method=request.service_method,
            message_id=request.message_id,
        )
        response.header.message_status_type = status
        response.header.message_type = MessageType.Response
        response.header.serialize_type = request.header.serialize_type
        if status == MessageStatusType.Error:
            response.metadata = {"rpcx_error": str(value)}
        else:
            response.payload = value
        
        # send response
        await client.send(response.encode())

    # async def send_stream_chunk(self, request_id: int, value: Any) -> None:
    #     await self.send_msg(ResponseStreamChunk(request_id, value))

    # async def send_stream_end(self, request_id: int) -> None:
    #     await self.send_msg(ResponseStreamEnd(request_id))

    async def serve(self, host: str = "localhost", port: int = 33632, raise_on_error: bool = False) -> None:
        """
        This is the main receive loop for the server.
        """

        def log_error(exc: Exception) -> None:
            LOG.exception("Internal error", exc_info=exc)
            if raise_on_error:
                raise exc
            
        async def listen(**kwargs):
            listener = await create_tcp_listener(**kwargs)
            await listener.serve(self.handle_request)

        print(f"Start listening on {host}:{port}")

        await listen(local_host=host, local_port=port)

        # async def wrap_task(
        #     task: Callable[..., Coroutine[None, None, None]],
        #     *args: Any,
        #     task_status: TaskStatus[None],
        # ) -> None:
        #     try:
        #         await task(*args, task_status=task_status)
        #     except Exception as exc:
        #         log_error(exc)

        # async with anyio.create_task_group() as task_group:
        #     async for data in self.stream:
        #         try:
        #             msg = message_from_bytes(data)
        #             LOG.debug("Receive %s", msg)
        #             if isinstance(msg, Request):
        #                 await task_group.start(wrap_task, self.handle_request, msg)
        #             else:
        #                 await self.handle_event(msg)
        #         except anyio.get_cancelled_exc_class():  # pragma: nocover
        #             raise
        #         except Exception as exc:  # internal error!
        #             # If we're testing, we'll want to re-raise.
        #             # For a production deployment, log it but keep the loop alive.
        #             log_error(exc)
