import os
import time
import random
import grpc
import importlib
import json
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from typing import List, Any
from queue import Queue, LifoQueue, Full, Empty
from google.protobuf.message import Message

from .log import get_logger, init_log
from .strings import pascal_case
from .signals import handle_exit
from .consul import lookup_service
from .cache import ExpiringLruCache
from .redis import make_redis_client
from .timer import timing


MAX_MESSAGE_LENGTH = 1024 ** 3  # 1GiB
CONNECTION_POOL_SIZE = 4
CACHE_SIZE = 32
CACHE_TTL = 300  # 5 min


class ConnectionError(Exception):
    pass


class TimeoutError(Exception):
    pass


def script_init(
    script_name,
    *,
    maintainers: List[str],
    conf_file: str = None,
    description: str = None,
    restart_interval: int = 3600 * 24 * 3,
    service: bool = False,
    add_args: Any = None,
):
    """
    初始化一个脚本
    """
    script_meta = dict(
        script_name=script_name,
        script_type="service" if service else "script",
        maintainers=maintainers,
        description=description,
        restart_interval=restart_interval,
    )
    # kv_client.zadd("inf:script_info", time.time(), json.dumps(script_meta))

    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, help="Port to use for this service")
    parser.add_argument("--dry-run", action="store_true", default=False, help="Dry run")
    # deprecated
    parser.add_argument(
        "--online",
        action="store_true",
        default=False,
        help="whether this is an online environment",
    )
    parser.add_argument("--env", help="Environment to use")
    parser.add_argument("--db-env", help="Database environment to use")
    parser.add_argument("--console-log-level", default="info", help="console log level")
    parser.add_argument("--file-log-level", default="info", help="file log level")
    if add_args:
        add_args(parser)

    args = parser.parse_args()
    init_log(
        script_name,
        console_level=args.console_log_level,
        file_level=args.file_log_level,
    )
    return args


def script_alive():
    """
    answer health check calls
    """


class ConnectionPool:
    def __init__(
        self,
        max_connections=50,
        timeout=20,
        connection_class=None,
        queue_class=LifoQueue,
        **connection_kwargs,
    ):

        self.queue_class = queue_class  # 使用一个队列来存放连接
        self.timeout = timeout  # 增加了超时功能
        self.max_connections = max_connections
        self.connection_class = connection_class
        self.connection_kwargs = connection_kwargs

        self.reset()  # 调用 reset 初始化一些属性

    def reset(self):
        self.pid = os.getpid()
        self._check_lock = threading.Lock()

        # 首先在队列中填满 None，后面会用到，这里很关键
        self.pool = self.queue_class(self.max_connections)
        while True:
            try:
                self.pool.put_nowait(None)
            except Full:
                break
        # Keep a list of actual connection instances so that we can
        # disconnect them later.
        self._connections = []

    def _checkpid(self):
        # 如果当前的 connection 是 fork 来的，直接关闭链接
        if self.pid != os.getpid():
            with self._check_lock:
                if self.pid == os.getpid():
                    # 另一个线程已经检查了，直接返回
                    return
                self.disconnect()
                self.reset()

    def make_connection(self):
        # 创建一个链接，貌似和上面的函数没有什么区别。。
        connection = self.connection_class(**self.connection_kwargs)
        # 一直往这个数组里面怼可能有内存泄露风险
        self._connections.append(connection)
        return connection

    def get_connection(self):
        """
        获取一个新的连接，最长等待 timeout 秒

        如果我们读取到的新连接是 None 的话，就会创建一个新的连接。因为我们使用的
        是 LIFO 队列，也就是栈，所以我们优先得到的是已经创建的链接，而不是最开始
        放进去的 None。也就是我们只有在需要的时候才会创建新的连接，也就是说连接
        数量是按需增长的。
        """
        # 确保没有更换进程
        self._checkpid()

        # 尝试获取一个连接，如果在 timeout 时间内失败的话，抛出 ConnectionError
        connection = None
        try:
            connection = self.pool.get(block=True, timeout=self.timeout)
        except Empty:
            # 需要注意的是这个错误并不会被 redis 捕获，需要用户自己处理
            raise ConnectionError("No connection available.")

        # 如果真的没有连接可用了，直接创建一个新的连接
        if connection is None:
            connection = self.make_connection()

        return connection

    def release(self, connection):
        # 释放连接到连接池
        self._checkpid()
        if connection.pid != self.pid:
            return

        # Put the connection back into the pool.
        try:
            self.pool.put_nowait(connection)
        except Full:
            # perhaps the pool has been reset() after a fork? regardless,
            # we don't want this connection
            pass

    def disconnect(self):
        # 释放所有的连接
        for connection in self._connections:
            connection.disconnect()


class GrpcConnection:
    """
    not thread safe, use connection pool to maintain thread-safety
    """

    def __init__(
        self,
        service_name,
        service_idl=None,
        ip=None,
        port=None,
        max_message_length=MAX_MESSAGE_LENGTH,
    ):

        self._max_message_length = max_message_length
        if service_idl is None:
            service_idl = service_name
        self._service_name = service_name
        self._service_idl = service_idl
        self._ip = ip
        self._port = port
        self._logger = get_logger("grpc_client")
        self.pid = os.getpid()

        # import grpc generated files
        # same as `import idl.service_idl_pb2 as messagelib`
        self._messagelib = importlib.import_module("idl." + service_idl + "_pb2")
        # same as `import idl.service_idl_pb2_grpc as stublib`
        self._stublib = importlib.import_module("idl." + service_idl + "_pb2_grpc")

        stub_name = pascal_case(service_idl.split(".")[-1]) + "Stub"
        self._client_stub = getattr(self._stublib, stub_name)

        self._stub = None

    def connect(self):
        if self._stub:
            return
        if not (self._ip and self._port):
            endpoints = lookup_service(self._service_name)
            ip, port = random.choice(endpoints)
        else:
            ip, port = self._ip, self._port
        channel = grpc.insecure_channel(
            f"{ip}:{port}",
            options=[
                ("grpc.max_send_message_length", self._max_message_length),
                ("grpc.max_receive_message_length", self._max_message_length),
            ],
        )
        self._stub = self._client_stub(channel)

    @classmethod
    def connect_all(cls, service_name, service_idl):
        connections = []
        endpoints = lookup_service(service_name)
        for ip, port in endpoints:
            connection = cls(service_name, service_idl, ip, port)
            connection.connect()
            connections.append(connection)
        return connections

    def disconnect(self):
        self._stub = None
        self._ip = None
        self._port = None

    def __getattr__(self, attr):
        def wrapped(**kwargs):
            # prep the request
            try:
                request_classname = attr + "Request"
                Request = getattr(self._messagelib, request_classname)
                req = Request()
                for k, v in kwargs.items():
                    if isinstance(v, list):
                        getattr(req, k).extend(v)
                    elif isinstance(v, dict):
                        getattr(req, k).update(v)
                    elif isinstance(v, Message):
                        getattr(req, k).CopyFrom(v)
                    else:
                        setattr(req, k, v)

                if self._stub is None:
                    self.connect()

                # call the server
                rsp = getattr(self._stub, attr)(req)
                return rsp
            except Exception as e:
                self._logger.exception(
                    "call % error, args=%s, error=%s", attr, kwargs, e
                )
                raise

        return wrapped


class GrpcClient:
    def __init__(
        self,
        service_name,
        service_idl=None,
        ip=None,
        port=None,
        max_message_length=MAX_MESSAGE_LENGTH,
        max_connections=50,
        timeout=20,
    ):
        self._service_name = service_name
        self._service_idl = service_idl
        self._connection_pool = ConnectionPool(
            service_name=service_name,
            service_idl=service_idl,
            ip=ip,
            port=port,
            max_message_length=MAX_MESSAGE_LENGTH,
            max_connections=max_connections,
            connection_class=GrpcConnection,
            timeout=timeout,
        )

    def broadcast(self, method, **kwargs):
        connections = GrpcConnection.connect_all(self._service_name, self._service_idl)
        ret = []
        for connection in connections:
            ret.append(getattr(connection, method)(**kwargs))
        return ret

    def __getattr__(self, attr):
        def wrapped(**kwargs):
            # 执行每条命令都会调用该方法
            pool = self._connection_pool
            # 弹出一个连接
            connection = pool.get_connection()
            try:
                return getattr(connection, attr)(**kwargs)
            except grpc.RpcError as e:
                # 如果是连接问题，关闭有问题的连接，下面再次使用这个连接的时候会重新连接。
                connection.disconnect()
                return getattr(connection, attr)(**kwargs)
            finally:
                # 不管怎样都要把这个连接归还到连接池
                pool.release(connection)

        return wrapped


def make_client2(service_name, *, service_idl=None, conf=None, **kwargs):
    if conf is not None:
        ip = conf.get(service_name + ".ip")
        port = conf.get(service_name + ".port")
        if ip and port:
            return GrpcClient(
                service_name, service_idl=service_idl, ip=ip, port=port, **kwargs
            )

    try:
        return GrpcClient(service_name, service_idl=service_idl, **kwargs)
    except Exception:
        return None


# deprecated
def make_client(service_name, client_stub, *args, **kwargs):

    endpoints = lookup_service(service_name)
    server_address = random.choice(endpoints)
    gigabyte = 1024 ** 3
    channel = grpc.insecure_channel(
        f"{server_address[0]}:{server_address[1]}",
        options=[
            ("grpc.max_send_message_length", gigabyte),
            ("grpc.max_receive_message_length", gigabyte),
        ],
    )
    stub = client_stub(channel)
    return stub


class MetricsInterceptor(grpc.ServerInterceptor):
    def __init__(self, servicer):
        self._servicer = servicer
        self._handlers = {}

    def intercept_service(self, continuation, handler_call_details):
        method = handler_call_details.method
        handler = self._handlers.get(method)
        if not handler:
            raw_handler = getattr(self._servicer, method)
            if getattr(raw_handler, "_timing", False):
                handler = raw_handler
            else:
                handler = grpc.unary_unary_rpc_method_handler(timing(raw_handler))
            self._handlers[method] = handler
        return handler


def run_service2(
    service_name,
    servicer,
    *,
    service_idl: str = None,
    server_type: str = "thread",
    max_workers: int = 4,
    port: int = None,
    bind_ip: str = "[::]",
    should_register: bool = False,  # deprecated
    should_timing: bool = False,
    logger=None,
    conf=None,
):
    """
    :service_name: service name to register in consul
    :servicer: the service class instance
    :service_idl: the IDL to use for this service
    :server_type: one of `thread`, `process`, `asyncio`
    :max_workers: max worker count for thread and process pools
    :port: port to listen on
    :bind_ip: ip address to bind to
    :should_register: DEPRECATED, whether to register service to consul
    :should_timing: whether to send metrics automatically
    """
    assert server_type in ("thread", "process", "asyncio"), "invalid server type"
    if service_idl is None:
        service_idl = service_name
    if logger is None:
        logger = get_logger("run_service2")
    if server_type == "thread":
        executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="worker"
        )
    elif server_type == "process":
        executor = ProcessPoolExecutor(max_workers=max_workers)
    elif server_type == "asyncio":
        from .grpc.executor import AsyncioExecutor

        executor = AsyncioExecutor()
    if should_timing:
        interceptors = (MetricsInterceptor(servicer),)
    else:
        interceptors = []
    server = grpc.server(executor, interceptors=interceptors)
    stublib = importlib.import_module("idl." + service_idl + "_pb2_grpc")
    stub_name = pascal_case(service_idl.split(".")[-1])
    add_to_server = getattr(stublib, f"add_{stub_name}Servicer_to_server")
    add_to_server(servicer, server)

    # if conf is specified, load ip and port from conf file
    if conf is not None:
        conf_ip = conf.get(f"{service_name}.ip")
        conf_port = conf.get(f"{service_name}.port")
    else:
        conf_ip = None
        conf_port = None

    if conf_ip:
        bind_ip = conf_ip

    if conf_port:
        port = conf_port

    if port is None and conf_port is None:
        port = random.randint(5000, 6000)

    server.add_insecure_port(f"{bind_ip}:{port}")

    # exit handler
    def exit():
        server.stop(grace=True)
        logger.info("exiting service %s on %s:%s", service_name, bind_ip, port)

    with handle_exit(exit):
        logger.info("starting service %s on %s:%s", service_name, bind_ip, port)
        server.start()
        while True:
            time.sleep(3600)


# DEPRECATED
def run_service(service_name, *, server=None, port=10086, bind_ip="[::]"):
    """
    初始化一个服务
    """
    pass
