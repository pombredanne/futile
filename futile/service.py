import os
import time
import random
import grpc
import importlib
import argparse
import signal
import socket
from concurrent.futures import ThreadPoolExecutor
from typing import List, Any
from google.protobuf.message import Message

from .connection_pool import ConnectionPool
from .number import ensure_int
from .log import get_logger, init_log
from .strings import pascal_case
from .timeutil import parse_time_string
from .signals import handle_exit
from .consul import lookup_service as consul_lookup_service
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


def is_k8s_env():
    return os.getenv("IS_K8S_ENV")


def lookup_service(service_name):
    if os.getenv("IS_K8S_ENV"):
        ip = socket.gethostbyname(service_name)
        port = os.getenv("K8S_PORT0")
        return [(ip, port)]
    else:
        return consul_lookup_service(service_name)


def script_init(
    script_name,
    *,
    maintainers: List[str],
    conf_file: str = None,
    description: str = None,
    restart_interval: str = "7d",
    service: bool = False,
    add_args: Any = None,
    no_argparse: bool = False,
):
    """
    初始化一个脚本
    """
    logger = get_logger("script_init")
    script_meta = dict(
        script_name=script_name,
        script_type="service" if service else "script",
        maintainers=maintainers,
        description=description,
        restart_interval=restart_interval,
    )
    # TODO store script meta to consule

    logger.info("set restart interval to %s", restart_interval)
    restart = parse_time_string(restart_interval)
    signal.alarm(ensure_int(restart * random.randint(90, 110) / 100))

    if no_argparse:
        return None

    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, help="Port to use for this service")
    parser.add_argument("--dry-run", action="store_true", default=False, help="Dry run")
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
            if not endpoints:
                raise RuntimeError("%s all services are down", self._service_name)
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
        def wrapped(req, **kwargs):
            # prep the request
            try:
                if req is None:
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
        self._ip = ip
        self._port = port

    # XXX broadcast does not work with K8S clusterIP
    def broadcast(self, method, **kwargs):
        if self._ip:
            connections = [
                GrpcConnection(
                    service_name=self._service_name,
                    service_idl=self._service_idl,
                    ip=self._ip,
                    port=self._port,
                )
            ]
        else:
            connections = GrpcConnection.connect_all(
                self._service_name, self._service_idl
            )
        ret = []
        for connection in connections:
            ret.append(getattr(connection, method)(None, **kwargs))
        return ret

    def __getattr__(self, attr):
        def wrapped(*args, **kwargs):
            if len(args) > 1:
                raise ValueError("only kwargs are accepted")
            elif len(args) == 1:
                req = args[0]
            else:
                req = None
            # 执行每条命令都会调用该方法
            pool = self._connection_pool
            # 弹出一个连接
            connection = pool.get_connection()
            try:
                return getattr(connection, attr)(req, **kwargs)
            except grpc.RpcError as e:
                # 如果是连接问题，关闭有问题的连接，下面再次使用这个连接的时候会重新连接。
                connection.disconnect()
                return getattr(connection, attr)(req, **kwargs)
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
    ip: str = "0.0.0.0",
    port: int = None,
    logger=None,
    conf=None,
):
    """
    :service_name: service name to register
    :servicer: the service class instance
    :service_idl: the IDL to use for this service
    :server_type: one of [`thread`, `asyncio`]
    :max_workers: max worker count for thread and process pools
    :ip: ip address to bind to
    :port: port to listen on
    :should_register: DEPRECATED, whether to register service to consul
    """
    assert server_type in ("thread", "asyncio"), "invalid server type"
    if service_idl is None:
        service_idl = service_name
    if logger is None:
        logger = get_logger("run_service2")
    if server_type == "thread":
        executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="worker"
        )
    elif server_type == "asyncio":
        from .grpc.executor import AsyncioExecutor

        executor = AsyncioExecutor()

    server = grpc.server(executor)
    stublib = importlib.import_module("idl." + service_idl + "_pb2_grpc")
    stub_name = pascal_case(service_idl.split(".")[-1])
    add_to_server = getattr(stublib, f"add_{stub_name}Servicer_to_server")
    add_to_server(servicer, server)

    # 线上环境直接使用 k8s 提供端口
    if is_k8s_env():
        port = os.getenv("K8S_PORT0")
    else:
        # if conf is specified, load ip and port from conf file
        if conf is not None:
            if conf.get(f"{service_name}.ip"):
                ip = conf.get(f"{service_name}.ip")
            if conf.get(f"{service_name}.port"):
                port = conf.get(f"{service_name}.port")
            else:
                raise RuntimeError("local port not specified")
        else:
            raise RuntimeError("local conf not specified")

    server.add_insecure_port(f"{ip}:{port}")

    # exit handler
    def exit():
        server.stop(grace=True)
        logger.info("exiting service %s on %s:%s", service_name, ip, port)

    with handle_exit(exit):
        logger.info("starting service %s on %s:%s", service_name, ip, port)
        server.start()
        while True:
            time.sleep(3600)
