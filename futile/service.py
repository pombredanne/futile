import time
import random
import grpc
import importlib
import json
import argparse
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from typing import List, Any

from .log import get_logger, init_log
from .strings import pascal_case
from .signals import handle_exit
from .consul import register_service, deregister_service, lookup_service
from .cache import ExpiringLruCache
from .redis import make_redis_client


MAX_MESSAGE_LENGTH = 1024 ** 3  # 1GiB
CONNECTION_POOL_SIZE = 4
CACHE_SIZE = 32
CACHE_TTL = 300  # 5 min


def script_init(script_name, *,
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
        script_type='service' if service else 'script',
        maintainers=maintainers,
        description=description,
        restart_interval=restart_interval,
    )
    kv_client = make_redis_client()
    kv_client.zadd('inf:script_info', time.time(), json.dumps(script_meta))

    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', type=int, help="Port to use for this service") 
    parser.add_argument('-d', '--dry-run', action='store_true', default=False, help="Dry run")
    parser.add_argument('-o', '--online', action='store_true', default=False,
                        help="whether this is an online environment")
    parser.add_argument('--console-log-level', default='info',
                        help='console log level')
    parser.add_argument('--file-log-level', default='info',
                        help='file log level')
    if add_args:
        add_args(parser)

    args = parser.parse_args()
    init_log(script_name, console_level=args.console_log_level,
             file_level=args.file_log_level)
    return args


def script_term():
    pass


class GrpcClient:

    # TODO think about threadsafety

    def __init__(self, service_name, *,
                 ip = None,
                 port = None,
                 max_message_length=MAX_MESSAGE_LENGTH,
                 connection_pool_size=CONNECTION_POOL_SIZE,
                 cache_enabled=False,
                 cache_size=CACHE_SIZE,
                 cache_ttl=CACHE_TTL):

        self._max_message_length = max_message_length
        self._connection_pool_size = connection_pool_size
        self._cache_size = cache_size
        self._cache_ttl = cache_ttl

        # 导入 grpc 生成的文件
        # same as `import idl.service_name_pb2 as messagelib`
        self._messagelib = importlib.import_module('idl.' + service_name + '_pb2')
        # same as `import idl.service_name_pb2_grpc as stublib`
        self._stublib = importlib.import_module('idl.' + service_name + '_pb2_grpc')

        self._service_name = service_name
        stub_name = pascal_case(service_name.split('.')[-1]) + 'Stub'
        self._client_stub = getattr(self._stublib, stub_name)

        # build a connection pool
        self._clients = []
        if ip and port:
            client = self._make_client(ip, port)
            self._clients.append(((ip, port), client))
        else:
            for server_address, client in self._make_clients():
                self._clients.append((server_address, client))
                if len(self._clients) >= connection_pool_size:
                    break
        if cache_enabled:
            self._cache = ExpiringLruCache(cache_size, default_timeout=cache_ttl)
        else:
            self._cache = None

    def _make_client(self, ip, port):
        channel = grpc.insecure_channel(
            f'{ip}:{port}',
            options=[
                ('grpc.max_send_message_length', self._max_message_length),
                ('grpc.max_receive_message_length', self._max_message_length)
            ]
        )
        client = self._client_stub(channel)
        return client

    def _make_clients(self):
        endpoints = lookup_service(self._service_name)
        random.shuffle(endpoints)
        for server_address in endpoints:
            client = self._make_client(*server_address)
            yield server_address, client

    def _serialize_args(self, **kwargs):
        # FIXME maybe buggy
        return json.dumps(kwargs, sort_keys=True)

    def __getattr__(self, attr):

        def wrapped(*args, **kwargs):

            if args:
                raise ValueError('no positional arguments allowed')

            # check the cache
            if self._cache:
                cache_key = attr + self._serialize_args(**kwargs)
                rsp = self._cache.get(cache_key)
                if rsp is not None:
                    return rsp

            # prep the request
            request_classname = attr + 'Request'
            Request = getattr(self._messagelib, request_classname)
            req = Request()
            for k, v in kwargs.items():
                if isinstance(v, list):
                    getattr(req, k).extend(v)
                elif isinstance(v, dict):
                    getattr(req, k).update(v)
                else:
                    setattr(req, k, v)

            # make the call
            # add more pooling technology here
            _, stub = random.choice(self._clients)
            rsp = getattr(stub, attr)(req)
            return rsp

        return wrapped


def make_client2(service_name, *, conf=None, **kwargs):
    if conf is not None:
        ip = conf.get(service_name + '.ip')
        port = conf.get(service_name + '.port')
        return GrpcClient(service_name, ip=ip, port=port, **kwargs)

    return GrpcClient(service_name, **kwargs)


def make_client(service_name, client_stub, *args, **kwargs):

    endpoints = lookup_service(service_name)
    server_address = random.choice(endpoints)
    gigabyte = 1024 ** 3
    channel = grpc.insecure_channel(
        f'{server_address[0]}:{server_address[1]}',
        options=[
            ('grpc.max_send_message_length', gigabyte),
            ('grpc.max_receive_message_length', gigabyte)
        ]
    )
    stub = client_stub(channel)
    return stub


def run_service2(service_name, servicer, *,
                 server_type: str = 'thread',
                 max_workers: int = 4,
                 port: int = 6000,
                 bind_ip: str = '[::]',
                 register: bool = False,
                 logger = None,
                 conf = None,
                 ):
    """
    :service_name: service name to register in consul
    :servicer: the service class instance
    :server_type: one of `thread`, `process`, `asyncio`
    :max_workers: max worker count for thread and process pools
    :port: port to listen on
    :bind_ip: ip address to bind to
    :register: whether to register service to consul
    """
    assert server_type in ('thread', 'process', 'asyncio'), 'invalid server type'
    if logger is None:
        logger = get_logger('run_service2')
    if server_type == 'thread':
        executor = ThreadPoolExecutor(max_workers=max_workers,
                                      thread_name_prefix='worker'
                                      )
    elif server_type == 'process':
        executor = ProcessPoolExecutor(max_workers=max_workers,)
    elif server_type == 'asyncio':
        from .grpc.executor import AsyncioExecutor
        executor = AsyncioExecutor()
    server = grpc.server(executor)
    stublib = importlib.import_module('idl.' + service_name + '_pb2_grpc')
    stub_name = pascal_case(service_name.split('.')[-1])
    add_to_server = getattr(stublib, f'add_{stub_name}Servicer_to_server')
    add_to_server(servicer, server)

    # if conf is specified, load ip and port from conf file
    if conf is not None:
        bind_ip = conf.get(f'{service_name}.ip')
        port = conf.get(f'{service_name}.port')
    server.add_insecure_port(f'{bind_ip}:{port}')

    # exit handler
    def exit():
        if register:
            deregister_service(service_name)
        server.stop(grace=True)

    with handle_exit(exit):
        logger.info('starting service %s on %s:%s', service_name, bind_ip, port)
        server.start()
        if register:
            register_service(service_name, port=port)
        while True:
            time.sleep(3600)


# DEPRECATED
def run_service(service_name, *, server=None, port=10086, bind_ip='[::]'):
    """
    初始化一个服务
    """
    server.add_insecure_port(f'{bind_ip}:{port}')

    def exit():
        deregister_service(service_name)
        server.stop(grace=True)

    with handle_exit(exit):
        server.start()
        register_service(service_name, port=port)
        while True:
            time.sleep(3600)
