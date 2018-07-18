import time
from typing import List
from .signal import handle_exit
from .consul import register_service, deregister_service


def script_init(script_name, *,
                maintainers: List[str],
                conf_file: str,
                description: str,
                restart_interval: int = 3600 * 24 * 3,
                service: bool = False
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
        conf_file=conf_file,
    )


def run_service(server, service_name, port):
    """
    初始化一个服务
    """
    server.add_insecure_port(f'[::]:{port}')

    def exit():
        deregister_service(service_name)
        server.stop(grace=True)

    with handle_exit(exit):
        server.start()
        register_service(service_name, port=port)
        while True:
            time.sleep(3600)
