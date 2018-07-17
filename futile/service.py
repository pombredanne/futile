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

def service_init():
    """
    初始化一个服务
    """
