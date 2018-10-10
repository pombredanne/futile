# coding: utf-8

import os
import yaml
import json

from .array import merge_dict
from .log import get_logger

__all__ = ['Conf', 'get_conf']


class Conf(dict):

    """
    从 $CONFPATH 中载入conf文件

    可以通过 # include 注释包含其他文件

    当尝试读取配置变量时，尝试从配置文件中读取或者环境变量中读取
    """

    @classmethod
    def load_file(cls, conf_file: str = None):
        if conf_file is not None:
            conf_string = cls._read_file(conf_file)
            return cls.load_string(conf_string)
        else:
            return Conf()

    @classmethod
    def _read_file(cls, conf_file: str):
        conf_path = os.environ.get('CONFPATH')
        if conf_path is None:
            conf_path = '.'
        abspath = os.path.join(conf_path, conf_file)
        conf_lines = []
        with open(abspath, 'r', encoding='utf-8') as f:
            for line in f:
                if line.startswith('# include'):
                    included_file = line.replace('# include', '').strip()
                    conf_lines.append(cls._read_file(included_file))
                else:
                    conf_lines.append(line)
        return '\n'.join(conf_lines)

    @classmethod
    def load_string(cls, conf_string: str):
        conf_dict = yaml.load(conf_string)
        conf_json = json.dumps(conf_dict)
        # load and dump and load, just to use object_hook
        conf = json.loads(conf_json, object_hook=cls)
        return conf

    @classmethod
    def merge(cls, conf, another_conf):
        return cls(merge_dict(conf, another_conf))

    def __getattr__(self, key):
        """
        首先尝试从配置文件中读取，如果读取不到，尝试从环境变量中读取
        """
        val = self.get(key, None)
        if val is not None:
            return val
        env_val = os.environ.get(key, None)
        return env_val

    def __str__(self):
        return json.dumps(self, indent=4, ensure_ascii=False)

    __setattr__ = dict.__setitem__


def get_conf(conf_file: str = None):
    if conf_file is not None:
        return Conf.load_file(conf_file)
    else:
        return Conf()
