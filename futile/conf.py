# coding: utf-8

import os
import yaml
import json

from .array import merge_dict

__all__ = ['Conf']


def yaml_include(loader, node):
    filename = os.path.join(os.path.dirname(loader.name), node.value)

    with open(filename, 'r', encoding='utf-8') as f:
        return yaml.load(f)

yaml.add_constructor("!include", yaml_include)


class Conf(dict):

    @classmethod
    def load_file(cls, conf_file):
        with open(conf_file, 'r', encoding='utf-8') as f:
            return cls.load_string(f.read())

    @classmethod
    def load_string(cls, conf_string):
        conf_dict = yaml.load(conf_string)
        conf_json = json.dumps(conf_dict)
        conf = json.loads(conf_json, object_hook=cls)  # load and dump and load, just to use object_hook
        return conf

    @classmethod
    def merge(cls, conf, another_conf):
        return cls(merge_dict(conf, another_conf))

    def __getattr__(self, key):
        return self.get(key, None)

    def __str__(self):
        return json.dumps(self, indent=4, ensure_ascii=False)

    __setattr__ = dict.__setitem__
