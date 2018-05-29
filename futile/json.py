import json
import re

from .array import compact

class JsonObject(object):

    def __init__(self, d):
        self.__dict__ = d


def json_at(obj, path, default=None):
    '''
    if there is number in path, first try using it as string, then integer
    if a number is wrapped in [], use it as number only
    if a part is `*`, will return a list of result.
    >>> obj = {'a': [0, 1, 2]}
    >>> json_at(obj, 'a.[0]')
    0
    >>> obj = {'a': {'b': [{'c': 0}]}}
    >>> json_at(obj, 'a.b.0.c')
    0
    >>> obj = {'0': [0, 1, 2]}
    >>> json_at(obj, '.0.0')
    0
    >>> obj = {'0': [0, 1, 2]}
    >>> print(json_at(obj, '.[0].0'))
    None
    >>> obj = {'result': [{'id': 1}, {'id': 2}]}
    >>> print(json_at(obj, 'result.0.id'))
    1
    >>> print(json_at(obj, 'result.*.id'))
    [1, 2]
    >>> obj = [1, 2, 3]
    >>> print(json_at(obj, '*'))
    [1, 2, 3]
    >>> obj = {'foo': 'bar'}
    >>> print(json_at(obj, '*'))
    ['bar']
    '''
    if isinstance(obj, str):
        obj = json.loads(obj)
    objs = [obj]
    path_parts = compact(path.split('.'))
    try:
        for idx, part in enumerate(path_parts):
            new_objs = []
            if part == '*':
                for obj in objs:
                    if isinstance(obj, list):
                        new_objs.extend(obj)
                    else:
                        new_objs.extend(obj.values())
            # [x] force number
            elif re.match(r'\[\d+\]', part):
                part = int(part[1:-1])
                for obj in objs:
                    new_objs.append(obj[part])
            elif re.match(r'\d+', part):
                try:
                    # try x as string
                    for obj in objs:
                        new_objs.append(obj[part])
                except (KeyError, IndexError, TypeError):
                    # convert to number
                    int_part = int(part)
                    for obj in objs:
                        new_objs.append(obj[int_part])
            else:
                # dict access
                for obj in objs:
                    new_objs.append(obj[part])
            objs = new_objs
    except (KeyError, IndexError, TypeError) as e:
        return default
    if len(objs) == 1 and '*' not in path:
        return objs[0]
    return objs

def json_loads(s):
    return json.loads(s, object_hook=JsonObject)


def json_load(to_load):
    """load json from filename, string or file object"""
    if isinstance(to_load, str):
        try:
            return json.loads(to_load)
        except Exception:
            try:
                with open(to_load) as f:
                    return json.load(f)
            except Exception:
                return None
    elif isinstance(to_load, io.IOBase):
        try:
            return json.load(f)
        except Exception:
            return None
    return None


def json_dumps(obj, **kwargs):
    return json.dumps(obj, default=str, **kwargs)

