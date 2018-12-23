import json
from datetime import date
import pickle
import time
import itertools

# Threadsafe incremental counter
id_counter = itertools.count(start=1)


def construct_msg(name, data=None, meta=None):
    """Prepare a data message dict

    :param str name: name of message type
    :param dict data: data key
    :param dict meta: extra keys to add to the root
    :return: message dict
    :rtype: dict
    """
    d = {
        'name': name,
        'data': data,
        'ts': time.time(),
        'id': next(id_counter)
    }
    if meta:
        # NOTE: meta must not contain keys that overwrite other data
        d.update(meta)
    return d


def build_msg(name, data=None, meta=None):
    """Prepare data for transfer using ZMQ

    :param str name: name of message type
    :param dict data: data key
    :param dict meta: extra keys to add to the root
    :return: pickled message
    :rtype: bytes
    """
    d = construct_msg(name, data, meta)
    return d


def to_msg(msg_data):
    """Serialize prebuild message
    :param dict msg_data: message data dict
    :return: pickled bytes
    :rtype: bytes
    """
    return json.dumps(msg_data)


def from_msg(raw_msg):
    """Convert message from ZMQ to dict

    Note that the format returned is different from the inputs to to_msg.

    :param raw_msg: pickled bytes
    :type raw_msg: bytes
    :return: dict with name, data and extra metadata keys
    :rtype: dict
    """
    return pickle.loads(raw_msg)
