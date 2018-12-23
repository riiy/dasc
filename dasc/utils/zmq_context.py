import zmq
import os
import logging
import threading

log = logging.getLogger(__name__)

_global = {
    'pid': None,
    'context': None
}


def get_context():
    """Get ZMQ context for this process

    NOT THREADSAFE! Make sure we don't have more than one thread trying to
    get the context! Run it once from the main thread.

    :rtype: zmq.Context
    """
    current_pid = os.getpid()
    if _global['pid']:
        # We already have a context for this process
        if current_pid == _global['pid']:
            # Return existing context
            log.trace('Returning existing ZMQ context for PID %s', current_pid)
            return _global['context']
        else:
            error = (
                'zmq.Context was created for PID %s, but now in PID %s'.format(
                _global['pid'], current_pid))
            raise RuntimeError(error)

    else:
        # No context yet
        log.info('ZMQ context created for PID %s', current_pid)
        _global['context'] = zmq.Context()
        _global['pid'] = current_pid
        return _global['context']

