import threading
import zmq
import time
import abc
import sys
import os

from dasc.utils.messages import from_msg, to_msg

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))


import logging

log = logging.getLogger(__name__)


class SerializingSocket(zmq.Socket):
    """A class with some extra serialization methods

    """

    def send_msg(self, obj, flags=0, protocol=-1):
        return self.send(to_msg(obj), flags=flags)

    def recv_msg(self, flags=0):
        return from_msg(self.recv(flags))


class SerializingContext(zmq.Context):
    """
    This is a customized context class, used for testing
    """
    _socket_class = SerializingSocket


client_context = None


def get_context():
    global client_context
    if client_context is None:
        client_context = SerializingContext()
    return client_context


class BaseClient(object):
    __metaclass__ = abc.ABCMeta
    role = 'BaseClient'

    def __init__(self, i=1, times=10):
        self.context = get_context()
        # patch the context._socket_class
        self.context._socket_class = SerializingSocket
        self.times = times
        self.sleep = 1
        self.identify = '%s-%s' % (self.role, i)

    @abc.abstractmethod
    def run(self):
        """Method that subclass should override"""


class Client(BaseClient):
    """
    Concurrent testing client, demo the multiple threading model
    """

    role = 'EchoClient'

    def run(self):
        log.warning('%s starting...' % self.identify)
        context = self.context
        socket = context.socket(zmq.REQ)
        socket.connect('tcp://localhost:5555')

        for i in range(self.times):
            message = '%s request %s' % (self.identify, i)
            request = ['echo', {'message': message}]
            log.warning('%s is sending request: %s' % (self.identify, request))
            socket.send_msg(request)
            result = socket.recv_msg()
            assert result['status'] == 'OK'
            log.warning('%s got result: %s' % (self.identify, result))
            time.sleep(self.sleep)
        log.warning('%s exited' % self.identify)
        socket.close()




def run_test():

    clients = []
    concurrent_clients = 1
    for i in range(concurrent_clients):
        co = Client(i, times=1)
        client = threading.Thread(target=co.run, name=co.identify)
        client.start()
        clients.append(client)


    for client in clients:
        client.join()



if __name__ == "__main__":

    run_test()
