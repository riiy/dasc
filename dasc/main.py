# -*- coding: utf-8 -*-
import logging
import threading

import zmq

from dasc.utils import get_handlers
from dasc.utils.messages import build_msg
from dasc.utils.zmq_context import get_context

log = logging.getLogger("dasc")


class ServerWorker(threading.Thread):
    """ServerWorker"""



    def __init__(self,handler_path, context, i, worker_url):
        threading.Thread.__init__(self)
        self.context = context
        self.name = "Worker-%d" % i
        self.running = True
        self.worker_url = worker_url
        self.handlers=get_handlers(handler_path)

    def handle(self, handler, arguments):
        log.info('%s is handling func: %s, arguments: %r',
                 self.name, handler, arguments)
        func = self.handlers.get(handler)
        if func:
            try:
                result = func(**arguments)
                log.info('%s is handling func: %s,result:%s',
                         self.name, handler, result)
                return ('OK', {'result': result})
            except Exception as e:
                log.exception("%s handler func %s error,error message %s", self.name, handler, e.message)
                return ('TraderError', {'error_msg': e.msg,
                                        'error_code': e.code})
            except Exception as e:
                log.exception('%s is raising exception for func: %s, %s',
                              self.name, handler, arguments, exc_info=True)
                data = {'reason': str(e),
                        'suggestion': 'base on exception type, retry or fail'}
                return ('EXCEPTION', data)
        else:
            data = {'reason': 'not supported method %s' % handler,
                    'suggestion': 'please check the server version or the input'}
            log.info("not support method %s", handler)
            return ('ERROR', data)

    def run(self):
        socket = self.context.socket(zmq.REP)
        socket.identity = self.name.encode('utf-8')
        socket.connect(self.worker_url)

        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)

        log.info('%s start listening' % self.name)
        while self.running:
            socks = dict(poller.poll(1000))
            if socks:
                handler, arguments = socket.recv_json()
                status, data = self.handle(handler, arguments)
                result = build_msg(name=handler, data=data, meta={'status': status})
                import json
                json.dumps(result)
                socket.send_json(result)

        socket.close()
        log.info('%s exited' % self.name)

    def stop(self):
        self.running = False


class Server(object):
    worker_url = 'inproc://dasc_workers'
    def __init__(self,  handler_path, client_url='tcp://*:5555',worker_numbers=5):
        self.context = get_context()
        self.worker_numbers = worker_numbers
        self.workers = []
        self.client_url = client_url
        self.handler_path=handler_path


    def start_workers(self):
        for i in range(1, self.worker_numbers + 1):
            worker = ServerWorker(self.handler_path, self.context, i,self.worker_url)
            worker.start()
            self.workers.append(worker)

    def stop_workers(self):
        for w in self.workers:
            w.stop()
            w.join()

    def run(self):
        log.info('API server starting...')
        context = self.context
        client_sock = context.socket(zmq.ROUTER)
        client_sock.bind(self.client_url)

        worker_sock = context.socket(zmq.DEALER)
        worker_sock.bind(self.worker_url)

        self.start_workers()

        try:
            zmq.device(zmq.QUEUE, client_sock, worker_sock)
        except BaseException as e:
            if isinstance(e, KeyboardInterrupt):
                log.info('Keyboard interrupt received')
            else:
                log.error('Exit due to exception: %s', str(e), exc_info=True)
        self.stop_workers()

        client_sock.close()
        worker_sock.close()
        log.info('API server exited')
