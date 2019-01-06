#-*- coding:utf-8 -*-

import pika
import threading
import datetime
import time
import functools
import signal
import sys


class CoreMQWrapper(object):
    def __init__(self, private_exchange_name='private_exchange', server_ip = 'localhost'):
        self.req_queue_name = 'req_front1'
        self.rsp_queue_name = 'rsp_front1'
        self.private_exchange_name = private_exchange_name
        self.receive_request_thread = None
        self.host = server_ip
        self.rsp_channel = None


    def connect(self):
        self.rsp_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.rsp_channel = self.rsp_connection.channel()
        self.rsp_channel.queue_declare(queue=self.rsp_queue_name)

        self.private_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.private_channel = self.private_connection.channel()
        self.private_channel.exchange_declare(exchange=self.private_exchange_name, exchange_type='fanout')

        self.receive_request_thread = threading.Thread(target= self.receive_request_work)
        self.receive_request_thread.start()

    def disconnect(self):
        pass

    def receive_request_work(self):
        req_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
        rep_channel = req_connection.channel()
        rep_channel.queue_declare(queue=self.req_queue_name)
        rep_channel.basic_consume(functools.partial(self.callback, tag='req_front1'), queue=self.req_queue_name,no_ack=True)
        rep_channel.start_consuming()
        print('leave req receive thread')

    def callback(self,ch, method, properties, body, tag):
        print('method=>[%s]' % str(method))
        print('properties=>[%s]' % str(properties))
        print('On[%s]received[%s]' %(tag, body.decode()))
        msg = 'rsp2[%s]' % body.decode()
        private_msg = 'private[%s]' % msg
        target_queue_name = self.get_rsp_queue_name(tag)
        self.send(target_queue_name, msg)
        self.boardcast(private_msg)

    def get_rsp_queue_name(self, tag):
        return self.rsp_queue_name

    def send(self, queue_name, msg):
        self.rsp_channel.basic_publish(exchange='', routing_key=queue_name, body=msg)

    def boardcast(self, msg):
        self.private_channel.basic_publish(exchange=self.private_exchange_name, routing_key='',body=msg)


if __name__ == '__main__':
    core = CoreMQWrapper()
    core.connect()
    while 1== 1:
        time.sleep(1)