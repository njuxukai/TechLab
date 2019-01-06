#-*- coding:utf-8 -*-

import pika
import threading
import datetime
import time
import functools
import signal
import sys

class FrontMQWrapper(object):
    def __init__(self, front_id, private_exchange_name = 'private_exchange', server_ip = 'localhost'):
        self.req_queue_name = 'req_front%d' % front_id
        self.rsp_queue_name = 'rsp_front%d' % front_id
        self.private_exchange_name = private_exchange_name
        self.host = server_ip
        self.stopped = False
        self.rsp_connection = None
        self.rsp_channel = None
        self.private_connection = None
        self.private_channel = None
        self.receive_response_thread = None
        self.receive_private_thread = None
        self.req_connection = None
        self.req_channel = None
        self.mock_req_generate_thread = None
        self.properties = None

    def connect(self):
        hdrs = {'source_queue': self.req_queue_name,
                'target_queue' : self.rsp_queue_name}
        self.properties = pika.BasicProperties(app_id='front1',
                                               content_type='application/json',
                                               headers = hdrs)
        self.req_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.req_channel = self.req_connection.channel()
        self.req_channel.queue_declare(queue=self.req_queue_name)
        
        self.stopped = False
        self.receive_response_thread = threading.Thread(target=self.receive_private_work)        
        self.receive_private_thread =  threading.Thread(target=self.receive_response_work)
        self.receive_response_thread.start()
        self.receive_private_thread.start()

        self.mock_req_generate_thread = threading.Thread(target=self.mock_req_generate_work)
        self.mock_req_generate_thread.start()

    def disconnect(self):
        self.stopped = True
        if self.mock_req_generate_thread:
            self.mock_req_generate_thread.join()
            self.mock_req_generate_thread = None
        if self.rsp_channel:
            self.rsp_channel.stop_consuming()
        if self.rsp_connection:
            self.rsp_connection.close()
        if self.private_channel:
            self.private_channel.stop_consuming()
        if self.private_connection:
            self.private_connection.close()
        if self.receive_private_thread:
            self.receive_private_thread.join()
        if self.receive_response_thread:
            self.receive_response_thread.join()

    def receive_response_work(self):
        self.rsp_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
        self.rsp_channel = self.rsp_connection.channel()
        self.rsp_channel.queue_declare(queue=self.rsp_queue_name)
        self.rsp_channel.basic_consume(functools.partial(self.callback, tag='rsp'), queue=self.rsp_queue_name,no_ack=True)
        self.rsp_channel.start_consuming()

    def receive_private_work(self):
        self.private_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
        self.private_channel = self.private_connection.channel()
        self.private_channel.exchange_declare(exchange=self.private_exchange_name, exchange_type='fanout')
        result = self.private_channel.queue_declare(exclusive=True)
        queue_name = result.method.queue
        self.private_channel.queue_bind(exchange=self.private_exchange_name, queue=queue_name)
        self.private_channel.basic_consume(functools.partial(self.callback, tag='private'), queue=queue_name, no_ack=True)
        self.private_channel.start_consuming()

    def callback(self, ch, method, properties, body, tag):
        print('ch=>%s' % str(ch))
        print('method=>%s' % str(method))
        print('properties=>%s' % str(property))
        print("%s@%s" % (tag, body.decode()))

    def send_request(self, msg):
        self.req_channel.basic_publish(exchange='', routing_key=self.req_queue_name, properties=self.properties, body='Hello,world')

    def mock_req_generate_work(self):
        while not self.stopped:
            msg = 'Req@%s' % datetime.datetime.now().strftime('%H:%M:%S')
            self.send_request(msg)
            print('generate[%s] and send out' % msg)
            time.sleep(5)


if __name__ == '__main__':
    front_node = FrontMQWrapper(1)
    front_node.connect()
    while  1 == 1:
        s = input("'c'退出\n")
        if len(s) > 0 and s[0] == 'c':
            front_node.disconnect()
            break

