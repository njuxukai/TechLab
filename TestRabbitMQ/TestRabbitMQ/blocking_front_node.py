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
        self.send_connection = None
        self.send_channel = None
        self.send_thread = None
        self.properties = None

        self.receive_connection = None
        self.receive_channel = None
        self.receive_thread = None

    def connect(self):
        hdrs = {'source_queue': self.req_queue_name,
                'target_queue' : self.rsp_queue_name}
        self.properties = pika.BasicProperties(app_id='front1',
                                               content_type='application/json',                  
                                               headers = hdrs)        
        self.stopped = False
        self.send_thread = threading.Thread(target=self.mock_req_generate_work)
        self.send_thread.start()
        self.receive_thread = threading.Thread(target=self.receive_work)
        self.receive_thread.start()

    def disconnect(self):
        self.stopped = True
        if self.send_channel:
            self.send_channel.stop_consuming()
        if self.receive_channel:
            self.receive_channel.stop_consuming()
        if self.receive_thread:
            self.receive_thread.join()
        if self.send_thread:
            self.send_thread.join()


    def receive_work(self):
        self.receive_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.receive_channel = self.receive_connection.channel()
        #rsp queue
        self.receive_channel.queue_declare(queue=self.rsp_queue_name)
        self.receive_channel.basic_consume(self.callback, queue=self.rsp_queue_name,no_ack=True)
        #private queue
        self.receive_channel.exchange_declare(exchange=self.private_exchange_name, exchange_type='fanout')
        result = self.receive_channel.queue_declare(exclusive=True)
        queue_name = result.method.queue
        self.receive_channel.queue_bind(exchange=self.private_exchange_name, queue=queue_name)
        self.receive_channel.basic_consume(self.callback, queue=queue_name, no_ack=True)
        self.receive_channel.start_consuming()


    def callback(self, ch, method, properties, body):
        #print('ch=>%s' % str(ch))
        #print('method=>%s' % str(method))
        #print('properties=>%s' % str(properties))
        print("body=>%s" % body.decode())


    def mock_req_generate_work(self):
        self.send_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.send_channel = self.send_connection.channel()
        self.send_channel.queue_declare(queue=self.req_queue_name)
        while not self.stopped:
            msg = 'Req@%s' % datetime.datetime.now().strftime('%H:%M:%S.%f')
            self.send_channel.basic_publish(exchange='', routing_key=self.req_queue_name, properties=self.properties, body=msg)
            print('generate[%s] and send out' % msg)
            time.sleep(5)


if __name__ == '__main__':
    front_node = FrontMQWrapper(2)
    front_node.connect()
    while  1 == 1:
        s = input("'c'退出\n")
        if len(s) > 0 and s[0] == 'c':
            front_node.disconnect()
            break

