#-*- coding:utf-8 -*-

import pika
import threading
import datetime
import time
import functools
import signal
import sys
import copy
import queue


class CoreMQWrapper(object):
    def __init__(self, private_exchange_name='private_exchange', server_ip = 'localhost'):
        self.req_queue_name = 'req_front1'
        self.rsp_queue_name = 'rsp_front1'
        self.req_queue_name2 = 'req_front2'
        self.rsp_queue_name2 = 'rsp_front2'
        self.private_exchange_name = private_exchange_name
        self.receive_request_thread = None
        self.host = server_ip
        self.rsp_channel = None

        self.receive_connection = None
        self.receive_channel = None
        self.send_connection = None
        self.send_channel = None
        self.receive_thread = None
        self.send_thread = None
        self.receive_buffer = queue.Queue()
        self.send_buffer = queue.Queue()
        self.process_thread = None
        self.stopping = True

    def connect(self):
        self.stopping = False      
        self.send_thread = threading.Thread(target=self.send_work)
        self.process_thread = threading.Thread(target=self.process_db_work)
        self.receive_thread = threading.Thread(target=self.receive_work)
        self.send_thread.start()
        self.process_thread.start()
        self.receive_thread.start()


    def disconnect(self):
        self.stopping = True
        self.receive_channel.stop_consuming()
        self.receive_thread.join()
        self.process_thread.join()
        self.send_channel.stop_consuming()
        self.send_thread.join()

    def receive_work(self):
        self.receive_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.receive_channel = self.receive_connection.channel()
        self.receive_channel.queue_declare(queue=self.req_queue_name)
        self.receive_channel.queue_declare(queue=self.req_queue_name2)
        self.receive_channel.basic_consume(self.callback, queue=self.req_queue_name,no_ack=True)
        self.receive_channel.basic_consume(self.callback, queue=self.req_queue_name2,no_ack=True)
        self.receive_channel.start_consuming()

    def send_work(self):
        self.send_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.send_channel = self.send_connection.channel()
        self.send_channel.queue_declare(queue = self.rsp_queue_name)
        self.send_channel.queue_declare(queue = self.rsp_queue_name2)
        self.send_channel.exchange_declare(exchange = self.private_exchange_name, exchange_type='fanout')
        while not self.stopping:
            try:
                send_task = self.send_buffer.get(block = True, timeout = 0.1)
                self.send_channel.basic_publish(exchange = send_task[0], 
                                                routing_key = send_task[1], 
                                                properties = send_task[2], 
                                                body =send_task[3],
                                                immediate=False)
            except:
                pass


    def callback(self,ch, method, properties, body):
        print('property[%s]' % str(properties))
        print('body[%s]' %(body.decode())) 
        self.receive_buffer.put((properties, body))

    def process_db_work(self):
        while not (self.stopping  and self.receive_buffer.empty()):
            try:
                pack = self.receive_buffer.get(block=True, timeout = 0.1)
                self.process_request(pack[0], pack[1])
            except:
                pass

    def process_request(self, properties, body):
        dt_str = datetime.datetime.now().strftime('%H:%M:%S.%f')
        rsp_msg = 'Response@%s[%s]' % (dt_str, body.decode())
        private_msg = 'Private@%s[%s]' % (dt_str, body.decode())
        send_properties = pika.BasicProperties(app_id='db_core', 
                                               content_type='application/json', 
                                               headers = properties.headers)
        if 'target_queue' in properties.headers:
            self.send_buffer.put(('', properties.headers['target_queue'], send_properties, rsp_msg))
        self.send_buffer.put((self.private_exchange_name, '', send_properties, private_msg))


if __name__ == '__main__':
    core = CoreMQWrapper()
    core.connect()
    while 1== 1:
        time.sleep(1)