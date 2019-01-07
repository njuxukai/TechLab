#-*- coding:utf-8 -*-

import pika
import threading
import datetime
import time
import functools
import signal
import sys
import copy


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


    def connect(self):
        self.rsp_connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.rsp_channel = self.rsp_connection.channel()
        self.rsp_channel.queue_declare(queue=self.rsp_queue_name)
        self.rsp_channel.queue_declare(queue=self.rsp_queue_name2)

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
        rep_channel.queue_declare(queue=self.req_queue_name2)
        rep_channel.basic_consume(self.callback, queue=self.req_queue_name,no_ack=True)
        rep_channel.basic_consume(self.callback, queue=self.req_queue_name2,no_ack=True)
        rep_channel.start_consuming()
        print('leave req receive thread')

    def callback(self,ch, method, properties, body):
        print('method=>[%s]' % str(method))
        print('properties=>[%s]' % str(properties))
        print('Received[%s]' %(body.decode())) 
        
        
               
        rsp_msg = 'Rsp[%s]' % body.decode()
        private_msg = 'Private[%s]' % body.decode()
        if 'target_queue' in properties.headers:
            send_properties = pika.BasicProperties(app_id='db_core',content_type='application/json',headers=properties.headers)
            #self.rsp_channel.basic_publish(exchange='', routing_key=properties.headers['target_queue'], properties=send_properties, body=rsp_msg)
            self.send(properties.headers['target_queue'], send_properties, rsp_msg)
        self.boardcast(send_properties, private_msg)

    def get_rsp_queue_name(self, tag):
        return self.rsp_queue_name

    def send(self, queue_name, send_properties, msg):
        self.rsp_channel.basic_publish(exchange='', routing_key=queue_name, properties=send_properties, body=msg)
        #self.rsp_channel.basic_publish(exchange='', routing_key=queue_name,  body=msg)

    def boardcast(self, send_properties, msg):
        self.private_channel.basic_publish(exchange=self.private_exchange_name, routing_key='',properties=send_properties, body=msg)


if __name__ == '__main__':
    core = CoreMQWrapper()
    core.connect()
    while 1== 1:
        time.sleep(1)