#-*- coding:utf-8 -*-

import pika
import sys
import time

if __name__ == '__main__':
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='hello')
    while 1 == 1:
        channel.basic_publish(exchange='', routing_key='hello', body='Hello,world')
        time.sleep(1)
    connection.close()

