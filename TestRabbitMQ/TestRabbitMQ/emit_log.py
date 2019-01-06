#-*- coding:utf-8 -*-

import pika
import sys
import datetime 
import time

if __name__ == '__main__':
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='logs', exchange_type='fanout')
    
    while 1 == 1:
        message = datetime.datetime.now().strftime('%H:%M:%S')
        print('send=> %r' %message)
        channel.basic_publish(exchange='logs', routing_key='',body=message)
        time.sleep(1.1)
    connection.close()