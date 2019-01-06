#-*- coding:utf-8 -*-
import pika

def callback(ch, method, prperties, body):
    print('%s' % body)

if __name__ == '__main__':
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='hello')
    channel.basic_consume(callback, queue='hello',no_ack=True)
    channel.start_consuming()
