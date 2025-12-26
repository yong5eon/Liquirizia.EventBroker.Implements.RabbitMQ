# -*- coding: utf-8 -*-

from Liquirizia.EventBroker import Consumer as BaseConsumer, EventHandler

from .Serializer import Decoder

from .Event import Event

from pika import BlockingConnection

__all__ = (
	'Consumer'
)


class Consumer(BaseConsumer):
	"""Consumer of Event Broker for RabbitMQ"""
	def __init__(
		self,
		connection: BlockingConnection,
		decode: Decoder,
		handler: EventHandler,
		qos: int = 1,
		offset: int = None,
	):
		self.connection = connection
		self.decode = decode
		self.channel = self.connection.channel()
		self.channel.basic_qos(0, qos, False)
		self.channel.auto_decode = False
		self.offset = offset
		self.handler = handler
		self.queues = set()
		return

	def __del__(self):
		if self.channel and self.channel.is_open:
			self.channel.close()
		return

	def subs(self, queue:str):
		self.channel.basic_consume(
			queue,
			Callback(self.decode, self.handler, queue),
			auto_ack=False,
			exclusive=False,
			arguments={'x-stream-offset': self.offset} if self.offset is not None else None
		)
		return

	def run(self):
		self.channel.start_consuming()
		return

	def stop(self):
		self.channel.stop_consuming()
		return


class Callback(object):
	def __init__(self, decode: Decoder, handler: EventHandler, queue: str):
		self.decode = decode
		self.handler = handler
		self.queue = queue
		return
	def __call__(self, channel, method, properties, body):
		return self.handler(Event(
			channel,
			self.queue,
			method.consumer_tag,
			method.delivery_tag,
			properties,
			self.decode(body, properties.content_type, properties.content_encoding),
		))
