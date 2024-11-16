# -*- coding: utf-8 -*-

from Liquirizia.EventBroker import Consumer as BaseConsumer, EventHandler

from .Event import Event

from Liquirizia.System.Util import SetTimer

from pika import BlockingConnection

from typing import Optional

__all__ = (
	'Consumer'
)


class Consumer(BaseConsumer):
	"""Consumer of Event Broker for RabbitMQ"""

	def __init__(
		self,
		connection: BlockingConnection,
		queue: str,
		handler: EventHandler = None,
		qos: int = 1,
	):
		self.connection = connection
		self.queue = queue
		self.channel = self.connection.channel()
		self.channel.auto_decode = False
		self.channel.basic_qos(0, qos, False)
		self.handler = handler
		return

	def __del__(self):
		if self.channel and self.channel.is_open:
			self.channel.close()
		return

	def read(self, timeout: float = None) -> Optional[Event]:
		self.event = None
		def callback(channel, method, properties, body):
			self.event = Event(
				channel,
				self.queue,
				method.consumer_tag,
				method.delivery_tag,
				properties,
				body,
			)
			self.channel.stop_consuming()
			return
		self.channel.basic_consume(self.queue, callback, auto_ack=False, exclusive=False)
		if timeout:
			def stop():
				try:
					self.channel.stop_consuming()
				except Exception as e:
					pass
				return
			SetTimer(timeout, stop)
		self.channel.start_consuming()
		return self.event

	def __callback__(self, channel, method, properties, body):
		return self.handler(Event(
			channel,
			self.queue,
			method.consumer_tag,
			method.delivery_tag,
			properties,
			body,
		))

	def run(self):
		if not self.handler: raise RuntimeError('EventHandler is not initialized')
		self.channel.basic_consume(self.queue, self.__callback__, auto_ack=False, exclusive=False)
		self.channel.start_consuming()
		return

	def stop(self):
		self.channel.stop_consuming()
		return
