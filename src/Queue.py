# -*- coding: utf-8 -*-

from Liquirizia.EventBroker import Queue as BaseQueue, Gettable

from Liquirizia.Serializer import SerializerHelper
from Liquirizia.System.Util import SetTimer

from .Event import Event

from pika import BlockingConnection, BasicProperties

from time import time
from uuid import uuid4

from typing import Optional, Dict

__all__ = (
	'Queue'
)


class Queue(BaseQueue, Gettable):
	"""Queue of Event Broker for RabbitMQ"""
	def __init__(
		self,
		connection: BlockingConnection,
		name: str = None,
	):
		self.connection = connection
		self.channel = self.connection.channel()
		self.channel.auto_decode = False
		self.queue = name
		self.event = None
		return

	def __del__(self):
		if self.channel and self.channel.is_open:
			self.channel.close()
		return
	
	def __str__(self): return self.queue

	def send(
		self,
		body,
		format: str = 'application/json',
		charset: str = 'utf-8',
		event: str = None,
		headers: Dict = {},
		priority: int = None,
		expiration: int = None,
		timestamp: int = None,
		persistent: bool = True,
		id: str = None,
	):
		if not timestamp: timestamp = int(time()),
		if not id: id = uuid4().hex,
		properties = BasicProperties(
			type=event if event else '',
			headers=headers,
			content_type=format,
			content_encoding=charset,
			priority=priority,
			timestamp=timestamp,
			expiration=expiration,
			message_id=id,
			delivery_mode=2 if persistent else 1,
		)
		body = SerializerHelper.Encode(body, format, charset) if body else None
		self.channel.basic_publish(
			exchange='',
			routing_key=self.queue,
			properties=properties,
			body=body
		)
		return id

	def get(self, timeout: int = None) -> Optional[Event]:
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
		timer = None
		if timeout:
			def stop():
				self.channel.stop_consuming()
				return
			timer = SetTimer(timeout, stop)
		self.channel.basic_consume(self.queue, callback, auto_ack=False, exclusive=False)
		self.channel.basic_qos(0, 1, False)
		self.channel.start_consuming()
		if timeout:
			timer.stop()
		return self.event

