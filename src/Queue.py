# -*- coding: utf-8 -*-

from Liquirizia.EventBroker import (
	Queue as BaseQueue,
	Poppable,
)

from .Serializer import Encoder, Decoder
from .Event import Event

from pika import BlockingConnection, BasicProperties

from time import time, sleep
from uuid import uuid4

from typing import Optional, Dict

__all__ = (
	'Queue'
)


class Queue(BaseQueue, Poppable):
	"""Queue of Event Broker for RabbitMQ"""
	def __init__(
		self,
		connection: BlockingConnection,
		encode: Encoder,
		decode: Decoder,
		name: str = None,
	):
		self.connection = connection
		self.encode = encode
		self.decode = decode
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
		event: str = None,
		headers: Dict = {},
		priority: int = None,
		expiration: int = None,
		timestamp: int = None,
		persistent: bool = True,
		id: str = None,
	):
		if not timestamp: timestamp = int(time())
		if not id: id = uuid4().hex
		properties = BasicProperties(
			type=event if event else '',
			headers=headers,
			content_type=self.encode.format,
			content_encoding=self.encode.charset,
			priority=priority,
			timestamp=timestamp,
			expiration=expiration,
			message_id=id,
			delivery_mode=2 if persistent else 1,
		)
		self.channel.basic_publish(
			exchange='',
			routing_key=self.queue,
			properties=properties,
			body=self.encode(body)
		)
		return id

	def pop(self, timeout: int = None) -> Optional[Event]:
		channel = self.channel
		channel.basic_qos(0, 1, False)
		deadline = time() + (timeout / 1000) if timeout else None
		while True:
			method, properties, body = channel.basic_get(queue=self.queue, auto_ack=False)
			if method:
				return Event(
					channel,
					self.queue,
					None,
					method.delivery_tag,
					properties,
					self.decode(body, properties.content_type, properties.content_encoding),
				)
			if deadline and time() >= deadline:
				break
			sleep(0.1)
		return
