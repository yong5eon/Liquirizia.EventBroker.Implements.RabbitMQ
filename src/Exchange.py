# -*- coding: utf-8 -*-

from Liquirizia.EventBroker import Exchange as BaseExchange

from .Serializer import Encoder

from pika import BlockingConnection, BasicProperties

from time import time
from enum import Enum
from uuid import uuid4

from typing import Dict

__all__ = (
	'Exchange',
)


class Exchange(BaseExchange):
	"""Exchange of Event Broker for RabbitMQ"""
	def __init__(
		self,
		connection: BlockingConnection,
		encode: Encoder,
		name: str = None,
	):
		self.connection = connection
		self.encode = encode
		self.channel = self.connection.channel()
		self.channel.auto_decode = False
		self.exchange = name
		return

	def __del__(self):
		if self.channel and self.channel.is_open:
			self.channel.close()
		return
	
	def __str__(self): return self.exchange

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
			exchange=self.exchange,
			routing_key=event if event else '',
			properties=properties,
			body=self.encode(body),
		)
		return id
