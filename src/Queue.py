# -*- coding: utf-8 -*-

from Liquirizia.EventBroker import Queue as BaseQueue, Exchange

from Liquirizia.Serializer import SerializerHelper

from .Bind import Parameters

from pika import BlockingConnection, BasicProperties

from time import time
from uuid import uuid4

from typing import Union, Dict

__all__ = (
	'Queue'
)


class Queue(BaseQueue): pass
class Queue(BaseQueue):
	"""
	Queue of Event Broker for RabbitMQ
	"""
	def __init__(
		self,
		connection: BlockingConnection,
		name: str = None,
	):
		self.connection = connection
		self.channel = self.connection.channel()
		self.channel.auto_decode = False
		self.queue = name
		return

	def __del__(self):
		if self.channel and self.channel.is_open:
			self.channel.close()
		return
	
	def __str__(self): return self.queue

	def create(
		self,
		name: str,
		expires: int = None,
		ttl: int = None,
		limit: int = None,
		size: int = None,
		durable: bool = True,
		autodelete: bool = False,
		errorQueue: Union[str, Queue] = None,
		errorExchange: Union[str, Exchange] = None,
	):
		args = {}
		if errorQueue:
			args['x-dead-letter-exchange'] = ''
			args['x-dead-letter-routing-key'] = str(errorQueue)
		if errorExchange:
			args['x-dead-letter-exchange'] = str(errorExchange)
		if expires:
			args['x-expires'] = expires
		if ttl:
			args['x-message-ttl'] = ttl
		if limit:
			args['x-max-length'] = limit
		if size:
			args['x-max-length-bytes'] = size
		self.channel.queue_declare(
			name,
			durable=durable,
			auto_delete=autodelete,
			arguments=args
		)
		self.queue = name
		return

	def remove(self):
		if self.queue:
			self.channel.queue_delete(self.queue)
		return


	def bind(self, exchange: Union[str, Exchange], event: str = None, parameters: Parameters = None):
		self.channel.queue_bind(
			self.queue,
			str(exchange),
			routing_key=event if event else '',
			arguments=parameters
		)
		return

	def unbind(self, exchange: Union[str, Exchange], event: str = None, parameters: Parameters = None):
		self.channel.queue_unbind(
			self.queue,
			str(exchange),
			routing_key=event if event else '',
			arguments=parameters,
		)
		return

	def send(
		self,
		body,
		format: str = 'application/json',
		charset: str = 'utf-8',
		event: str = None,
		headers: Dict = {},
		priority: int = None,
		expiration: int = None,
		timestamp: int = int(time()),
		persistent: bool = True,
		id: str = uuid4().hex,
	):
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
