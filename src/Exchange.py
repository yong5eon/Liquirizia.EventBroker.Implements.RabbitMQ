# -*- coding: utf-8 -*-

from Liquirizia.EventBroker import Exchange as BaseExchange

from Liquirizia.Serializer import SerializerHelper

from .Bind import Parameters

from pika import BlockingConnection, BasicProperties

from time import time
from enum import Enum
from uuid import uuid4

from typing import Union, Dict

__all__ = (
	'Exchange',
	'ExchangeType',
)

class ExchangeType(str, Enum):
	Direct = 'direct'
	FanOut = 'fanout'
	Topic  = 'topic'
	Header = 'headers'
	def __str__(self): return str(self.value)


class Exchange(BaseExchange): pass
class Exchange(BaseExchange):
	"""Exchange of Event Broker for RabbitMQ"""
	def __init__(
		self,
		connection: BlockingConnection,
		name: str = None,
	):
		self.connection = connection
		self.channel = self.connection.channel()
		self.channel.auto_decode = False
		self.exchange = name
		return

	def __del__(self):
		if self.channel and self.channel.is_open:
			self.channel.close()
		return
	
	def __str__(self): return self.exchange

	def create(
		self,
		name: str,
		type: ExchangeType,
		durable: bool = True,
		autodelete: bool = False,
		alter: Union[str, Exchange] = None,
	):
		args = {}
		if alter:
			args['alternate-exchange'] = str(alter)
		self.channel.exchange_declare(
			name,
			str(type),
			durable=durable,
			auto_delete=autodelete,
			arguments=args
		)
		self.exchange = name
		return

	def remove(self):
		if self.exchange:
			self.channel.exchange_delete(self.exchange)
		return

	def bind(
		self,
		exchange: Union[str, Exchange],
		event: str = None,
		parameters: Parameters= None,
	):
		self.channel.exchange_bind(
			self.exchange,
			str(exchange),
			routing_key=event if event else '',
			arguments=parameters
		)
		return

	def unbind(
		self,
		exchange: Union[str, Exchange],
		event: str = None,
		parameters: Parameters = None,
	):
		self.channel.exchange_unbind(
			self.exchange,
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
			exchange=self.exchange,
			routing_key=event if event else '',
			properties=properties,
			body=body
		)
		return id
