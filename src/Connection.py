# -*- coding: utf-8 -*-

from Liquirizia.EventBroker import (
	Connection as BaseConnection, 
	GetExchange,
	GetQueue,
	GetConsumer,
	EventHandler,
)

from .Configuration import Configuration

from .Exchange import Exchange
from .Queue import Queue
from .Consumer import Consumer

from pika import BlockingConnection, ConnectionParameters, PlainCredentials
from pika.exceptions import *

__all__ = (
	'Connection'
)


class Connection(BaseConnection, GetExchange, GetQueue, GetConsumer):
	"""
	Connection of Event Broker for RabbitMQ
	"""
	def __init__(self, conf: Configuration):
		self.conf = conf
		self.parameters = ConnectionParameters(
			host=self.conf.host,
			port=self.conf.port,
			credentials=PlainCredentials(self.conf.username, self.conf.password),
			virtual_host=self.conf.vhost,
			ssl_options=self.conf.ssl,
			blocked_connection_timeout=self.conf.timeout,
			heartbeat=self.conf.heartbeat
		)
		self.connection = None
		return

	def __del__(self):
		self.close()
		return

	def connect(self):
		if self.connection and self.connection.is_open:
			return
		self.connection = BlockingConnection(parameters=self.parameters)
		return

	def exchange(self, exchange: str = None):
		return Exchange(self.connection, exchange)

	def queue(self, queue: str = None):
		return Queue(self.connection, queue)

	def consumer(self, queue: str, handler: EventHandler = None, qos: int = 1):
		return Consumer(self.connection, queue, handler, qos=qos)

	def close(self):
		if self.connection and self.connection.is_open:
			self.connection.close()
			self.connection = None
		return
