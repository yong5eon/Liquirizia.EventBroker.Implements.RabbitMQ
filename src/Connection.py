# -*- coding: utf-8 -*-

from Liquirizia.EventBroker import Connection as ConnectionBase, Callback, Error
from Liquirizia.EventBroker.Errors import *

from .Configuration import Configuration
from .Topic import Topic
from .Queue import Queue
from .Consumer import Consumer

from pika import BlockingConnection, ConnectionParameters, PlainCredentials
from pika.exceptions import *

__all__ = (
	'Connection'
)


class Connection(ConnectionBase):
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
		try:
			if self.connection and self.connection.is_open:
				return
			self.connection = BlockingConnection(parameters=self.parameters)
		except (ConnectionOpenAborted, ConnectionClosed) as e:
			raise ConnectionRefusedError(str(e), error=e)
		except ConnectionBlockedTimeout as e:
			raise ConnectionTimeoutError(str(e), error=e)
		except ConnectionWrongStateError as e:
			raise ConnectionError(str(e), error=e)
		except AMQPError as e:
			raise Error(str(e), error=e)
		return

	def topic(self, topic: str = None):
		return Topic(self.connection, topic)

	def queue(self, queue: str = None):
		return Queue(self.connection, queue)

	def consumer(self, callback: Callback, count: int = 1):
		return Consumer(self.connection, callback, count=count)

	def close(self):
		try:
			if self.connection and self.connection.is_open:
				self.connection.close()
				self.connection = None
		except AMQPError:
			self.connection = None
		return
