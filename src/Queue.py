# -*- coding: utf-8 -*-

from Liquirizia.EventBroker import Queue as QueueBase, Error
from Liquirizia.EventBroker.Errors import *

from Liquirizia.EventBroker.Serializer import SerializerHelper
from Liquirizia.EventBroker.Serializer.Errors import (
	NotSupportedError as SerializerNotSupportedError,
	EncodeError as SerializerEncodeError,
)

from Liquirizia.System.Util import GenerateUUID

from .Response import Response

from pika import BlockingConnection, BasicProperties
from pika.exceptions import *

from time import time

__all__ = (
	'Queue'
)


class Queue(QueueBase):
	"""
	Queue of Event Broker for RabbitMQ
	"""
	def __init__(self, connection: BlockingConnection, queue: str = None, count: int = 1):
		try:
			self.connection = connection
			self.channel = self.connection.channel()
			self.channel.auto_decode = False
			self.channel.basic_qos(0, count, False)
			self.queue = queue
		except (ChannelWrongStateError, ChannelError, ChannelError) as e:
			raise Error(str(e), error=e)
		except (ConnectionOpenAborted, ConnectionClosed) as e:
			raise ConnectionClosedError(str(e), error=e)
		except ConnectionWrongStateError as e:
			raise ConnectionError(str(e), error=e)
		except AMQPError as e:
			raise Error(str(e), error=e)
		return

	def __del__(self):
		try:
			if self.channel and self.channel.is_open:
				self.channel.close()
		except TypeError:
			pass
		return

	def declare(
		self,
		queue: str,
		errorTopic: str = None,
		errorQueue: str = None,
		expires: int = None,
		ttl: int = None,
		limit: int = None,
		size: int = None,
		persistent: bool = True
	):
		try:
			args = {}
			if errorQueue:
				args['x-dead-letter-exchange'] = ''
				args['x-dead-letter-routing-key'] = errorQueue
			if errorTopic:
				args['x-dead-letter-exchange'] = errorTopic
			if expires:
				args['x-expires'] = expires
			if ttl:
				args['x-message-ttl'] = ttl
			if limit:
				args['x-max-length'] = limit
			if size:
				args['x-max-length-bytes'] = size
			self.channel.queue_declare(
				queue,
				durable=persistent,
				auto_delete=not persistent,
				arguments=args
			)
			self.queue = queue
		except (ChannelWrongStateError, ChannelError, ChannelError) as e:
			raise Error(str(e), error=e)
		except (ConnectionOpenAborted, ConnectionClosed) as e:
			raise ConnectionClosedError(str(e), error=e)
		except ConnectionWrongStateError as e:
			raise ConnectionError(str(e), error=e)
		except AMQPError as e:
			raise Error(str(e), error=e)
		return

	def bind(self, topic: str, event: str):
		try:
			self.channel.queue_bind(self.queue, topic, routing_key=event)
		except (ChannelWrongStateError, ChannelError, ChannelError) as e:
			raise Error(str(e), error=e)
		except (ConnectionOpenAborted, ConnectionClosed) as e:
			raise ConnectionClosedError(str(e), error=e)
		except ConnectionWrongStateError as e:
			raise ConnectionError(str(e), error=e)
		except AMQPError as e:
			raise Error(str(e), error=e)
		except BaseException as e:
			raise Error(str(e), error=e)
		return

	def send(
		self,
		event,
		body=None,
		format: str = 'application/json',
		charset: str = 'utf-8',
		headers: dict = None,
		priority: int = None,
		expiration: int = None,
		timestamp: int = int(time()),
		persistent: bool = True,
		id: str = GenerateUUID(),
	):
		try:
			properties = BasicProperties(
				type=event,
				headers={},
				content_type=format,
				content_encoding=charset,
				priority=priority,
				timestamp=timestamp,
				expiration=expiration,
				message_id=id,
				delivery_mode=2 if persistent else 1,
			)
			for k, v in headers.items() if headers and isinstance(headers, dict) else {}:
				properties.headers[k] = v
			body = SerializerHelper.Encode(body, format, charset) if body else None
			self.channel.basic_publish(
				exchange='',
				routing_key=self.queue,
				properties=properties,
				body=body
			)
			return id
		except SerializerNotSupportedError as e:
			raise NotSupportedTypeError(format, charset)
		except SerializerEncodeError as e:
			raise EncodeError(body, format, charset)
		except (ChannelWrongStateError, ChannelError, ChannelError) as e:
			raise Error(str(e), error=e)
		except (ConnectionOpenAborted, ConnectionClosed) as e:
			raise ConnectionClosedError(str(e), error=e)
		except ConnectionWrongStateError as e:
			raise ConnectionError(str(e), error=e)
		except AMQPError as e:
			raise Error(str(e), error=e)
		return

	def qos(self, count: int = 0):
		try:
			self.channel.basic_qos(0, count, False)
		except (ChannelWrongStateError, ChannelError, ChannelError) as e:
			raise Error(str(e), error=e)
		except (ConnectionOpenAborted, ConnectionClosed) as e:
			raise ConnectionClosedError(str(e), error=e)
		except ConnectionWrongStateError as e:
			raise ConnectionError(str(e), error=e)
		except AMQPError as e:
			raise Error(str(e), error=e)
		return

	def receive(self, timeout: int = None):
		try:
			method, properties, body = next(self.channel.consume(
				self.queue,
				auto_ack=True,
				exclusive=False,
				inactivity_timeout=timeout/1000 if timeout else None
			))
			if not method or not properties:
				raise TimeoutError(str(timeout))
			return Response(
				properties=properties,
				body=body
			)
		except (ChannelWrongStateError, ChannelError, ChannelError) as e:
			raise Error(str(e), error=e)
		except (ConnectionOpenAborted, ConnectionClosed) as e:
			raise ConnectionClosedError(str(e), error=e)
		except ConnectionWrongStateError as e:
			raise ConnectionError(str(e), error=e)
		except AMQPError as e:
			raise Error(str(e), error=e)

	def unbind(self, topic: str, event: str):
		try:
			self.channel.queue_unbind(self.queue, topic, routing_key=event)
		except (ChannelWrongStateError, ChannelError, ChannelError) as e:
			raise Error(str(e), error=e)
		except (ConnectionOpenAborted, ConnectionClosed) as e:
			raise ConnectionClosedError(str(e), error=e)
		except ConnectionWrongStateError as e:
			raise ConnectionError(str(e), error=e)
		except AMQPError as e:
			raise Error(str(e), error=e)
		return

	def remove(self):
		try:
			if self.queue:
				self.channel.queue_delete(self.queue)
		except (ChannelWrongStateError, ChannelError, ChannelError) as e:
			raise Error(str(e), error=e)
		except (ConnectionOpenAborted, ConnectionClosed) as e:
			raise ConnectionClosedError(str(e), error=e)
		except ConnectionWrongStateError as e:
			raise ConnectionError(str(e), error=e)
		except AMQPError as e:
			raise Error(str(e), error=e)
		return
