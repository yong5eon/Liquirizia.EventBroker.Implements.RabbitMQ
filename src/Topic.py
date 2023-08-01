# -*- coding: utf-8 -*-

from Liquirizia.EventBroker import Topic as TopicBase, Error
from Liquirizia.EventBroker.Errors import *

from Liquirizia.Serializer import SerializerHelper
from Liquirizia.Serializer.Errors import (
	NotSupportedError as SerializerNotSupportedError,
	EncodeError as SerializerEncodeError,
)
from Liquirizia.System.Util import GenerateUUID

from pika import BlockingConnection, BasicProperties
from pika.exceptions import *

from time import time

__all__ = (
	'Topic'
)


class Topic(TopicBase):
	"""
	Topic of Event Broker for RabbitMQ
	"""
	def __init__(self, connection: BlockingConnection, topic: str = None):
		try:
			self.connection = connection
			self.channel = self.connection.channel()
			self.channel.auto_decode = False
			self.topic = topic
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

	def declare(self, topic: str, alter: str = None, persistent: bool = True):
		try:
			args = {}
			if alter:
				self.channel.exchange_declare(
					alter,
					'fanout',
					durable=persistent,
					auto_delete=not persistent,
				)
				args['alternate-exchange'] = alter
			self.channel.exchange_declare(
				topic,
				'topic',
				durable=persistent,
				auto_delete=not persistent,
				arguments=args
			)
			self.topic = topic
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
			self.channel.exchange_bind(self.topic, topic, routing_key=event)
		except (ChannelWrongStateError, ChannelError, ChannelError) as e:
			raise Error(str(e), error=e)
		except (ConnectionOpenAborted, ConnectionClosed) as e:
			raise ConnectionClosedError(str(e), error=e)
		except ConnectionWrongStateError as e:
			raise ConnectionError(str(e), error=e)
		except AMQPError as e:
			raise Error(str(e), error=e)
		return

	def publish(
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
				exchange=self.topic,
				routing_key=event,
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

	def unbind(self, topic: str, event: str):
		try:
			self.channel.exchange_unbind(self.topic, topic, routing_key=event)
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
			if self.topic:
				self.channel.exchange_delete(self.topic)
		except (ChannelWrongStateError, ChannelError, ChannelError) as e:
			raise Error(str(e), error=e)
		except (ConnectionOpenAborted, ConnectionClosed) as e:
			raise ConnectionClosedError(str(e), error=e)
		except ConnectionWrongStateError as e:
			raise ConnectionError(str(e), error=e)
		except AMQPError as e:
			raise Error(str(e), error=e)
		return
