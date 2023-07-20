# -*- coding: utf-8 -*-

from Liquirizia.EventBroker import Event as EventBase, Error
from Liquirizia.EventBroker.Errors import *

from Liquirizia.EventBroker.Serializer import SerializerHelper

from pika.exceptions import *

__all__ = (
	'Event'
)


class Event(EventBase):
	"""
	Event of Event Broker for RabbitMQ
	"""
	def __init__(self, channel, consumer, transaction, properties, body):
		self.channel = channel
		self.consumer = consumer
		self.transaction = transaction
		self.properties = properties
		self.props = properties.headers
		self.payload = SerializerHelper.Decode(
			body,
			format=properties.content_type,
			charset=properties.content_encoding
		) if body else None
		self.length = len(body) if body else 0
		return

	def __repr__(self):
		return '{} - {} - {}, {}'.format(
			self.properties['type'],
			self.length,
			self.properties['content_type'],
			self.properties['content_encoding']
		)

	def ack(self):
		try:
			self.channel.basic_ack(self.transaction)
		except (ChannelWrongStateError, ChannelError, ChannelError) as e:
			raise Error(str(e), error=e)
		except (ConnectionOpenAborted, ConnectionClosed) as e:
			raise ConnectionClosedError(str(e), error=e)
		except ConnectionWrongStateError as e:
			raise ConnectionError(str(e), error=e)
		except AMQPError as e:
			raise Error(str(e), error=e)
		return

	def nack(self):
		try:
			self.channel.basic_nack(self.transaction, requeue=True)
		except (ChannelWrongStateError, ChannelError, ChannelError) as e:
			raise Error(str(e), error=e)
		except (ConnectionOpenAborted, ConnectionClosed) as e:
			raise ConnectionClosedError(str(e), error=e)
		except ConnectionWrongStateError as e:
			raise ConnectionError(str(e), error=e)
		except AMQPError as e:
			raise Error(str(e), error=e)
		return

	def reject(self):
		try:
			self.channel.basic_nack(self.transaction, requeue=False)
		except (ChannelWrongStateError, ChannelError, ChannelError) as e:
			raise Error(str(e), error=e)
		except (ConnectionOpenAborted, ConnectionClosed) as e:
			raise ConnectionClosedError(str(e), error=e)
		except ConnectionWrongStateError as e:
			raise ConnectionError(str(e), error=e)
		except AMQPError as e:
			raise Error(str(e), error=e)
		return

	def headers(self):
		return self.props

	def header(self, key):
		if key not in self.props:
			return None
		return self.props[key]

	@property
	def src(self):
		return self.consumer

	@property
	def id(self):
		return self.properties.message_id

	@property
	def type(self):
		return self.properties.type

	@property
	def body(self):
		return self.payload
