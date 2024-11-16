# -*- coding: utf-8 -*-

from Liquirizia.EventBroker import Event as BaseEvent

from Liquirizia.Serializer import SerializerHelper

__all__ = (
	'Event'
)


class Event(BaseEvent):
	"""
	Event of Event Broker for RabbitMQ
	"""
	def __init__(self, channel, queue, consumer, transaction, properties, body):
		self.channel = channel
		self.queue = queue
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
		return 'Event(id={}, body={}, type={}, headers={})'.format(
			self.properties.message_id,
			self.payload,
			self.properties.type,
			self.props,
		)

	def ack(self):
		self.channel.basic_ack(self.transaction)
		return

	def nack(self):
		self.channel.basic_nack(self.transaction, requeue=True)
		return

	def reject(self):
		self.channel.basic_nack(self.transaction, requeue=False)
		return

	def headers(self):
		return self.props

	def header(self, key):
		if key not in self.props:
			return None
		return self.props[key]

	@property
	def src(self):
		return self.queue

	@property
	def id(self):
		return self.properties.message_id

	@property
	def type(self):
		return self.properties.type

	@property
	def format(self):
		return self.properties.content_type
	
	@property
	def charset(self):
		return self.properties.content_encoding

	@property
	def body(self):
		return self.payload
