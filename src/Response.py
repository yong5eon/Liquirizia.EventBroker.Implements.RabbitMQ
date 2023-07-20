# -*- coding: utf-8 -*-

from Liquirizia.EventBroker import Response as ResponseBase
from Liquirizia.EventBroker.Serializer import SerializerHelper

__all__ = (
	'Response'
)


class Response(ResponseBase):
	"""
	Response of Event Broker for RabbitMQ
	"""
	def __init__(self, properties, body):
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

	def headers(self):
		return self.props

	def header(self, key):
		if key not in self.props:
			return None
		return self.props[key]

	@property
	def id(self):
		return self.properties.message_id

	@property
	def type(self):
		return self.properties.type

	@property
	def body(self):
		return self.payload
