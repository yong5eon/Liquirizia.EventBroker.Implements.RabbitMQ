# -*- coding: utf-8 -*-

from Liquirizia.EventBroker import EventHandler

from .Event import Event

__all__ = (
	'Callback'
)

class Callback(object):
	"""Callback of Consumer"""

	def __init__(self, handler: EventHandler, queue: str):
		self.handler = handler
		self.queue = queue
		return

	def __call__(self, channel, method, properties, body):
		return self.handler(Event(
			channel,
			self.queue,
			method.consumer_tag,
			method.delivery_tag,
			properties,
			body,
		))
