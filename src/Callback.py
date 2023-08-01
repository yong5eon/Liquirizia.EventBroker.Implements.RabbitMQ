# -*- coding: utf-8 -*-

from .Event import Event

__all__ = (
	'Callback'
)

class Callback(object):
	"""Callback of Consumer"""

	def __init__(self, callback: callable, queue: str):
		self.callback = callback
		self.queue = queue
		return

	def __call__(self, channel, method, properties, body):
		return self.callback(Event(
			channel,
			self.queue,
			method.consumer_tag,
			method.delivery_tag,
			properties,
			body,
		))
