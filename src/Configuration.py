# -*- coding: utf-8 -*-

from Liquirizia.EventBroker.Configuration import Configuration as ConfigurationBase

from pika import SSLOptions
from ssl import create_default_context

__all__ = (
	'Configuration'
)


class Configuration(ConfigurationBase):
	"""
	Configuration of Event Broker for RabbitMQ
	"""
	def __init__(self, host, port, username=None, password=None, ssl=False, vhost='/', timeout=None, heartbeat=None):
		self.host = host
		self.port = port
		self.username = username
		self.password = password
		self.ssl = SSLOptions(create_default_context(), host) if ssl else None
		self.vhost = vhost
		self.timeout = timeout/1000 if timeout else None
		self.heartbeat = int(heartbeat*2/1000) if heartbeat else None
		return
