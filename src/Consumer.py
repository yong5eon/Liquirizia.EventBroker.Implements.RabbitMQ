# -*- coding: utf-8 -*-

from Liquirizia.EventBroker import Consumer as ConsumerBase, Callback, Error
from Liquirizia.EventBroker.Errors import *

from .Event import Event

from pika import BlockingConnection
from pika.exceptions import *

__all__ = (
	'Consumer'
)


class Consumer(ConsumerBase):
	"""
	Consumer of Event Broker for RabbitMQ
	"""

	INTERVAL = 1000

	def __init__(self, connection: BlockingConnection, callback: Callback, count: int = 1):
		self.connection = connection
		self.channel = self.connection.channel()
		self.channel.auto_decode = False
		self.channel.basic_qos(0, count, False)
		self.callback = callback
		return

	def __del__(self):
		try:
			if self.channel and self.channel.is_open:
				self.channel.close()
		except TypeError:
			pass
		return

	def qos(self, count: int):
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

	def consume(self, queue: str):
		try:
			return self.channel.basic_consume(queue, self.event, auto_ack=False, exclusive=False)
		except (ChannelWrongStateError, ChannelError, ChannelError) as e:
			raise Error(str(e), error=e)
		except (ConnectionOpenAborted, ConnectionClosed) as e:
			raise ConnectionClosedError(str(e), error=e)
		except ConnectionWrongStateError as e:
			raise ConnectionError(str(e), error=e)
		except AMQPError as e:
			raise Error(str(e), error=e)
		return

	def event(self, channel, method, properties, body):
		return self.callback(Event(
			channel,
			method.consumer_tag,
			method.delivery_tag,
			properties,
			body,
		))

	def run(self, interval: int = None):
		try:
			while True:
				self.connection.process_data_events(interval/1000 if interval else self.__class__.INTERVAL/1000)
		except (ChannelWrongStateError, ChannelError, ChannelError) as e:
			raise Error(str(e), error=e)
		except (ConnectionOpenAborted, ConnectionClosed) as e:
			raise ConnectionClosedError(str(e), error=e)
		except ConnectionWrongStateError as e:
			raise ConnectionError(str(e), error=e)
		except AMQPError as e:
			raise Error(str(e), error=e)

	def stop(self):
		self.channel.stop_consuming()
		return
