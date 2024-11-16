# -*- coding: utf-8 -*-

from Liquirizia.Test import *

from Liquirizia.EventBroker import Helper, EventHandler
from Liquirizia.EventBroker.Implements.RabbitMQ import *
from Liquirizia.EventBroker.Implements.RabbitMQ.Bind import *

from Liquirizia.System.Util import SetTimer

from queue import SimpleQueue

class TestEventHandler(EventHandler):
	def __init__(self, v: SimpleQueue):
		self.v = v
		return

	def __call__(self, event: Event):
		try:
			self.v.put(event.body)
			event.ack()
		except RuntimeError:
			event.nack()  # if you want requeue message
		return


class TestEventBroker(Case):
	@classmethod
	def setUpClass(cls) -> None:
		Helper.Set(
			'Sample',
			Connection,
			Configuration(
				host='127.0.0.1',
				port=5672,
				username='guest',
				password='guest',
			)
		)
		return super().setUpClass()

	@Parameterized(
		{'i': True},
		{'i': 1},
		{'i': 1.0},
		{'i': 'abc'},
		{'i': [1,2,3]},
		{'i': {'a': True, 'b':1, 'c': 1.0, 'd': 'abc'}},
	)
	@Order(1)
	def testQueue(self, i):
		con: Connection = Helper.Get('Sample')
		queue = con.queue()
		queue.create('queue')
		queue.send(i)
		reader = con.consumer('queue')
		_ = reader.read()
		_.ack()
		ASSERT_IS_EQUAL(i, _.body)
		queue.remove()
		return
	
	@Parameterized(
		{'i': True},
		{'i': 1},
		{'i': 1.0},
		{'i': 'abc'},
		{'i': [1,2,3]},
		{'i': {'a': True, 'b':1, 'c': 1.0, 'd': 'abc'}},
	)
	@Order(2)
	def testFanout(self, i):
		con: Connection = Helper.Get('Sample')
		exchange = con.exchange()
		exchange.create('fanout', ExchangeType.Direct)
		queue = con.queue()
		queue.create('fanout.queue')
		queue.bind(exchange)
		exchange.send(i)
		reader = con.consumer('fanout.queue')	
		_ = reader.read()
		_.ack()
		ASSERT_IS_EQUAL(i, _.body)
		queue.remove()
		exchange.remove()
		return

	@Parameterized(
		{'i': True, 'event': 'false', 'recv': False},
		{'i': 1, 'event': 'true', 'recv': True},
		{'i': 1.0, 'event': 'false', 'recv': False},
		{'i': 'abc', 'event': 'true', 'recv': True},
		{'i': [1,2,3], 'event': 'false', 'recv': False},
		{'i': {'a': True, 'b':1, 'c': 1.0, 'd': 'abc'}, 'event': 'true', 'recv': True},
	)
	@Order(3)
	def testDirect(self, i, event, recv):
		con: Connection = Helper.Get('Sample')
		exchange = con.exchange()
		exchange.create('direct', ExchangeType.Direct)
		queue = con.queue()
		queue.create('direct.queue')
		queue.bind(exchange, event='true')
		exchange.send(i, event=event)
		reader = con.consumer('direct.queue')	
		_ = reader.read(timeout=0.1)
		if recv and _:
			ASSERT_IS_EQUAL(i, _.body)
			_.ack()
		else:
			ASSERT_IS_NONE(_)
		queue.remove()
		exchange.remove()
		return

	@Parameterized(
		{'i': True, 'event': 'a.false', 'recv': False},
		{'i': 1, 'event': 'b.true', 'recv': True},
		{'i': 1.0, 'event': 'c.false', 'recv': False},
		{'i': 'abc', 'event': 'd.true', 'recv': True},
		{'i': [1,2,3], 'event': 'e.false', 'recv': False},
		{'i': {'a': True, 'b':1, 'c': 1.0, 'd': 'abc'}, 'event': 'f.true', 'recv': True},
	)
	@Order(4)
	def testTopic(self, i, event, recv):
		con: Connection = Helper.Get('Sample')
		exchange = con.exchange()
		exchange.create('topic', ExchangeType.Topic)
		queue = con.queue()
		queue.create('topic.queue')
		queue.bind(exchange, event='*.true')
		exchange.send(i, event=event)
		reader = con.consumer('topic.queue')	
		_ = reader.read(timeout=0.1)
		if recv and _:
			ASSERT_IS_EQUAL(i, _.body)
			_.ack()
		else:
			ASSERT_IS_NONE(_)
		queue.remove()
		exchange.remove()
		return
	
	@Parameterized(
		{'i': True, 'headers': {'event': 'true'}, 'status': True},
		{'i': 1, 'headers': {'event': 'true'}, 'status': True},
		{'i': 1.0, 'headers': {'event': 'false'}, 'status': False},
		{'i': 'abc', 'headers': {'event': 'true'}, 'status': True},
		{'i': [1,2,3], 'headers': {'event': 'false'}, 'status': False},
		{'i': {'a': True, 'b':1, 'c': 1.0, 'd': 'abc'}, 'headers': {'event': 'true'}, 'status': True},
	)
	@Order(5)
	def testHeader(self, i, headers, status):
		con: Connection = Helper.Get('Sample')
		exchange = con.exchange()
		exchange.create('header', ExchangeType.Header)
		queue = con.queue()
		queue.create('header.queue')
		queue.bind(exchange, parameters=All(event='true'))
		exchange.send(i, headers=headers)
		reader = con.consumer('header.queue')	
		_ = reader.read(timeout=0.1)
		if status and _:
			ASSERT_IS_EQUAL(i, _.body)
			_.ack()
		else:
			ASSERT_IS_NONE(_)
		queue.remove()
		exchange.remove()
		return
	