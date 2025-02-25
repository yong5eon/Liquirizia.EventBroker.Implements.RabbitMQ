# -*- coding: utf-8 -*-

from Liquirizia.Test import *

from Liquirizia.EventBroker import Helper
from Liquirizia.EventBroker.Implements.RabbitMQ import *

from Liquirizia.System.Utils import SetTimer

from queue import SimpleQueue



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
				timeout=5000,
				heartbeat=1000,
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
		con.createQueue('queue')
		queue = con.queue('queue')
		queue.send(i)
		reader = con.queue('queue')
		_ = reader.get()
		_.ack()
		ASSERT_IS_EQUAL(i, _.body)
		con.deleteQueue('queue')
		return
	
	@Parameterized(
		{'i': True},
		{'i': False},
		{'i': 1},
		{'i': 1.0},
		{'i': ''},
		{'i': 'abc'},
		{'i': []},
		{'i': [1,2,3]},
		{'i': {}},
		{'i': {'a': True, 'b':1, 'c': 1.0, 'd': 'abc'}},
	)
	@Order(2)
	def testFanout(self, i):
		con: Connection = Helper.Get('Sample')
		con.createExchange('fanout', ExchangeType.Direct)
		con.createQueue('fanout.queue')
		con.bindExchangeToQueue('fanout', 'fanout.queue')
		exchange = con.exchange('fanout')
		queue = con.queue('fanout.queue')	
		exchange.send(i)
		_ = queue.get()
		_.ack()
		ASSERT_IS_EQUAL(i, _.body)
		con.deleteQueue('fanout.queue')
		con.deleteExchange('fanout')
		return

	@Parameterized(
		{'i': True, 'event': 'false', 'status': False},
		{'i': 1, 'event': 'true', 'status': True},
		{'i': 1.0, 'event': 'false', 'status': False},
		{'i': 'abc', 'event': 'true', 'status': True},
		{'i': [1,2,3], 'event': 'false', 'status': False},
		{'i': {'a': True, 'b':1, 'c': 1.0, 'd': 'abc'}, 'event': 'true', 'status': True},
	)
	@Order(3)
	def testDirect(self, i, event, status):
		con: Connection = Helper.Get('Sample')
		con.createExchange('direct', ExchangeType.Direct)
		con.createQueue('direct.queue')
		con.bindExchangeToQueue('direct', 'direct.queue', event='true')
		exchange = con.exchange('direct')
		queue = con.queue('direct.queue')	
		exchange.send(i, event=event)
		_ = queue.get(timeout=500)
		if status:
			ASSERT_IS_EQUAL(i, _.body)
			_.ack()
		else:
			ASSERT_IS_NONE(_)
		con.deleteQueue('direct.queue')
		con.deleteExchange('direct')
		return

	@Parameterized(
		{'i': True, 'event': 'a.false', 'status': False},
		{'i': 1, 'event': 'b.true', 'status': True},
		{'i': 1.0, 'event': 'c.false', 'status': False},
		{'i': 'abc', 'event': 'd.true', 'status': True},
		{'i': [1,2,3], 'event': 'e.false', 'status': False},
		{'i': {'a': True, 'b':1, 'c': 1.0, 'd': 'abc'}, 'event': 'f.true', 'status': True},
	)
	@Order(4)
	def testTopic(self, i, event, status):
		con: Connection = Helper.Get('Sample')
		con.createExchange('topic', ExchangeType.Topic)
		con.createQueue('topic.queue')
		con.bindExchangeToQueue('topic', 'topic.queue', event='*.true')
		exchange = con.exchange('topic')
		queue = con.queue('topic.queue')	
		exchange.send(i, event=event)
		_ = queue.get(timeout=500)
		if status:
			ASSERT_IS_EQUAL(i, _.body)
			_.ack()
		else:
			ASSERT_IS_NONE(_)
		con.deleteQueue('topic.queue')
		con.deleteExchange('topic')
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
		con.createExchange('header', ExchangeType.Header)
		con.createQueue('header.queue')
		con.bindExchangeToQueue('header', 'header.queue', parameters=All(event='true'))
		exchange = con.exchange('header')
		queue = con.queue('header.queue')	
		exchange.send(i, headers=headers)
		_ = queue.get(timeout=500)
		if status:
			ASSERT_IS_EQUAL(i, _.body)
			_.ack()
		else:
			ASSERT_IS_NONE(_)
		con.deleteQueue('header.queue')
		con.deleteExchange('header')
		return

	@Parameterized(
		{'i': True},
		{'i': 1},
		{'i': 1.0},
		{'i': 'abc'},
		{'i': [1,2,3]},
		{'i': {'a': True, 'b':1, 'c': 1.0, 'd': 'abc'}},
	)
	@Order(6)
	def testConsumer(self, i):
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
		con: Connection = Helper.Get('Sample')
		con.createQueue('queue')
		queue = con.queue('queue')
		queue.send(i)
		q = SimpleQueue()
		consumer = con.consumer(TestEventHandler(q))
		def stop(timer):
			consumer.stop()
			return
		SetTimer(100, stop)
		consumer.subs('queue')
		consumer.run()
		_ = q.get(timeout=0.1)
		ASSERT_IS_EQUAL(i, _)
		con.deleteQueue('queue')
		return
	
