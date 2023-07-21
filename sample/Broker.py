# -*- coding: utf-8 -*-

from Liquirizia.EventBroker import EventBrokerHelper
# from Liquirizia.EventBroker.Implements.RabbitMQ import (
from src import (
	Configuration,
	Connection,
)

from random import randint

if __name__ == '__main__':

	# 이벤트 브로커 설정
	EventBrokerHelper.Set(
		'Sample',
		Connection,
		Configuration(
			host='127.0.0.1',
			port=5672,
			username='guest',
			password='guest',
		)
	)

	broker = EventBrokerHelper.Get('Sample')

	topic = broker.topic('topic.sample')
	queue = broker.queue('queue.sample')

	topic.publish(
		'event.sample',
		str(randint(0, 1000)),
		format='text/plain',
		charset='utf-8'
	)
	topic.publish(
		'event.sample.error',
		str(randint(0, 1000)),
		format='text/plain',
		charset='utf-8'
	)
	queue.send(
		'event.sample',
		str(randint(0, 1000)),
		format='text/plain',
		charset='utf-8'
	)

	reply = broker.queue()
	reply.declare('queue.reply', persistent=False)

	id = topic.publish(
		'event.sample',
		str(randint(0, 1000)),
		format='text/plain',
		charset='utf-8',
		headers={
			'X-Reply-Broker': 'Sample',
			'X-Reply-Broker-Queue': 'queue.reply'
		}
	)

	res = reply.recv()
	print('{} : {} - {}'.format(id, res.body, res.header('X-Reply-Message-Id')))

	id = queue.send(
		'event.sample',
		str(randint(0, 1000)),
		format='text/plain',
		charset='utf-8',
		headers={
			'X-Reply-Broker': 'Sample',
			'X-Reply-Broker-Queue': 'queue.reply'
		}
	)

	res = reply.recv()
	print('{} : {} - {}'.format(id, res.body, res.header('X-Reply-Message-Id')))

	EventBrokerHelper.Publish('Sample', 'topic.sample', event='event.sample', body=str(randint(0, 1000)), format='text/plain', charset='utf-8')

	EventBrokerHelper.CreateQueue('Sample', 'queue.reply.2', persistent=False)
	id = EventBrokerHelper.Publish('Sample', 'topic.sample', event='event.sample', body=str(randint(0, 1000)), format='text/plain', charset='utf-8', headers={
		'X-Reply-Broker': 'Sample',
		'X-Reply-Broker-Queue': 'queue.reply.2'
	})
	res = EventBrokerHelper.Recv('Sample', 'queue.reply.2')
	print('{} : {} - {}'.format(id, res.body, res.header('X-Reply-Message-Id')))

	EventBrokerHelper.CreateQueue('Sample', 'queue.reply.3', persistent=False)
	id = EventBrokerHelper.Send('Sample', 'queue.sample', event='event.sample', body=str(randint(0, 1000)), format='text/plain', charset='utf-8', headers={
		'X-Reply-Broker': 'Sample',
		'X-Reply-Broker-Queue': 'queue.reply.3'
	})
	res = EventBrokerHelper.Recv('Sample', 'queue.reply.3')
	print('{} : {} - {}'.format(id, res.body, res.header('X-Reply-Message-Id')))
