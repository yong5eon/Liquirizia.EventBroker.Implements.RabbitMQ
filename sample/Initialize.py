# -*- coding: utf-8 -*-

from Liquirizia.EventBroker import EventBrokerHelper
from Liquirizia.EventBroker.Implements.RabbitMQ import (
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

	topic = broker.topic()
	topic.declare('topic.sample', alter='topic.error.route')

	queue = broker.queue()
	queue.declare('queue.error')
	queue.bind('topic.error.route', '*')

	queue = broker.queue()
	queue.declare('queue.sample', errorQueue='queue.error')
	queue.bind('topic.sample', 'event.sample')
