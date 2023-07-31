# -*- coding: utf-8 -*-

from Liquirizia.EventBroker import EventBrokerHelper, Callback
from Liquirizia.EventBroker.Implements.RabbitMQ import (
	Configuration,
	Connection,
	Event,
)

if __name__ == '__main__':

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

	class SampleCallback(Callback):
		def __call__(self, event: Event):
			try:
				print(event.body)
				event.ack()
				if event.header('X-Reply-Broker') and event.header('X-Reply-Broker-Queue'):
					EventBrokerHelper.Send(
						event.header('X-Reply-Broker'),
						event.header('X-Reply-Broker-Queue'),
						event=event.type,
						body=event.body,
						format='text/plain',
						charset='utf-8',
						headers={
							'X-Reply-Message-Id': event.id
						}
					)
			except RuntimeError:
				event.nack()  # if you want requeue message
				# event.reject()  # if you want drop message or move to DLQ or DLE
			return

	consumer = broker.consumer(callback=SampleCallback())
	consumer.consume('queue.sample')
	consumer.run()
