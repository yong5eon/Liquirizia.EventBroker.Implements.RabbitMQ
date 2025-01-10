# Sample for Liquirizia.EventBroker.Implements.RabbitMQ

from Liquirizia.EventBroker import Helper
from Liquirizia.EventBroker.Implements.RabbitMQ import (
	Configuration,
	Connection,
	ExchangeType,
	Event,
	EventHandler,
)
from Liquirizia.System.Util import SetTimer

if __name__ == '__main__':

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

	con: Connection = Helper.Get('Sample')
	con.createExchange('topic', ExchangeType.Topic)
	con.createQueue('queue')
	con.bindExchangeToQueue('topic', 'queue')

	exchange = con.exchange('topic')
	exchange.send({'a': True, 'b': 1, 'c':1.0, 'd': 'abc'})
	queue = con.queue('queue')
	queue.send({'a': False, 'b': 2, 'c':2.0, 'd': 'def'})

	reader = con.queue('queue')
	e = reader.get()
	print(e)
	e.ack()
	e = reader.get()
	print(e)
	e.ack()

	exchange.send({'a': True, 'b': 1, 'c':1.0, 'd': 'abc'})
	queue.send({'a': False, 'b': 2, 'c':2.0, 'd': 'def'})

	class SampleEventHandler(EventHandler):
		def __call__(self, event: Event):
			try:
				print(event.body)
				event.ack()
			except RuntimeError:
				event.nack()  # if you want requeue message
				# event.reject()  # if you want drop message or move to DLQ or DLE
			return
		
	consumer = con.consumer(
		handler=SampleEventHandler(),
		qos=1
	)

	def stop():
		consumer.stop()
		return
	
	SetTimer(100, stop)

	consumer.subs('queue')
	consumer.run()

	con.deleteQueue('queue')
	con.deleteExchange('topic')

