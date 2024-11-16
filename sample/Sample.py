

from Liquirizia.EventBroker import Helper, EventHandler
from Liquirizia.EventBroker.Implements.RabbitMQ import (
	Configuration,
	Connection,
	ExchangeType,
	Event,
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

	exchange = con.exchange()
	exchange.create('topic', ExchangeType.Topic)
	queue = con.queue()
	queue.create('queue')
	queue.bind(exchange)

	exchange.send({'a': True, 'b': 1, 'c':1.0, 'd': 'abc'})
	queue.send({'a': True, 'b': 1, 'c':1.0, 'd': 'abc'})

	reader = con.consumer('queue')
	e = reader.read()
	print(e)
	e.ack()
	e = reader.read()
	print(e)
	e.ack()

	exchange.send({'a': True, 'b': 1, 'c':1.0, 'd': 'abc'})
	queue.send({'a': True, 'b': 1, 'c':1.0, 'd': 'abc'})

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
		queue='queue',
		handler=SampleEventHandler(),
		qos=1
	)

	def stop():
		consumer.stop()
		return
	
	SetTimer(0.1, stop)

	consumer.run()

	queue.remove()
	exchange.remove()
