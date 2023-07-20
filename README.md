# Liquirizia.EventBroker.Implements.RabbitMQ
RabbitMQ 를 사용하는 이벤트 브로커

## 이벤트 컨슈머
```python
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

	# 토픽 선언
	topic = broker.topic()
	topic.declare('topic.sample', alter='topic.error.route', persistent=False)

	# 에러 처리를 위한 토픽 선언
	e = broker.topic()
	e.declare('topic.error', persistent=False)

	# 에러 처리를 큐 선언
	q = broker.queue()
	q.declare('queue.error', persistent=False)
	q.bind('topic.error', '*')
	q.bind('topic.error.route', '*')

	# 큐 선언
	queue = broker.queue()
	queue.declare('queue.sample', errorQueue='queue.error', persistent=False)
	queue.bind('topic.sample', 'event.sample')

	# 이벤트 처리를 위한 콜백 인터페이스 구현
	class SampleCallback(Callback):
		def __call__(self, event: Event):
			try:
				print(event.body)
				event.ack()
			except RuntimeError:
				event.nack()  # if you want requeue message
				# event.reject()  # if you want drop message or move to DLQ or DLE
			return

	# 컨슈머 정의 및 동작
	consumer = broker.consumer(callback=SampleCallback())
	consumer.consume('queue.sample')
	consumer.run()
```

## 이벤트 퍼블리셔
```python
from Liquirizia.EventBroker import EventBrokerHelper
from Liquirizia.EventBroker.Implements.RabbitMQ import (
	Configuration,
	Connection,
)

from random import randint

if __name__ == '__main__':

	# 이벤트 프로커 설정
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

	# 토픽에 메세지 퍼블리싱
	EventBrokerHelper.Publish('Sample', 'topic.sample', event='event.sample', body=str(randint(0, 1000)), format='text/plain', charset='utf-8')
	# 큐에 메세지 퍼블리싱
	EventBrokerHelper.Send('Sample', 'queue.sample', event='event.sample', body=str(randint(0, 1000)), format='text/plain', charset='utf-8')
```

## RPC(Remote Process Call)

### 원격 프로세스 요청
```python
from Liquirizia.EventBroker import EventBrokerHelper
from Liquirizia.EventBroker.Implements.RabbitMQ import (
	Configuration,
	Connection,
)

from random import randint

if __name__ == '__main__':

	# 이벤트 프로커 설정
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

	# 응답 큐 생성
	EventBrokerHelper.CreateQueue('Sample', 'queue.reply')	
	# 큐에 메세지 퍼블리싱
	EventBrokerHelper.Send(
		'Sample', 
		'queue.sample', 
		event='event.sample', 
		body=str(randint(0, 1000)), 
		format='text/plain', 
		charset='utf-8',
		headers={
			'x-broker': 'Sample',
			'x-broker-reply': 'queue.reply'
		}
	)
	# 응답 요청
	response = EventBrokerHelper.Recv('Sample', 'queue.reply')
	# TODO : do something with response
```

### 원격 프로세스 처리
```python
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

	# 큐 선언
	queue = broker.queue()
	queue.declare('queue.sample', persistent=False)
	queue.bind('topic.sample', 'event.sample')

	# 이벤트 처리를 위한 콜백 인터페이스 구현
	class SampleCallback(Callback):
		def __call__(self, event: Event):
			try:
				# TODO : do something
				EventBrokerHelper.Send(
					event.header('x-broker'),
					event.header('x-broker-reply'),
					event=event.type,
					body=event.body,
					format='plain/text',
					charset='utf-8',
					headers={
						'x-message-id': event.id
					}
				)
				event.ack()
			except RuntimeError:
				event.nack()  # if you want requeue message
				# event.reject()  # if you want drop message or move to DLQ or DLE
			return

	# 컨슈머 정의 및 동작
	consumer = broker.consumer(callback=SampleCallback())
	consumer.consume('queue.sample')
	consumer.run()
```
