const amqp = require('./main');

test('getting a single message from queue', async () => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createChannel();
  await channel.assertQueue('test-queue', { durable: true });

  await channel.sendToQueue('test-queue', 'test-content', {
    headers: { groupBy: 'groupness' }
  });

  const message = await channel.get('test-queue', { noAck: true });
  const emptyQueueResponse = await channel.get('test-queue', { noAck: true });

  expect(message).toMatchObject({
    content: 'test-content',
    properties: {
      headers: { groupBy: 'groupness' }
    }
  });
  expect(emptyQueueResponse).toEqual(false);
});

test('consuming messages', async () => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createChannel();
  await channel.assertQueue('test-queue', { durable: true });
  const consumer = jest.fn();

  await channel.sendToQueue('test-queue', 'test-message-1');

  await channel.prefetch(10);
  const { consumerTag } = await channel.consume('test-queue', consumer);

  await channel.sendToQueue('test-queue', 'test-message-2');

  await channel.cancel(consumerTag);

  await channel.sendToQueue('test-queue', 'test-message-3');

  expect(consumer.mock.calls).toMatchObject([
    [{ content: 'test-message-1', properties: { headers: expect.anything() } }],
    [{ content: 'test-message-2' }]
  ]);
});

test('nackinkg a message puts it back to queue', async () => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createChannel();
  await channel.assertQueue('test-queue', { durable: true });

  await channel.sendToQueue('test-queue', 'test-content');

  const message = await channel.get('test-queue');
  const afterRead = await channel.get('test-queue');

  await channel.nack(message);

  const reRead = await channel.get('test-queue');

  expect(afterRead).toEqual(false);
  expect(reRead.content).toEqual('test-content');
});

test('checkQueue return status for the queue', async () => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createChannel();
  await channel.assertQueue('test-queue', { durable: true });
  await channel.sendToQueue('test-queue', 'test-content-1');
  await channel.sendToQueue('test-queue', 'test-content-2');

  const status = await channel.checkQueue('test-queue');

  expect(status).toEqual({
    queue: 'test-queue',
    messageCount: 2
  })
});

test('purgeQueue deletes messages from queue', async () => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createChannel();
  await channel.assertQueue('test-queue', { durable: true });
  await channel.sendToQueue('test-queue', 'test-content-1');
  await channel.sendToQueue('test-queue', 'test-content-2');

  await channel.purgeQueue('test-queue');

  const message = await channel.get('test-queue');
  expect(message).toEqual(false);
});

test('direct exchange', async () => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createChannel();

  await channel.assertExchange('retry-exchange', 'direct');
  await channel.assertQueue('retry-queue-10s');
  await channel.assertQueue('retry-queue-20s');
  await channel.bindQueue('retry-queue-10s', 'retry-exchange', 'some-target-queue');
  await channel.bindQueue('retry-queue-20s', 'retry-exchange', 'some-other-queue');

  await channel.publish('retry-exchange', 'some-target-queue', 'content-1')

  expect(await channel.get('retry-queue-10s')).toMatchObject({
    content: 'content-1',
    fields: {
      exchange: 'retry-exchange',
      routingKey: 'some-target-queue'
    }
  });
  expect(await channel.get('retry-queue-20s')).toEqual(false);
});

test('headers exchange', async () => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createChannel();

  await channel.assertExchange('retry-exchange', 'headers');
  await channel.assertQueue('retry-queue-10s');
  await channel.assertQueue('retry-queue-20s');
  await channel.bindQueue('retry-queue-10s', 'retry-exchange', '', { retryCount: 1 });
  await channel.bindQueue('retry-queue-20s', 'retry-exchange', '', { retryCount: 2 });

  await channel.publish('retry-exchange', 'some-target-queue', 'content-1', {
    headers: { retryCount: 1 }
  });
  await channel.publish('retry-exchange', 'some-other-queue', 'content-2', {
    headers: { retryCount: 2 }
  });

  expect(await channel.get('retry-queue-10s')).toMatchObject({
    content: 'content-1',
    fields: {
      exchange: 'retry-exchange',
      routingKey: 'some-target-queue'
    }
  });
  expect(await channel.get('retry-queue-20s')).toMatchObject({ content: 'content-2'});
});

test('emitting on a channel triggers on callbacks', async () => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createChannel();
  const listener = jest.fn();

  channel.on('close', listener);
  channel.emit('close');

  expect(listener).toBeCalled();
});

test('it should always set header property of messages even if not set', async () => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createChannel();
  await channel.assertQueue('test-queue');

  await channel.sendToQueue('test-queue', 'test-content');

  const message = await channel.get('test-queue');

  expect(message).toMatchObject({
    content: 'test-content',
    properties: {
      headers: expect.anything()
    }
  });
});

it('should not put nack-ed messages back to queue if requeue is set to false', async () => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createChannel();
  await channel.assertQueue('test-queue');

  await channel.sendToQueue('test-queue', 'test-content');

  const message = await channel.get('test-queue');
  await channel.nack(message, false, false);

  const reRead = await channel.get('test-queue');

  expect(reRead).toEqual(false);
});

test('assert queue should return object with property "queue"', async () => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createChannel();
  const queue = await channel.assertQueue('test-queue');
  expect(queue).toMatchObject({
    queue: 'test-queue'
  });
});

test('assert empty queue should create new queue with random name with prefix "amq.gen-"', async () => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createChannel();
  const queue1 = await channel.assertQueue('');
  const queue2 = await channel.assertQueue('');

  expect(queue1.queue).toMatch(/^amq.gen-\w+/);
  expect(queue2.queue).toMatch(/^amq.gen-\w+/);
  expect(queue1.queue).not.toBe(queue2.queue);
});