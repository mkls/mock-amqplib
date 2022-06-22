const amqp = require('./main');

const generateQueueName = () => `test-queue-${Math.random()}`;
const generateExchangeName = () => `test-exchange-${Math.random()}`;

test('getting a single message from queue', async () => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createChannel();
  const queueName = generateQueueName();
  await channel.assertQueue(queueName, { durable: true });

  await channel.sendToQueue(queueName, 'test-content', {
    headers: { groupBy: 'groupness' }
  });

  const message = await channel.get(queueName, { noAck: true });
  const emptyQueueResponse = await channel.get(queueName, { noAck: true });

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
  const queueName = generateQueueName();
  await channel.assertQueue(queueName, { durable: true });
  const consumer = jest.fn();

  await channel.sendToQueue(queueName, 'test-message-1');

  await channel.prefetch(10);
  const { consumerTag } = await channel.consume(queueName, consumer);

  await channel.sendToQueue(queueName, 'test-message-2');

  await channel.cancel(consumerTag);

  await channel.sendToQueue(queueName, 'test-message-3');

  expect(consumer.mock.calls).toMatchObject([
    [{ content: 'test-message-1', properties: { headers: expect.anything() } }],
    [{ content: 'test-message-2' }]
  ]);
});

test('nackinkg a message puts it back to queue', async () => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createChannel();
  const queueName = generateQueueName();
  await channel.assertQueue(queueName, { durable: true });

  await channel.sendToQueue(queueName, 'test-content');

  const message = await channel.get(queueName);
  const afterRead = await channel.get(queueName);

  await channel.nack(message);

  const reRead = await channel.get(queueName);

  expect(afterRead).toEqual(false);
  expect(reRead.content).toEqual('test-content');
});

test('checkQueue return status for the queue', async () => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createChannel();
  const queueName = generateQueueName();
  await channel.assertQueue(queueName, { durable: true });
  await channel.sendToQueue(queueName, 'test-content-1');
  await channel.sendToQueue(queueName, 'test-content-2');

  const status = await channel.checkQueue(queueName);

  expect(status).toEqual({
    queue: queueName,
    messageCount: 2
  });
});

test('purgeQueue deletes messages from queue', async () => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createChannel();
  const queueName = generateQueueName();
  await channel.assertQueue(queueName, { durable: true });
  await channel.sendToQueue(queueName, 'test-content-1');
  await channel.sendToQueue(queueName, 'test-content-2');

  await channel.purgeQueue(queueName);

  const message = await channel.get(queueName);
  expect(message).toEqual(false);
});

test('default exchange', async () => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createChannel();
  const targetQueueName = generateQueueName();
  const anotherQueueName = generateQueueName();
  await channel.assertQueue(targetQueueName);
  await channel.assertQueue(anotherQueueName);
  await channel.publish('', targetQueueName, 'content-1');

  expect(await channel.get(targetQueueName)).toMatchObject({
    content: 'content-1',
    fields: {
      exchange: '',
      routingKey: targetQueueName
    }
  });
  expect(await channel.get(anotherQueueName)).toEqual(false);
});

test('direct exchange', async () => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createChannel();
  await channel.assertExchange('retry-exchange', 'direct');
  await channel.assertQueue('retry-queue-10s');
  await channel.assertQueue('retry-queue-20s');
  await channel.bindQueue('retry-queue-10s', 'retry-exchange', 'some-target-queue');
  await channel.bindQueue('retry-queue-20s', 'retry-exchange', 'some-other-queue');

  await channel.publish('retry-exchange', 'some-target-queue', 'content-1');

  expect(await channel.get('retry-queue-10s')).toMatchObject({
    content: 'content-1',
    fields: {
      exchange: 'retry-exchange',
      routingKey: 'some-target-queue'
    }
  });
  expect(await channel.get('retry-queue-20s')).toEqual(false);
});

test('x-delayed-message exchange', async () => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createChannel();
  const queueName = generateQueueName();
  await channel.assertExchange('retry-exchange', 'x-delayed-message');
  await channel.assertQueue('retry-queue-10s');
  await channel.assertQueue('retry-queue-20s');
  await channel.bindQueue('retry-queue-10s', 'retry-exchange', 'some-target-queue');
  await channel.bindQueue('retry-queue-20s', 'retry-exchange', 'some-other-queue');

  await channel.publish('retry-exchange', 'some-target-queue', 'content-1');

  expect(await channel.get('retry-queue-10s')).toMatchObject({
    content: 'content-1',
    fields: {
      exchange: 'retry-exchange',
      routingKey: 'some-target-queue'
    }
  });
  expect(await channel.get('retry-queue-20s')).toEqual(false);

  await channel.assertExchange('retry-exchange-with-options', 'x-delayed-message', {
    durable: true,
    arguments: { 'x-delayed-type': 'direct' }
  });
  await channel.assertQueue('retry-queue-10s-options');
  await channel.assertQueue('retry-queue-20s-options');
  await channel.bindQueue(
    'retry-queue-10s-options',
    'retry-exchange-with-options',
    'some-target-queue-options'
  );
  await channel.bindQueue(
    'retry-queue-20s-options',
    'retry-exchange-with-options',
    'some-other-queue-options'
  );

  await channel.publish('retry-exchange-with-options', 'some-target-queue-options', 'content-2');

  expect(await channel.get('retry-queue-10s-options')).toMatchObject({
    content: 'content-2',
    fields: {
      exchange: 'retry-exchange-with-options',
      routingKey: 'some-target-queue-options'
    }
  });
  expect(await channel.get('retry-queue-20s-options')).toEqual(false);
});

const routingKeys = [
  'a1', '2b', '3c4', 'a1.2b', 'a1.3c4', 'a1.d', '2b.3c4', '2b.d', '3c4.d',
  'a1.2b.3c4', 'a1.3c4.d', 'a1.2b.d', '2b.3c4.d', 'a1.2b.3c4.d'
];
const cases = [
  { pattern: 'some.pattern',  result: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] },
  { pattern: 'a1.2b',         result: [0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] },
  { pattern: 'a1.*.*',        result: [0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0] },
  { pattern: 'a1.*.#',        result: [0, 0, 0, 1, 1, 1, 0, 0, 0, 1, 1, 1, 0, 1] },
  { pattern: '*.2b.*',        result: [0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0] },
  { pattern: '#.3c4',         result: [0, 0, 1, 0, 1, 0, 1, 0, 0, 1, 0, 0, 0, 0] },
  { pattern: '*.#.d',         result: [0, 0, 0, 0, 0, 1, 0, 1, 1, 0, 1, 1, 1, 1] },
  { pattern: '*',             result: [1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] },
  { pattern: '*.#.*',         result: [0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1] },
  { pattern: '#.*.*.#',       result: [0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1] },
  { pattern: '#.*',           result: [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1] },
  { pattern: '#.#.#',         result: [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1] },
  { pattern: '#',             result: [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1] },
];
test.each(cases)('topic exchange: $pattern', async (test) => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createChannel();
  const queueName = generateQueueName();
  const exchangeName = generateExchangeName();
  await channel.assertExchange(exchangeName, 'topic');
  await channel.assertQueue(queueName);
  await channel.bindQueue(queueName, exchangeName, test.pattern);

  for (const key of routingKeys) {
    const i = routingKeys.indexOf(key);
    await channel.publish(exchangeName, key, 'content-1');
    const expected = test.result[i] ? {
      content: 'content-1',
      fields: {
        exchange: exchangeName,
        routingKey: key
      },
      properties: {}
    } : false;
    expect(await channel.get(queueName)).toEqual(expected);
  }
});

test('headers exchange', async () => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createChannel();

  await channel.assertExchange('retry-exchange', 'headers');
  await channel.assertQueue('retry-queue-10s');
  await channel.assertQueue('retry-queue-20s');
  await channel.bindQueue('retry-queue-10s', 'retry-exchange', '', {
    retryCount: 1
  });
  await channel.bindQueue('retry-queue-20s', 'retry-exchange', '', {
    retryCount: 2
  });

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
  expect(await channel.get('retry-queue-20s')).toMatchObject({
    content: 'content-2'
  });
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
  const queueName = generateQueueName();
  await channel.assertQueue(queueName);

  await channel.sendToQueue(queueName, 'test-content');

  const message = await channel.get(queueName);

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
  const queueName = generateQueueName();
  await channel.assertQueue(queueName);

  await channel.sendToQueue(queueName, 'test-content');

  const message = await channel.get(queueName);
  await channel.nack(message, false, false);

  const reRead = await channel.get(queueName);

  expect(reRead).toEqual(false);
});

test('assert queue should return object with property "queue"', async () => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createChannel();
  const queueName = generateQueueName();
  const queue = await channel.assertQueue(queueName);
  expect(queue).toMatchObject({
    queue: queueName
  });
});

test('assert empty queue should create new queue with random name with prefix "amq.gen-"', async () => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createChannel();
  const queue1 = await channel.assertQueue('');
  const queue2 = await channel.assertQueue('');

  expect(queue1.queue).toMatch(/^amq.gen-\w{22}/);
  expect(queue2.queue).toMatch(/^amq.gen-\w{22}/);
  expect(queue1.queue).not.toBe(queue2.queue);
});

test('assert "credentials" to have been defined with required methods', () => {
  expect(amqp.credentials).toBeDefined();
  expect(amqp.credentials.plain).toBeDefined();
  expect(amqp.credentials.amqplain).toBeDefined();
  expect(amqp.credentials.external).toBeDefined();
});

test('assert required methods of "credentials"', () => {
  expect(amqp.credentials.plain('user', 'pass')).toEqual({
    mechanism: 'PLAIN',
    response: expect.any(Function),
    username: 'user',
    password: 'pass'
  });

  expect(amqp.credentials.amqplain('user', 'pass')).toEqual({
    mechanism: 'AMQPLAIN',
    response: expect.any(Function),
    username: 'user',
    password: 'pass'
  });

  expect(amqp.credentials.external()).toEqual({
    mechanism: 'EXTERNAL',
    response: expect.any(Function)
  });
});

test('ensure consuming messages works even is queues is asserted multiple times', async () => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createChannel();
  const queueName = generateQueueName();
  await channel.assertQueue(queueName);
  const receivedMessages = [];
  await channel.consume(queueName, ({ content }) => {
    receivedMessages.push(content);
  });

  await channel.assertQueue(queueName);
  await channel.sendToQueue(queueName, 1);
  await channel.assertQueue(queueName);
  await channel.sendToQueue(queueName, 2);

  expect(receivedMessages).toEqual([1, 2]);
});

test('promise race condition for publishing to another queue from consumer', async () => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createChannel();

  const queueName = generateQueueName();
  const errorQueueName = generateQueueName();

  const listener = jest.fn().mockResolvedValue(true);

  const cb = async () => {
    await listener();
    await channel.sendToQueue(errorQueueName);
  }

  await channel.assertQueue(queueName);
  await channel.assertQueue(errorQueueName);

  await channel.consume(queueName, cb);
  await channel.consume(errorQueueName, listener);

  await channel.sendToQueue(queueName, 2);

  expect(listener).toBeCalledTimes(2);
});


test('emitting on a connection triggers on callbacks', async () => {
  const connection = await amqp.connect('some-random-uri');
  const listener = jest.fn();

  connection.on('error', listener);
  connection.emit('error');

  expect(listener).toBeCalled();
});
