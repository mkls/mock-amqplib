const amqp = require('./main');

const generateQueueName = () => `test-queue-${Math.random()}`;
const generateExchangeName = () => `test-exchange-${Math.random()}`;
const sleep = timeMs => new Promise(resolve => {
  setTimeout(resolve, timeMs);
});

test('getting a single message from queue', async () => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createChannel();
  const queueName = generateQueueName();
  await channel.assertQueue(queueName, { durable: true });

  channel.sendToQueue(queueName, 'test-content', {
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

  channel.sendToQueue(queueName, 'test-message-1');

  await channel.prefetch(10);
  const { consumerTag } = await channel.consume(queueName, consumer);

  channel.sendToQueue(queueName, 'test-message-2');

  await channel.cancel(consumerTag);

  channel.sendToQueue(queueName, 'test-message-3');

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

  channel.sendToQueue(queueName, 'test-content');

  const message = await channel.get(queueName);
  const afterRead = await channel.get(queueName);

  channel.nack(message);

  const reRead = await channel.get(queueName);

  expect(afterRead).toEqual(false);
  expect(reRead.content).toEqual('test-content');
});

test('checkQueue return status for the queue', async () => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createChannel();
  const queueName = generateQueueName();
  await channel.assertQueue(queueName, { durable: true });
  channel.sendToQueue(queueName, 'test-content-1');
  channel.sendToQueue(queueName, 'test-content-2');

  const status = await channel.checkQueue(queueName);

  expect(status).toEqual({
    queue: queueName,
    messageCount: 2
  });
});

test('assertExchange and checkExchange return exchange name', async () => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createChannel();
  const exchangeName = generateExchangeName();

  const assertResult =  await channel.assertExchange(exchangeName, 'direct');
  const checkResult =  await channel.checkExchange(exchangeName);

  expect(assertResult).toEqual({ exchange: exchangeName });
  expect(checkResult).toEqual(assertResult);
});

test('purgeQueue deletes messages from queue', async () => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createChannel();
  const queueName = generateQueueName();
  await channel.assertQueue(queueName, { durable: true });
  channel.sendToQueue(queueName, 'test-content-1');
  channel.sendToQueue(queueName, 'test-content-2');

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
const topicCases = [
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
test.each(topicCases)('topic exchange: $pattern', async (test) => {
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
    const message = await channel.get(queueName);
    if (test.result[i]) {
      expect(message).toMatchObject({
        content: 'content-1',
        fields: {
          exchange: exchangeName,
          routingKey: key
        },
        properties: {}
      });
    } else {
      expect(message).toEqual(false);
    }
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

test('should send and get message via ConfirmChannel', async () => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createConfirmChannel();
  const targetQueueName = generateQueueName();
  const anotherQueueName = generateQueueName();
  await channel.assertQueue(targetQueueName);
  await channel.assertQueue(anotherQueueName);

  await new Promise((resolve, reject) => {
    channel.sendToQueue(targetQueueName, 'content-1', {}, (err, result) => {
      if (err) {
        reject(err);
        return;
      }
      resolve(result);
    });
  });

  expect(await channel.get(targetQueueName)).toMatchObject({
    content: 'content-1',
    fields: {
      exchange: '',
      routingKey: targetQueueName
    }
  });
  expect(await channel.get(anotherQueueName)).toEqual(false);
});

test('direct exchange via ConfirmChannel', async () => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createConfirmChannel();
  const targetQueueName = generateQueueName();
  const anotherQueueName = generateQueueName();
  const exchangeName = generateExchangeName();
  await channel.assertExchange(exchangeName, 'direct');
  await channel.assertQueue(targetQueueName);
  await channel.assertQueue(anotherQueueName);
  await channel.bindQueue(targetQueueName, exchangeName, 'some-target-queue');
  await channel.bindQueue(anotherQueueName, exchangeName, 'some-other-queue');

  await new Promise((resolve, reject) => {
    channel.publish(exchangeName, 'some-target-queue', 'content-1', {}, (err, result) => {
      if (err) {
        reject(err);
        return;
      }
      resolve(result);
    });
  });

  expect(await channel.get(targetQueueName)).toMatchObject({
    content: 'content-1',
    fields: {
      exchange: exchangeName,
      routingKey: 'some-target-queue'
    }
  });
  expect(await channel.get(anotherQueueName)).toEqual(false);
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

  channel.sendToQueue(queueName, 'test-content');

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

  channel.sendToQueue(queueName, 'test-content');

  const message = await channel.get(queueName);
  channel.nack(message, false, false);

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
  channel.sendToQueue(queueName, 1);
  await channel.assertQueue(queueName);
  channel.sendToQueue(queueName, 2);

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
    channel.sendToQueue(errorQueueName);
  }

  await channel.assertQueue(queueName);
  await channel.assertQueue(errorQueueName);

  await channel.consume(queueName, cb);
  await channel.consume(errorQueueName, listener);

  channel.sendToQueue(queueName, 2);

  await sleep(0);
  expect(listener).toBeCalledTimes(2);
});


test('emitting on a connection triggers on callbacks', async () => {
  const connection = await amqp.connect('some-random-uri');
  const listener = jest.fn();

  connection.on('error', listener);
  connection.emit('error');

  expect(listener).toBeCalled();
});

const ttlCases = [
  { wait: 3, queueTtl: 5, msgTtl: undefined, result: true },
  { wait: 7, queueTtl: 5, msgTtl: undefined, result: false },
  { wait: 3, queueTtl: undefined, msgTtl: 5, result: true },
  { wait: 7, queueTtl: undefined, msgTtl: 5, result: false },
  { wait: 3, queueTtl: 7, msgTtl: 5, result: true },
  { wait: 6, queueTtl: 7, msgTtl: 5, result: false },
  { wait: 8, queueTtl: 7, msgTtl: 5, result: false },
]
test.each(ttlCases)(`should receive=$result message when message TTL=$msgTtl and queue TTL=$queueTtl after waiting $wait`, async (test) => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createChannel();
  const queueName = generateQueueName();
  const queueArgs = test.queueTtl >= 0 ? {
    'x-message-ttl': test.queueTtl,
  } : undefined;
  await channel.assertQueue(queueName, { durable: true, arguments: queueArgs });

  const publishOptions = test.msgTtl >= 0 ? {
    expiration: test.msgTtl,
  } : undefined;
  channel.sendToQueue(queueName, 'test-content', publishOptions);
  await sleep(test.wait);

  const message = await channel.get(queueName);

  expect(!!message).toEqual(test.result);
});

const dlxCases = [
  { wait: 2,   qTtl: [4, 4, 4],  msgTtl: undefined,  result: 0 },
  { wait: 6,   qTtl: [4, 4, 4],  msgTtl: undefined,  result: 1 },
  { wait: 10,  qTtl: [4, 4, 4],  msgTtl: undefined,  result: 2 },
  { wait: 14,  qTtl: [4, 4, 4],  msgTtl: undefined,  result: undefined },
  { wait: 1,   qTtl: [4, 4, 4],  msgTtl: 2,          result: 0 },
  { wait: 3,   qTtl: [4, 4, 4],  msgTtl: 2,          result: 1 },
  { wait: 5,   qTtl: [4, 4, 4],  msgTtl: 2,          result: 1 },
  { wait: 8,   qTtl: [4, 4, 4],  msgTtl: 2,          result: 2 },
  { wait: 12,  qTtl: [4, 4, 4],  msgTtl: 2,          result: undefined },
]
test.each(dlxCases)('should route message with TTL=$msgTtl to queue ' +
    '#$result when DLX defined on expiration after waiting $wait', async (test) => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createChannel();
  const queues = test.qTtl.map(() => generateQueueName());
  const getArgs = i => test.qTtl[i] >= 0 ? {
    'x-message-ttl': test.qTtl[i],
    'x-dead-letter-exchange': i + 1 < queues.length ? '' : undefined,
    'x-dead-letter-routing-key': i + 1 < queues.length ? queues[i + 1] : undefined,
  } : undefined;
  await Promise.all(queues.map((q, i) => channel.assertQueue(q, {
    arguments: getArgs(i),
  })));

  const publishOptions = test.msgTtl >= 0 ? {
    expiration: test.msgTtl,
  } : undefined;
  channel.sendToQueue(queues[0], 'test-content', publishOptions);
  await sleep(test.wait);

  for (let i = 0; i < queues.length; i++) {
    const queue = queues[i];
    const message = await channel.get(queue);
    expect(!!message).toEqual(test.result === i);
  }
});

test('should route to DLX after nack (requeue=false)', async () => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createChannel();
  const exchange = generateExchangeName();
  const routingKey = 'key';
  const nackQueue = generateQueueName();
  const targetQueue = generateQueueName();

  await channel.assertExchange(exchange, 'direct');
  await channel.assertQueue(nackQueue, {
    arguments: {
      'x-dead-letter-exchange': exchange,
      'x-dead-letter-routing-key': routingKey,
    },
  });
  await channel.assertQueue(targetQueue);
  await channel.bindQueue(targetQueue, exchange, routingKey);
  channel.sendToQueue(nackQueue, 'test-content');

  const message = await channel.get(nackQueue);
  expect(message).toBeTruthy();

  await channel.nack(message, false, false);

  const msg = await channel.get(targetQueue);
  expect(msg).toBeTruthy();
  expect(msg).toMatchObject({
    content: 'test-content',
    fields: { exchange, routingKey },
    properties: {
      headers: {
        'x-first-death-exchange': '', // first send was to queue directly
        'x-first-death-queue': nackQueue,
        'x-first-death-reason': 'rejected',
        'x-death': [
          {
            count: 1,
            exchange: '',
            queue: nackQueue,
            reason: 'rejected',
            'routing-keys': [nackQueue],
          },
        ],
      },
    },
  });
});

test('should emit return event when exchange:mandatory=true', async () => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createConfirmChannel();
  const queue = generateQueueName();
  const exchange = generateExchangeName();
  await channel.assertExchange(exchange, 'direct');
  await channel.assertQueue(queue);
  await channel.bindQueue(queue, exchange, 'right-pattern');

  const returnCallback = jest.fn();
  channel.on('return', msg => {
    returnCallback(msg);
  });
  await new Promise((resolve, reject) => {
    channel.publish(exchange, 'wrong-key', 'content-1', { mandatory: true }, (err, result) =>
      err ? reject(err) : resolve(result)
    );
  });
  expect(await channel.get(queue)).toEqual(false);
  expect(returnCallback).toBeCalledWith(expect.objectContaining({
    content: 'content-1',
    fields: { exchange, routingKey: 'wrong-key' },
  }));
});

test('should send to alternate exchange if specified', async () => {
  const connection = await amqp.connect('some-random-uri');
  const channel = await connection.createConfirmChannel();
  const emptyQueue = generateQueueName();
  const targetQueue = generateQueueName();
  const emptyExchange = generateExchangeName();
  const targetExchange = generateExchangeName();

  await channel.assertExchange(emptyExchange, 'topic', { alternateExchange: targetExchange });
  await channel.assertExchange(targetExchange, 'topic');
  await channel.assertQueue(emptyQueue);
  await channel.assertQueue(targetQueue);
  await channel.bindQueue(emptyQueue, emptyExchange, 'wrong.*');
  await channel.bindQueue(targetQueue, targetExchange, 'right.*');

  await new Promise((resolve, reject) => {
    channel.publish(emptyExchange, 'right.key', 'content-1', {}, (err, result) =>
        err ? reject(err) : resolve(result)
    );
  });
  expect(await channel.get(emptyQueue)).toEqual(false);
  expect(await channel.get(targetQueue)).toMatchObject({
    content: 'content-1',
    fields: { exchange: targetExchange, routingKey: 'right.key' },
  });
});
