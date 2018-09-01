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
    [{ content: 'test-message-1' }],
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
