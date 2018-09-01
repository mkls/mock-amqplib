const queues = {};

const createQueue = () => {
  let messages = [];
  let subscriber = null;

  return {
    add: item => {
      if (subscriber) {
        subscriber(item);
      } else {
        messages.push(item);
      }
    },
    get: () => messages.shift() || false,
    addConsumer: consumer => {
      messages.forEach(item => consumer(item));
      messages = [];
      subscriber = consumer;
    },
    stopConsume: () => (subscriber = null),
    getMessageCount: () => messages.length
  };
};

const createChannel = async () => ({
  on: () => {},
  close: () => {},
  assertQueue: async queuName => {
    queues[queuName] = createQueue();
  },
  sendToQueue: async (queueName, content, options = {}) => {
    queues[queueName].add({
      content,
      fields: {
        exchange: '',
        routingKey: queueName
      },
      properties: options
    });
  },
  get: async (queueName, { noAck } = {}) => {
    return queues[queueName].get();
  },
  prefetch: async () => {},
  consume: async (queueName, consumer) => {
    queues[queueName].addConsumer(consumer);
    return { consumerTag: queueName };
  },
  cancel: async consumerTag => queues[consumerTag].stopConsume(),
  ack: async () => {},
  nack: async message => {
    queues[message.fields.routingKey].add(message);
  },
  checkQueue: queueName => ({
    queue: queueName,
    messageCount: queues[queueName].getMessageCount()
  })
});

module.exports = {
  connect: async () => ({
    createChannel,
    close: () => {}
  })
};
