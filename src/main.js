const EventEmitter = require('events')

const queues = {};
const exchanges = {};

const createQueue = () => {
  let messages = [];
  let subscriber = null;

  return {
    add: async item => {
      if (subscriber) {
        await subscriber(item);
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
    getMessageCount: () => messages.length,
    purge: () => (messages = [])
  };
};

const createFanoutExchange = () => {
  const bindings = [];
  return {
    bindQueue: (queueName, pattern, options) => {
      bindings.push({
        targetQueue: queueName,
        options,
        pattern
      });
    },
    getTargetQueues: (routingKey, options = {}) => {
      return [...bindings.map(binding => binding.targetQueue)];
    }
  };
};

const createDirectExchange = () => {
  const bindings = [];
  return {
    bindQueue: (queueName, pattern, options) => {
      bindings.push({
        targetQueue: queueName,
        options,
        pattern
      });
    },
    getTargetQueues: (routingKey, options = {}) => {
      const matchingBinding = bindings.find(binding => binding.pattern === routingKey);
      return [matchingBinding.targetQueue];
    }
  };
};

const createHeadersExchange = () => {
  const bindings = [];
  return {
    bindQueue: (queueName, pattern, options) => {
      bindings.push({
        targetQueue: queueName,
        options,
        pattern
      });
    },
    getTargetQueues: (routingKey, options = {}) => {
      const isMatching = (binding, headers) =>
        Object.keys(binding.options).every(key => binding.options[key] === headers[key]);
      const matchingBinding = bindings.find(binding => isMatching(binding, options.headers || {}));
      return [matchingBinding.targetQueue];
    }
  };
};

const createChannel = async () => ({
  ...EventEmitter.prototype,
  close: () => {},
  assertQueue: async queueName => {
    if (!queueName) {
      queueName = generateRandomQueueName();
    }
    if (!(queueName in queues)) {
      queues[queueName] = createQueue();
    }
    return { queue: queueName };
  },
  assertExchange: async (exchangeName, type) => {
    let exchange;

    switch(type) {
      case 'fanout':
        exchange = createFanoutExchange();
        break;
      case 'direct':
      case 'x-delayed-message':
        exchange = createDirectExchange();
        break;
      case 'headers':
        exchange = createHeadersExchange();
        break;
    }

    exchanges[exchangeName] = exchange;
    return { exchange: exchangeName };
  },
  bindQueue: async (queue, sourceExchange, pattern, options = {}) => {
    const exchange = exchanges[sourceExchange];
    exchange.bindQueue(queue, pattern, options);
  },
  publish: async (exchangeName, routingKey, content, options = {}) => {
    const exchange = exchanges[exchangeName];
    const queueNames = exchange.getTargetQueues(routingKey, options);
    const message = {
      content,
      fields: {
        exchange: exchangeName,
        routingKey
      },
      properties: options
    };

    for(const queueName of queueNames) {
      queues[queueName].add(message);
    }
  },
  sendToQueue: async (queueName, content, { headers } = {}) => {
    await queues[queueName].add({
      content,
      fields: {
        exchange: '',
        routingKey: queueName
      },
      properties: { headers: headers || {} }
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
  nack: async (message, allUpTo = false, requeue = true) => {
    if (requeue) {
      queues[message.fields.routingKey].add(message);
    }
  },
  checkQueue: queueName => ({
    queue: queueName,
    messageCount: queues[queueName].getMessageCount()
  }),
  checkExchange: async exchangeName => ({
    exchange: exchangeName,
  }),
  purgeQueue: queueName => queues[queueName].purge()
});

const generateRandomQueueName = () => {
  const ABC = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_';
  let res = 'amq.gen-';
  for( let i=0; i<22; i++ ){
    res += ABC[(Math.floor(Math.random() * ABC.length))];
  }
  return res;
};

const credentials = {
  plain: (username, password) => ({
    mechanism: 'PLAIN',
    response: () => '',
    username,
    password
  }),
  amqplain: (username, password) => ({
    mechanism: 'AMQPLAIN',
    response: () => '',
    username,
    password
  }),
  external: () => ({
    mechanism: 'EXTERNAL',
    response: () => '',
  })
}

module.exports = {
  connect: async () => ({
    ...EventEmitter.prototype,
    createChannel,
    isConnected: true,
    close: function () {
      this.emit('close')
    }
  }),
  credentials
};
