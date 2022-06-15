const EventEmitter = require('events')

const DEFAULT_EXCHANGE_NAME = '';

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

const createTopicExchange = () => {
  const bindings = [];
  const maskToRegexp = mask => {
    const words = mask.split('.');
    const del = '\\.';
    let strForRegexp = '^';
    let moveDel = false;
    for (let i = 0; i < words.length; i++) {
      const word = words[i];
      const first = i === 0;
      const prefix = !first && !moveDel ? del : '';

      moveDel = false;
      if (word === '*') {
        strForRegexp += `${prefix}\\w+`;
      } else if (word === '#') {
        if (first) {
          moveDel = true;
          strForRegexp += `(\\w+${del}?)*`;
        } else {
          strForRegexp += `(${prefix}${prefix && '?'}\\w+)*`;
        }
      } else {
        strForRegexp += prefix + word;
      }
    }
    strForRegexp += '$';
    console.log('lol', mask, strForRegexp);
    return new RegExp(strForRegexp);
  }
  return {
    bindQueue: (queueName, pattern, options) => {
      bindings.push({
        targetQueue: queueName,
        options,
        pattern
      });
    },
    getTargetQueues: (routingKey, options = {}) => {
      const matchingBinding = bindings.filter(binding => maskToRegexp(binding.pattern).test(routingKey));
      return matchingBinding.map(b => b.targetQueue);
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

const queues = {};
const exchanges = {
  [DEFAULT_EXCHANGE_NAME]: createDirectExchange()
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
      const exchange = exchanges[DEFAULT_EXCHANGE_NAME];
      exchange.bindQueue(queueName, queueName);
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
      case 'topic':
        exchange = createTopicExchange();
        break;
      case 'headers':
        exchange = createHeadersExchange();
        break;
    }

    exchanges[exchangeName] = exchange;
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
  sendToQueue: async function (queueName, content, options = { headers: {} }) {
    return this.publish(DEFAULT_EXCHANGE_NAME, queueName, content, options);
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
