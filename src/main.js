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
    return true;
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
    return true;
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

const createConfirmChannel = async () => {
  const basic = await createChannel();

  // for waitForConfirms
  const pendingPublishes = [];

  const addPromise = async () => new Promise(outerResolve => {
    let resolver, rejector;
    const promise = new Promise((resolve, reject) => {
      resolver = resolve;
      rejector = reject;
    });
    pendingPublishes.push(promise);
    // setImmediate to make sure promise has finished his assignment task
    setImmediate(() => {
      outerResolve({ promise, resolver, rejector });
    });
  });

  const handler = (func, ...args) => {
    const cb = args[args.length - 1];
    const params = args.slice(0, -1);
    const promiseStaff = { promise: undefined, resolver: undefined, rejector: undefined };
    addPromise()
        // get promise resolver/rejector and call main func
        .then(
            ret => {
              Object.assign(promiseStaff, ret);
              func(...params); // main call
            })
        // resolve or reject, remove promise from array, callback call
        .then(
            ret => {
              promiseStaff.resolver && promiseStaff.resolver();
              const i = pendingPublishes.indexOf(promiseStaff.promise);
              pendingPublishes.splice(i, 1);
              process.nextTick(cb, null, ret);
            },
            rej => {
              promiseStaff.rejector && promiseStaff.rejector();
              const i = pendingPublishes.indexOf(promiseStaff.promise);
              pendingPublishes.splice(i, 1);
              process.nextTick(cb, rej);
            },
        );
    // to mimic stream.write behaviour
    return true;
  }

  return {
    ...basic,
    publish: (exchange, routingKey, content, options, cb) =>
        handler(basic.publish, exchange, routingKey, content, options, cb),
    sendToQueue: (queue, content, options, cb) =>
        handler(basic.sendToQueue, queue, content, options, cb),
    waitForConfirms: async () => Promise.all(pendingPublishes.slice())
  };
};

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
    createConfirmChannel,
    isConnected: true,
    close: function () {
      this.emit('close')
    }
  }),
  credentials
};
