const EventEmitter = require('events');

const DEFAULT_EXCHANGE_NAME = '';
const queueNameSymbol = Symbol('queueNameSymbol');
const deadLetterHandler = Symbol('deadLetterHandler');

const createQueue = (options, channel) => {
  let messages = [];
  let subscriber = null;
  const getTtl = () => {
    if (
        options &&
        options.arguments &&
        !isNaN(Number(options.arguments['x-message-ttl']))
    ) {
      return Number(options.arguments['x-message-ttl']);
    }
    return undefined;
  };
  const clearExpiration = msg => {
    if (msg && msg.fields) {
      clearTimeout(msg[deadLetterHandler]);
      delete msg[deadLetterHandler];
    }
    return msg;
  };
  const setExpiration = msg => {
    const msgTtl = Number(msg.properties.expiration);
    const queueTtl = getTtl();
    const ttl = msgTtl >= 0 && queueTtl >= 0 ? Math.min(msgTtl, queueTtl)
        : msgTtl >= 0 ? msgTtl
            : queueTtl >= 0 ? queueTtl
                : undefined;

    if (ttl >= 0) {
      msg[deadLetterHandler] = setTimeout(() => {
        const index = messages.indexOf(msg);
        messages.splice(index, 1);
        deadLetterProceed(channel, clearExpiration(msg), 'expired', ttl === msgTtl);
      }, ttl);
    }
    return msg;
  };

  return {
    add: async item => {
      if (subscriber) {
        await subscriber(item);
      } else {
        messages.push(setExpiration(item));
      }
    },
    get: () => clearExpiration(messages.shift()) || false,
    addConsumer: consumer => {
      messages.forEach(item => consumer(clearExpiration(item)));
      messages = [];
      subscriber = consumer;
    },
    stopConsume: () => (subscriber = null),
    getMessageCount: () => messages.length,
    purge: () => (messages = []),
    getDeadLetterInfo: () => {
      if (options && options.arguments) {
        const {
          'x-dead-letter-exchange': exchange,
          'x-dead-letter-routing-key': routingKey,
        } = options.arguments;
        return { exchange, routingKey };
      }
      return {};
    },
  };
};

const createFanoutExchange = options => {
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
      return bindings.map(b => b.targetQueue);
    },
    getOptions: () => options,
  };
};

const createDirectExchange = options => {
  const bindings = [];
  return {
    bindQueue: (queueName, pattern, options) => {
      bindings.push({
        targetQueue: queueName,
        options,
        pattern
      });
    },
    getTargetQueues: (routingKey, options = {}) =>
      bindings.filter(b => b.pattern === routingKey).map(b => b.targetQueue),
    getOptions: () => options,
  };
};

const createTopicExchange = options => {
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
    return new RegExp(strForRegexp);
  }
  return {
    bindQueue: (queueName, pattern, options) => {
      bindings.push({
        targetQueue: queueName,
        options,
        pattern,
        patternRegexp: maskToRegexp(pattern)
      });
    },
    getTargetQueues: (routingKey, options = {}) =>
      bindings.filter(b => b.patternRegexp.test(routingKey)).map(b => b.targetQueue),
    getOptions: () => options,
  };
};

const createHeadersExchange = options => {
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
      const isMatching = (binding, headers = {}) =>
        Object.keys(binding.options).every(key => binding.options[key] === headers[key]);
      return bindings.filter(b => isMatching(b, options.headers)).map(b => b.targetQueue);
    },
    getOptions: () => options,
  };
};

const queues = {};
const exchanges = {
  [DEFAULT_EXCHANGE_NAME]: createDirectExchange({}),
};

const deadLetterProceed = (channel, message, reason, perMessageTtl = false) => {
  const queueName = message[queueNameSymbol];
  const {
    exchange: dlExchange,
    routingKey: dlRoutingKey = message.fields.routingKey,
  } = queues[queueName].getDeadLetterInfo();
  if (dlExchange === undefined) {
    return;
  }
  const msg = { ...message };

  if (!msg.properties.headers) {
    msg.properties.headers = {};
  }
  if (!msg.properties.headers['x-death']) {
    msg.properties.headers['x-death'] = [];
  }
  if (!msg.properties.headers['x-first-death-reason']) {
    msg.properties.headers['x-first-death-reason'] = reason;
    msg.properties.headers['x-first-death-queue'] = queueName;
    msg.properties.headers['x-first-death-exchange'] = msg.fields.exchange;
  }

  const dlEntry = {
    count: msg.properties.headers['x-death'].filter(
        v => v.queue === queueName && v.reason === reason
    ).length + 1,
    exchange: msg.fields.exchange,
    queue: queueName,
    reason,
    'routing-keys': [msg.fields.routingKey],
    time: { '!': 'timestamp', value: Date.now() / 1000 },
  };

  if (reason === 'expired' && perMessageTtl) {
    dlEntry['original-expiration'] = msg.properties.expiration;
    delete msg.properties.expiration;
  }

  msg.properties.headers['x-death'].unshift(dlEntry);

  channel.publish(dlExchange, dlRoutingKey, msg.content, msg.properties, () => {});
};

const createChannel = async () => ({
  ...EventEmitter.prototype,
  close: () => {},
  assertQueue: async function (queueName, options) {
    if (!queueName) {
      queueName = generateRandomQueueName();
    }
    if (!(queueName in queues)) {
      queues[queueName] = createQueue(options, this);
      const exchange = exchanges[DEFAULT_EXCHANGE_NAME];
      exchange.bindQueue(queueName, queueName);
    }
    return { queue: queueName };
  },
  assertExchange: async (exchangeName, type, options = {}) => {
    let exchange;

    switch(type) {
      case 'fanout':
        exchange = createFanoutExchange(options);
        break;
      case 'direct':
      case 'x-delayed-message':
        exchange = createDirectExchange(options);
        break;
      case 'topic':
        exchange = createTopicExchange(options);
        break;
      case 'headers':
        exchange = createHeadersExchange(options);
        break;
    }

    exchanges[exchangeName] = exchange;
    return { exchange: exchangeName };
  },
  bindQueue: async (queue, sourceExchange, pattern, options = {}) => {
    const exchange = exchanges[sourceExchange];
    exchange.bindQueue(queue, pattern, options);
  },
  publish: function (exchangeName, routingKey, content, options = {}) {
    const exchange = exchanges[exchangeName];
    const queueNames = exchange.getTargetQueues(routingKey, options);
    const { mandatory } = options;
    const message = {
      content,
      fields: {
        exchange: exchangeName,
        routingKey
      },
      properties: options
    };
    // do not send these options to consumer
    delete message.properties.mandatory;

    if (!queueNames.length) {
      const { alternateExchange } = exchange.getOptions();
      if (mandatory) {
        this.emit('return', message);
      } else if (alternateExchange) {
        return this.publish(alternateExchange, routingKey, content, options);
      }
    }

    for(const queueName of queueNames) {
      message[queueNameSymbol] = queueName;
      queues[queueName].add(message);
    }
    return true;
  },
  sendToQueue: function (queueName, content, options = { headers: {} }) {
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
  ack: () => {},
  nack: function (message, allUpTo = false, requeue = true) {
    if (requeue) {
      queues[message[queueNameSymbol]].add(message);
    } else {
      deadLetterProceed(this, message, 'rejected');
    }
  },
  checkQueue: async queueName => ({
    queue: queueName,
    messageCount: queues[queueName].getMessageCount()
  }),
  checkExchange: async exchangeName => ({
    exchange: exchangeName,
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

  // bind new context to all methods
  for (const key in basic) {
    if (typeof basic[key] === 'function') {
      basic[key] = basic[key].bind(basic);
    }
  }
  return {
    ...basic,
    publish: (exchange, routingKey, content, options, cb) =>
        handler(basic.publish, exchange, routingKey, content, options, cb),
    sendToQueue: (queue, content, options, cb) =>
        handler(basic.sendToQueue, queue, content, options, cb),
    waitForConfirms: async () => Promise.all(pendingPublishes.slice()),
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
  }),
}

module.exports = {
  connect: async () => ({
    ...EventEmitter.prototype,
    createChannel,
    createConfirmChannel,
    isConnected: true,
    close: function () {
      this.emit('close');
    }
  }),
  credentials,
};
