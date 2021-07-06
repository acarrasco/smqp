"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.Queue = Queue;
exports.Consumer = Consumer;

var _shared = require("./shared");

var _Message = require("./Message");

const prv = Symbol('private');
const publicMethods = ['ack', 'ackAll', 'assertConsumer', 'cancel', 'close', 'consume', 'delete', 'dequeueMessage', 'dismiss', 'get', 'getState', 'nack', 'nackAll', 'off', 'on', 'peek', 'purge', 'queueMessage', 'recover', 'reject', 'stop', 'unbindConsumer' //'messageCount',
];

class _Queue {
  constructor(name, options = {}, eventEmitter) {
    this[prv] = {
      eventEmitter,
      consumers: [],
      pendingMessageCount: 0,
      onMessageConsumed: _Queue.prototype.onMessageConsumed.bind(this)
    };
    this.name = name || `smq.qname-${(0, _shared.generateId)()}`;
    this.options = {
      autoDelete: true,
      ...options
    };

    if (this.options.maxLength === undefined) {
      this.options.maxLength = Infinity;
    }

    this.messages = [];
    publicMethods.forEach(fn => {
      this[fn] = _Queue.prototype[fn].bind(this);
    });
  }

  get messageCount() {
    return this.messages.length;
  }

  get consumers() {
    return this[prv].consumers.slice();
  }

  get consumerCount() {
    return this[prv].consumers.length;
  }

  get stopped() {
    return this[prv].stopped;
  }

  get exclusive() {
    return this[prv].exclusivelyConsumed;
  }

  get maxLength() {
    return this.options.maxLength;
  }

  set maxLength(maxLength) {
    this.options.maxLength = maxLength;
  }

  get capacity() {
    return this.getCapacity();
  }

  getCapacity() {
    return this.options.maxLength - this.messages.length;
  }

  ack(message, allUpTo) {
    this[prv].onMessageConsumed(message, 'ack', allUpTo);
  }

  ackAll() {
    this.getPendingMessages().forEach(msg => msg.ack(false));
  }

  assertConsumer(onMessage, consumeOptions = {}, owner) {
    const consumers = this[prv].consumers;

    if (!consumers.length) {
      return this.consume(onMessage, consumeOptions, owner);
    }

    for (const consumer of consumers) {
      if (consumer.onMessage !== onMessage) continue;

      if (consumeOptions.consumerTag && consumeOptions.consumerTag !== consumer.consumerTag) {
        continue;
      } else if ('exclusive' in consumeOptions && consumeOptions.exclusive !== consumer.options.exclusive) {
        continue;
      }

      return consumer;
    }

    return this.consume(onMessage, consumeOptions, owner);
  }

  cancel(consumerTag) {
    const consumers = this[prv].consumers;
    const idx = consumers.findIndex(c => c.consumerTag === consumerTag);
    if (idx === -1) return;
    return this.unbindConsumer(consumers[idx]);
  }

  close() {
    this[prv].consumers.splice(0).forEach(consumer => consumer.cancel());
    this[prv].exclusivelyConsumed = false;
  }

  consume(onMessage, consumeOptions = {}, owner) {
    const consumers = this[prv].consumers;

    if (this[prv].exclusivelyConsumed && consumers.length) {
      throw new Error(`Queue ${this.name} is exclusively consumed by ${consumers[0].consumerTag}`);
    } else if (consumeOptions.exclusive && consumers.length) {
      throw new Error(`Queue ${this.name} already has consumers and cannot be exclusively consumed`);
    }

    const consumerEmitter = {
      emit: (eventName, ...args) => {
        if (eventName === 'consumer.cancel') {
          this.unbindConsumer(consumer);
        }

        this.emit(eventName, ...args);
      },
      on: this.on
    };
    const consumer = Consumer(this, onMessage, consumeOptions, owner, consumerEmitter);
    consumers.push(consumer);
    consumers.sort(_shared.sortByPriority);
    this[prv].exclusivelyConsumed = consumer.options.exclusive;
    this.emit('consume', consumer);
    const pendingMessages = this.consumeMessages(consumer.capacity, consumer.options);
    if (pendingMessages.length) consumer.push(pendingMessages);
    return consumer;
  }

  delete({
    ifUnused,
    ifEmpty
  } = {}) {
    const consumers = this[prv].consumers;
    if (ifUnused && consumers.length) return;
    if (ifEmpty && this.messages.length) return;
    const messageCount = this.messages.length;
    this.stop();
    const deleteConsumers = consumers.splice(0);
    deleteConsumers.forEach(consumer => {
      consumer.cancel();
    });
    this.messages.splice(0);
    this.emit('delete', this);
    return {
      messageCount
    };
  }

  dequeueMessage(message) {
    if (message.pending) return this.nack(message, false, false);
    message.consume({});
    this.nack(message, false, false);
  }

  dismiss(onMessage) {
    const consumer = this[prv].consumers.find(c => c.onMessage === onMessage);
    if (!consumer) return;
    this.unbindConsumer(consumer);
  }

  get({
    noAck,
    consumerTag
  } = {}) {
    const message = this.consumeMessages(1, {
      noAck,
      consumerTag
    })[0];
    if (!message) return;
    if (noAck) this.dequeue(message);
    return message;
  }

  getState() {
    return {
      name: this.name,
      options: { ...this.options
      },
      ...(this.messages.length ? {
        messages: JSON.parse(JSON.stringify(this.messages))
      } : undefined)
    };
  }

  nack(message, allUpTo, requeue = true) {
    this[prv].onMessageConsumed(message, 'nack', allUpTo, requeue);
  }

  nackAll(requeue = true) {
    this.getPendingMessages().forEach(msg => msg.nack(false, requeue));
  }

  off(eventName, handler) {
    if (!this[prv].eventEmitter || !this[prv].eventEmitter.off) return;
    const pattern = `queue.${eventName}`;
    return this[prv].eventEmitter.off(pattern, handler);
  }

  on(eventName, handler) {
    if (!this[prv].eventEmitter || !this[prv].eventEmitter.on) return;
    const pattern = `queue.${eventName}`;
    return this[prv].eventEmitter.on(pattern, handler);
  }

  peek(ignoreDelivered) {
    const message = this.messages[0];
    if (!message) return;
    if (!ignoreDelivered) return message;
    if (!message.pending) return message;

    for (let idx = 1; idx < this.messages.length; idx++) {
      if (!this.messages[idx].pending) {
        return this.messages[idx];
      }
    }
  }

  purge() {
    const toDelete = this.messages.filter(({
      pending
    }) => !pending);
    this[prv].pendingMessageCount = 0;
    toDelete.forEach(message => this.dequeue(message));
    if (!this.messages.length) this.emit('depleted', this);
    return toDelete.length;
  }

  queueMessage(fields, content, properties, onMessageQueued) {
    if (this[prv].stopped) return;
    const messageProperties = { ...properties
    };
    const messageTtl = this.options.messageTtl;

    if (messageTtl) {
      messageProperties.expiration = messageProperties.expiration || messageTtl;
    }

    const message = (0, _Message.Message)(fields, content, messageProperties, this[prv].onMessageConsumed);
    const capacity = this.getCapacity();
    this.messages.push(message);
    this[prv].pendingMessageCount++;
    let discarded;

    const evictOld = () => {
      const evict = this.get();
      if (!evict) return;
      evict.nack(false, false);
      return evict === message;
    };

    switch (capacity) {
      case 0:
        discarded = evictOld();

      case 1:
        this.emit('saturated');
    }

    if (onMessageQueued) onMessageQueued(message);
    this.emit('message', message);
    return discarded ? 0 : this.consumeNext();
  }

  recover(state) {
    this[prv].stopped = false;
    const consumers = this[prv].consumers;

    if (!state) {
      consumers.slice().forEach(c => c.recover());
      return this.consumeNext();
    }

    this.name = state.name;
    this.messages.splice(0);
    let continueConsume;

    if (consumers.length) {
      consumers.forEach(c => c.nackAll(false));
      continueConsume = true;
    }

    if (!state.messages) return this;
    state.messages.forEach(({
      fields,
      content,
      properties
    }) => {
      if (properties.persistent === false) return;
      const msg = (0, _Message.Message)({ ...fields,
        redelivered: true
      }, content, properties, this[prv].onMessageConsumed);
      this.messages.push(msg);
    });
    this[prv].pendingMessageCount = this.messages.length;
    consumers.forEach(c => c.recover());

    if (continueConsume) {
      this.consumeNext();
    }

    return this;
  }

  reject(message, requeue = true) {
    this[prv].onMessageConsumed(message, 'nack', false, requeue);
  }

  stop() {
    this[prv].stopped = true;
    this.consumers.forEach(consumer => consumer.stop());
  }

  unbindConsumer(consumer) {
    const idx = this[prv].consumers.indexOf(consumer);
    if (idx === -1) return;
    this[prv].consumers.splice(idx, 1);

    if (this[prv].exclusivelyConsumed) {
      this[prv].exclusivelyConsumed = false;
    }

    consumer.stop();

    if (this.options.autoDelete && !this[prv].consumers.length) {
      return this.delete();
    }

    consumer.nackAll(true);
  }
  /* private methods */


  dequeue(message) {
    const msgIdx = this.messages.indexOf(message);
    if (msgIdx === -1) return;
    this.messages.splice(msgIdx, 1);
    return true;
  }

  consumeNext() {
    const consumers = this[prv].consumers;
    if (this[prv].stopped) return;
    if (!this[prv].pendingMessageCount) return;
    if (!consumers.length) return;
    const readyConsumers = consumers.filter(consumer => consumer.ready);
    if (!readyConsumers.length) return 0;
    let consumed = 0;

    for (const consumer of readyConsumers) {
      const msgs = this.consumeMessages(consumer.capacity, consumer.options);
      if (!msgs.length) return consumed;
      consumer.push(msgs);
      consumed += msgs.length;
    }

    return consumed;
  }

  consumeMessages(n, consumeOptions) {
    if (this[prv].stopped || !this[prv].pendingMessageCount || !n) return [];
    const now = Date.now();
    const msgs = [];
    const evict = [];

    for (const message of this.messages) {
      if (message.pending) continue;

      if (message.ttl && message.ttl < now) {
        evict.push(message);
        continue;
      }

      message.consume(consumeOptions);
      this[prv].pendingMessageCount--;
      msgs.push(message);
      if (! --n) break;
    }

    for (const expired of evict) this.nack(expired, false, false);

    return msgs;
  }

  onMessageConsumed(message, operation, allUpTo, requeue) {
    if (this[prv].stopped) return;
    const pending = allUpTo && this.getPendingMessages(message);
    const {
      properties
    } = message;
    let deadLetter = false;

    switch (operation) {
      case 'ack':
        {
          if (!this.dequeue(message)) return;
          break;
        }

      case 'nack':
        if (requeue) {
          this.requeueMessage(message);
          break;
        }

        if (!this.dequeue(message)) return;
        deadLetter = !!this.options.deadLetterExchange;
        break;
    }

    let capacity;
    if (!this.messages.length) this.emit('depleted', this);else if ((capacity = this.getCapacity()) === 1) {
      this.emit('ready', capacity);
    }
    if (!pending || !pending.length) this.consumeNext();

    if (!requeue && properties.confirm) {
      this.emit('message.consumed.' + operation, {
        operation,
        message: { ...message
        }
      });
    }

    if (deadLetter) {
      const deadMessage = (0, _Message.Message)(message.fields, message.content, { ...properties,
        expiration: undefined
      });

      if (this.options.deadLetterRoutingKey) {
        deadMessage.fields.routingKey = this.options.deadLetterRoutingKey;
      }

      this.emit('dead-letter', {
        deadLetterExchange: this.options.deadLetterExchange,
        message: deadMessage
      });
    }

    if (pending && pending.length) {
      pending.forEach(msg => msg[operation](false, requeue));
    }
  }

  getPendingMessages(fromAndNotIncluding) {
    if (!fromAndNotIncluding) return this.messages.filter(msg => msg.pending);
    const msgIdx = this.messages.indexOf(fromAndNotIncluding);
    if (msgIdx === -1) return [];
    return this.messages.slice(0, msgIdx).filter(msg => msg.pending);
  }

  requeueMessage(message) {
    const msgIdx = this.messages.indexOf(message);
    if (msgIdx === -1) return;
    this[prv].pendingMessageCount++;
    this.messages.splice(msgIdx, 1, (0, _Message.Message)({ ...message.fields,
      redelivered: true
    }, message.content, message.properties, this[prv].onMessageConsumed));
  }

  emit(eventName, content) {
    if (!this[prv].eventEmitter || !this[prv].eventEmitter.emit) return;
    const routingKey = `queue.${eventName}`;
    this[prv].eventEmitter.emit(routingKey, content);
  }

}

function Queue(name, options = {}, eventEmitter) {
  return new _Queue(name, options, eventEmitter);
}

function Consumer(queue, onMessage, options = {}, owner, eventEmitter) {
  if (typeof onMessage !== 'function') {
    throw new Error('message callback is required and must be a function');
  }

  options = {
    prefetch: 1,
    priority: 0,
    noAck: false,
    ...options
  };
  if (!options.consumerTag) options.consumerTag = `smq.ctag-${(0, _shared.generateId)()}`;
  let ready = true,
      stopped = false,
      consuming;
  const internalQueue = Queue(`${options.consumerTag}-q`, {
    maxLength: options.prefetch
  }, {
    emit: onInternalQueueEvent
  });
  const consumer = {
    queue,

    get consumerTag() {
      return options.consumerTag;
    },

    get messageCount() {
      return internalQueue.messageCount;
    },

    get capacity() {
      return internalQueue.capacity;
    },

    get queueName() {
      return queue.name;
    },

    get ready() {
      return ready && !stopped;
    },

    get stopped() {
      return stopped;
    },

    options,
    on,
    onMessage,
    ackAll,
    cancel,
    nackAll,
    prefetch,
    push,
    recover,
    stop
  };
  return consumer;

  function push(messages) {
    messages.forEach(message => {
      internalQueue.queueMessage(message.fields, message, message.properties, onInternalMessageQueued);
    });

    if (!consuming) {
      consume();
    }
  }

  function onInternalMessageQueued(msg) {
    const message = msg.content;
    message.consume(options, onConsumed);

    function onConsumed() {
      internalQueue.dequeueMessage(msg);
    }
  }

  function consume() {
    if (stopped) return;
    consuming = true;
    const msg = internalQueue.get();

    if (!msg) {
      consuming = false;
      return;
    }

    msg.consume(options);
    const message = msg.content;
    message.consume(options, onConsumed);
    if (options.noAck) msg.content.ack();
    onMessage(msg.fields.routingKey, msg.content, owner);
    consuming = false;
    return consume();

    function onConsumed() {
      msg.nack(false, false);
    }
  }

  function onInternalQueueEvent(eventName) {
    switch (eventName) {
      case 'queue.saturated':
        {
          ready = false;
          break;
        }

      case 'queue.depleted':
      case 'queue.ready':
        ready = true;
        break;
    }
  }

  function nackAll(requeue) {
    internalQueue.messages.slice().forEach(msg => {
      msg.content.nack(false, requeue);
    });
  }

  function ackAll() {
    internalQueue.messages.slice().forEach(msg => {
      msg.content.ack(false);
    });
  }

  function cancel(requeue = true) {
    emit('cancel', consumer);
    nackAll(requeue);
  }

  function prefetch(value) {
    options.prefetch = internalQueue.maxLength = value;
  }

  function emit(eventName, content) {
    const routingKey = `consumer.${eventName}`;
    eventEmitter.emit(routingKey, content);
  }

  function on(eventName, handler) {
    const pattern = `consumer.${eventName}`;
    return eventEmitter.on(pattern, handler);
  }

  function recover() {
    stopped = false;
  }

  function stop() {
    stopped = true;
  }
}