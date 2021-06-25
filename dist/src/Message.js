"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.Message = Message;

var _shared = require("./shared");

const privateAttributes = new WeakMap();
const publicMethods = ['consume', 'ack', 'nack', 'reject'];

function Message(fields = {}, content, properties = {}, onConsumed) {
  if (!(this instanceof Message)) {
    return new Message(fields, content, properties, onConsumed);
  }

  const internal = {};
  privateAttributes.set(this, internal);
  internal.onConsumed = onConsumed;
  internal.pending = false;
  internal.messageId = properties.messageId || `smq.mid-${(0, _shared.generateId)()}`;
  const messageProperties = { ...properties,
    messageId: internal.messageId
  };
  const timestamp = messageProperties.timestamp = properties.timestamp || Date.now();

  if (properties.expiration) {
    internal.ttl = messageProperties.ttl = timestamp + parseInt(properties.expiration);
  }

  this.fields = { ...fields,
    consumerTag: undefined
  };
  this.content = content;
  this.properties = messageProperties;
  publicMethods.forEach(fn => {
    this[fn] = Message.prototype[fn].bind(this);
  });
}

Object.defineProperty(Message.prototype, 'messageId', {
  get() {
    return privateAttributes.get(this).messageId;
  }

});
Object.defineProperty(Message.prototype, 'ttl', {
  get() {
    return privateAttributes.get(this).ttl;
  }

});
Object.defineProperty(Message.prototype, 'consumerTag', {
  get() {
    return this.fields.consumerTag;
  }

});
Object.defineProperty(Message.prototype, 'pending', {
  get() {
    return privateAttributes.get(this).pending;
  }

});

Message.prototype.consume = function ({
  consumerTag
} = {}, consumedCb) {
  const internal = privateAttributes.get(this);
  internal.pending = true;
  this.fields.consumerTag = consumerTag;
  internal.consumedCallback = consumedCb;
};

Message.prototype.reset = function () {
  privateAttributes.get(this).pending = false;
};

Message.prototype.ack = function (allUpTo) {
  if (privateAttributes.get(this).pending) {
    this.consumed('ack', allUpTo);
  }
};

Message.prototype.nack = function (allUpTo, requeue = true) {
  if (!privateAttributes.get(this).pending) return;
  this.consumed('nack', allUpTo, requeue);
};

Message.prototype.reject = function (requeue = true) {
  this.nack(false, requeue);
};

Message.prototype.consumed = function (operation, allUpTo, requeue) {
  const internal = privateAttributes.get(this);
  [internal.consumedCallback, internal.onConsumed, this.reset.bind(this)].forEach(fn => {
    if (fn) fn(this, operation, allUpTo, requeue);
  });
};