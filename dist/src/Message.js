"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.Message = Message;

var _shared = require("./shared");

const privateAttributes = {
  onConsumed: Symbol('onConsumed'),
  pending: Symbol('pending'),
  messageId: Symbol('messageId'),
  ttl: Symbol('ttl'),
  consumedCallback: Symbol('consumedCallback')
};
const publicMethods = ['consume', 'ack', 'nack', 'reject'];

class _Message {
  constructor(fields = {}, content, properties = {}, onConsumed) {
    this[privateAttributes.onConsumed] = onConsumed;
    this[privateAttributes.pending] = false;
    this[privateAttributes.messageId] = properties.messageId || `smq.mid-${(0, _shared.generateId)()}`;
    const messageProperties = { ...properties,
      messageId: this[privateAttributes.messageId]
    };
    const timestamp = messageProperties.timestamp = properties.timestamp || Date.now();

    if (properties.expiration) {
      this[privateAttributes.ttl] = messageProperties.ttl = timestamp + parseInt(properties.expiration);
    }

    this.fields = { ...fields,
      consumerTag: undefined
    };
    this.content = content;
    this.properties = messageProperties;
    publicMethods.forEach(fn => {
      this[fn] = _Message.prototype[fn].bind(this);
    });
  }

  get messageId() {
    return this[privateAttributes.messageId];
  }

  get ttl() {
    return this[privateAttributes.ttl];
  }

  get consumerTag() {
    return this.fields.consumerTag;
  }

  get pending() {
    return this[privateAttributes.pending];
  }

  consume({
    consumerTag
  } = {}, consumedCb) {
    this[privateAttributes.pending] = true;
    this.fields.consumerTag = consumerTag;
    this[privateAttributes.consumedCallback] = consumedCb;
  }

  reset() {
    this[privateAttributes.pending] = false;
  }

  ack(allUpTo) {
    if (this[privateAttributes.pending]) {
      this.consumed('ack', allUpTo);
    }
  }

  nack(allUpTo, requeue = true) {
    if (!this[privateAttributes.pending]) return;
    this.consumed('nack', allUpTo, requeue);
  }

  reject(requeue = true) {
    this.nack(false, requeue);
  }

  consumed(operation, allUpTo, requeue) {
    [this[privateAttributes.consumedCallback], this[privateAttributes.onConsumed], this.reset.bind(this)].forEach(fn => {
      if (fn) fn(this, operation, allUpTo, requeue);
    });
  }

}

function Message(fields = {}, content, properties = {}, onConsumed) {
  return new _Message(fields, content, properties, onConsumed);
}