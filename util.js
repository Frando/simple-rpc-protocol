const PAYLOAD = Symbol('payload')

const binary = {
  decode (msg) { return msg },
  encode (msg, buf, offset) {
    msg.copy(buf, offset)
  },
  encodingLength (msg) { return msg.length }
}

const json = {
  decode (message) {
    return JSON.parse(message.toString())
  },
  encodingLength (message) {
    const encoded = this._encode(message)
    return encoded.length
  },
  encode (message, buf, offset) {
    const encoded = this._encode(message)
    encoded.copy(buf, offset)
  },
  _encode (message) {
    if (typeof message === 'object') {
      if (!message[PAYLOAD]) message[PAYLOAD] = Buffer.from(JSON.stringify(message))
      return message[PAYLOAD]
    }
    return Buffer.from(JSON.stringify(message))
  }
}

module.exports = { json, binary }
