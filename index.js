const { EventEmitter } = require('events')
const { randomBytes } = require('crypto')
const { Duplex, Readable } = require('streamx')
const varint = require('varint')
const SMC = require('simple-message-channels')

const { json, binary } = require('./util')

class Router extends EventEmitter {
  constructor () {
    super()
    this.repo = new CommandRepo()
    this.remotes = {}
  }

  connection (stream, name) {
    const protocol = new CommandProtocol({
      send: (msg) => stream.write(msg),
      oncall: (cmd, args, channel) => {
        this.oncall(cmd, args, channel, protocol)
      }
    })
    stream.on('data', data => protocol.recv(data))
    protocol.on('announce', (msg) => {
      this.onannounce(msg, protocol)
    })
  }

  onannounce (msg, protocol) {
    const { name, commands } = msg
    this.remotes[name] = {
      name, commands, protocol
    }
    this.emit('remote', name, this.remotes[name])
  }

  call (name, cmd, args, cb) {
    if (!this.remotes[name]) return cb(new Error('Remote not found: ' + name))
    this.remotes[name].call(cmd, args, cb)
  }

  oncall (cmd, args, channel, protocol) {
    if (this.repo.has(cmd)) return this.repo.call(cmd, args, channel)

    const { name, command } = parseScopedCommand(cmd)
    const other = this.remotes[name]
    if (!other) return channel.error(new Error('Command not found'))
    const otherChannel = other.protocol.call(command, args)
    const proxy = otherChannel.proxy
    proxy.pipe(channel.proxy).pipe(proxy)
    otherChannel.on('remote-error', msg => channel.error(msg))
    otherChannel.on('reply', msg => channel.reply(msg))
  }
}

class Endpoint {
  constructor (opts) {
    this.stream = opts.stream
    this.protocol = new CommandProtocol({
      send: this.stream.write.bind(this.stream),
      oncall: this.oncall.bind(this)
    })
    this.stream.on('data', data => this.protocol.recv(data))

    this.repo = opts.repo || new CommandRepo()
  }

  command (cmd, cb) {
    this.repo.add(cmd, cb)
  }

  announce (message) {
    this.protocol.announce(message)
  }

  oncall (cmd, args, channel) {
    this.repo.oncall(cmd, args, channel)
  }

  call (cmd, args, cb) {
    this.protocol.call(cmd, args, cb)
  }
}

class CommandProtocol extends EventEmitter {
  constructor (handlers) {
    super()
    this.handlers = handlers
    this.smc = new SMC({
      onmessage,
      context: this,
      types: [
        { context: this, onmessage: onannounce, encoding: json },
        { context: this, onmessage: onopen, encoding: json },
        { context: this, onmessage: oncommand, encoding: json },
        { context: this, onmessage: onreply, encoding: json },
        { context: this, onmessage: ondata, encoding: binary },
        { context: this, onmessage: onerror, encoding: json },
        { context: this, onmessage: onfin, encoding: json }
      ]
    })

    this.local = [null]
    this.remote = []
    this.channels = {}
  }

  announce (message) {
    this.send(0, 0, message)
  }

  call (cmd, args, cb) {
    const channel = this.createLocalChannel()
    if (cb) {
      channel.once('reply', msg => cb(null, msg, channel.proxy))
      channel.once('remote-error', err => cb(err, null, channel.proxy))
    }
    channel.open()
    channel.command(cmd, args)
    return channel
  }

  attachLocal (channel) {
    let ch = this.local.indexOf(null)
    if (ch < 1) {
      this.local.push(null)
      ch = this.local.length - 1
    }
    this.local[ch] = channel
    channel.localId = ch
  }

  attachRemote (channel, ch) {
    this.remote[ch] = channel
    channel.remoteId = ch
    if (!channel.localId) this.attachLocal(channel)
  }

  createLocalChannel () {
    const id = randomBytes(16).toString('hex')
    const channel = this.createChannel(id)
    this.attachLocal(channel)
    return channel
  }

  createChannel (id) {
    if (!this.channels[id]) {
      this.channels[id] = new CommandChannel(id, {
        oncall: this.handlers.oncall,
        send: this.send.bind(this)
      })
    }
    return this.channels[id]
  }

  send (ch, type, msg) {
    const payload = this.smc.send(ch, type, msg)
    this.handlers.send(payload)
  }

  recv (buf) {
    this.smc.recv(buf)
  }
}

function onopen (ch, message, self) {
  const { id } = message
  let channel
  channel = self.createChannel(id)
  self.attachRemote(channel, ch)
  channel.onopen()
}

function oncommand (ch, message, self) {
  if (self.remote[ch]) self.remote[ch].oncommand(message)
}

function onreply (ch, message, self) {
  if (self.remote[ch]) self.remote[ch].onreply(message)
}

function ondata (ch, message, self) {
  if (self.remote[ch]) self.remote[ch].ondata(message)
}

function onerror (ch, message, self) {
  if (self.remote[ch]) self.remote[ch].onerror(message)
}

function onfin (ch, message, self) {
  if (self.remote[ch]) self.remote[ch].onfin(message)
}

function onannounce (ch, message, self) {
  if (ch === 0) self.emit('announce', message, self)
}

function onmessage (ch, type, message, self) {
  if (type !== 15) return
  const id = varint.decode(message)
  const m = message.slice(varint.decode.bytes)
  if (self.handlers.onextension) self.handlers.onextension(ch, id, m)
}

class CommandRepo {
  constructor () {
    this.commands = {}
    this.remoteCommands = {}
  }

  add (command, opts) {
    if (typeof opts === 'function') opts = { oncall: opts }
    opts.command = command
    this.commands[command] = opts
  }

  has (command) {
    return !!this.commands[command]
  }

  oncall (cmd, args, channel) {
    if (!this.commands[cmd]) return this.error('Command not found: ' + cmd)
    this.commands[cmd].oncall(args, channel.proxy)
  }

  error (err, contxt) {
    console.error(err)
  }
}

class ChannelProxy extends Duplex {
  constructor (channel) {
    super()
    this.channel = channel
    this.on('finish', () => channel.fin())
    channel.on('reply', msg => this.emit('reply', msg))
  }

  reply (msg) {
    this.channel.reply(msg)
  }

  error (err) {
    this.channel.error(err)
  }

  _write (data, cb) {
    this.channel.data(data)
    cb()
  }

  _read (cb) {
    cb(null)
  }
}

class CommandChannel extends EventEmitter {
  constructor (id, handlers) {
    super()
    this.id = id
    // should be { oncall, send }
    this.handlers = handlers
    this.proxy = new ChannelProxy(this)
  }

  open () {
    if (this._opened) return
    this._opened = true
    const msg = { id: this.id }
    this._send(1, msg)
  }

  command (cmd, args) {
    if (this._commandSent) return this.destroy('Cannot send more than one command per channel')
    this._commandSent = true
    const msg = { cmd, args }
    this._send(2, msg)
  }

  reply (msg) {
    if (!this._commandReceived) return this.destroy('Cannot reply before receiving a command')
    this._send(3, msg)
  }

  data (msg) {
    this._send(4, msg)
  }

  error (msg) {
    if (msg instanceof Error) msg = msg.toString()
    if (typeof msg === 'object' && msg.toString) msg = msg.toString()
    this._send(5, msg)
  }

  fin () {
    const msg = {}
    this._send(6, msg)
    this.destroy()
  }

  _send (type, msg) {
    let id = this.localId
    this.handlers.send(id, type, msg)
  }

  onopen () {
    if (!this._opened) this.open()
  }

  oncommand (msg) {
    if (this._commandReceived) return this.destroy('Cannot receive more than on command per channel')
    this._commandReceived = true
    const { cmd, args } = msg
    this.handlers.oncall(cmd, args, this)
  }

  onreply (msg) {
    if (!this._commandSent) this.destroy('Cannot receive replies before sending a command')
    this.emit('reply', msg)
  }

  ondata (msg) {
    if (!this._commandSent) this.destroy('Cannot receive data before sending a command')
    this.proxy.push(msg)
  }

  onerror (msg) {
    this.emit('remote-error', msg)
  }

  createLogStream () {
    const stream = new Readable({ read () {} })
    this.on('remote-error', msg => stream.push(msg))
    return stream
  }

  onfin () {
    this.proxy.push(null)
    this.emit('remote-close')
    this.destroy()
  }

  destroy (err) {
    this.closed = true
    if (err) this.emit('error', err)
  }
}

module.exports = { CommandChannel, CommandRepo, CommandProtocol, Router, Endpoint }

function parseScopedCommand (cmd) {
  if (!cmd.startsWith('@')) return false
  let [name, ...other] = cmd.trim().substring(1).split(/ (.+)/)
  const command = other.join(' ').trim()
  if (!command.length) return false
  return { name, command }
}
