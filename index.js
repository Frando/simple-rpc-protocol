const { EventEmitter } = require('events')
const { Duplex } = require('streamx')
const varint = require('varint')
const SMC = require('simple-message-channels')

const { json, binary, uuid } = require('./util')

class Router extends EventEmitter {
  constructor (opts = {}) {
    super()
    this.opts = opts
    this.repo = new CommandRepo()
    this.remotes = {}
  }

  connection (stream, name) {
    const endpoint = new Endpoint({
      repo: this.repo,
      stream,
      name,
      onannounce: msg => this.onannounce(msg, endpoint)
    })
  }

  announce () {
    // Announce to all connected remotes.
    // TODO: This can loop for two connected routers.
    // TODO: Rethink announce format (allow patch).
    const manifest = {
      name: this.opts.name,
      commands: this.repo.manifest()
    }
    for (let remote of Object.values(this.remotes)) {
      remote.endpoint.announce(manifest)
    }
  }

  onannounce (msg, endpoint) {
    const self = this
    const { name, commands } = msg
    this.remotes[name] = {
      name, commands, endpoint
    }

    this.emit('remote', name, this.remotes[name])

    for (let [cmd, opts] of Object.entries(commands)) {
      const scopedName = `@${name} ${cmd}`
      this.repo.add(scopedName, {
        ...opts,
        oncall (args, channel) {
          const remoteChannel = self.call(name, cmd, args)
          pipe(channel.channel, remoteChannel)
        }
      })
    }

    this.announce()
  }

  call (name, cmd, args, cb) {
    if (!this.remotes[name]) return cb(new Error('Remote not found: ' + name))
    return this.remotes[name].endpoint.call(cmd, args, cb)
  }
}

class Endpoint {
  constructor (opts) {
    this.stream = opts.stream
    this.name = opts.name
    this.opts = opts
    this.protocol = new CommandProtocol({
      send: this.stream.write.bind(this.stream),
      oncall: this.oncall.bind(this),
      onannounce: this.onannounce.bind(this)
    })
    this.stream.on('data', data => this.protocol.recv(data))

    this.protocol.on('announce', msg => {
      this.remoteManifest = msg
    })

    this.repo = opts.repo || new CommandRepo(opts.commands)
  }

  command (name, oncall) {
    this.repo.add(name, oncall)
  }

  commands (commands) {
    this.repo.batch(commands)
  }

  announce (opts) {
    this.protocol.announce({
      name: this.name,
      commands: this.repo.manifest(),
      ...opts
    })
  }

  call (cmd, args, cb) {
    return this.protocol.call(cmd, args, cb)
  }

  oncall (cmd, args, channel) {
    this.repo.oncall(cmd, args, channel)
  }

  onannounce (msg) {
    this.remoteManifest = msg
    if (this.opts.onannounce) this.opts.onannounce(msg)
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
        { context: this, onmessage: onlog, encoding: json },
        { context: this, onmessage: onfin, encoding: json },
        { context: this, onmessage: onclose, encoding: json }
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
      channel.once('reply', msg => cb(null, msg, channel.io))
      channel.once('remote-error', err => cb(err, null, channel.io))
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
    const id = uuid()
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

function onannounce (ch, message, self) {
  if (ch === 0 && self.handlers.onannounce) self.handlers.onannounce(message, self)
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

function onlog (ch, message, self) {
  if (self.remote[ch]) self.remote[ch].onlog(message)
}

function onfin (ch, message, self) {
  if (self.remote[ch]) self.remote[ch].onfin(message)
}

function onclose (ch, message, self) {
  if (self.remote[ch]) self.remote[ch].onclose(message)
}

function onmessage (ch, type, message, self) {
  if (type !== 15) return
  const id = varint.decode(message)
  const m = message.slice(varint.decode.bytes)
  if (self.handlers.onextension) self.handlers.onextension(ch, id, m)
}

class CommandRepo {
  constructor (commands) {
    this.commands = commands || {}
  }

  batch (commands) {
    Object.entries(commands).forEach((name, opts) => this.add(name, opts))
  }

  add (name, opts) {
    if (typeof opts === 'function') opts = { oncall: opts }
    opts.name = name
    this.commands[name] = opts
  }

  has (name) {
    return !!this.commands[name]
  }

  oncall (cmd, args, channel) {
    if (!this.commands[cmd]) return this.error('Command not found: ' + cmd)
    this.commands[cmd].oncall(args, channel.io)
  }

  error (err, contxt) {
    console.error(err)
  }

  manifest () {
    let manifest = {}
    for (let [name, opts] of Object.entries(this.commands)) {
      manifest[name] = {
        mode: opts.mode,
        help: opts.help,
        args: opts.args
      }
    }
    return manifest
  }
}

class DataChannel extends Duplex {
  constructor (channel) {
    super()
    this.channel = channel
    this.on('finish', () => channel.fin())
    this.once('error', err => this.error(err))
    channel.on('reply', msg => this.emit('reply', msg))
  }

  get id () {
    return this.channel.id
  }

  get log () {
    return this.channel.log
  }

  reply (msg) {
    this.channel.reply(msg)
  }

  error (err) {
    this.channel.close(err)
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
    this.io = new DataChannel(this)
    this.log = new Duplex()
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

  log (msg) {
    if (msg instanceof Error) msg = { error: msg.toString() }
    this._send(5, msg)
  }

  fin () {
    const msg = {}
    this._send(6, msg)
    this._localClosed = true
    if (this._remoteClosed) this.destroy()
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
    this.io.push(msg)
  }

  onlog (msg) {
    this.log.push(msg)
  }

  onfin () {
    this.io.push(null)
    this._remoteClosed = true
    this.emit('remote-close')
    if (this._localClosed) this.destroy()
  }

  onclose (error) {
    if (error) this.remoteError = error
    this.destroy(error)
  }

  destroy (err) {
    this.closed = true
    this.io.destroy()
    this.log.destroy()
    if (typeof err === 'string') err = new Error(err)
    if (err && this.remoteError) this.emit('remote-error', err)
    else if (err) this.emit('error', err)
    this.emit('close', err)
  }
}

function pipe (a, b) {
  a.io.pipe(b.io).pipe(a.io)
  a.log.pipe(b.log).pipe(a.log)
  a.once('close', err => b.close(err))
  b.once('close', err => a.close(err))
  a.on('reply', msg => b.reply(msg))
  b.on('reply', msg => a.reply(msg))
  // b.on('remote-error', err => b.emit('remote-error', err))
}

module.exports = { CommandChannel, CommandRepo, CommandProtocol, Endpoint, Router }
