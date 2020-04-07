const { EventEmitter } = require('events')
const { Duplex } = require('streamx')
const varint = require('varint')
const SMC = require('simple-message-channels')
// const debug = require('debug')('rpc')
const codecs = require('codecs')

const { json, uuid } = require('./util')

// Singleton counter for debug names
let ID = 0

// Message types
const Typ = {
  Announce: 0,
  Open: 1,
  Command: 2,
  Reply: 3,
  Data: 4,
  Log: 5,
  Fin: 6,
  Close: 7,
  Extension: 15
}

class Router extends EventEmitter {
  constructor (opts = {}) {
    super()
    this.opts = opts
    this.repo = new CommandRepo()
    this.remotes = {}
    this.endpoints = []
  }

  connection (stream, name) {
    const endpoint = new Endpoint({
      repo: this.repo,
      stream,
      name
    })
    endpoint.on('remote-manifest', manifest => this.onannounce(manifest, endpoint))
    this.endpoints.push(endpoint)

    const manifest = {
      name: this.opts.name,
      commands: this.repo.manifest()
    }
    endpoint.announce(manifest)
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

  command (name, oncall) {
    this.repo.add(name, oncall)
  }

  commands (commands) {
    this.repo.add(commands)
  }

  service (name, commands, opts) {
    this.repo.service(name, commands, opts)
  }

  onannounce (msg, endpoint) {
    const self = this
    const { name, commands } = msg
    // debug('received announce from %s (%s commands)', name, Object.keys(commands).length)
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

  call (name, cmd, args, env, cb) {
    if (!this.remotes[name]) return cb(new Error('Remote not found: ' + name))
    return this.remotes[name].endpoint.call(cmd, args, env, cb)
  }
}

class Endpoint extends EventEmitter {
  constructor (opts) {
    super()
    const self = this

    this.stream = opts.stream
    this.name = opts.name
    this.repo = opts.repo || new CommandRepo(opts.commands)
    this.opts = opts

    this.protocol = new CommandProtocol({
      name: this.name,
      send (buf) {
        self.stream.write(buf)
      },
      oncall (cmd, args, channel) {
        self.repo.oncall(cmd, args, channel)
      },
      onannounce (manifest) {
        self.remoteManifest = manifest
        self.emit('remote-manifest', manifest)
      }
    })

    // This pipes the transport stream into the protocol.
    this.stream.on('data', data => this.protocol.recv(data))
    // this.stream.on('close', () => this.close())
  }

  command (name, oncall) {
    this.repo.add(name, oncall)
  }

  commands (commands) {
    this.repo.batch(commands)
  }

  service (name, commands, opts) {
    this.repo.service(name, commands, opts)
  }

  announce (opts) {
    this.protocol.announce({
      name: this.name,
      commands: this.repo.manifest(),
      ...opts
    })
  }

  call (cmd, args, env, cb) {
    return this.protocol.call(cmd, args, env, cb)
  }

  oncall (cmd, args, channel) {
    this.repo.oncall(cmd, args, channel)
  }

  onannounce (msg) {
    this.remoteManifest = msg
    this.emit('remote-manifest', msg)
  }
}

class CommandProtocol extends EventEmitter {
  constructor (handlers) {
    super()
    this._name = handlers.name || ('proto-' + ++ID)
    this.handlers = handlers
    this.smc = new SMC({
      onmessage: this.onmessage.bind(this)
    })

    this.local = [null]
    this.remote = []
    this.channels = {}
  }

  onmessage (ch, typ, message) {
    const self = this

    switch (typ) {
      case Typ.Announce:
        message = json.decode(message)
        // debug('[%s ch%s] recv Announce %o', self._name, ch, message)
        if (ch === 0 && self.handlers.onannounce) self.handlers.onannounce(message, self)
        self.remoteManifest = message
        return

      case Typ.Open:
        message = json.decode(message)
        // debug('[%s ch%s] recv Open %o', self._name, ch, message)
        const { id } = message
        const channel = self.createChannel(id)
        self.attachRemote(channel, ch)
        channel.onopen(message)
        return

      case Typ.Extension:
        const extid = varint.decode(message)
        const m = message.slice(varint.decode.bytes)
        if (self.handlers.onextension) self.handlers.onextension(ch, extid, m)
        return
    }

    if (!self.remote[ch]) return
    self.remote[ch].onmessage(typ, message)
  }

  announce (message) {
    // debug('announce', message)
    message = json.encode(message)
    this.send(0, Typ.Announce, message)
  }

  call (cmd, args, env, cb) {
    if (typeof env === 'function') {
      cb = env
      env = undefined
    }

    const opts = {}
    if (this.remoteManifest && this.remoteManifest.commands[cmd]) {
      opts.encoding = this.remoteManifest.commands[cmd].encoding
    }

    // TODO: Error if command does not exist?

    const channel = this.createLocalChannel(opts)
    if (cb) {
      channel.once('reply', msg => cb(null, msg, channel.io))
      channel.once('remote-error', err => cb(err, null, channel.io))
    }
    channel.open(env)
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

  createLocalChannel (opts) {
    const id = uuid()
    const channel = this.createChannel(id, opts)
    this.attachLocal(channel)
    return channel
  }

  createChannel (id, opts = {}) {
    if (!this.channels[id]) {
      this.channels[id] = new CommandChannel(id, {
        name: this._name,
        oncall: this.handlers.oncall,
        send: this.send.bind(this)
      }, opts)
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

class CommandRepo {
  constructor (commands) {
    this.commands = commands || {}
  }

  service (name, commands, defaultOpts = {}) {
    for (let [cmd, opts] of Object.entries(commands)) {
      const scopedName = `@${name} ${cmd}`
      if (typeof opts === 'function') opts = { oncall: opts }
      opts = { ...defaultOpts, ...opts }
      this.add(scopedName, opts)
    }
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
    const command = this.commands[cmd]
    if (command.encoding) {
      channel.setDataEncoding(command.encoding)
    }
    if (command.onopen) {
      command.onopen(channel.env, channel, (err) => {
        if (err) return channel.destroy(err)
        else invoke()
      })
    } else {
      invoke()
    }
    function invoke () {
      command.oncall(args, channel.io)
    }
  }

  manifest () {
    let manifest = {}
    for (let [name, opts] of Object.entries(this.commands)) {
      manifest[name] = {
        mode: opts.mode,
        encoding: opts.encoding,
        help: opts.help,
        title: opts.help,
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

  get env () {
    return this.channel.env
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
  constructor (id, handlers, opts = {}) {
    super()
    this.id = id
    // should be { oncall, send }
    this.handlers = handlers
    this.io = new DataChannel(this)
    this.log = new Duplex()

    this.channelEncoding = codecs(opts.encoding || 'binary')
    this.mode = opts.mode || 'any'
    this.env = {}
  }

  setDataEncoding (encoding) {
    this.channelEncoding = codecs(encoding)
  }

  onmessage (typ, message) {
    if (typ === Typ.Data) {
      message = this.channelEncoding.decode(message)
    } else if (message.length) {
      message = json.decode(message)
    } else {
      message = undefined
    }
    switch (typ) {
      case Typ.Open: return this.onopen(message)
      case Typ.Command: return this.oncommand(message)
      case Typ.Reply: return this.onreply(message)
      case Typ.Data: return this.ondata(message)
      case Typ.Log: return this.onlog(message)
      case Typ.Fin: return this.onfin(message)
      case Typ.Close: return this.onclose(message)
    }
  }

  open (env) {
    if (this._opened) return
    this._opened = true
    const msg = { id: this.id, env }
    this._send(Typ.Open, msg)
  }

  command (cmd, args) {
    // debug('[%s ch%s] send Command %s %o', this.handlers.name, this.localId, cmd, args)
    if (this._commandSent) return this.destroy(new Error('Cannot send more than one command per channel'))
    this._commandSent = true
    const msg = { cmd, args }
    this._send(Typ.Command, msg)
  }

  reply (msg) {
    if (!this._commandReceived) return this.destroy('Cannot reply before receiving a command')
    if (!(this.mode === 'any' || this.mode === 'async')) return this.destroy('Cannot reply in streaming mode')
    this._send(Typ.Reply, msg)
  }

  data (msg) {
    if (!(this.mode === 'any' || this.mode === 'streaming')) return this.destroy('Cannot use channel in non-stream mode')
    this._send(Typ.Data, msg)
  }

  log (msg) {
    if (msg instanceof Error) msg = { error: msg.toString() }
    this._send(Typ.Log, msg)
  }

  close (msg) {
    let err
    if (msg instanceof Error) {
      msg = { error: msg.toString() }
      err = msg
    }
    this._send(Typ.Close, msg)
    this._localClosed = true
    if (this._remoteClosed) this.destroy(err)
  }

  fin () {
    this._send(Typ.Fin)
    this._localClosed = true
    if (this._remoteClosed) this.destroy()
  }

  _send (typ, message) {
    if (typ === Typ.Data) {
      message = this.channelEncoding.encode(message)
    } else if (message !== undefined) {
      message = json.encode(message)
    } else {
      message = Buffer.alloc(0)
    }
    let id = this.localId
    this.handlers.send(id, typ, message)
  }

  onopen (message) {
    if (message.env && typeof message.env === 'object') {
      this.env = message.env
    }
    if (!this._opened) this.open()
  }

  oncommand (msg) {
    if (this._commandReceived) return this.destroy('Cannot receive more than one command per channel')
    this._commandReceived = true
    const { cmd, args } = msg
    // debug('[%s ch%s] recv Command %s %o', this.handlers.name, this.localId, cmd, args)
    this.handlers.oncall(cmd, args, this)
  }

  onreply (msg) {
    if (!this._commandSent) this.destroy('Cannot receive replies before sending a command')
    this.emit('reply', msg)
  }

  ondata (msg) {
    // if (!this._commandSent) this.destroy('Cannot receive data before sending a command')
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

  onclose (message) {
    const { error } = message
    this.emit('remote-close')
    this._remoteClosed = true
    if (error) {
      this.remoteError = message
      let remoteError = new Error('Remote closed with error: ' + error)
      this.destroy(remoteError)
      this.emit('error', remoteError)
    } else {
      this.destroy()
    }
  }

  destroy (err) {
    if (this.closed) return
    this.closed = true
    if (!this._remoteClosed) {
      this.close(err)
    }
    this.io.destroy()
    this.log.destroy()
    if (typeof err === 'string') err = new Error(err)
    if (err && this.remoteError) this.emit('remote-error', err)
    // else if (err) this.emit('error', err)
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
