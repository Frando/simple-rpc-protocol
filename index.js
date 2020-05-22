const { EventEmitter } = require('events')
const { Duplex } = require('streamx')
const varint = require('varint')
const SMC = require('simple-message-channels')
const debug = require('debug')('rpc')
const codecs = require('codecs')

const { json, uuid } = require('./util')

const CommandRepo = require('./lib/repo')

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
    this._cnt = 0
  }

  connection (stream, opts = {}) {
    const endpoint = new Endpoint({
      repo: this.repo,
      stream,
      name: opts.name || ('connection:' + ++this._cnt)
    })
    this.endpoints.push(endpoint)

    if (opts.allowExpose) {
      endpoint.on('remote-manifest', manifest => {
        this.onannounce(manifest, endpoint)
      })
    }

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
    for (const endpoint of this.endpoints) {
      endpoint.announce(manifest)
    }
  }

  command (name, ...args) {
    this.repo.add(name, ...args)
  }

  commands (commands) {
    this.repo.batch(commands)
  }

  service (name, commands, opts) {
    this.repo.service(name, commands, opts)
  }

  onannounce (msg, endpoint) {
    const self = this
    const { name, commands } = msg
    debug('received announce from %s (%s commands)', name, Object.keys(commands).length)
    this.remotes[name] = {
      name, commands, endpoint
    }

    this.emit('remote', name, this.remotes[name])

    for (const [cmd, opts] of Object.entries(commands)) {
      const scopedName = `@${name} ${cmd}`
      this.repo.add(scopedName, {
        ...opts,
        oncall (args, channel) {
          if (!self.remotes[name]) return channel.error(new Error('Remote not found: ' + name))
          const remote = self.remotes[name]
          if (!opts.mode || opts.mode === 'async') {
            remote.endpoint.call(cmd, args, channel.env, (err, args, remoteChannel) => {
              if (err) return channel.error(err)
              channel.reply(args)
              pipe(channel, remoteChannel)
            })
          }
          if (opts.mode === 'streaming') {
            const remoteChannel = remote.endpoint.callStream(cmd, args, channel.env)
            pipe(channel, remoteChannel)
          }
        }
      })
    }

    this.announce()
  }

  // call (name, cmd, args, env, cb) {
  //   if (!this.remotes[name]) return cb(new Error('Remote not found: ' + name))
  //   this.remotes[name].endpoint.call(cmd, args, env, cb)
  // }

  close () {
    for (const endpoint of this.endpoints) {
      endpoint.close()
    }
    this.endpoints = []
    this.remotes = {}
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
    this.remoteManifest = null
    this.passthrough = opts.passthrough

    this.protocol = new CommandProtocol({
      name: this.name,
      send (buf) {
        if (self._closed) return
        self.stream.write(buf)
      },
      oncall (cmd, args, channel) {
        if (self._closed) return
        self.repo.oncall(cmd, args, channel.io)
      },
      onannounce (manifest) {
        if (self._closed) return
        self.remoteManifest = manifest
        self.emit('remote-manifest', manifest)
      }
    })

    this.stream.on('close', () => this.close())

    // This pipes the transport stream into the protocol.
    this.stream.on('data', data => this.protocol.recv(data))
  }

  close () {
    this._closed = true
    this.emit('close')
    this.stream.destroy()
    this.protocol.destroy()
  }

  command (name, ...args) {
    this.repo.add(name, ...args)
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

  ready (cb) {
    if (!cb) cb = promiseCallback()

    if (this.remoteManifest) cb()
    else this.once('remote-manifest', (_manifest) => cb())

    if (cb.promise) return cb.promise
  }

  call (command, args, env, cb) {
    if (typeof args === 'function') {
      cb = args
      args = undefined
    } else if (typeof env === 'function') {
      cb = env
      env = undefined
    }

    if (!cb) cb = promiseCallback()

    if (!this.remoteManifest) {
      this.once('remote-manifest', () => this.call(command, args, env, cb))
      return cb.promise
    }

    const [error, channel] = this._createCommand({
      command,
      args,
      env,
      mode: 'async',
      passthrough: this.passthrough
    })

    if (error) {
      cb(error)
      return cb.promise
    }

    let called
    channel.once('reply', msg => {
      if (called) return
      called = true
      cb(null, msg, channel.io)
    })
    channel.once('remote-error', err => {
      if (called) return
      called = true
      cb(err)
    })

    return cb.promise
  }

  callStream (command, args, env) {
    if (!this.remoteManifest) {
      const proxy = new Duplex()
      proxy.logs = new Duplex()
      process.nextTick(() => {
        const error = new Error('No remote manifest received')
        proxy.emit('error', error)
        proxy.destroy(error)
      })
      return proxy
    }

    const [error, channel] = this._createCommand({
      command,
      args,
      env,
      mode: 'streaming',
      passthrough: this.passthrough
    })

    if (error) {
      const proxy = new Duplex()
      proxy.logs = new Duplex()
      process.nextTick(() => {
        proxy.emit('error', error)
        proxy.destroy(error)
      })
      return proxy
    }

    return channel.io
  }

  _createCommand (opts) {
    if (!this.remoteManifest) {
      return [new Error('Cannot create commmand channel before receiving remote manifest')]
    }

    const {
      command: cmd,
      args,
      env,
      mode,
      passthrough
    } = opts

    const command = this.remoteManifest.commands[cmd]
    if (!command) {
      return [new Error(`Command not found: "${cmd}"`)]
    }
    if (command.mode !== mode) {
      // console.log(new Error().stack)
      return [new Error(`Mode mismatch: Command "${cmd}" is "${command.mode}" but was called as "${mode}"`)]
    }

    const channelOpts = {
      mode: command.mode
    }
    if (!passthrough) {
      if (command.encoding) {
        channelOpts.encoding = command.encoding
      }
      if (command.logEncoding) {
        channelOpts.logEncoding = command.logEncoding
      }
    }

    const channel = this.protocol.createLocalChannel(channelOpts)

    channel.open(env)
    channel.command(cmd, args)
    return [null, channel]
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
  static id () {
    if (!CommandProtocol._id) CommandProtocol._id = 0
    return ++CommandProtocol._id
  }

  constructor (handlers) {
    super()
    this._name = handlers.name || ('proto-' + CommandProtocol.id())
    this.handlers = handlers
    this.smc = new SMC({
      onmessage: this.onmessage.bind(this)
    })

    this.local = [null]
    this.remote = []
    this.channels = {}
  }

  destroy () {
    for (const channel of Object.values(this.channels)) {
      channel.destroy()
    }
  }

  onmessage (ch, typ, message) {
    const self = this

    switch (typ) {
      case Typ.Announce: {
        message = json.decode(message)
        // debug('[%s ch%s] recv Announce %o', self._name, ch, message)
        if (ch === 0 && self.handlers.onannounce) self.handlers.onannounce(message, self)
        self.remoteManifest = message
        return
      }

      case Typ.Open: {
        message = json.decode(message)
        // debug('[%s ch%s] recv Open %o', self._name, ch, message)
        const { id } = message
        const channel = this.createRemoteChannel(id, ch)
        channel.onopen(message)
        return
      }

      case Typ.Extension: {
        const extid = varint.decode(message)
        const m = message.slice(varint.decode.bytes)
        if (self.handlers.onextension) self.handlers.onextension(ch, extid, m)
        return
      }
    }

    if (!self.remote[ch]) return
    self.remote[ch].onmessage(typ, message)
  }

  announce (message) {
    message = json.encode(message)
    this.send(0, Typ.Announce, message)
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

  createRemoteChannel (id, ch) {
    const channel = this.createChannel(id)
    this.attachRemote(channel, ch)
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

class CommandChannel extends EventEmitter {
  constructor (id, handlers, opts = {}) {
    super()
    this.id = id
    // should be { oncall, send }
    this.handlers = handlers
    this._name = this.handlers.name
    this.io = new DataChannel(this)
    this.logs = new LogChannel(this)

    this.channelEncoding = codecs(opts.encoding || 'binary')
    this.logEncoding = codecs(opts.logEncoding || 'json')
    this.mode = opts.mode
    this.env = {}
  }

  setDataEncoding (encoding) {
    this.channelEncoding = codecs(encoding)
  }

  setLogEncoding (encoding) {
    this.logEncoding = codecs(encoding)
  }

  onmessage (typ, message) {
    debug(this._name, 'recv', typ, message.toString())
    if (typ === Typ.Data) {
      message = this.channelEncoding.decode(message)
    } else if (typ === Typ.Log) {
      message = this.logEncoding.decode(message)
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
    // if (!(this.mode === 'any' || this.mode === 'async')) return this.destroy('Cannot reply in streaming mode')
    this._send(Typ.Reply, msg)
  }

  data (msg) {
    // if (!(this.mode === 'any' || this.mode === 'streaming')) return this.destroy('Cannot use channel in non-stream mode')
    this._send(Typ.Data, msg)
  }

  log (msg) {
    // if (!(this.mode === 'any' || this.mode === 'streaming')) return this.destroy('Cannot use channel in non-stream mode')
    if (msg instanceof Error) msg = { error: msg.toString() }
    this._send(Typ.Log, msg)
  }

  close (msg) {
    if (this._localClosed) return
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
    debug(this._name, 'send', typ, message, Typ.Log)
    if (typ === Typ.Data) {
      message = this.channelEncoding.encode(message)
    } else if (typ === Typ.Log) {
      message = this.logEncoding.encode(message)
    } else if (message !== undefined) {
      message = json.encode(message)
    } else {
      message = Buffer.alloc(0)
    }
    debug(this._name, 'send', typ, message)
    const id = this.localId
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
    this.logs.push(msg)
  }

  onfin () {
    this.io.push(null)
    this.logs.push(null)
    this._remoteClosed = true
    this.emit('remote-close')
    if (this._localClosed) this.destroy()
  }

  onclose (message) {
    let error
    if (message && message.error) error = message.error
    this.emit('remote-close')
    this._remoteClosed = true
    if (error) {
      this.remoteError = new Error('Remote closed with error: ' + error)
      this.destroy(this.remoteError, { remote: true })
    } else {
      this.destroy(null, { remote: true })
    }
  }

  destroy (err, opts = {}) {
    if (this.closed) return
    this.closed = true
    if (!this._remoteClosed) {
      this.close(err)
    }

    if (opts.remote) this.emit('remote-error', this.remoteError)

    if (!this._hasCallback) {
      this.io.destroy(err)
    } else {
      this.io.destroy()
    }

    this.logs.destroy()

    this.emit('close', err)
  }
}

class LogChannel extends Duplex {
  constructor (channel) {
    super()
    this.channel = channel
    this.on('error', err => {
      if (!this.channel.remoteError) this.channel.close(err)
    })
  }

  _write (message, next) {
    this.channel.log(message)
    next()
  }

  _read (next) {
    next()
  }
}

class DataChannel extends Duplex {
  constructor (channel) {
    super()
    this.channel = channel
    this.on('finish', () => {
      channel.fin()
    })
    this.on('error', err => {
      if (!this.channel.remoteError) this.channel.close(err)
    })
    channel.on('reply', msg => {
      this.emit('reply', msg)
    })
  }

  get env () {
    return this.channel.env
  }

  get id () {
    return this.channel.id
  }

  get logs () {
    return this.channel.logs
  }

  log (args) {
    this.channel.logs.write(args)
  }

  setEncoding (encoding) {
    this.channel.setDataEncoding(encoding)
  }

  setLogEncoding (encoding) {
    this.channel.setLogEncoding(encoding)
  }

  reply (msg) {
    this.channel.reply(msg)
  }

  error (err) {
    if (this._errorSent) return
    this._errorSent = true
    this.channel.close(err)
  }

  _write (data, next) {
    this.channel.data(data)
    next()
  }

  _read (cb) {
    cb(null)
  }
}

function pipe (a, b) {
  a.pipe(b).pipe(a)
  a.logs.pipe(b.logs).pipe(a.logs)
  a.once('close', err => b.destroy(err))
  b.once('close', err => a.destroy(err))
  a.on('error', err => b.error(err))
  b.on('error', err => a.error(err))
  // a.on('reply', msg => b.reply(msg))
  // b.on('reply', msg => a.reply(msg))
  // b.on('remote-error', err => b.emit('remote-error', err))
}

module.exports = { CommandChannel, CommandRepo, CommandProtocol, Endpoint, Router }


function promiseCallback () {
  let presolve, preject
  const promise = new Promise((resolve, reject) => {
    presolve = resolve
    preject = reject
  })
  const cb = (err, ...args) => {
    if (err) return preject(err)
    if (!args.length) return presolve()
    if (args.length === 1) return presolve(args[0])
    presolve(args)
  }
  cb.promise = promise
  return cb
}
