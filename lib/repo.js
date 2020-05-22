module.exports = class CommandRepo {
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
    Object.entries(commands).forEach(([name, opts]) => this.add(name, opts))
  }

  add (name, opts = {}) {
    if (typeof opts === 'function') {
      opts = { oncall: opts }
    }
    if (!opts.mode) opts.mode = 'async'
    opts.name = name
    this.commands[name] = opts
  }

  has (name) {
    return !!this.commands[name]
  }

  get (name) {
    return this.commands[name]
  }

  oncall (cmd, args, channel) {
    if (!this.commands[cmd]) return channel.error('Command not found: ' + cmd)
    const command = this.commands[cmd]
    if (command.encoding) {
      channel.setEncoding(command.encoding)
    }
    if (command.logEncoding) {
      channel.setLogEncoding(command.logEncoding)
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
      command.oncall(args, channel)
    }
  }

  manifest () {
    const manifest = {}
    for (const [name, opts] of Object.entries(this.commands)) {
      manifest[name] = {
        mode: opts.mode,
        encoding: opts.encoding,
        logEncoding: opts.logEncoding,
        help: opts.help,
        title: opts.help,
        args: opts.args
      }
    }
    return manifest
  }
}
