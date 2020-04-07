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
    if (!this.commands[cmd]) return channel.error('Command not found: ' + cmd)
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
      command.oncall(args, channel)
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
