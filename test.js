const tape = require('tape')
const { Readable, Writable, Transform } = require('streamx')
const duplexify = require('duplexify')
const pump = require('pump')

const { Router, Endpoint } = require('.')

class PassThrough extends Transform {
  transform (chunk, _enc, next) {
    this.push(chunk)
    next()
  }
}

tape('basics', t => {
  const [server, client] = create()
  server.command('echo', (args, channel) => {
    t.equal(args, 'hello world', 'server args recv')
    channel.reply(args.toUpperCase())
    const rs = new Readable({
      read (cb) { cb() }
    })
    rs.pipe(channel)
    rs.push(Buffer.from('hi'))
    rs.push(null)
  })
  server.announce()

  client.call('echo', 'hello world', (err, msg, channel) => {
    t.error(err, 'no err')
    t.equal(msg, 'HELLO WORLD', 'client uppercase res')
    channel.on('data', d => {
      t.equal(d.toString(), 'hi', 'client channel recv')
    })
    channel.on('end', () => {
      t.end()
    })
  })
})

tape('router', t => {
  const server = new Router()
  const [c1a, c1b] = duplexPair()
  const [c2a, c2b] = duplexPair()

  const client1 = new Endpoint({ stream: c1a, name: 'client1' })
  const client2 = new Endpoint({ stream: c2a, name: 'client2' })

  client2.command('echo', (args, channel) => {
    channel.reply(args.toUpperCase())
  })
  client2.announce({
    name: 'echoservice'
  })

  server.connection(c1b)
  server.connection(c2b, { allowExpose: true })

  setTimeout(() => {
    client1.call('@echoservice echo', 'hello world', (err, msg) => {
      t.error(err)
      t.equal(msg, 'HELLO WORLD')
      t.end()
    })
  }, 100)
})

tape('service and env', t => {
  t.plan(4)
  const [client, server] = create()

  const service = {
    name: 'echos',
    commands: {
      loud (args, channel) {
        channel.reply(args[0] + channel.env.emphasis)
      },
      louder (args, channel) {
        channel.reply(args[0].toUpperCase() + channel.env.emphasis)
      }
    },
    opts: {
      onopen (env, _channel, cb) {
        if (!env.emphasis) return cb(new Error('Cannot shout without emphasis'))
        cb()
      }
    }
  }
  server.service(service.name, service.commands, service.opts)
  server.announce()

  const env = { emphasis: '!!' }
  const args = ['hi']
  client.call('@echos loud', args, env, (err, msg) => {
    t.error(err)
    t.equal(msg, 'hi!!')
  })
  client.call('@echos louder', args, env, (err, msg) => {
    t.error(err)
    t.equal(msg, 'HI!!')
  })
})

tape('pipe end', t => {
  t.plan(3)
  const [client, server] = create()
  server.command('end', (_args, channel) => {
    channel.reply()
    channel.setEncoding('json')
    const rs = new Readable({ read () {} })
    rs.pipe(channel)
    rs.push('foo')
    rs.push(null)
  })
  server.announce()

  client.call('end', (err, msg, channel) => {
    t.error(err)
    channel.setEncoding('json')
    const ws = new Writable({
      write (chunk, next) {
        t.equal(chunk, 'foo', 'received a chunk')
        next()
      }
    })
    ws.on('close', () => t.pass('ws closed'))
    channel.pipe(ws)
  })
})

tape('pipe error', t => {
  t.plan(4)
  const [client, server] = create()
  server.command('end', (_args, channel) => {
    channel.reply()
    channel.setEncoding('json')
    const rs = new Readable({ read (next) { next() } })
    rs.pipe(channel, err => {
      channel.error(err)
    })
    rs.push('foo')
    setTimeout(() => {
      rs.destroy(new Error('fail'))
    }, 0)
  })
  server.announce()

  client.call('end', (err, _msg, channel) => {
    t.error(err, 'reply ok')
    channel.setEncoding('json')
    const ws = new Writable({
      write (chunk, next) {
        t.equal(chunk, 'foo', 'received a chunk')
        next()
      }
    })
    ws.on('close', () => t.pass('ws closed'))
    channel.pipe(ws, err => {
      t.equal(err.message, 'Readable stream closed before ending', 'channel err')
    })
  })
})

tape('error handling', t => {
  t.plan(3)
  const [client, server] = create()
  server.command('failing', (_args, channel) => {
    channel.error(new Error('No!'))
  })
  server.command('failing-stream', (_args, channel) => {
    channel.reply()
    channel.error(new Error('Nono!'))
  })
  server.announce()

  client.call('failing', [], (err) => {
    t.equal(err.message, 'Remote closed with error: Error: No!')
  })
  client.call('failing-stream', (err, msg, channel) => {
    t.error(err)
    channel.on('error', err => {
      t.equal(err.message, 'Remote closed with error: Error: Nono!')
    })
  })
})

function duplexPair () {
  const s1read = new PassThrough()
  const s1write = new PassThrough()
  const s2write = new PassThrough()
  const s2read = new PassThrough()
  const s1 = duplexify(s1write, s1read)
  const s2 = duplexify(s2write, s2read)
  s2write.pipe(s1read)
  s1write.pipe(s2read)
  return [s1, s2]
}


function watch (emitter, name) {
  const emit = emitter.emit.bind(emitter)
  emitter.emit = function (ev, ...args) {
    if (ev === 'pipe') {
      console.log('stream [%s] %s', name, ev)
    } else {
      console.log('stream [%s] %s %o', name, ev, args)
    }
    emit(ev, ...args)
  }
}

function create () {
  const [s1, s2] = duplexPair()
  const server = new Endpoint({ stream: s1, name: 'server' })
  const client = new Endpoint({ stream: s2, name: 'client' })
  return [client, server]
}

