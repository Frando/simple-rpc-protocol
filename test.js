const tape = require('tape')
const { Readable, Writable, Transform } = require('streamx')
const duplexify = require('duplexify')
// const pump = require('pump')
const collect = require('stream-collector')

const { Router, Endpoint } = require('.')

class PassThrough extends Transform {
  transform (chunk, enc, next) {
    this.push(chunk)
    next()
  }
}

tape('basics: async', async t => {
  const [server, client] = create()
  server.command('echo', {
    mode: 'async',
    oncall (args, channel) {
      t.equal(args, 'hello world', 'server args recv')
      channel.reply(args.toUpperCase())
    }
  })
  server.announce()
  const [result] = await client.call('echo', 'hello world')
  t.equal(result, 'HELLO WORLD', 'echo reply ok')
})

tape('basics: streaming ', async t => {
  const [server, client] = create()
  server.command('echostream', {
    mode: 'streaming',
    encoding: 'utf8',
    oncall (args, channel) {
      const [suffix] = args
      channel.on('data', data => {
        channel.write(data + suffix)
      })
    }
  })

  server.announce()
  await client.ready()

  await new Promise(resolve => {
    const stream = client.callStream('echostream', ['foo'])
    stream.write('hello')
    stream.on('error', err => {
      t.fail(err)
      t.end()
    })
    stream.once('data', data => {
      t.equal(data, 'hellofoo', 'stream reply ok')
      resolve()
    })
  })
})

tape('logging', t => {
  const [server, client] = create()
  server.command('echo', (args, channel) => {
    t.equal(args, 'hello world', 'server args recv')
    channel.log('hello')
    channel.log('done')
    channel.reply(args.toUpperCase())
    channel.end()
  })
  server.announce()

  client.call('echo', 'hello world', (err, msg, channel) => {
    t.error(err, 'no err')
    t.equal(msg, 'HELLO WORLD', 'client uppercase res')
    collect(channel.logs, (err, logs) => {
      t.error(err)
      t.deepEqual(logs, ['hello', 'done'])
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
      onopen (env, channel, cb) {
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
  server.command('end', (args, channel) => {
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
  server.command('end', (args, channel) => {
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

  client.call('end', (err, msg, channel) => {
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
  server.command('failing', (args, channel) => {
    channel.error(new Error('No!'))
  })
  server.command('failing-stream', (args, channel) => {
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

tape('cli', t => {
  const { spawn } = require('child_process')
  const [server, client] = create()
  server.command('echo', {
    encoding: 'utf8',
    logEncoding: 'utf8',
    mode: 'streaming',
    oncall (args, channel) {
      const nodeArgs = ['-e', `
        console.error("start")
        console.log("hi " + process.env.FOO)
        console.error("end")
      `]
      const proc = spawn('node', nodeArgs, {
        env: channel.env
      })
      proc.stdout.pipe(channel)
      proc.stderr.pipe(channel.logs)
    }
  })
  server.announce()
  const env = { FOO: 'bar' }
  client.ready(() => {
    const channel = client.callStream('echo', [], env)
    // console.log('channel', channel)
    channel.on('error', err => {
      t.fail(err)
    })
    let pending = 2
    collect(channel, (err, res) => {
      t.error(err)
      t.equal(res.join(''), 'hi bar\n')
      if (--pending === 0) t.end()
    })
    collect(channel.logs, (err, res) => {
      t.error(err)
      t.equal(res.join(''), 'start\nend\n')
      if (--pending === 0) t.end()
    })
  })
})

function create () {
  const [s1, s2] = duplexPair()
  const server = new Endpoint({ stream: s1, name: 'server' })
  const client = new Endpoint({ stream: s2, name: 'client' })
  return [server, client]
}

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

// function watch (emitter, name) {
//   const emit = emitter.emit.bind(emitter)
//   emitter.emit = function (ev, ...args) {
//     if (ev === 'pipe') {
//       console.log('stream [%s] %s', name, ev)
//     } else {
//       console.log('stream [%s] %s %o', name, ev, args)
//     }
//     emit(ev, ...args)
//   }
// }
