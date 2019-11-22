const tape = require('tape')
const { Readable } = require('streamx')
const duplexify = require('duplexify')
const { PassThrough } = require('stream')

const { Router, Endpoint } = require('.')

tape('basics', t => {
  const [s1, s2] = duplexPair()
  const server = new Endpoint({ stream: s1, name: 'server' })
  const client = new Endpoint({ stream: s2, name: 'client' })

  server.command('echo', (args, channel) => {
    t.equal(args, 'hello world')
    channel.reply(args.toUpperCase())
    const rs = new Readable({
      read (cb) { cb() }
    })
    rs.pipe(channel)
    rs.push(Buffer.from('hi'))
    rs.push(null)
  })

  client.call('echo', 'hello world', (err, msg, channel) => {
    t.error(err)
    t.equal(msg, 'HELLO WORLD')
    channel.on('data', d => {
      t.equal(d.toString(), 'hi', 'hi received')
    })
    channel.on('end', () => {
      t.end()
    })
  })
})

tape('router', t => {
  const server = new Router()
  const [c1a, c1b] = duplexPair()
  const client1 = new Endpoint({ stream: c1a })
  const [c2a, c2b] = duplexPair()
  const client2 = new Endpoint({ stream: c2a })

  server.connection(c1b)
  server.connection(c2b)

  client2.command('echo', (args, channel) => {
    channel.reply(args.toUpperCase())
  })
  client2.announce({
    name: 'echoservice'
  })

  setTimeout(() => {
    client1.call('@echoservice echo', 'hello world', (err, msg) => {
      t.error(err)
      t.equal(msg, 'HELLO WORLD')
      t.end()
    })
  }, 10)
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
