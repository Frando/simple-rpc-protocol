const net = require('net')
const { Transform } = require('stream')
const { Endpoint } = require('.')

const PORT = 22022

const command = process.argv[2]

if (command === 'listen') {
  // Create a TCP server and call a function on connections.
  net.createServer(onconnection).listen(PORT, () => {
    console.log(`shoutstream listening on localhost:${PORT}`)
  })
} else if (command === 'connect') {
  console.log('type quietly and wait for the echo')
  // Connect to a TCP server.
  const socket = net.connect(PORT)
  // Init an RPC endpoint on the stream.
  const endpoint = new Endpoint({ stream: socket })
  // Wait until the remote end sent us their command list
  endpoint.once('remote-manifest', () => {
    // Call a command, in streaming mode.
    const channel = endpoint.callStream('shoutstream')
    process.stdin.pipe(channel).pipe(process.stdout)
  })
} else {
  console.error('usage: node example.js [listen|connect]')
}

function onconnection (socket) {
  const endpoint = new Endpoint({ stream: socket })
  // Expose a command
  endpoint.command('shoutstream', {
    // Modes are 'streaming' and 'async'
    mode: 'streaming',
    // Set an encoding for the streaming channel
    encoding: 'utf8',
    oncall (args, channel) {
      channel.pipe(new Transform({
        objectMode: true,
        transform (chunk, enc, next) {
          this.push(chunk.toUpperCase())
          next()
        }
      })).pipe(channel)
    }
  })
  endpoint.announce()
}
