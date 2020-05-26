# simple-rpc-protocol

A simple RPC protocol that uses [simple-message-channels](https://github.com/mafintosh/simple-message-channels/) as wire protocol.

Supports routing between different endpoints and fast binary messaging.

## Examples

See [test.js](test.js) and [shoutstream-example.js](shoutstream-example.js) for now.

### Usage

```javascript
// server.js
// provide a command
const { Endpoint } = require('simple-rpc-protocol')
const net = require('net')
// pipe the transport stream into any binary stream
net.createServer(socket => {
  const endpoint = new Endpoint()
  endpoint.command('shout', {
    oncall (args, channel) {
      channel.reply(args[0].toUpperCase())
    }
  })
  endpoint.command('shoutstream', {
    mode: 'streaming'
    oncall (args, channel) {
      channel.pipe(new Transform({
        transform (chunk, enc, next) {
          this.push(chunk.toUpperCase())
        }
      }).pipe(channel)
    }
  }))

  socket.pipe(endpoint).pipe(socket)
  endpoint.announce()
}).listen(8000)

// client.js
// at the other end of the stream:
const { Endpoint } = require('simple-rpc-protocol')
const net = require('net')
const socket = net.connect(8000)
const endpoint = new Endpoint()
endpoint.pipe(socket).pipe(endoint)

// Call an async command.
const uppercased = await endpoint.call('echo', ['hello'])
// uppercased === 'HELLO'

// Call a streaming command.
const channel = endpoint.callStream('shoutstream')
process.stdin.pipe(channel).pipe(process.stdout)
```
