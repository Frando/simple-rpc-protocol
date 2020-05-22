# simple-rpc-protocol

A simple RPC protocol that uses [simple-message-channels](https://github.com/mafintosh/simple-message-channels/) as wire protocol.

Supports routing between different endpoints and fast binary messaging.

## Examples

See [test.js](test.js) and [shoutstream-example.js](shoutstream-example.js) for now.

### Usage

```javascript
// provide a command
const { Endpoint } = require('simple-rpc-protocol')
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
})

// pipe the transport stream into any binary stream
endpoint.pipe(socket).pipe(endoint)

// at the other end of the stream

const { Endpoint } = require('simple-rpc-protocol')
const endpoint = new Endpoint()
endpoint.pipe(socket).pipe(endoint)

const uppercased = await endpoint.call('echo', ['hello'])

endpoint.callStream('shoutstream', process.stdin).pipe(process.stdout)
// uppercased === 'HELLO'
```
