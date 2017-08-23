## /ws and /wss -- websockets

If you want browsers to connect to e.g. `/dns4/example.com/tcp/443/wss/ipfs/QmFoo`

- [ ] An SSL cert matching the `/dns4` or `/dns6` name
- [ ] go-ipfs listening on `/ip4/127.0.0.1/tcp/8081/ws`
  - 8081 is just an example
  - note that it's `/ws` here, not `/wss` -- go-ipfs can't currently do SSL, see the next point
- [ ] nginx
  - configured with the SSL cert
  - listening on port 443
  - forwarding to 127.0.0.1:8081
