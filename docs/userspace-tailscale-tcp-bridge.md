# Glass over userspace Tailscale on macOS

Glass normally sends its browser stream over UDP WebTransport. A macOS
`tailscaled --tun=userspace-networking` process has no system Tailscale
interface for Glass to bind, and Tailscale Serve forwards TCP/HTTPS rather
than UDP. This local fork adds an optional WebSocket transport that Tailscale
Serve can proxy to Glass on loopback. It carries Glass's existing datagram and
stream protocol through a local bridge, so the existing access token and host
permission checks still apply.

The option is off unless `GLASS_TCP_BRIDGE=1` is set for the host process.
`/config.json` then advertises `tcpBridge: true`, and the browser uses
`wss://<page-host>/tcp-bridge` instead of WebTransport. Other Glass hosts
opened from that page still use their own native transport.

## Local verification

Build this fork and run it separately from an installed Glass service:

```sh
cargo build -p glass-host --bin glass-host
GLASS_CONFIG_DIR=/tmp/glass-bridge-test-config GLASS_TCP_BRIDGE=1 \
  target/debug/glass-host --test --bind 127.0.0.1 \
  --http-port 18080 --https-port 0 --wt-port 14433 \
  --token-file /tmp/glass-bridge-test-token \
  --log-dir /tmp/glass-bridge-test-logs --web-dir web
```

From another terminal:

```sh
GLASS_TEST_TOKEN_FILE=/tmp/glass-bridge-test-token \
  node tests/e2e/tcp-bridge-client.mjs
```

The test authenticates with the host and receives a video packet through the
browser transport. Test mode uses a generated pattern and never captures or
controls the real desktop.

## Future tailnet deployment

This has **not** been enabled on the user's Mac. The current signed Glass
installation and userspace Tailscale daemon remain separate.

To serve the browser from a `.ts.net` name, the tailnet must first have HTTPS
Certificates enabled by its owner. Certificate issuance publishes that device
name to the public Certificate Transparency log. A browser also needs to run
Tailscale on the viewing device. With those conditions met, the deployment
would run a built host binary (macOS will require Screen Recording and
Accessibility approval for that binary), enable
`GLASS_TCP_BRIDGE=1` in its LaunchAgent, allow the exact tailnet hostname in
Glass's `allowed-hosts`, and configure the **existing** userspace daemon's
`tailscale serve` to proxy HTTPS to Glass's loopback HTTP port. No macOS VPN
interface is needed on the host. A cross-network browser connection and
measured latency must be verified before calling that deployment complete.

This TCP mode may incur head-of-line blocking on lossy networks. The bridge
drops queued datagrams when its output queue fills to keep video from lagging;
Glass's existing keyframe recovery then repairs the picture. Native UDP
WebTransport remains the faster path when a normal Tailscale interface is
available.
