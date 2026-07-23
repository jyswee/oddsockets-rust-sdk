# OddSockets Rust SDK - live demo

A runnable, two-client honest regression that exercises the SDK against the
**real** OddSockets platform. Nothing is mocked: each client performs
Manager -> Worker discovery over HTTP and opens its own Socket.IO WebSocket,
and the two clients talk to each other through the worker's broadcast fan-out.

## What it proves

- **Scenario 1 - core pub/sub.** `bob` publishes a nonce-tagged message; `alice`
  receives it on her broadcast receiver over a *separate* connection.
- **Scenario 2 - enhanced (Slack-like) events.** `bob` fires
  `enhanced.start_typing` and `enhanced.add_reaction`; `alice` receives
  `user_typing` and `reaction_added` on her public `on()` surface, across the
  wire.

Both clients are independent `OddSocketsClient`s, so a green run is genuine
proof the enhanced surface is wired to the real transport, not a local echo.

## Run it

```bash
export ODDSOCKETS_API_KEY="ak_..."   # get a free key at https://oddsockets.com
cargo run
```

Exit codes: `0` all scenarios passed, `1` missing key / setup error, `2` a
scenario timed out waiting for a live message.

See [PROOF.txt](PROOF.txt) for a captured transcript of a live green run.
