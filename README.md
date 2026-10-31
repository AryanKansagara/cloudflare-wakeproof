# WakeProof — Cloudflare-inspired lifecycle experiment

Cloudflare provides networking/security and Workers compute, including stateful Durable Objects. Its hibernation documentation explains that an object's memory can disappear while connections remain, and that attachments and durable storage serve different roles. This project explores recovery invariants using those constraints; it is independent and does not assert Cloudflare lacks lifecycle tooling.

**Implemented: local Node.js lifecycle fault model with actual disk persistence. Cloudflare runtime integration is unverified.** No Workers API, hosted Durable Object, WebSocket server, physical hibernation or browser client runs here. The harness retains serialized connection attachments outside each replaced JavaScript actor, imitating a deliberately narrow reconstruction contract. JS is appropriate because lifecycle and storage semantics are the target, not native inference.

Node 20+ standard library only:

```sh
npm test
npm run demo
node cli.mjs reserve ./state.json alice op1
node cli.mjs reserve ./state.json alice op1
node cli.mjs inspect ./state.json
```

The two reserve commands use separate real processes: first returns `applied: true`, second returns the same receipt with `applied: false`. The CLI uses default capacity two. `demo.json` records an actual local run: warm memory-only and persistent controls pass; replacement and replay expose lost acknowledged state and duplicate application in the memory-only control; persistent reconstruction retains receipts and returns the original operation identity. The experiment never counts rejecting every operation as a success: acknowledgements and final durable state are reported. The demo also reduces a failing schedule to a deletion-1-minimal loss witness, excluding invalid/disconnected schedules.

## Architecture and contract

A single serial dispatch queue owns actor mutations, connection changes, and reconstruction. Connections have fresh UUID identities; reconnect invalidates old handles. A versioned serialized attachment restores only connection identity, not reservations. A versioned JSON receipt ledger stores operation ID, client, slot and sequence. An acknowledged reservation must remain present, a logical operation must apply at most once, and acknowledged slots/capacity cannot conflict. Reusing an operation ID for another client fails explicitly. The oracle keeps acknowledgements outside the replaced actor and checks after every successful reserve and reconstruction.

Persistence writes a unique temporary file and atomically renames it before acknowledgement. Errors fail closed and do not replace in-memory state. This is single-owner local filesystem persistence, with no multi-process writer locking, fsync/power-loss guarantee, database transaction or distributed consistency claim. Actual process restart is tested sequentially. A missing file means first initialization; malformed existing files, mismatched capacities and invalid receipts fail without resetting business state. Do not delete the state file to simulate reconstruction: reconstruction retains storage.

Capacity is 1–64, identities use 1–80 restricted ASCII characters, and the local attachment parser accepts only version one, valid identities, and up to 16,384 encoded bytes. That is an authored local bound, not validation of Cloudflare serialization internals. Disk fixtures are trusted local inputs; no large-file parser hardening or public network authentication is provided. Public APIs are a lab interface: callers must use LifecycleLab's queue for concurrent dispatch; a directly instantiated RoomActor is not a concurrent database. Deletion minimization finds a local minimum, not the globally shortest schedule.

Tests include paired negative/positive controls, acknowledgement loss, duplicate application, capacity competition, sequential real process restart, corruption preservation, failed-write recovery, stale/reconnected handles, invalid/oversized attachments, conflicts, repeated reconstruction and reducer validity. The CI runs those local checks only.

## Next validation and existing overlap

Cloudflare already supplies persistence, lifecycle and hibernation APIs. The inferred opportunity is an invariant-driven application test harness. Next stage needs a Cloudflare account, a Workers adapter, a deployed object, real WebSocket clients and independent host lifecycle evidence; object replacement in this repository cannot establish hosted hibernation behavior. No credentials or deployment are required here.

Primary sources:

- https://developers.cloudflare.com/durable-objects/best-practices/websockets/
- https://developers.cloudflare.com/durable-objects/concepts/durable-object-lifecycle/
- https://developers.cloudflare.com/durable-objects/api/state/
