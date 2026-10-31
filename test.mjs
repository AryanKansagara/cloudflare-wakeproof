import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, readFile, writeFile, readdir } from 'node:fs/promises';
import { join, dirname } from 'node:path';
import { tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
import { LifecycleLab, DiskStore, parseAttachment, validateState, runSchedule, minimize } from './lab.mjs';

async function fixture(t, options = {}) {
  const dir = await mkdtemp(join(tmpdir(), 'wakeproof-test-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const path = join(dir, 'room.json');
  const lab = await LifecycleLab.open({ path, ...options });
  return { lab, dir, path };
}
const schedule = [{ type: 'connect', clientId: 'alice' }, { type: 'reserve', clientId: 'alice', opId: 'op1' }, { type: 'reconstruct' }, { type: 'reserve', clientId: 'alice', opId: 'op1' }];

test('paired warm controls accept useful work with no invariant violation', async t => {
  const { dir } = await fixture(t);
  for (const persistent of [false, true]) {
    const result = await runSchedule({ path: join(dir, `warm-${persistent}.json`), persistent, schedule: schedule.slice(0, 2) });
    assert.equal(result.state.receipts.length, 1); assert.equal(result.events.filter(e => e.type === 'ack').length, 1); assert.deepEqual(result.violations, []);
  }
});
test('memory-only reconstruction exposes acknowledged loss and duplicate application', async t => {
  const { path } = await fixture(t);
  const result = await runSchedule({ path, persistent: false, schedule });
  assert.ok(result.violations.includes('acknowledged reservation lost: op1'));
  assert.ok(result.violations.includes('operation applied twice: op1'));
  assert.equal(result.events.filter(e => e.type === 'ack' && e.applied).length, 2);
});
test('persistent reconstruction returns the same acknowledged receipt, no duplicate effect', async t => {
  const { path } = await fixture(t);
  const result = await runSchedule({ path, persistent: true, schedule });
  const acks = result.events.filter(e => e.type === 'ack');
  assert.equal(acks.length, 2); assert.equal(acks[0].applied, true); assert.equal(acks[1].applied, false);
  assert.deepEqual({ ...acks[0], applied: false }, acks[1]); assert.deepEqual(result.violations, []);
});
test('competing queued requests never overbook capacity; rejection does not poison queue', async t => {
  const { lab } = await fixture(t, { capacity: 1 });
  const a = await lab.connect('alice'), b = await lab.connect('bob');
  const outcomes = await Promise.allSettled([lab.reserve(a, 'a'), lab.reserve(b, 'b')]);
  assert.equal(outcomes.filter(x => x.status === 'fulfilled').length, 1);
  assert.match(outcomes[1].reason.message, /capacity exhausted/);
  assert.equal((await lab.reserve(a, 'a')).applied, false);
  assert.deepEqual(lab.report().violations, []); assert.equal(lab.report().state.receipts.length, 1);
});
test('separate actual CLI processes recover persisted operation identity', async t => {
  const { path } = await fixture(t);
  const cli = join(dirname(fileURLToPath(import.meta.url)), 'cli.mjs');
  const invoke = args => {
    const child = spawnSync(process.execPath, [cli, ...args], { encoding: 'utf8' });
    assert.equal(child.status, 0, child.stderr); return JSON.parse(child.stdout);
  };
  const first = invoke(['reserve', path, 'alice', 'op1']);
  const second = invoke(['reserve', path, 'alice', 'op1']);
  assert.equal(first.applied, true); assert.equal(second.applied, false);
  assert.deepEqual({ ...first, applied: false }, second);
  assert.equal(invoke(['inspect', path]).receipts.length, 1);
});
test('disk acknowledgement observes committed state and leaves no temporary files', async t => {
  const { lab, path, dir } = await fixture(t); const h = await lab.connect('alice');
  const receipt = await lab.reserve(h, 'op1');
  const disk = JSON.parse(await readFile(path, 'utf8'));
  assert.deepEqual(disk.receipts[0], { clientId: 'alice', opId: 'op1', slot: receipt.slot, sequence: receipt.sequence });
  assert.deepEqual(await readdir(dir), ['room.json']);
});
test('malformed existing storage fails closed and preserves original bytes', async t => {
  const { path } = await fixture(t); const corrupt = '{"receipt": BROKEN'; await writeFile(path, corrupt);
  await assert.rejects(LifecycleLab.open({ path }), /malformed durable JSON/);
  assert.equal(await readFile(path, 'utf8'), corrupt);
});
test('schema corruption, duplicate receipts, and capacity changes are rejected', async t => {
  const { lab, path } = await fixture(t); const h = await lab.connect('alice'); await lab.reserve(h, 'op1');
  const original = JSON.parse(await readFile(path, 'utf8'));
  await assert.rejects(LifecycleLab.open({ path, capacity: 3 }), /invalid durable state/);
  const corrupt = { ...original, receipts: [original.receipts[0], { ...original.receipts[0], sequence: 2 }], sequence: 2 };
  await writeFile(path, JSON.stringify(corrupt)); await assert.rejects(LifecycleLab.open({ path }), /invalid durable receipt/);
  assert.deepEqual(JSON.parse(await readFile(path, 'utf8')), corrupt);
});
test('failed writes produce no acknowledgement or in-memory application and can retry', async t => {
  const { path } = await fixture(t); const store = new DiskStore(path, 2); const original = store.save.bind(store); let fail = true;
  store.save = async state => { if (fail) throw new Error('injected write error'); return original(state); };
  const lab = await LifecycleLab.open({ store }); const h = await lab.connect('alice');
  await assert.rejects(lab.reserve(h, 'op1'), /injected write/);
  assert.equal(lab.report().state.receipts.length, 0); assert.equal(lab.report().events.filter(e => e.type === 'ack').length, 0);
  fail = false; assert.equal((await lab.reserve(h, 'op1')).applied, true); assert.equal(lab.report().state.receipts.length, 1);
});
test('reconnect invalidates stale handles; attachments are not business storage', async t => {
  const { lab } = await fixture(t); const old = await lab.connect('alice'); await lab.reserve(old, 'op1');
  const fresh = await lab.connect('alice'); assert.notEqual(old.connectionId, fresh.connectionId);
  await assert.rejects(lab.reserve(old, 'op2'), /stale connection/);
  await lab.reconstruct(); assert.equal((await lab.reserve(fresh, 'op1')).applied, false);
  await lab.disconnect(fresh); await assert.rejects(lab.reserve(fresh, 'op2'), /stale connection/);
  const again = await lab.connect('alice'); assert.equal((await lab.reserve(again, 'op1')).applied, false); assert.deepEqual(lab.report().violations, []);
});
test('malformed, inaccessible, oversized and unsupported attachments reject explicitly', () => {
  for (const encoded of [undefined, '', '{', 'x'.repeat(16385), JSON.stringify({ version: 2 }), JSON.stringify({ version: 1, clientId: '../bad', connectionId: 'a' })]) assert.throws(() => parseAttachment(encoded));
  assert.deepEqual(parseAttachment(JSON.stringify({ version: 1, clientId: 'alice', connectionId: 'connection-1' })), { version: 1, clientId: 'alice', connectionId: 'connection-1' });
});
test('attachment tampering blocks reconstruct without replacing the healthy actor', async t => {
  const { lab } = await fixture(t); const handle = await lab.connect('alice'); await lab.reserve(handle, 'op1');
  const prior = lab.actor; const encoded = lab.connections.get(handle.connectionId);
  lab.connections.set(handle.connectionId, '{'); await assert.rejects(lab.reconstruct(), /malformed attachment/); assert.equal(lab.actor, prior);
  lab.connections.set(handle.connectionId, encoded); await lab.reconstruct(); assert.equal((await lab.reserve(handle, 'op1')).applied, false);
});
test('same operation ID with another client is conflict, not a valid replay', async t => {
  const { lab } = await fixture(t); const alice = await lab.connect('alice'), bob = await lab.connect('bob');
  await lab.reserve(alice, 'op1'); await lab.reconstruct(); await assert.rejects(lab.reserve(bob, 'op1'), /identity conflict/);
  assert.equal(lab.report().state.receipts.length, 1); assert.deepEqual(lab.report().violations, []);
});
test('repeated reconstruction retains sequence and capacity constraints', async t => {
  const { lab } = await fixture(t); const h = await lab.connect('alice');
  await lab.reserve(h, 'op1'); for (let n = 0; n < 12; n++) await lab.reconstruct();
  assert.equal((await lab.reserve(h, 'op2')).sequence, 2); await lab.reconstruct(); await assert.rejects(lab.reserve(h, 'op3'), /capacity/);
  assert.deepEqual(lab.report().violations, []);
});
test('memory-only counterexample also detects conflicting acknowledged slots', async t => {
  const { lab } = await fixture(t, { persistent: false }); const a = await lab.connect('alice'), b = await lab.connect('bob');
  await lab.reserve(a, 'op1'); await lab.reconstruct(); await lab.reserve(b, 'op2');
  assert.ok(lab.report().violations.includes('acknowledged slot collision: 0'));
});
test('deletion reducer keeps executable failing evidence and reaches a 1-minimum', async t => {
  const { dir } = await fixture(t); let n = 0;
  const fails = async candidate => {
    try { return (await runSchedule({ path: join(dir, `reduce-${n++}.json`), persistent: false, schedule: candidate })).violations.length > 0; }
    catch { return false; }
  };
  const noisy = [schedule[0], { type: 'connect', clientId: 'noise' }, ...schedule.slice(1)];
  const reduced = await minimize(noisy, fails);
  assert.equal(reduced.length, 3); assert.equal(await fails(reduced), true);
  for (let i = 0; i < reduced.length; i++) assert.equal(await fails(reduced.filter((_, j) => i !== j)), false);
});
test('invalid operation identities and unsupported configurations reject', async t => {
  const { lab, path } = await fixture(t); const h = await lab.connect('alice');
  await assert.rejects(lab.reserve(h, ''), /invalid opId/); await assert.rejects(lab.connect('../bad'), /invalid clientId/);
  await assert.rejects(LifecycleLab.open({ path, capacity: 0 }), /capacity/);
  assert.throws(() => validateState({ version: 1, capacity: 1, sequence: 1, receipts: [] }, 1), /state header/);
});
