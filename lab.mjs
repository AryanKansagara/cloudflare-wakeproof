import { readFile, writeFile, rename, unlink, mkdir } from 'node:fs/promises';
import { dirname } from 'node:path';
import { randomUUID } from 'node:crypto';

const MAX_ATTACHMENT_BYTES = 16384; // Local contract bound, not a claim about all host runtime limits.
export class ContractError extends Error {}
function identity(value, field) {
  if (typeof value !== 'string' || !/^[A-Za-z0-9_.:-]{1,80}$/.test(value)) throw new ContractError(`invalid ${field}`);
  return value;
}
function capacityCheck(capacity) {
  if (!Number.isInteger(capacity) || capacity < 1 || capacity > 64) throw new ContractError('capacity must be 1..64');
}
export function validateState(state, capacity) {
  capacityCheck(capacity);
  if (!state || state.version !== 1 || state.capacity !== capacity || !Array.isArray(state.receipts)
      || state.receipts.length > capacity || state.sequence !== state.receipts.length) throw new ContractError('invalid durable state header');
  const ids = new Set(), slots = new Set();
  for (const [i, receipt] of state.receipts.entries()) {
    identity(receipt?.opId, 'opId'); identity(receipt.clientId, 'clientId');
    if (!Number.isInteger(receipt.slot) || receipt.slot < 0 || receipt.slot >= capacity || slots.has(receipt.slot)
        || ids.has(receipt.opId) || receipt.sequence !== i + 1) throw new ContractError('invalid durable receipt');
    ids.add(receipt.opId); slots.add(receipt.slot);
  }
  return structuredClone(state);
}
function emptyState(capacity) { capacityCheck(capacity); return { version: 1, capacity, sequence: 0, receipts: [] }; }
export function parseAttachment(encoded) {
  if (typeof encoded !== 'string' || Buffer.byteLength(encoded) > MAX_ATTACHMENT_BYTES) throw new ContractError('inaccessible or oversized attachment');
  let data; try { data = JSON.parse(encoded); } catch { throw new ContractError('malformed attachment'); }
  if (!data || data.version !== 1) throw new ContractError('unsupported attachment version');
  identity(data.clientId, 'attachment clientId'); identity(data.connectionId, 'attachment connectionId');
  return { version: 1, clientId: data.clientId, connectionId: data.connectionId };
}
export class DiskStore {
  constructor(path, capacity) { this.path = path; this.capacity = capacity; capacityCheck(capacity); }
  async load() {
    let raw;
    try { raw = await readFile(this.path, 'utf8'); }
    catch (error) { if (error.code === 'ENOENT') return emptyState(this.capacity); throw error; }
    let state; try { state = JSON.parse(raw); } catch { throw new ContractError('malformed durable JSON; refusing reset'); }
    return validateState(state, this.capacity);
  }
  async save(state) {
    validateState(state, this.capacity);
    await mkdir(dirname(this.path), { recursive: true });
    const temporary = `${this.path}.${randomUUID()}.tmp`;
    try { await writeFile(temporary, JSON.stringify(state) + '\n', { flag: 'wx' }); await rename(temporary, this.path); }
    finally { await unlink(temporary).catch(error => { if (error.code !== 'ENOENT') throw error; }); }
  }
}
export class RoomActor {
  static async open(store, persistent = true) {
    return new RoomActor(store, persistent, persistent ? await store.load() : emptyState(store.capacity));
  }
  constructor(store, persistent, state) { this.store = store; this.persistent = persistent; this.state = state; }
  snapshot() { return structuredClone(this.state); }
  async reserve(clientId, opId) {
    identity(clientId, 'clientId'); identity(opId, 'opId');
    const existing = this.state.receipts.find(receipt => receipt.opId === opId);
    if (existing) {
      if (existing.clientId !== clientId) throw new ContractError('operation identity conflict');
      return { ...existing, applied: false };
    }
    const used = new Set(this.state.receipts.map(receipt => receipt.slot));
    let slot = 0; while (used.has(slot)) slot++;
    if (slot >= this.state.capacity) throw new ContractError('capacity exhausted');
    const receipt = { clientId, opId, slot, sequence: this.state.sequence + 1 };
    const next = { ...this.state, sequence: receipt.sequence, receipts: [...this.state.receipts, receipt] };
    if (this.persistent) await this.store.save(next); // ACK only after atomic replacement succeeds.
    this.state = next;
    return { ...receipt, applied: true };
  }
}
// The harness owns one serial event queue, as an explicit local model assumption.
// The actor is not safe for independent direct concurrent mutation or multiple writers.
export class LifecycleLab {
  static async open({ path, capacity = 2, persistent = true, store } = {}) {
    const storage = store ?? new DiskStore(path, capacity);
    return new LifecycleLab(storage, persistent, await RoomActor.open(storage, persistent));
  }
  constructor(store, persistent, actor) {
    this.store = store; this.persistent = persistent; this.actor = actor;
    this.connections = new Map(); this.clients = new Map(); this.tail = Promise.resolve();
    this.acks = new Map(); this.applied = new Map(); this.events = []; this.violations = [];
  }
  enqueue(fn) {
    const pending = this.tail.then(fn);
    this.tail = pending.catch(() => {}); // A rejected operation must not poison later work.
    return pending;
  }
  connect(clientId) {
    return this.enqueue(() => {
      identity(clientId, 'clientId');
      const prior = this.clients.get(clientId); if (prior) this.connections.delete(prior);
      const connectionId = randomUUID();
      const attachment = { version: 1, clientId, connectionId };
      this.clients.set(clientId, connectionId); this.connections.set(connectionId, JSON.stringify(attachment));
      this.events.push({ type: 'connect', clientId });
      return { clientId, connectionId };
    });
  }
  disconnect(handle) {
    return this.enqueue(() => {
      this.requireConnection(handle); this.clients.delete(handle.clientId); this.connections.delete(handle.connectionId);
      this.events.push({ type: 'disconnect', clientId: handle.clientId });
    });
  }
  requireConnection(handle) {
    if (!handle || this.clients.get(handle.clientId) !== handle.connectionId) throw new ContractError('stale connection');
    const attachment = parseAttachment(this.connections.get(handle.connectionId));
    if (attachment.clientId !== handle.clientId || attachment.connectionId !== handle.connectionId) throw new ContractError('connection attachment identity mismatch');
    return attachment;
  }
  reserve(handle, opId) {
    return this.enqueue(async () => {
      this.requireConnection(handle);
      const receipt = await this.actor.reserve(handle.clientId, opId);
      if (receipt.applied) this.applied.set(opId, (this.applied.get(opId) ?? 0) + 1);
      this.acks.set(opId, { ...receipt });
      this.events.push({ type: 'ack', ...receipt });
      this.inspectInvariants();
      return receipt;
    });
  }
  reconstruct() {
    return this.enqueue(async () => {
      // Serialize attachments across the instance replacement; no external network remains open.
      const serialized = JSON.stringify([...this.connections.entries()]);
      const restored = new Map(JSON.parse(serialized));
      for (const [id, encoded] of restored) {
        const a = parseAttachment(encoded);
        if (a.connectionId !== id || this.clients.get(a.clientId) !== id) throw new ContractError('restored attachment identity mismatch');
      }
      const replacement = await RoomActor.open(this.store, this.persistent);
      this.actor = replacement; this.connections = restored;
      this.events.push({ type: 'reconstruct' }); this.inspectInvariants();
    });
  }
  inspectInvariants() {
    const state = this.actor.snapshot(); const found = [];
    for (const [opId, ack] of this.acks) {
      const stored = state.receipts.find(receipt => receipt.opId === opId);
      if (!stored || stored.clientId !== ack.clientId || stored.slot !== ack.slot || stored.sequence !== ack.sequence) found.push(`acknowledged reservation lost: ${opId}`);
    }
    for (const [opId, count] of this.applied) if (count > 1) found.push(`operation applied twice: ${opId}`);
    const owners = new Map();
    for (const [opId, receipt] of this.acks) {
      if (owners.has(receipt.slot) && owners.get(receipt.slot) !== opId) found.push(`acknowledged slot collision: ${receipt.slot}`);
      owners.set(receipt.slot, opId);
    }
    if (this.acks.size > state.capacity) found.push('acknowledged capacity exceeded');
    for (const violation of found) if (!this.violations.includes(violation)) this.violations.push(violation);
    return found;
  }
  report() {
    return { policy: this.persistent ? 'persistent' : 'memory_only_negative_control',
      scope: 'local JS object replacement and actual disk state; no Cloudflare host or WebSocket validation',
      state: this.actor.snapshot(), events: structuredClone(this.events), violations: [...this.violations] };
  }
}
export async function runSchedule({ path, persistent, schedule, capacity = 2 }) {
  const lab = await LifecycleLab.open({ path, persistent, capacity }); const handles = new Map();
  for (const action of schedule) {
    switch (action.type) {
      case 'connect': handles.set(action.clientId, await lab.connect(action.clientId)); break;
      case 'reserve': {
        if (!handles.has(action.clientId)) throw new ContractError('schedule has no connected client');
        await lab.reserve(handles.get(action.clientId), action.opId); break;
      }
      case 'reconstruct': await lab.reconstruct(); break;
      default: throw new ContractError('unknown schedule action');
    }
  }
  return lab.report();
}
// Delta reduction of an authored failing schedule. Invalid/disconnected schedules are not evidence.
// Caller must provide a fresh store path for EACH predicate evaluation.
export async function minimize(schedule, fails) {
  let reduced = structuredClone(schedule), changed = true;
  while (changed) {
    changed = false;
    for (let i = 0; i < reduced.length; i++) {
      const candidate = reduced.filter((_, index) => index !== i);
      if (await fails(candidate)) { reduced = candidate; changed = true; break; }
    }
  }
  return reduced; // 1-minimal under deletion, not globally minimum.
}
