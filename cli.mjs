import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { LifecycleLab, runSchedule, minimize } from './lab.mjs';

async function main(args) {
  if (args[0] === 'reserve' && args.length === 4) {
    const [, path, clientId, opId] = args;
    const lab = await LifecycleLab.open({ path }); const connection = await lab.connect(clientId);
    console.log(JSON.stringify(await lab.reserve(connection, opId))); return;
  }
  if (args[0] === 'inspect' && args.length === 2) {
    const lab = await LifecycleLab.open({ path: args[1] }); console.log(JSON.stringify(lab.report().state)); return;
  }
  if (args.length && !(args.length === 1 && args[0] === 'demo')) throw new Error('Usage: node cli.mjs [demo | reserve STATE_FILE CLIENT OP_ID | inspect STATE_FILE]');
  const directory = await mkdtemp(join(tmpdir(), 'wakeproof-'));
  try {
    const warm = [{ type: 'connect', clientId: 'alice' }, { type: 'reserve', clientId: 'alice', opId: 'op1' }];
    const noisy = [warm[0], { type: 'connect', clientId: 'noise' }, warm[1], { type: 'reconstruct' }, { type: 'reserve', clientId: 'alice', opId: 'op1' }];
    const results = []; let n = 0;
    for (const [label, schedule] of [['warm', warm], ['reconstruction_and_replay', noisy]]) {
      for (const persistent of [false, true]) results.push({ label, ...await runSchedule({ path: join(directory, `state-${n++}.json`), persistent, schedule }) });
    }
    const minimal = await minimize(noisy, async schedule => {
      try { return (await runSchedule({ path: join(directory, `reduce-${n++}.json`), persistent: false, schedule })).violations.length > 0; }
      catch { return false; }
    });
    console.log(JSON.stringify({ integration_status: 'unverified', scope: 'local lifecycle fault model, actual atomic disk file updates', results,
      minimized_loss_schedule: minimal, minimization: '1-minimal under deletion; invalid schedules excluded' }, null, 2));
  } finally { await rm(directory, { recursive: true, force: true }); }
}
main(process.argv.slice(2)).catch(error => { console.error(error.message); process.exitCode = 1; });
