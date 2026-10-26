import { runWorker } from '../../dist/worker.js';

const original = console.log.bind(console);
console.log = (...values: unknown[]) => {
  const first = values[0];
  if (typeof first === 'string' && first.includes('"event":"worker_started"')) {
    throw new Error('startup diagnostic sink unavailable');
  }
  original(...values);
};
await runWorker();
