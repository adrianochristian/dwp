import { runWorker } from '../../dist/worker.js';

// Short budgets exercise the production shutdown state machine without making
// the integration suite wait fifteen seconds for each fault scenario.
await runWorker({ drainMs: 50, cleanupMs: 50, deadlineMs: 500 });
