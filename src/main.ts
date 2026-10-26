import { createApplication } from './application.js';
import { createDatabase } from './infrastructure/persistence/database.js';

const port = process.env.PORT ?? '3000';
if (!/^\d+$/.test(port) || BigInt(port) > 65535n) {
  throw new Error('PORT must be an integer between 0 and 65535');
}

const app = await createApplication(await createDatabase());
await app.listen(Number(port), '0.0.0.0');
try { console.log(JSON.stringify({ event: 'server_started', address: await app.getUrl() })); }
catch { /* A diagnostic sink failure must not terminate the listening API. */ }
