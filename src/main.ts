import { createApplication } from './application.js';

const port = process.env.PORT ?? '3000';
if (!/^\d+$/.test(port) || BigInt(port) > 65535n) {
  throw new Error('PORT must be an integer between 0 and 65535');
}

const app = await createApplication();
await app.listen(Number(port), '0.0.0.0');
console.log(JSON.stringify({ event: 'server_started', address: await app.getUrl() }));
