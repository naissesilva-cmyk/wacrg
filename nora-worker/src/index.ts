import { createServer } from 'node:http';
import { BaileysSender } from './baileys-sender.js';
import { processNextMessage } from './outbox.js';
import { createWorkerDatabase } from './supabase-db.js';

const config = loadConfig();
if (config.allowDirectSend) throw new Error('ALLOW_DIRECT_SEND must remain false; Nora only dispatches through the outbox.');
if (config.processor !== 'baileys') throw new Error('WHATSAPP_OUTBOX_PROCESSOR must be baileys for this worker.');

const db = createWorkerDatabase(config.supabaseUrl, config.workerToken);
const sender = new BaileysSender(
  config.authDir,
  async (phoneE164) => {
    const affected = await db.registerOptOut(config.tenantId, phoneE164);
    if (affected === 0) console.warn('[worker] opt-out recebido sem lead correspondente');
  },
  async (message) => {
    await db.registerInbound(config.tenantId, message);
    console.log('[worker] inbound WhatsApp registrado', { providerMessageId: message.providerMessageId });
  },
  async (pairing) => {
    await db.recordPairingState(config.tenantId, config.instanceId, pairing.state, pairing.qrCode ?? null, pairing.qrExpiresAt ?? null, pairing.lastError ?? null);
  },
);

let stopping = false;
let lastLeaseRenewalAt = 0;

const healthServer = createServer((request, response) => {
  const leaseFresh = lastLeaseRenewalAt > 0 && Date.now() - lastLeaseRenewalAt < config.leaseSeconds * 1000;
  const connected = sender.isReady();
  const path = request.url?.split('?')[0] ?? '/';

  if (path !== '/healthz' && path !== '/readyz') {
    response.writeHead(404, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
    response.end(JSON.stringify({ error: 'not_found' }));
    return;
  }

  const ready = !stopping && leaseFresh && connected;
  const healthy = !stopping && leaseFresh;
  const ok = path === '/readyz' ? ready : healthy;
  response.writeHead(ok ? 200 : 503, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
  response.end(JSON.stringify({ status: ok ? 'ok' : 'not_ready', lease_fresh: leaseFresh, whatsapp_connected: connected, stopping }));
});
healthServer.listen(config.healthPort, '0.0.0.0', () => console.log('[worker] health endpoint ativo', { port: config.healthPort }));

async function main(): Promise<void> {
  const acquired = await db.renewLease(config.tenantId, config.instanceId, config.leaseSeconds);
  if (!acquired) throw new Error('worker_lease_unavailable');
  lastLeaseRenewalAt = Date.now();
  await db.recordPairingState(config.tenantId, config.instanceId, 'starting');
  try {
    const quarantined = await db.recoverStale(config.tenantId, config.staleAfterSeconds);
    if (quarantined > 0) console.warn('[worker] stale dispatches quarantined', { count: quarantined });
    await sender.start();
    console.log('[worker] Nora WhatsApp outbox worker started');
    while (!stopping) {
      try {
        await ensureLease();
        const result = await processNextMessage(db, sender, {
          tenantId: config.tenantId,
          minIntervalSeconds: config.minIntervalSeconds,
          minContactIntervalSeconds: config.minContactIntervalSeconds,
          maxContactMessagesPerDay: config.maxContactMessagesPerDay,
          maxAttempts: config.maxAttempts,
          baseBackoffSeconds: config.baseBackoffSeconds,
        });
        if (result !== 'idle') console.log('[worker] dispatch result', result);
        await sleep(result === 'idle' ? config.pollIntervalMs : 250);
      } catch (error) {
        if (error instanceof Error && error.message === 'worker_lease_lost') throw error;
        console.error('[worker] iteration failed', error instanceof Error ? error.message : 'unknown_error');
        await sleep(config.errorBackoffMs);
      }
    }
  } finally {
    sender.stop();
    await db.recordPairingState(config.tenantId, config.instanceId, 'disconnected').catch(()=>undefined);
    await db.releaseLease(config.tenantId, config.instanceId).catch((error) => console.error('[worker] lease release failed', error instanceof Error ? error.message : 'unknown_error'));
  }
}

async function ensureLease(): Promise<void> {
  if (Date.now() - lastLeaseRenewalAt < config.leaseRenewIntervalMs) return;
  const renewed = await db.renewLease(config.tenantId, config.instanceId, config.leaseSeconds);
  if (!renewed) throw new Error('worker_lease_lost');
  lastLeaseRenewalAt = Date.now();
}

function shutdown(signal: string): void {
  if (stopping) return;
  stopping = true;
  console.log(`[worker] ${signal}; stopping`);
  healthServer.close();
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
void main().catch(async (error) => {
  console.error('[worker] fatal startup error', error);
  await db.recordPairingState(config.tenantId, config.instanceId, 'error', null, null, error instanceof Error ? error.message : 'fatal_startup_error').catch(()=>undefined);
  healthServer.close();
  process.exitCode = 1;
});

function loadConfig() {
  const required = (name: string): string => {
    const value = process.env[name]?.trim();
    if (!value) throw new Error(`Missing required environment variable ${name}`);
    return value;
  };
  const number = (name: string, fallback: number): number => {
    const value = Number(process.env[name] ?? fallback);
    if (!Number.isFinite(value) || value <= 0) throw new Error(`Invalid ${name}`);
    return value;
  };
  const integer = (name: string, fallback: number): number => {
    const value = number(name, fallback);
    if (!Number.isInteger(value) || value > 65535) throw new Error(`Invalid ${name}`);
    return value;
  };
  const uuid = (name: string): string => {
    const value = required(name);
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value)) throw new Error(`Invalid ${name}`);
    return value;
  };
  return {
    supabaseUrl: required('SUPABASE_URL'),
    workerToken: required('WHATSAPP_WORKER_TOKEN'),
    tenantId: uuid('WHATSAPP_TENANT_ID'),
    instanceId: process.env.WORKER_INSTANCE_ID?.trim() || `${process.env.HOSTNAME?.trim() || 'nora-worker'}-${process.pid}`,
    authDir: process.env.BAILEYS_AUTH_DIR?.trim() || '/tmp/nora-auth',
    processor: process.env.WHATSAPP_OUTBOX_PROCESSOR?.trim() || 'baileys',
    allowDirectSend: process.env.ALLOW_DIRECT_SEND === 'true',
    healthPort: integer('HEALTH_PORT', 10000),
    minIntervalSeconds: number('MIN_INTERVAL_BETWEEN_SENDS', 8),
    minContactIntervalSeconds: number('MIN_INTERVAL_BETWEEN_CONTACT_SENDS', 1800),
    maxContactMessagesPerDay: number('MAX_CONTACT_MESSAGES_PER_DAY', 3),
    pollIntervalMs: number('OUTBOX_POLL_INTERVAL_MS', 2000),
    errorBackoffMs: number('WORKER_ERROR_BACKOFF_MS', 5000),
    maxAttempts: number('OUTBOX_MAX_ATTEMPTS', 5),
    baseBackoffSeconds: number('OUTBOX_BASE_BACKOFF_SECONDS', 60),
    staleAfterSeconds: number('OUTBOX_STALE_AFTER_SECONDS', 300),
    leaseSeconds: number('WORKER_LEASE_SECONDS', 60),
    leaseRenewIntervalMs: number('WORKER_LEASE_RENEW_INTERVAL_MS', 20000),
  };
}

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
