export type OutboxMessage = {
  id: string;
  tenant_id: string;
  lead_id: string | null;
  phone_e164: string;
  message_body: string;
  attempts: number;
};

export type DispatchAuthorization = {
  authorized?: boolean;
  allowed?: boolean;
  reason: string;
  message_id: string;
};

export type ClaimOptions = {
  minIntervalSeconds: number;
  minContactIntervalSeconds: number;
  maxContactMessagesPerDay: number;
};

export interface WorkerDatabase {
  claimNext(tenantId: string, options: ClaimOptions): Promise<OutboxMessage | null>;
  recheck(tenantId: string, messageId: string): Promise<DispatchAuthorization>;
  recordProviderAck(tenantId: string, messageId: string, providerMessageId: string): Promise<void>;
  complete(tenantId: string, messageId: string, providerMessageId: string): Promise<void>;
  fail(tenantId: string, messageId: string, error: string, retrySafe: boolean, maxAttempts: number, baseBackoffSeconds: number): Promise<void>;
  quarantine(tenantId: string, messageId: string, reason: string): Promise<void>;
  recoverStale(tenantId: string, staleAfterSeconds: number): Promise<number>;
  registerOptOut(tenantId: string, phoneE164: string): Promise<number>;
  renewLease(tenantId: string, instanceId: string, leaseSeconds: number): Promise<boolean>;
  releaseLease(tenantId: string, instanceId: string): Promise<void>;
}

export interface WhatsAppSender {
  isReady(): boolean;
  send(phoneE164: string, body: string, idempotencyKey: string): Promise<{ id: string }>;
}

export class DispatchError extends Error {
  constructor(message: string, readonly retrySafe: boolean) {
    super(message);
    this.name = 'DispatchError';
  }
}

export type ProcessResult = 'idle' | 'cancelled' | 'sent' | 'retry' | 'failed' | 'uncertain';

export async function processNextMessage(
  db: WorkerDatabase,
  sender: WhatsAppSender,
  options: ClaimOptions & {
    tenantId: string;
    maxAttempts: number;
    baseBackoffSeconds: number;
  },
): Promise<ProcessResult> {
  if (!sender.isReady()) return 'idle';

  const message = await db.claimNext(options.tenantId, options);
  if (!message) return 'idle';

  const finalCheck = await db.recheck(options.tenantId, message.id);
  if (!finalCheck.allowed) return 'cancelled';

  if (!sender.isReady()) {
    await db.fail(options.tenantId, message.id, 'whatsapp_not_connected', true, options.maxAttempts, options.baseBackoffSeconds);
    return 'retry';
  }

  let provider: { id: string };
  try {
    provider = await sender.send(message.phone_e164, message.message_body, message.id);
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error);
    if (error instanceof DispatchError) {
      await db.fail(options.tenantId, message.id, detail, error.retrySafe, options.maxAttempts, options.baseBackoffSeconds);
      return error.retrySafe ? 'retry' : 'failed';
    }
    await db.quarantine(options.tenantId, message.id, 'provider_outcome_unknown');
    return 'uncertain';
  }

  try {
    await db.recordProviderAck(options.tenantId, message.id, provider.id);
  } catch {
    // The provider already acknowledged the send. Do not retry because ACK persistence failed.
    // Completion may still succeed; otherwise the item remains uncertain for human recovery.
  }

  try {
    await db.complete(options.tenantId, message.id, provider.id);
    return 'sent';
  } catch {
    return 'uncertain';
  }
}
