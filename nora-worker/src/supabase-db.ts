import type { ClaimOptions, DispatchAuthorization, OutboxMessage, WorkerDatabase } from './outbox.js';

export interface InboundWhatsAppMessage { phoneE164: string; providerMessageId: string; text: string; receivedAt?: string; }
export type PairingState = 'starting' | 'qr' | 'connected' | 'disconnected' | 'error';

type GatewayResponse<T> = { ok?: boolean; data?: T; error?: string; detail?: string };

type ExtendedWorkerDatabase = WorkerDatabase & {
  registerOptOut(tenantId:string,phoneE164:string):Promise<number>;
  registerInbound(tenantId:string,message:InboundWhatsAppMessage):Promise<string>;
  renewLease(tenantId:string,instanceId:string,leaseSeconds:number):Promise<boolean>;
  releaseLease(tenantId:string,instanceId:string):Promise<void>;
  recoverStale(tenantId:string,staleAfterSeconds:number):Promise<number>;
  recordPairingState(tenantId:string,instanceId:string,state:PairingState,qrCode?:string|null,qrExpiresAt?:string|null,lastError?:string|null):Promise<void>;
};

export function createWorkerDatabase(url: string, workerToken: string): ExtendedWorkerDatabase {
  return new GatewayWorkerDatabase(url, workerToken);
}

class GatewayWorkerDatabase implements ExtendedWorkerDatabase {
  private readonly endpoint: string;
  constructor(url:string, private readonly workerToken:string) {
    this.endpoint = `${url.replace(/\/+$/, '')}/functions/v1/whatsapp-worker-gateway`;
  }

  async claimNext(tenantId:string,options:ClaimOptions):Promise<OutboxMessage|null>{
    const data=await this.call<OutboxMessage[]>('claim',tenantId,{min_interval_seconds:options.minIntervalSeconds,min_contact_interval_seconds:options.minContactIntervalSeconds,max_contact_messages_per_day:options.maxContactMessagesPerDay});
    return firstOrNull<OutboxMessage>(data);
  }
  async recheck(tenantId:string,messageId:string):Promise<DispatchAuthorization>{
    const data=await this.call<DispatchAuthorization[]>('recheck',tenantId,{message_id:messageId});
    return first<DispatchAuthorization>(data);
  }
  async complete(tenantId:string,messageId:string,providerMessageId:string):Promise<void>{
    const data=await this.call<boolean>('complete',tenantId,{message_id:messageId,provider_message_id:providerMessageId});
    if(data!==true)throw new Error('dispatch_completion_rejected');
  }
  async fail(tenantId:string,messageId:string,detail:string,retrySafe:boolean,maxAttempts:number,baseBackoffSeconds:number):Promise<void>{
    await this.call('fail',tenantId,{message_id:messageId,error:detail,retry_safe:retrySafe,max_attempts:maxAttempts,base_backoff_seconds:baseBackoffSeconds});
  }
  async recoverStale(tenantId:string,staleAfterSeconds:number):Promise<number>{
    const data=await this.call<number>('recover_stale',tenantId,{stale_after_seconds:staleAfterSeconds});
    return typeof data==='number'?data:0;
  }
  async registerOptOut(tenantId:string,phoneE164:string):Promise<number>{
    const data=await this.call<number>('opt_out',tenantId,{phone_e164:phoneE164});
    return typeof data==='number'?data:0;
  }
  async registerInbound(tenantId:string,message:InboundWhatsAppMessage):Promise<string>{
    const data=await this.call<string>('inbound',tenantId,{phone_e164:message.phoneE164,provider_message_id:message.providerMessageId,message_text:message.text,received_at:message.receivedAt??new Date().toISOString()});
    if(typeof data!=='string')throw new Error('inbound_registration_rejected');
    return data;
  }
  async renewLease(tenantId:string,instanceId:string,leaseSeconds:number):Promise<boolean>{
    const data=await this.call<boolean>('lease_renew',tenantId,{instance_id:instanceId,lease_seconds:leaseSeconds});
    return data===true;
  }
  async releaseLease(tenantId:string,instanceId:string):Promise<void>{
    await this.call('lease_release',tenantId,{instance_id:instanceId});
  }
  async recordPairingState(tenantId:string,instanceId:string,state:PairingState,qrCode:string|null=null,qrExpiresAt:string|null=null,lastError:string|null=null):Promise<void>{
    await this.call('pairing_state',tenantId,{instance_id:instanceId,state,qr_code:qrCode,qr_expires_at:qrExpiresAt,last_error:lastError});
  }

  private async call<T=unknown>(action:string,tenantId:string,payload:Record<string,unknown>):Promise<T>{
    const response=await fetch(this.endpoint,{method:'POST',headers:{'content-type':'application/json','x-nora-worker-token':this.workerToken},body:JSON.stringify({action,tenant_id:tenantId,...payload}),signal:AbortSignal.timeout(20000)});
    const body=await response.json().catch(()=>({})) as GatewayResponse<T>;
    if(!response.ok||body.error)throw new Error(body.detail||body.error||`worker_gateway_http_${response.status}`);
    return body.data as T;
  }
}

function first<T>(data:unknown):T{if(!Array.isArray(data)||data.length===0)throw new Error('Dispatch RPC returned no result');return data[0] as T;}
function firstOrNull<T>(data:unknown):T|null{if(!Array.isArray(data)||data.length===0)return null;return data[0] as T;}
