import { createCipheriv, createDecipheriv, createHash, hkdfSync, randomBytes } from 'node:crypto';
import { mkdir, readdir, readFile, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

export type AuthSnapshot = Record<string,string>;
const MAX_FILES=256, MAX_BYTES=2*1024*1024, MAX_FILE_BYTES=256*1024;
const SAFE_NAME=/^[A-Za-z0-9._:@-]{1,180}$/;
const ENCRYPTED_VALUE=/^v1\.([A-Za-z0-9+/=]+)\.([A-Za-z0-9+/=]+)\.([A-Za-z0-9+/=]+)$/;

export async function snapshotAuthDirectory(authDir:string):Promise<{snapshot:AuthSnapshot;checksum:string}|null>{
  await mkdir(authDir,{recursive:true});
  const entries=(await readdir(authDir,{withFileTypes:true})).filter(e=>e.isFile()).sort((a,b)=>a.name.localeCompare(b.name));
  if(entries.length===0)return null;
  if(entries.length>MAX_FILES)throw new Error('auth_snapshot_file_count_exceeded');
  const snapshot:AuthSnapshot={}; let bytes=0;
  for(const entry of entries){
    if(!SAFE_NAME.test(entry.name))throw new Error('auth_snapshot_unsafe_filename');
    const content=await readFile(join(authDir,entry.name),'utf8');
    const size=Buffer.byteLength(content,'utf8');
    if(size>MAX_FILE_BYTES)throw new Error('auth_snapshot_file_too_large');
    bytes+=size; if(bytes>MAX_BYTES)throw new Error('auth_snapshot_too_large');
    snapshot[entry.name]=content;
  }
  return {snapshot,checksum:checksum(snapshot)};
}

export async function restoreAuthDirectory(authDir:string,snapshot:AuthSnapshot):Promise<void>{
  const entries=Object.entries(snapshot).sort(([a],[b])=>a.localeCompare(b));
  if(entries.length===0||entries.length>MAX_FILES)throw new Error('auth_snapshot_file_count_invalid');
  let bytes=0;
  for(const [name,content] of entries){
    if(!SAFE_NAME.test(name)||typeof content!=='string')throw new Error('auth_snapshot_invalid_entry');
    const size=Buffer.byteLength(content,'utf8');
    if(size>MAX_FILE_BYTES)throw new Error('auth_snapshot_file_too_large');
    bytes+=size; if(bytes>MAX_BYTES)throw new Error('auth_snapshot_too_large');
  }
  await rm(authDir,{recursive:true,force:true}); await mkdir(authDir,{recursive:true});
  for(const [name,content] of entries)await writeFile(join(authDir,name),content,{encoding:'utf8',mode:0o600});
}

export function deriveAuthEncryptionKey(workerToken:string):Buffer{
  if(workerToken.trim().length<24)throw new Error('WHATSAPP_WORKER_TOKEN_too_short_for_auth_kdf');
  return Buffer.from(hkdfSync('sha256',Buffer.from(workerToken,'utf8'),Buffer.alloc(0),Buffer.from('nora:baileys-auth:v1','utf8'),32));
}

export function encryptAuthSnapshot(snapshot:AuthSnapshot,key:Buffer,context:string):AuthSnapshot{
  validateSnapshot(snapshot);
  const encrypted:AuthSnapshot={};
  for(const [name,content] of Object.entries(snapshot)){
    const iv=randomBytes(12);
    const cipher=createCipheriv('aes-256-gcm',key,iv);
    cipher.setAAD(Buffer.from(`${context}:${name}`,'utf8'));
    const ciphertext=Buffer.concat([cipher.update(content,'utf8'),cipher.final()]);
    const tag=cipher.getAuthTag();
    encrypted[name]=`v1.${iv.toString('base64')}.${tag.toString('base64')}.${ciphertext.toString('base64')}`;
  }
  return encrypted;
}

export function decryptAuthSnapshot(snapshot:AuthSnapshot,key:Buffer,context:string):AuthSnapshot{
  validateSnapshot(snapshot);
  const decrypted:AuthSnapshot={};
  for(const [name,value] of Object.entries(snapshot)){
    const match=ENCRYPTED_VALUE.exec(value);
    if(!match)throw new Error('auth_snapshot_encryption_required');
    try{
      const [,ivBase64,tagBase64,ciphertextBase64]=match;
      if(!ivBase64||!tagBase64||!ciphertextBase64)throw new Error('invalid_envelope');
      const iv=Buffer.from(ivBase64,'base64');
      const tag=Buffer.from(tagBase64,'base64');
      const ciphertext=Buffer.from(ciphertextBase64,'base64');
      if(iv.length!==12||tag.length!==16)throw new Error('invalid_envelope');
      const decipher=createDecipheriv('aes-256-gcm',key,iv);
      decipher.setAAD(Buffer.from(`${context}:${name}`,'utf8'));
      decipher.setAuthTag(tag);
      decrypted[name]=Buffer.concat([decipher.update(ciphertext),decipher.final()]).toString('utf8');
    }catch{throw new Error('auth_snapshot_decryption_failed');}
  }
  validateSnapshot(decrypted);
  return decrypted;
}

function validateSnapshot(snapshot:AuthSnapshot):void{
  const entries=Object.entries(snapshot);
  if(entries.length===0||entries.length>MAX_FILES)throw new Error('auth_snapshot_file_count_invalid');
  let bytes=0;
  for(const [name,content] of entries){
    if(!SAFE_NAME.test(name)||typeof content!=='string')throw new Error('auth_snapshot_invalid_entry');
    const size=Buffer.byteLength(content,'utf8');
    if(size>MAX_FILE_BYTES*2)throw new Error('auth_snapshot_file_too_large');
    bytes+=size;if(bytes>MAX_BYTES*2)throw new Error('auth_snapshot_too_large');
  }
}

function checksum(snapshot:AuthSnapshot):string{
  const canonical=JSON.stringify(Object.fromEntries(Object.entries(snapshot).sort(([a],[b])=>a.localeCompare(b))));
  return createHash('sha256').update(canonical).digest('hex');
}
