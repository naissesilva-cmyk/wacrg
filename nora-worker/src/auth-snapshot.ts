import { createHash } from 'node:crypto';
import { mkdir, readdir, readFile, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

export type AuthSnapshot = Record<string,string>;
const MAX_FILES=256, MAX_BYTES=2*1024*1024, MAX_FILE_BYTES=256*1024;
const SAFE_NAME=/^[A-Za-z0-9._:@-]{1,180}$/;

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

function checksum(snapshot:AuthSnapshot):string{
  const canonical=JSON.stringify(Object.fromEntries(Object.entries(snapshot).sort(([a],[b])=>a.localeCompare(b))));
  return createHash('sha256').update(canonical).digest('hex');
}
