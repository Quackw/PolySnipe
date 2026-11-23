// Additive indexes for the existing authoritative reads. No policy, identity,
// subscription, claim or delivery row is changed, cached or reconstructed.
const specs=[['idx_rtds_delivery_pair_history','deliveries',['wallet','chat_id','event_key','source_timestamp']],['idx_rtds_fence_pair_boundary','rtds_legacy_fences',['wallet','chat_id','before_ms']]];
export function ensureIngressIndexes(db){
 const selected=specs.filter(([,table])=>db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(table));
 for(const [name,table,columns] of selected){
  const prior=db.prepare("SELECT tbl_name FROM sqlite_master WHERE type='index' AND name=?").get(name);
  if(prior&&(prior.tbl_name!==table||JSON.stringify(db.prepare('PRAGMA index_info('+name+')').all().map(x=>x.name))!==JSON.stringify(columns)))throw Error('INGRESS_INDEX_NAME_CONFLICT');
 }
 db.exec('BEGIN IMMEDIATE');
 try{for(const [name,table,columns] of selected)db.exec(`CREATE INDEX IF NOT EXISTS ${name} ON ${table}(${columns.join(',')})`);db.exec('COMMIT');}
 catch(error){try{db.exec('ROLLBACK');}catch{}throw error;}
 return {verified_indexes:selected.length,data_rows_changed:0,delivery_state_cache:false};
}
