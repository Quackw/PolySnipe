import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { DatabaseSync } from 'node:sqlite';
test('production due query never replays unknown or possible-duplicate rows', () => {
  const source = fs.readFileSync(new URL('./fast.mjs', import.meta.url), 'utf8');
  const sql = source.match(/const due\s*=\s*db\.prepare\(\s*`([\s\S]*?)`\s*,?\s*\)/)[1];
  const db = new DatabaseSync(':memory:');
  db.exec(
    'CREATE TABLE deliveries(state TEXT,next_at_ms INTEGER,possible_duplicate INTEGER,ambiguous_retries INTEGER)'
  );
  const add = db.prepare('INSERT INTO deliveries VALUES(?,?,?,?)');
  add.run('DELIVERY_UNKNOWN', 0, 0, 0);
  add.run('PENDING', 0, 1, 0);
  add.run('FAILED_RETRYABLE', 0, 1, 0);
  assert.equal(db.prepare(sql).get(100), undefined);
  add.run('PENDING', 1, 0, 0);
  assert.equal(db.prepare(sql).get(100).state, 'PENDING');
  db.close();
});
