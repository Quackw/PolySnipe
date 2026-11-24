import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { ensureIngressIndexes } from './ingress-indexes.mjs';
function fixture() {
  const db = new DatabaseSync(':memory:');
  db.exec(
    "CREATE TABLE deliveries(event_key TEXT,chat_id INT,wallet TEXT,source_timestamp INT,state TEXT);CREATE TABLE rtds_legacy_fences(event_key TEXT,chat_id INT,wallet TEXT,before_ms INT);INSERT INTO deliveries VALUES('first',1,'wallet',1,'DELIVERED'),('unknown',1,'wallet',2,'DELIVERY_UNKNOWN');INSERT INTO rtds_legacy_fences VALUES('old',1,'wallet',1000)"
  );
  return db;
}
test('indexes preserve every data row and are idempotent', () => {
  const db = fixture();
  try {
    const a = db.prepare('SELECT * FROM deliveries ORDER BY event_key').all(),
      b = db.prepare('SELECT * FROM rtds_legacy_fences').all();
    for (let i = 0; i < 2; i++)
      assert.deepEqual(ensureIngressIndexes(db), {
        verified_indexes: 2,
        data_rows_changed: 0,
        delivery_state_cache: false
      });
    assert.deepEqual(db.prepare('SELECT * FROM deliveries ORDER BY event_key').all(), a);
    assert.deepEqual(db.prepare('SELECT * FROM rtds_legacy_fences').all(), b);
  } finally {
    db.close();
  }
});
test('existing authoritative pair reads use covering indexes rather than a history table scan', () => {
  const db = fixture();
  try {
    ensureIngressIndexes(db);
    for (const [sql, index] of [
      [
        "SELECT event_key,source_timestamp FROM deliveries WHERE wallet='wallet' AND chat_id=1",
        'idx_rtds_delivery_pair_history'
      ],
      [
        "SELECT MAX(before_ms) FROM rtds_legacy_fences WHERE wallet='wallet' AND chat_id=1",
        'idx_rtds_fence_pair_boundary'
      ]
    ])
      assert.ok(
        db
          .prepare('EXPLAIN QUERY PLAN ' + sql)
          .all()
          .some((x) => x.detail.includes(index))
      );
  } finally {
    db.close();
  }
});
test('an unrelated index with the same name is rejected without dropping it', () => {
  const db = fixture();
  try {
    db.exec('CREATE INDEX idx_rtds_delivery_pair_history ON deliveries(state)');
    assert.throws(() => ensureIngressIndexes(db), /NAME_CONFLICT/);
    assert.deepEqual(
      db
        .prepare('PRAGMA index_info(idx_rtds_delivery_pair_history)')
        .all()
        .map((x) => x.name),
      ['state']
    );
    assert.equal(
      db.prepare("SELECT COUNT(*) n FROM sqlite_master WHERE name='idx_rtds_fence_pair_boundary'").get().n,
      0
    );
  } finally {
    db.close();
  }
});
test('a rollback executable can retain the additive indexes and original delivery states', () => {
  const db = fixture();
  try {
    ensureIngressIndexes(db);
    db.prepare("UPDATE deliveries SET state='DELIVERED' WHERE event_key='unknown'").run();
    assert.equal(
      db.prepare("SELECT state FROM deliveries WHERE event_key='unknown'").get().state,
      'DELIVERED'
    );
    assert.equal(db.prepare('SELECT COUNT(*) n FROM deliveries').get().n, 2);
  } finally {
    db.close();
  }
});
