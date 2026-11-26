/** @param {{pragma: (statement: string) => unknown}} db */
export function configureSqlite(db) {
  db.pragma("busy_timeout = 5000");
  db.pragma("journal_mode = WAL");
}
