import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";
import { configureSqlite } from "./sqlite-pragmas.mjs";

export function openDb(dbPath: string): Database.Database {
  if (dbPath !== ":memory:") {
    fs.mkdirSync(path.dirname(dbPath), { recursive: true });
  }

  const db = new Database(dbPath);
  configureSqlite(db);
  db.pragma("synchronous = NORMAL");
  db.pragma("foreign_keys = ON");

  return db;
}
