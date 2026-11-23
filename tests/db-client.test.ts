import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { expect, it, vi } from "vitest";
import { openDb } from "../src/db/client.js";

it("opens in-memory SQLite without creating a directory and configures waits and synchronization", () => {
  const mkdir = vi.spyOn(fs, "mkdirSync");
  const db = openDb(":memory:");
  try {
    expect(mkdir).not.toHaveBeenCalled();
    expect(db.memory).toBe(true);
    expect(db.pragma("busy_timeout", { simple: true })).toBe(5_000);
    expect(db.pragma("synchronous", { simple: true })).toBe(1);
    expect(db.pragma("foreign_keys", { simple: true })).toBe(1);
    expect(db.prepare("SELECT 1 AS value").get()).toEqual({ value: 1 });
  } finally {
    db.close();
    mkdir.mockRestore();
  }
});

it("uses WAL and NORMAL synchronization for on-disk databases", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "polysnipe-db-"));
  try {
    const db = openDb(path.join(root, "nested", "state.db"));
    try {
      expect(db.pragma("journal_mode", { simple: true })).toBe("wal");
      expect(db.pragma("synchronous", { simple: true })).toBe(1);
      expect(db.pragma("busy_timeout", { simple: true })).toBe(5_000);
    } finally {
      db.close();
    }
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});
