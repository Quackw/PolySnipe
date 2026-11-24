import { getEventListeners } from "node:events";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { main } from "../src/index.js";
import { openDb } from "../src/db/client.js";
import { notifyFatalError } from "../src/services/runtimeErrors.js";
import { testConfig } from "./helpers.js";

const runtime = vi.hoisted(() => ({
  botStart: vi.fn(),
  botStop: vi.fn(),
  schedulerStart: vi.fn(),
  schedulerStop: vi.fn(),
  schedulerDrain: vi.fn()
}));

vi.mock("../src/bot.js", () => ({
  createTelegramBot: () => ({
    api: { config: { use: vi.fn() } },
    isRunning: () => true,
    start: runtime.botStart,
    stop: runtime.botStop
  })
}));

vi.mock("../src/scheduler.js", () => ({
  PollScheduler: class {
    start = runtime.schedulerStart;
    stop = runtime.schedulerStop;
    stopAndDrain = runtime.schedulerDrain;
  }
}));

vi.mock("../src/db/client.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../src/db/client.js")>();
  return { openDb: vi.fn(actual.openDb) };
});

vi.mock("../src/services/runtimeErrors.js", () => ({ notifyFatalError: vi.fn() }));

type RuntimeEvent = "SIGTERM" | "SIGINT" | "uncaughtException" | "unhandledRejection";
let exitCode: typeof process.exitCode;

beforeEach(() => {
  exitCode = process.exitCode;
  vi.useFakeTimers();
  vi.resetAllMocks();
  vi.mocked(openDb).mockClear();
  vi.spyOn(console, "error").mockImplementation(() => {});
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
  process.exitCode = exitCode;
});

function launch(event: RuntimeEvent) {
  const existing = getEventListeners(process, event);
  let finishPolling!: () => void;
  runtime.botStart.mockImplementation(() => new Promise<void>((resolve) => { finishPolling = resolve; }));
  runtime.botStop.mockImplementation(async () => finishPolling());
  const finished = main(testConfig(":memory:"));
  const handler = getEventListeners(process, event).find((listener) => !existing.includes(listener))!;
  const db = vi.mocked(openDb).mock.results[0]!.value as ReturnType<typeof openDb>;
  return { finished, handler, db };
}

it("closes the actual database after both Telegram polling and the scheduler drain", async () => {
  let finishDrain!: () => void;
  runtime.schedulerDrain.mockImplementation(() => new Promise<void>((resolve) => { finishDrain = resolve; }));
  const { finished, handler, db } = launch("SIGTERM");
  handler();
  await vi.advanceTimersByTimeAsync(0);
  expect(runtime.botStop).toHaveBeenCalledOnce();
  expect(runtime.schedulerDrain).toHaveBeenCalledOnce();
  expect(db.open).toBe(true);
  finishDrain();
  await finished;
  await vi.advanceTimersByTimeAsync(0);
  expect(db.open).toBe(false);
});

it.each(["uncaughtException", "unhandledRejection"] as const)(
  "%s arms the hard deadline before notifying and proceeds after three seconds",
  async (event) => {
    const forcedExit = vi.spyOn(process, "exit").mockImplementation(() => undefined as never);
    const timer = vi.spyOn(globalThis, "setTimeout");
    const never = new Promise<void>(() => {});
    vi.mocked(notifyFatalError).mockImplementation(() => {
      expect(timer).toHaveBeenCalledWith(expect.any(Function), 20_000);
      return never;
    });
    runtime.schedulerDrain.mockResolvedValue(undefined);
    const { finished, handler, db } = launch(event);
    handler(new Error("fixture"));
    await vi.advanceTimersByTimeAsync(2_999);
    expect(notifyFatalError).toHaveBeenCalledOnce();
    expect(runtime.schedulerDrain).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    await finished;
    expect(db.open).toBe(false);
    expect(forcedExit).toHaveBeenCalledExactlyOnceWith(1);
  }
);

it("forces exit at the original hard deadline if the scheduler drain also hangs", async () => {
  const forcedExit = vi.spyOn(process, "exit").mockImplementation(() => undefined as never);
  vi.mocked(notifyFatalError).mockReturnValue(new Promise<void>(() => {}));
  let finishDrain!: () => void;
  runtime.schedulerDrain.mockImplementation(() => new Promise<void>((resolve) => { finishDrain = resolve; }));
  const { finished, handler, db } = launch("unhandledRejection");
  handler(new Error("fixture"));
  await vi.advanceTimersByTimeAsync(19_999);
  expect(runtime.schedulerDrain).toHaveBeenCalledOnce();
  expect(forcedExit).not.toHaveBeenCalled();
  expect(db.open).toBe(true);
  await vi.advanceTimersByTimeAsync(1);
  expect(forcedExit).toHaveBeenCalledExactlyOnceWith(1);
  finishDrain();
  await finished;
  await vi.advanceTimersByTimeAsync(0);
  expect(db.open).toBe(false);
});
