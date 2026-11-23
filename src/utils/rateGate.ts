import { sleep } from "./time.js";

export interface GateTicket {
  waitMs: number;
  release: () => void;
}

export class SlidingWindowRateGate {
  private readonly starts: number[] = [];
  private active = 0;
  private blockedUntilMs = 0;

  public constructor(
    private readonly limit: number,
    private readonly windowMs: number,
    private readonly concurrency: number
  ) {}

  public pauseFor(delayMs: number): void {
    if (!Number.isFinite(delayMs) || delayMs <= 0) {
      return;
    }
    this.blockedUntilMs = Math.max(this.blockedUntilMs, Date.now() + delayMs);
  }

  public async acquire(): Promise<GateTicket> {
    const startedAt = Date.now();

    while (true) {
      this.prune();
      const now = Date.now();
      if (now >= this.blockedUntilMs && this.active < this.concurrency && this.starts.length < this.limit) {
        this.active += 1;
        this.starts.push(now);
        return {
          waitMs: Date.now() - startedAt,
          release: () => {
            this.active = Math.max(0, this.active - 1);
          }
        };
      }

      const oldest = this.starts[0];
      const rateWait = oldest ? Math.max(0, this.windowMs - (now - oldest)) : 0;
      const pauseWait = Math.max(0, this.blockedUntilMs - now);
      const retryInMs = Math.max(10, rateWait, pauseWait, this.active >= this.concurrency ? 10 : 0);
      await sleep(retryInMs);
    }
  }

  private prune(): void {
    const now = Date.now();
    while (this.starts.length > 0) {
      const value = this.starts[0];
      if (value === undefined || now - value < this.windowMs) {
        break;
      }
      this.starts.shift();
    }
  }
}
