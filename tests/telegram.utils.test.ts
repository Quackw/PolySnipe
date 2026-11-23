import { describe, expect, it, vi } from "vitest";
import { replyHtml, splitForTelegram } from "../src/utils/telegram.js";

describe("splitForTelegram", () => {
  it("splits long text into bounded chunks", () => {
    const text = ["a".repeat(3000), "b".repeat(3000), "c".repeat(3000)].join("\n");
    const chunks = splitForTelegram(text, 3900);

    expect(chunks.length).toBeGreaterThan(1);
    expect(chunks.every((chunk) => chunk.length <= 3900)).toBe(true);
    expect(chunks.join("")).toContain("aaa");
    expect(chunks.join("")).toContain("bbb");
    expect(chunks.join("")).toContain("ccc");
  });
});

describe("replyHtml", () => {
  it("sends multiple replies for long messages and keeps extra options only on the first one", async () => {
    const reply = vi.fn(async () => undefined);
    const ctx = { reply } as never;
    const html = `${"a".repeat(3900)}\n${"b".repeat(3900)}`;

    await replyHtml(ctx, html, { reply_markup: { keyboard: [[{ text: "Subscribe" }]] } });

    expect(reply).toHaveBeenCalledTimes(2);
    const firstCall = reply.mock.calls[0] as unknown[] | undefined;
    const secondCall = reply.mock.calls[1] as unknown[] | undefined;
    expect(String(firstCall?.[0]).length).toBeLessThanOrEqual(3900);
    expect(String(secondCall?.[0]).length).toBeLessThanOrEqual(3900);
    expect(firstCall?.[1]).toMatchObject({ reply_markup: { keyboard: [[{ text: "Subscribe" }]] } });
    expect(secondCall?.[1]).not.toHaveProperty("reply_markup");
  });
});
