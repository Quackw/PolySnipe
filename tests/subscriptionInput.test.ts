import { describe, expect, it } from "vitest";
import { normalizeProxyAddress, parseAccountInput } from "../src/subscriptionInput.js";

describe("parseAccountInput", () => {
  it("parses profile URLs and strips query params", () => {
    expect(parseAccountInput("https://polymarket.com/@weflyhigh")).toEqual({
      kind: "profile_slug",
      slug: "weflyhigh"
    });
    expect(parseAccountInput("https://polymarket.com/@weflyhigh?tab=positions")).toEqual({
      kind: "profile_slug",
      slug: "weflyhigh"
    });
  });

  it("parses wallet urls and raw addresses", () => {
    const proxy = "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697";
    expect(parseAccountInput(`https://polymarket.com/${proxy}`)).toEqual({
      kind: "proxy_address",
      proxyAddress: proxy
    });
    expect(parseAccountInput(proxy.toUpperCase())).toEqual({
      kind: "proxy_address",
      proxyAddress: proxy
    });
  });

  it("rejects invalid inputs", () => {
    expect(parseAccountInput("https://example.com/@weflyhigh")).toBeNull();
    expect(parseAccountInput("hello world")).toBeNull();
  });
});

describe("normalizeProxyAddress", () => {
  it("normalizes valid addresses", () => {
    expect(normalizeProxyAddress("0x03E8A544E97EEFF5753BC1E90D46E5EF22AF1697")).toBe(
      "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697"
    );
  });

  it("returns null for invalid addresses", () => {
    expect(normalizeProxyAddress("0x123")).toBeNull();
  });
});
