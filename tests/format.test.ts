import { describe, expect, it } from "vitest";
import {
  renderActivityAlert,
  renderAdminPerformanceReport,
  renderSubscriptionCreated,
  renderUnsubscribed
} from "../src/services/format.js";

describe("renderActivityAlert", () => {
  it("formats generic activity alerts with account and market links", () => {
    const text = renderActivityAlert(
      {
        proxyAddress: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697",
        profileSlug: "weflyhigh"
      },
      [
        {
          proxyWallet: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697",
          timestamp: 100,
          type: "TRADE",
          title: "Will Iran strike Mina Al Ahmadi refinery by March 31",
          slug: "will-iran-strike-mina-al-ahmadi-refinery-by-march-31",
          side: "BUY",
          outcome: "YES",
          size: 12.345,
          usdcSize: 99.5,
          price: 0.1
        },
        {
          proxyWallet: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697",
          timestamp: 101,
          type: "TRADE",
          title: "Plain title without slug",
          side: "SELL",
          outcome: "NO",
          size: 12,
          usdcSize: 12.3
        }
      ]
    );

    expect(text.startsWith('<a href="https://polymarket.com/@weflyhigh">@weflyhigh</a>\n\n')).toBe(true);
    expect(text).toContain(
      '<a href="https://polymarket.com/market/will-iran-strike-mina-al-ahmadi-refinery-by-march-31">Will Iran strike Mina Al Ahmadi refinery by March 31</a>'
    );
    expect(text).toContain(
      '<a href="https://polymarket.com/market/will-iran-strike-mina-al-ahmadi-refinery-by-march-31">Will Iran strike Mina Al Ahmadi refinery by March 31</a> | <b>TRADE</b>, outcome: <b>YES</b>, side: <b>BUY</b>, shares: <b>12.35</b>, avg price: <b>10¢</b>, usdcSize: <b>$99.50</b>'
    );
    expect(text).toContain(
      "Plain title without slug | <b>TRADE</b>, outcome: <b>NO</b>, side: <b>SELL</b>, shares: <b>12</b>, usdcSize: <b>$12.30</b>"
    );
    expect(text).not.toContain("New Polymarket activity");
    expect(text).not.toContain("Proxy address");
    expect(text).not.toContain("Label:");
    expect(text).not.toContain("pps=");
  });

  it("omits optional fragments when fields are missing", () => {
    const text = renderActivityAlert(
      {
        proxyAddress: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697",
        profileSlug: "weflyhigh"
      },
      [
        {
          proxyWallet: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697",
          timestamp: 100,
          type: "TRADE",
          side: "BUY"
        }
      ]
    );

    expect(text).toContain("<b>TRADE</b>, side: <b>BUY</b>");
    expect(text).not.toContain("avg price");
    expect(text).not.toContain("usdcSize:");
    expect(text).not.toContain("undefined");
  });

  it("formats non-trade activities with the same generic template", () => {
    const text = renderActivityAlert(
      {
        proxyAddress: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697",
        profileSlug: "weflyhigh"
      },
      [
        {
          proxyWallet: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697",
          timestamp: 100,
          type: "REWARD",
          title: "Liquidity rewards",
          usdcSize: 5
        }
      ]
    );

    expect(text).toContain("Liquidity rewards | <b>REWARD</b>, usdcSize: <b>$5.00</b>");
  });

  it("shows the same compact trade values as the RTDS card", () => {
    const text = renderActivityAlert(
      { proxyAddress: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", profileSlug: "Spirit-LT" },
      [
        {
          proxyWallet: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697",
          timestamp: 100,
          type: "TRADE",
          title: "Will the price of Ethereum be above $2,700 on September 28?",
          slug: "ethereum-above-2700-on-september-28-2026",
          outcome: "Yes",
          side: "BUY",
          size: 100,
          price: 0.09,
          usdcSize: 9
        }
      ]
    );
    expect(text).toContain("shares: <b>100</b>, avg price: <b>9¢</b>, usdcSize: <b>$9.00</b>");
  });
});

describe("renderSubscriptionCreated", () => {
  it("formats a compact subscription confirmation", () => {
    const text = renderSubscriptionCreated(
      {
        proxyAddress: "0xa90ea765fd6ae21e3277d9f97e3ad6c149411400",
        profileSlug: "agurets",
        pseudonym: "Authentic-Lounge"
      },
      true
    );

    expect(text).toBe('<b>Subscribed to</b> <a href="https://polymarket.com/@agurets">@agurets</a>');
    expect(text).not.toContain("Proxy address");
    expect(text).not.toContain("Profile:");
    expect(text).not.toContain("Label:");
  });
});

describe("renderUnsubscribed", () => {
  it("formats a compact unsubscribe confirmation", () => {
    const text = renderUnsubscribed({
      proxyAddress: "0xa90ea765fd6ae21e3277d9f97e3ad6c149411400",
      profileSlug: "Spirit-LT",
      pseudonym: "Upbeat-Advance"
    });

    expect(text).toBe('<b>Unsubscribed from</b> <a href="https://polymarket.com/@Spirit-LT">@Spirit-LT</a>');
    expect(text).not.toContain("Tracked account removed");
    expect(text).not.toContain("Account:");
  });
});

describe("renderAdminPerformanceReport", () => {
  it("escapes bucket labels for Telegram HTML mode", () => {
    const text = renderAdminPerformanceReport({
      thresholdMs: 30_000,
      sampleCount: 2,
      classifiedSampleCount: 1,
      minDelayMs: 4_000,
      maxDelayMs: 40_000,
      avgDelayMs: 22_000,
      freshLocalMaxDelayMs: 12_000,
      alertsOverThresholdCount: 1,
      upstreamStaleCount: 0,
      delayBuckets: [
        { label: "<5s", count: 1 },
        { label: "5-10s", count: 0 },
        { label: "10-30s", count: 0 },
        { label: "30-60s", count: 1 },
        { label: "1-2m", count: 0 },
        { label: "2-5m", count: 0 },
        { label: "5-10m", count: 0 },
        { label: ">10m", count: 0 }
      ],
      delayReasonCounts: [
        { reason: "upstream_stale", count: 0 },
        { reason: "poll_fetch_slow", count: 1 },
        { reason: "dispatch_backlog", count: 0 },
        { reason: "telegram_slow", count: 0 }
      ]
    });

    expect(text).toContain("- &lt;5s: <b>1</b>");
    expect(text).not.toContain("- <5s:");
  });
});
