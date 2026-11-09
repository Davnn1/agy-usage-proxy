// /_proxy/stats: since = ts entry pertama dari replay file, byModel/bySource akumulasi.
const test = require("node:test");
const assert = require("node:assert");
const http = require("node:http");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { createProxyServer } = require("../index");

function listen(server) {
  return new Promise((resolve) => server.listen(0, "127.0.0.1", () => resolve(server.address().port)));
}

function getJson(port, p) {
  return new Promise((resolve, reject) => {
    http
      .get({ hostname: "127.0.0.1", port, path: p }, (res) => {
        let d = "";
        res.on("data", (c) => (d += c));
        res.on("end", () => resolve({ status: res.statusCode, body: JSON.parse(d) }));
      })
      .on("error", reject);
  });
}

test("stats: replay file -> since=first ts, totals/byModel/bySource akumulasi", async (t) => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "agytest-stats-"));
  const metricsFile = path.join(tmp, "metrics.jsonl");
  const rows = [
    { ts: "2026-01-01T00:00:00.000Z", source: "cli", model: "m-a", status: 200, stream: true, promptTokens: 10, outputTokens: 5, thinkingTokens: 2, cacheReadTokens: 3, cacheWriteTokens: null, totalTokens: 15, durationMs: 100 },
    { ts: "2026-01-01T00:01:00.000Z", source: "cli", model: "m-a", status: 429, stream: false, promptTokens: null, outputTokens: null, thinkingTokens: null, cacheReadTokens: null, cacheWriteTokens: null, totalTokens: null, durationMs: 20 },
    { ts: "2026-01-01T00:02:00.000Z", source: "acp", model: "m-b", status: 200, stream: true, promptTokens: 7, outputTokens: 1, thinkingTokens: null, cacheReadTokens: 1, cacheWriteTokens: null, totalTokens: 8, durationMs: 50 },
  ];
  fs.writeFileSync(metricsFile, rows.map((r) => JSON.stringify(r)).join("\n") + "\n");

  const { server } = createProxyServer({
    port: 0,
    upstreamHost: "127.0.0.1",
    upstreamPort: 1,
    upstreamTls: false,
    tokenStore: path.join(tmp, "token-tidak-dipakai.json"), // stats tidak butuh token
    metricsFile,
  });
  const port = await listen(server);
  t.after(() => server.close());

  const { status, body } = await getJson(port, "/_proxy/stats");
  assert.strictEqual(status, 200);
  assert.strictEqual(body.since, "2026-01-01T00:00:00.000Z"); // ts pertama, BUKAN process start
  assert.strictEqual(body.requests, 3);
  assert.strictEqual(body.errors, 1);
  assert.deepStrictEqual(body.totals, {
    promptTokens: 17,
    outputTokens: 6,
    thinkingTokens: 2,
    cacheReadTokens: 4,
    totalTokens: 23,
  });
  assert.strictEqual(body.byModel["m-a"].requests, 2);
  assert.strictEqual(body.byModel["m-a"].errors, 1);
  assert.strictEqual(body.byModel["m-a"].promptTokens, 10);
  assert.strictEqual(body.byModel["m-b"].requests, 1);
  assert.strictEqual(body.byModel["m-b"].totalTokens, 8);
  assert.strictEqual(body.bySource["cli"].requests, 2);
  assert.strictEqual(body.bySource["acp"].requests, 1);
  assert.strictEqual(body.bySource["acp"].promptTokens, 7);
});

test("stats: file kosong -> nol, since = init time", async (t) => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "agytest-stats-"));
  const { server } = createProxyServer({
    port: 0,
    upstreamHost: "127.0.0.1",
    upstreamPort: 1,
    upstreamTls: false,
    tokenStore: path.join(tmp, "x.json"),
    metricsFile: path.join(tmp, "metrics.jsonl"),
  });
  const port = await listen(server);
  t.after(() => server.close());
  const { body } = await getJson(port, "/_proxy/stats");
  assert.strictEqual(body.requests, 0);
  assert.strictEqual(body.errors, 0);
  assert.deepStrictEqual(body.byModel, {});
  assert.deepStrictEqual(body.bySource, {});
});
