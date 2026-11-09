// SSE usage extraction: fixtures + byte-split fuzz vs reference parser.
const test = require("node:test");
const assert = require("node:assert");
const metrics = require("../metrics");

const FIXTURE_SSE = [
  'data: {"candidates":[{"content":{"role":"model","parts":[{"text":"ok"}]},"index":0}]}',
  "",
  'data: {"candidates":[{"content":{"role":"model","parts":[{"text":" done"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":122,"candidatesTokenCount":5,"totalTokenCount":361,"thoughtsTokenCount":234},"modelVersion":"gemini-3.1-flash-lite"}',
  "",
].join("\r\n");

const FIXTURE_WRAPPED = [
  'data: {"response":{"candidates":[{"content":{"role":"model","parts":[{"text":"ok"}]}}]}}',
  "",
  'data: {"response":{"candidates":[{"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":50,"candidatesTokenCount":7,"thoughtsTokenCount":10,"cachedContentTokenCount":30,"totalTokenCount":67},"modelVersion":"gemini-3.8-flash"}}',
  "",
].join("\r\n");

const FIXTURE_CACHE_DETAILS = [
  'data: {"usageMetadata":{"promptTokenCount":100,"candidatesTokenCount":5,"cacheTokensDetails":[{"modality":"TEXT","tokenCount":40},{"modality":"IMAGE","tokenCount":20}],"totalTokenCount":105}}',
  "",
].join("\n");

test("usage extraction: raw Gemini SSE", () => {
  const r = metrics.parseSSEUsage(FIXTURE_SSE);
  assert.ok(r.usage);
  assert.strictEqual(r.usage.promptTokenCount, 122);
  assert.strictEqual(r.usage.candidatesTokenCount, 5);
  assert.strictEqual(r.usage.thoughtsTokenCount, 234);
  assert.strictEqual(r.usage.totalTokenCount, 361);
  assert.strictEqual(r.model, "gemini-3.1-flash-lite");

  const u = metrics.normUsage(r.usage);
  assert.strictEqual(u.promptTokens, 122);
  assert.strictEqual(u.outputTokens, 5);
  assert.strictEqual(u.thinkingTokens, 234);
  assert.strictEqual(u.cacheReadTokens, null);
  assert.strictEqual(u.cacheWriteTokens, null);
  assert.strictEqual(u.totalTokens, 361);
});

test("usage extraction: wrapped cloudcode {\"response\":X}", () => {
  const r = metrics.parseSSEUsage(FIXTURE_WRAPPED);
  assert.ok(r.usage);
  assert.strictEqual(r.usage.promptTokenCount, 50);
  assert.strictEqual(r.usage.cachedContentTokenCount, 30);
  assert.strictEqual(r.model, "gemini-3.8-flash");
  const u = metrics.normUsage(r.usage);
  assert.strictEqual(u.cacheReadTokens, 30);
});

test("usage extraction: cacheTokensDetails dijumlahkan", () => {
  const r = metrics.parseSSEUsage(FIXTURE_CACHE_DETAILS);
  const u = metrics.normUsage(r.usage);
  assert.strictEqual(u.cacheReadTokens, 60);
});

test("usage extraction: usageMetadata last-one-wins", () => {
  const sse = [
    'data: {"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":0,"totalTokenCount":1}}',
    'data: {"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":9,"totalTokenCount":10}}',
    "",
  ].join("\n");
  const r = metrics.parseSSEUsage(sse);
  assert.strictEqual(r.usage.totalTokenCount, 10);
});

test("fuzz: split SSE di SETIAP byte == reference parse", () => {
  const fixtures = [FIXTURE_SSE, FIXTURE_WRAPPED, FIXTURE_CACHE_DETAILS];
  for (const text of fixtures) {
    const ref = metrics.parseSSEUsage(text);
    const buf = Buffer.from(text, "utf8");
    for (let i = 0; i <= buf.length; i++) {
      const tracker = metrics.createUsageTracker();
      tracker.push(buf.slice(0, i));
      tracker.push(buf.slice(i));
      const got = tracker.end();
      assert.deepStrictEqual(
        got,
        { usage: ref.usage, model: ref.model },
        `split di byte ${i} beda (fixture len ${buf.length})`
      );
    }
  }
});

test("fuzz: split di setiap byte untuk payload multi-byte UTF-8", () => {
  const text =
    'data: {"candidates":[{"content":{"parts":[{"text":"halo é— 日本語"}]}}]}\n\n' +
    'data: {"usageMetadata":{"promptTokenCount":7,"candidatesTokenCount":3,"totalTokenCount":10}}\n\n';
  const ref = metrics.parseSSEUsage(text);
  const buf = Buffer.from(text, "utf8");
  for (let i = 0; i <= buf.length; i++) {
    const tracker = metrics.createUsageTracker();
    tracker.push(buf.slice(0, i));
    tracker.push(buf.slice(i));
    const got = tracker.end();
    assert.deepStrictEqual(got, { usage: ref.usage, model: ref.model }, `split di byte ${i}`);
  }
});
