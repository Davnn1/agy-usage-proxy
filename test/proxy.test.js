// Integration test: mock upstream + mock token endpoint, zero-dep node:test.
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

function request(port, { method = "GET", path: p = "/", headers = {}, body = null }) {
  return new Promise((resolve, reject) => {
    const req = http.request(
      { hostname: "127.0.0.1", port, path: p, method, headers },
      (res) => {
        const chunks = [];
        res.on("data", (c) => chunks.push(c));
        res.on("end", () =>
          resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks) })
        );
      }
    );
    req.on("error", reject);
    if (body) req.write(body);
    req.end();
  });
}

// World: mock token endpoint + mock upstream + proxy
async function makeWorld(t, { upstreamHandler, expiresIn = 3600, modelMap = "" } = {}) {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "agytest-"));
  const tokenStore = path.join(tmp, "acp_token.json");
  const metricsFile = path.join(tmp, "metrics.jsonl");

  let tokenHits = 0;
  const tokenServer = http.createServer((req, res) => {
    let d = "";
    req.on("data", (c) => (d += c));
    req.on("end", () => {
      tokenHits++;
      const params = new URLSearchParams(d);
      assert.strictEqual(params.get("grant_type"), "refresh_token");
      assert.strictEqual(params.get("client_id"), "cid");
      assert.strictEqual(params.get("client_secret"), "csecret");
      assert.strictEqual(params.get("refresh_token"), "rtok");
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ access_token: `at-${tokenHits}`, expires_in: expiresIn }));
    });
  });
  const tokenPort = await listen(tokenServer);

  fs.writeFileSync(
    tokenStore,
    JSON.stringify({
      client_id: "cid",
      client_secret: "csecret",
      refresh_token: "rtok",
      token_uri: `http://127.0.0.1:${tokenPort}/token`,
      project_id: "proj-1",
    })
  );

  const seen = [];
  const upstream = http.createServer((req, res) => {
    const chunks = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => {
      const rec = { method: req.method, url: req.url, headers: req.headers, body: Buffer.concat(chunks) };
      seen.push(rec);
      upstreamHandler(req, res, rec);
    });
  });
  const upstreamPort = await listen(upstream);

  const { server } = createProxyServer({
    port: 0,
    upstreamHost: "127.0.0.1",
    upstreamPort,
    upstreamTls: false,
    tokenStore,
    metricsFile,
    modelMap,
  });
  const proxyPort = await listen(server);

  t.after(() => {
    server.close();
    upstream.close();
    tokenServer.close();
  });

  return {
    proxyPort,
    tokenStore,
    metricsFile,
    seen,
    tokenHits: () => tokenHits,
    readEntries: () =>
      !fs.existsSync(metricsFile)
        ? []
        : fs
        .readFileSync(metricsFile, "utf8")
        .split("\n")
        .filter((l) => l.trim())
        .map(JSON.parse),
  };
}

test("auth: Bearer ter-inject + cache token (1 refresh untuk 2 request)", async (t) => {
  const w = await makeWorld(t, {
    upstreamHandler: (req, res) => {
      res.writeHead(200, { "content-type": "application/json" });
      res.end("{}");
    },
  });
  await request(w.proxyPort, { path: "/cli/foo" });
  await request(w.proxyPort, { path: "/cli/bar" });
  assert.strictEqual(w.tokenHits(), 1);
  assert.strictEqual(w.seen[0].headers["authorization"], "Bearer at-1");
  assert.strictEqual(w.seen[1].headers["authorization"], "Bearer at-1");
});

test("auth: cache expiry -> refresh ulang (expires_in < skew 60s)", async (t) => {
  const w = await makeWorld(t, {
    expiresIn: 30, // expires_at = now + 30s - 60s -> langsung kadaluarsa
    upstreamHandler: (req, res) => {
      res.writeHead(200, { "content-type": "application/json" });
      res.end("{}");
    },
  });
  await request(w.proxyPort, { path: "/cli/foo" });
  await request(w.proxyPort, { path: "/cli/foo" });
  assert.strictEqual(w.tokenHits(), 2);
  assert.strictEqual(w.seen[1].headers["authorization"], "Bearer at-2");
});

test("passthrough byte-identical (non-model), hop headers di-strip, host ditulis ulang", async (t) => {
  const payload = Buffer.from("raw-bytes-\x00\x01\xff-utuh", "latin1");
  const w = await makeWorld(t, {
    upstreamHandler: (req, res) => {
      res.writeHead(200, { "content-type": "application/octet-stream", "x-echo": "1" });
      res.end(payload);
    },
  });
  const r = await request(w.proxyPort, {
    method: "POST",
    path: "/acp/v1helper/raw/path?q=1",
    headers: {
      "content-type": "application/octet-stream",
      connection: "keep-alive, x-hop",
      "proxy-connection": "keep-alive",
      "x-hop": "hop",
    },
    body: payload,
  });
  assert.deepStrictEqual(r.body, payload);
  assert.strictEqual(r.headers["x-echo"], "1");
  assert.deepStrictEqual(w.seen[0].body, payload);
  assert.strictEqual(w.seen[0].url, "/v1helper/raw/path?q=1"); // prefix /acp di-strip; v-path bukan tag
  // connection direbuild Node keep-alive agent; yang penting nilai dari client tidak diteruskan
  assert.notStrictEqual(w.seen[0].headers["connection"], "keep-alive, x-hop");
  assert.strictEqual(w.seen[0].headers["proxy-connection"], undefined);
  assert.strictEqual(w.seen[0].headers["host"], "127.0.0.1"); // host upstream
});

test("model call: path rewrite /v1internal, body di-wrap {model, project, request}, SSE di-unwrap, usage tercatat", async (t) => {
  const sse =
    'data: {"response":{"candidates":[{"content":{"role":"model","parts":[{"text":"ok"}]}}]}}\r\n\r\n' +
    'data: {"response":{"candidates":[{"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":42,"candidatesTokenCount":3,"thoughtsTokenCount":7,"totalTokenCount":52},"modelVersion":"gemini-x"}}\r\n\r\n';
  const w = await makeWorld(t, {
    upstreamHandler: (req, res) => {
      res.writeHead(200, { "content-type": "text/event-stream" });
      res.end(sse);
    },
  });
  const innerBody = JSON.stringify({ contents: [{ role: "user", parts: [{ text: "hi" }] }], sessionId: "s1" });
  const r = await request(w.proxyPort, {
    method: "POST",
    path: "/cli/v1beta/models/gemini-x:streamGenerateContent?alt=sse",
    headers: { "content-type": "application/json" },
    body: innerBody,
  });
  // client terima versi UNWRAPPED
  assert.ok(r.body.toString().includes('"usageMetadata"'));
  assert.ok(!r.body.toString().includes('"response"'));
  // upstream terima rewrite
  assert.strictEqual(w.seen[0].url, "/v1internal:streamGenerateContent?alt=sse");
  const outer = JSON.parse(w.seen[0].body.toString());
  assert.strictEqual(outer.model, "gemini-x");
  assert.strictEqual(outer.project, "proj-1");
  assert.deepStrictEqual(outer.request, JSON.parse(innerBody));
  // metrics
  const entries = w.readEntries();
  assert.strictEqual(entries.length, 1);
  const e = entries[0];
  assert.strictEqual(e.source, "cli");
  assert.strictEqual(e.model, "gemini-x");
  assert.strictEqual(e.status, 200);
  assert.strictEqual(e.stream, true);
  assert.strictEqual(e.promptTokens, 42);
  assert.strictEqual(e.outputTokens, 3);
  assert.strictEqual(e.thinkingTokens, 7);
  assert.strictEqual(e.totalTokens, 52);
  assert.strictEqual(e.cacheReadTokens, null);
  assert.ok(typeof e.durationMs === "number");
  assert.ok(e.ts);
});

test("prefix /acp -> source acp; tanpa prefix + header x-agy-source; default agy", async (t) => {
  const sse = 'data: {"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}\r\n\r\n';
  const w = await makeWorld(t, {
    upstreamHandler: (req, res) => {
      res.writeHead(200, { "content-type": "text/event-stream" });
      res.end(sse);
    },
  });
  const call = (p, headers = {}) =>
    request(w.proxyPort, {
      method: "POST",
      path: p,
      headers: { "content-type": "application/json", ...headers },
      body: "{}",
    });
  await call("/acp/v1beta/models/m1:streamGenerateContent?alt=sse");
  await call("/v1beta/models/m2:streamGenerateContent?alt=sse", { "x-agy-source": "acp" });
  await call("/v1beta/models/m3:streamGenerateContent?alt=sse");
  const entries = w.readEntries();
  assert.strictEqual(entries[0].source, "acp");
  assert.strictEqual(entries[1].source, "acp");
  assert.strictEqual(entries[2].source, "agy");
});

test("status >=400 tercatat (fields null); non-model tidak tercatat", async (t) => {
  const w = await makeWorld(t, {
    upstreamHandler: (req, res) => {
      if (req.url.startsWith("/v1internal")) {
        res.writeHead(429, { "content-type": "application/json" });
        res.end('{"error":{"code":429}}');
      } else {
        res.writeHead(200, { "content-type": "application/json" });
        res.end("{}");
      }
    },
  });
  await request(w.proxyPort, { path: "/cli/not-a-model" });
  const r = await request(w.proxyPort, {
    method: "POST",
    path: "/cli/v1beta/models/mx:streamGenerateContent?alt=sse",
    headers: { "content-type": "application/json" },
    body: "{}",
  });
  assert.strictEqual(r.status, 429);
  assert.strictEqual(r.body.toString(), '{"error":{"code":429}}'); // error body byte-identical
  const entries = w.readEntries();
  assert.strictEqual(entries.length, 1);
  assert.strictEqual(entries[0].status, 429);
  assert.strictEqual(entries[0].promptTokens, null);
  assert.strictEqual(entries[0].model, "mx");
});

test("model fallback: prioritas variant suffix SEBELUM modelMap; model=aktual, modelRequested=asli", async (t) => {
  const sse = 'data: {"response":{"usageMetadata":{"promptTokenCount":5,"candidatesTokenCount":1,"totalTokenCount":6}}}\r\n\r\n';
  const tried = [];
  const w = await makeWorld(t, {
    modelMap: { "gemini-new": "gemini-old" },
    upstreamHandler: (req, res, rec) => {
      const outer = JSON.parse(rec.body.toString());
      tried.push(outer.model);
      if (outer.model === "gemini-new-low") {
        // variant suffix pertama yang hidup
        res.writeHead(200, { "content-type": "text/event-stream" });
        res.end(sse);
      } else {
        res.writeHead(404, { "content-type": "application/json" });
        res.end('{"error":{"code":404}}');
      }
    },
  });
  const r = await request(w.proxyPort, {
    method: "POST",
    path: "/cli/v1beta/models/gemini-new:streamGenerateContent?alt=sse",
    headers: { "content-type": "application/json" },
    body: "{}",
  });
  assert.strictEqual(r.status, 200);
  // asli -> -low (suffix menang, map gemini-old tidak pernah dicoba)
  assert.deepStrictEqual(tried, ["gemini-new", "gemini-new-low"]);
  const entries = w.readEntries();
  assert.strictEqual(entries.length, 1); // hanya hasil akhir yang dicatat
  assert.strictEqual(entries[0].status, 200);
  assert.strictEqual(entries[0].model, "gemini-new-low"); // model AKTUAL
  assert.strictEqual(entries[0].modelRequested, "gemini-new"); // nama asli
  assert.strictEqual(entries[0].promptTokens, 5);
});

test("model fallback: semua suffix 404 -> map manual dipakai", async (t) => {
  const sse = 'data: {"response":{"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}}\r\n\r\n';
  const tried = [];
  const w = await makeWorld(t, {
    modelMap: { "gemini-x": "gemini-mapped" },
    upstreamHandler: (req, res, rec) => {
      const outer = JSON.parse(rec.body.toString());
      tried.push(outer.model);
      if (outer.model === "gemini-mapped") {
        res.writeHead(200, { "content-type": "text/event-stream" });
        res.end(sse);
      } else {
        res.writeHead(404, { "content-type": "application/json" });
        res.end("{}");
      }
    },
  });
  const r = await request(w.proxyPort, {
    method: "POST",
    path: "/cli/v1beta/models/gemini-x:streamGenerateContent?alt=sse",
    headers: { "content-type": "application/json" },
    body: "{}",
  });
  assert.strictEqual(r.status, 200);
  assert.deepStrictEqual(tried, [
    "gemini-x",
    "gemini-x-low",
    "gemini-x-high",
    "gemini-x-medium",
    "gemini-x-thinking",
    "gemini-mapped",
  ]);
  const entries = w.readEntries();
  assert.strictEqual(entries[0].model, "gemini-mapped");
  assert.strictEqual(entries[0].modelRequested, "gemini-x");
});

test("tanpa fallback: modelRequested TIDAK ada", async (t) => {
  const sse = 'data: {"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}\r\n\r\n';
  const w = await makeWorld(t, {
    upstreamHandler: (req, res) => {
      res.writeHead(200, { "content-type": "text/event-stream" });
      res.end(sse);
    },
  });
  await request(w.proxyPort, {
    method: "POST",
    path: "/cli/v1beta/models/gemini-ok:streamGenerateContent?alt=sse",
    headers: { "content-type": "application/json" },
    body: "{}",
  });
  const entries = w.readEntries();
  assert.strictEqual(entries[0].model, "gemini-ok");
  assert.strictEqual("modelRequested" in entries[0], false);
});

test("agentId: /cli/<tag>/... -> agentId tercatat, tag di-strip dari forward, source tetap", async (t) => {
  const sse = 'data: {"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}\r\n\r\n';
  const w = await makeWorld(t, {
    upstreamHandler: (req, res) => {
      res.writeHead(200, { "content-type": "text/event-stream" });
      res.end(sse);
    },
  });
  const r = await request(w.proxyPort, {
    method: "POST",
    path: "/cli/agent-xyz/v1beta/models/m-tag:streamGenerateContent?alt=sse",
    headers: { "content-type": "application/json" },
    body: "{}",
  });
  assert.strictEqual(r.status, 200);
  // tag di-strip sebelum forward ke upstream
  assert.strictEqual(w.seen[0].url, "/v1internal:streamGenerateContent?alt=sse");
  const entries = w.readEntries();
  assert.strictEqual(entries.length, 1);
  assert.strictEqual(entries[0].agentId, "agent-xyz");
  assert.strictEqual(entries[0].source, "cli");
  assert.strictEqual(entries[0].model, "m-tag");
});

test("agentId: tanpa tag -> field ABSEN (backward compat), path API tidak ke-strip", async (t) => {
  const sse = 'data: {"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}\r\n\r\n';
  const w = await makeWorld(t, {
    upstreamHandler: (req, res) => {
      res.writeHead(200, { "content-type": "text/event-stream" });
      res.end(sse);
    },
  });
  // tanpa tag: /acp langsung ke /v1beta
  await request(w.proxyPort, {
    method: "POST",
    path: "/acp/v1beta/models/m-plain:streamGenerateContent?alt=sse",
    headers: { "content-type": "application/json" },
    body: "{}",
  });
  assert.strictEqual(w.seen[0].url, "/v1internal:streamGenerateContent?alt=sse");
  const entries = w.readEntries();
  assert.strictEqual("agentId" in entries[0], false);
  assert.strictEqual(entries[0].source, "acp");
  assert.strictEqual(entries[0].model, "m-plain");
});

test("agentId: non-model path dengan tag tetap diforward benar (tidak dicatat)", async (t) => {
  const w = await makeWorld(t, {
    upstreamHandler: (req, res) => {
      res.writeHead(200, { "content-type": "application/json" });
      res.end("{}");
    },
  });
  await request(w.proxyPort, { path: "/cli/agent-9/other/endpoint" });
  assert.strictEqual(w.seen[0].url, "/other/endpoint");
  assert.strictEqual(w.readEntries().length, 0);
});

test("identity: sessionId + labels.cascade_id -> sessionId/conversationId tercatat, body forward TIDAK berubah", async (t) => {
  const sse = 'data: {"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}\r\n\r\n';
  const w = await makeWorld(t, {
    upstreamHandler: (req, res) => {
      res.writeHead(200, { "content-type": "text/event-stream" });
      res.end(sse);
    },
  });
  const body = JSON.stringify({
    contents: [{ role: "user", parts: [{ text: "hi" }] }],
    sessionId: "-3750763034362895579",
    labels: { cascade_id: "40e87b5f-ddd9-41fb-bbfe-b36b58d6c2a7", client: "antigravity-cli" },
  });
  await request(w.proxyPort, {
    method: "POST",
    path: "/cli/v1beta/models/m-id:streamGenerateContent?alt=sse",
    headers: { "content-type": "application/json" },
    body,
  });
  const entries = w.readEntries();
  assert.strictEqual(entries[0].sessionId, "-3750763034362895579");
  assert.strictEqual(entries[0].conversationId, "40e87b5f-ddd9-41fb-bbfe-b36b58d6c2a7");
  // observer-only: inner request di-forward verbatim (sessionId & labels utuh)
  const outer = JSON.parse(w.seen[0].body.toString());
  assert.deepStrictEqual(outer.request, JSON.parse(body));
});

test("identity: tanpa sessionId/labels -> field ABSEN; root_cascade_id sebagai fallback", async (t) => {
  const sse = 'data: {"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}\r\n\r\n';
  const w = await makeWorld(t, {
    upstreamHandler: (req, res) => {
      res.writeHead(200, { "content-type": "text/event-stream" });
      res.end(sse);
    },
  });
  const call = (b) =>
    request(w.proxyPort, {
      method: "POST",
      path: "/cli/v1beta/models/m-id:streamGenerateContent?alt=sse",
      headers: { "content-type": "application/json" },
      body: b,
    });
  await call(JSON.stringify({ contents: [] })); // tanpa identitas
  await call(JSON.stringify({ contents: [], labels: { root_cascade_id: "root-1" } })); // fallback
  const entries = w.readEntries();
  assert.strictEqual("sessionId" in entries[0], false);
  assert.strictEqual("conversationId" in entries[0], false);
  assert.strictEqual(entries[1].conversationId, "root-1");
  assert.strictEqual("sessionId" in entries[1], false);
});

test("fallback tier-fidelity: suffix dipilih dari thinkingBudget body (bukan urutan tetap)", async (t) => {
  const sse = 'data: {"response":{"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}}\r\n\r\n';
  const tried = [];
  const w = await makeWorld(t, {
    upstreamHandler: (req, res, rec) => {
      const outer = JSON.parse(rec.body.toString());
      tried.push(outer.model);
      // hanya suffix yang sesuai budget yang "hidup"
      const alive = { "-high": "hb-neg1", "-low": "hb-1000", "-medium": "hb-4000" };
      res.writeHead(404, { "content-type": "application/json" });
      res.end("{}");
    },
  });
  const mk = (budget) =>
    JSON.stringify({
      contents: [],
      generationConfig: { thinkingConfig: { includeThoughts: true, thinkingBudget: budget } },
    });
  const call = (budget) =>
    request(w.proxyPort, {
      method: "POST",
      path: "/cli/v1beta/models/gm:streamGenerateContent?alt=sse",
      headers: { "content-type": "application/json" },
      body: mk(budget),
    });
  await call(-1); // unlimited -> high dulu
  await call(1000); // -> low dulu
  await call(4000); // -> medium dulu
  await call(0); // thinking off -> low
  // urutan percobaan per call (semua 404): [asli, intended, 3 suffix lain] = 5 entries
  assert.strictEqual(tried[0], "gm");
  assert.strictEqual(tried[1], "gm-high"); // budget -1 -> -high pertama
  assert.strictEqual(tried[5], "gm");
  assert.strictEqual(tried[6], "gm-low"); // budget 1000 -> -low pertama
  assert.strictEqual(tried[10], "gm");
  assert.strictEqual(tried[11], "gm-medium"); // budget 4000 -> -medium pertama
  assert.strictEqual(tried[15], "gm");
  assert.strictEqual(tried[16], "gm-low"); // budget 0 -> -low
});

test("model discovery statis: GET /v1beta/models dijawab proxy, tidak diforward, tidak dicatat", async (t) => {
  const w = await makeWorld(t, {
    upstreamHandler: (req, res) => {
      res.writeHead(500);
      res.end("should-not-be-called");
    },
  });
  const r = await request(w.proxyPort, { path: "/cli/v1beta/models?pageSize=1000" });
  assert.strictEqual(r.status, 200);
  const j = JSON.parse(r.body.toString());
  assert.ok(Array.isArray(j.models));
  assert.ok(j.models.length >= 5);
  assert.ok(j.models.some((m) => m.name === "models/gemini-3.8-flash"));
  assert.ok(j.models[0].supportedGenerationMethods.includes("streamGenerateContent"));
  assert.strictEqual(w.seen.length, 0); // upstream tidak disentuh
  assert.strictEqual(w.readEntries().length, 0); // tidak dicatat
  const r2 = await request(w.proxyPort, { path: "/acp/v1/models" });
  assert.strictEqual(r2.status, 200);
});

test("502 jelas kalau token store hilang", async (t) => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "agytest-"));
  const { server } = createProxyServer({
    port: 0,
    upstreamHost: "127.0.0.1",
    upstreamPort: 1,
    upstreamTls: false,
    tokenStore: path.join(tmp, "tidak-ada.json"),
    metricsFile: path.join(tmp, "m.jsonl"),
  });
  const port = await listen(server);
  t.after(() => server.close());
  const r = await request(port, { path: "/cli/foo" });
  assert.strictEqual(r.status, 502);
  assert.ok(r.body.toString().includes("Token store tidak ditemukan"));
});
