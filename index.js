// agy-usage-proxy: reverse-proxy logging untuk Antigravity CLI (agy) gateway mode.
// Terima Gemini wire protocol (POST /v1beta/models/{model}:streamGenerateContent?alt=sse)
// tanpa auth, attach Bearer dari refresh_token store lokal, forward ke cloudcode-pa
// v1internal, unwrap response SSE {"response":X} -> X, catat usage observer-only.
// Zero dependency. Pola: kimi-proxy.
const http = require("node:http");
const https = require("node:https");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { StringDecoder } = require("node:string_decoder");
const metrics = require("./metrics");

const DEFAULT_TOKEN_STORE = path.join(os.homedir(), ".gemini", "antigravity-acp", "acp_token.json");
const DEFAULT_METRICS = path.join(os.homedir(), ".local", "share", "agy-usage-proxy", "metrics.jsonl");
const REFRESH_SKEW_MS = 60 * 1000; // refresh kalau sisa umur < 60s
const CLIENT_METADATA = JSON.stringify({
  ideType: "IDE_UNSPECIFIED",
  platform: "PLATFORM_UNSPECIFIED",
  pluginType: "GEMINI",
});
const MODEL_PATH_RE = /^\/v1beta\/models\/([^:/]+):(\w+)/;
// agy >=1.3.2 melakukan model discovery: GET /v1beta/models?pageSize=1000 &
// /v1/models. cloudcode /v1internal tidak punya endpoint itu (404) dan kalau
// listing gagal agy jatuh ke default internal yang tidak ada di daily -> mati.
// Jawab statis dari proxy (bukan LLM call: tidak dicatat, tidak perlu token).
// URUTAN PENTING: agy >=1.3.2 memilih model PERTAMA dari list sebagai default
// (verified: dump list low-first -> kirim -low; plain-first -> kirim plain).
// Default = high (setara perilaku asli: nama polos + budget -1).
const STATIC_MODELS = [
  "gemini-3.8-flash-high",
  "gemini-3.8-flash-medium",
  "gemini-3.8-flash-low",
  "gemini-3.8-flash",
  "gemini-3.1-pro-low",
  "gemini-3.1-pro-high",
  "gemini-3.1-flash-lite",
  "gemini-3.1-flash-lite-preview",
  "gemini-2.5-flash",
  "gemini-2.5-flash-lite",
  "gemini-2.5-pro",
];
function modelListJson() {
  return JSON.stringify({
    models: STATIC_MODELS.map((m) => ({
      name: "models/" + m,
      displayName: m,
      inputTokenLimit: 1048576,
      outputTokenLimit: 65536,
      supportedGenerationMethods: ["generateContent", "streamGenerateContent"],
    })),
  });
}
// Fallback saat upstream 404 (model tidak ada di channel). Prioritas:
// nama asli -> suffix yang sesuai thinkingBudget body (tier fidelity, lihat
// DECISIONS: nama modulasi thinking walau body dominan) -> suffix lain ->
// map manual AGY_PROXY_MODEL_MAP.
const MODEL_SUFFIX_VARIANTS = ["-low", "-high", "-medium", "-thinking"];

// Tier yang diamati dari agy: low=1000, medium=4000, high=-1 (unlimited).
function intendedSuffix(thinkingBudget) {
  if (typeof thinkingBudget !== "number") return null;
  if (thinkingBudget === -1 || thinkingBudget > 4000) return "-high";
  if (thinkingBudget > 1000) return "-medium";
  return "-low"; // 0 (thinking off) s/d 1000
}

function loadConfig(overrides = {}) {
  const env = process.env;
  const cfg = {
    port: Number(env.AGY_PROXY_PORT || 9880),
    host: env.AGY_PROXY_HOST || "127.0.0.1",
    upstreamHost: env.AGY_PROXY_UPSTREAM || "daily-cloudcode-pa.googleapis.com",
    upstreamPort: 443,
    upstreamTls: true,
    tokenStore: env.AGY_PROXY_TOKEN_STORE || DEFAULT_TOKEN_STORE,
    metricsFile: env.AGY_PROXY_METRICS || DEFAULT_METRICS,
    tokenUriOverride: null, // test hook
    modelMap: parseModelMap(env.AGY_PROXY_MODEL_MAP || ""),
    ...overrides,
  };
  if (!cfg.upstreamTls) cfg.upstreamPort = overrides.upstreamPort || cfg.upstreamPort;
  return cfg;
}

function parseModelMap(s) {
  const map = {};
  for (const pair of s.split(",")) {
    const [from, to] = pair.split(":").map((x) => x && x.trim());
    if (from && to) map[from] = to;
  }
  return map;
}

// --- Auth (refresh_token -> access_token, cache sampai expires_in - 60s) ---
function createAuth(cfg) {
  let store = null; // { client_id, client_secret, refresh_token, token_uri, project_id }
  let tokenCache = null; // { access_token, expires_at_ms }
  let refreshLock = null;

  function loadStore() {
    try {
      store = JSON.parse(fs.readFileSync(cfg.tokenStore, "utf8"));
    } catch (err) {
      if (err.code === "ENOENT") {
        throw new Error(`Token store tidak ditemukan: ${cfg.tokenStore} (login agy dulu)`);
      }
      throw new Error(`Token store tidak bisa dibaca: ${err.message}`);
    }
    if (!store.refresh_token || !store.client_id || !store.token_uri) {
      throw new Error(`Token store tidak lengkap (butuh client_id/client_secret/refresh_token/token_uri)`);
    }
  }

  function postForm(urlStr, body) {
    return new Promise((resolve, reject) => {
      const u = new URL(urlStr);
      const proto = u.protocol === "http:" ? http : https;
      const req = proto.request(
        {
          hostname: u.hostname,
          port: u.port || (u.protocol === "http:" ? 80 : 443),
          path: u.pathname + u.search,
          method: "POST",
          headers: {
            "Content-Type": "application/x-www-form-urlencoded",
            "Content-Length": Buffer.byteLength(body),
          },
        },
        (res) => {
          let d = "";
          res.on("data", (c) => (d += c));
          res.on("end", () => {
            if (res.statusCode === 200) {
              try {
                resolve(JSON.parse(d));
              } catch (e) {
                reject(new Error("token endpoint: JSON parse gagal"));
              }
            } else {
              // jangan pernah sertakan secret; body error dari Google aman
              reject(new Error(`token endpoint HTTP ${res.statusCode}: ${d.slice(0, 300)}`));
            }
          });
        }
      );
      req.on("error", reject);
      req.write(body);
      req.end();
    });
  }

  async function refresh() {
    // single-flight: request lain tunggu refresh yang sedang jalan
    if (!refreshLock) {
      refreshLock = (async () => {
        const params = new URLSearchParams({
          grant_type: "refresh_token",
          client_id: store.client_id,
          client_secret: store.client_secret,
          refresh_token: store.refresh_token,
        });
        const result = await postForm(cfg.tokenUriOverride || store.token_uri, params.toString());
        if (!result.access_token) throw new Error("token endpoint: access_token kosong");
        tokenCache = {
          access_token: result.access_token,
          expires_at_ms: Date.now() + (result.expires_in || 3600) * 1000 - REFRESH_SKEW_MS,
        };
      })().finally(() => {
        refreshLock = null;
      });
    }
    return refreshLock;
  }

  return {
    async getAccessToken() {
      if (!store) loadStore();
      if (!tokenCache || tokenCache.expires_at_ms <= Date.now()) {
        await refresh();
      }
      return tokenCache.access_token;
    },
    projectId() {
      if (!store) loadStore();
      return store.project_id || null;
    },
  };
}

// --- SSE line-transform: unwrap {"response": X} -> X, track usage, framing dijaga ---
// Bukan byte-identical hanya untuk payload yang dibungkus cloudcode; sisanya verbatim.
function createUnwrapStream(onLine, onUsageEvent) {
  const decoder = new StringDecoder("utf8");
  let pending = "";
  function processLine(rawLine) {
    const hadCR = rawLine.endsWith("\r");
    const line = hadCR ? rawLine.slice(0, -1) : rawLine;
    const t = line.trim();
    if (!t.startsWith("data:")) {
      onLine(line + (hadCR ? "\r" : "") + "\n");
      return;
    }
    const payload = t.slice(5).trim();
    if (!payload || payload === "[DONE]") {
      onLine(line + (hadCR ? "\r" : "") + "\n");
      return;
    }
    let evt = null;
    try {
      evt = JSON.parse(payload);
    } catch (e) {
      /* bukan JSON: teruskan apa adanya */
    }
    if (evt && typeof evt === "object" && evt.response && typeof evt.response === "object") {
      const inner = JSON.stringify(evt.response); // stringify ONCE
      onLine("data: " + inner + (hadCR ? "\r" : "") + "\n");
      onUsageEvent(evt.response);
    } else {
      onLine(line + (hadCR ? "\r" : "") + "\n");
      if (evt) onUsageEvent(evt);
    }
  }
  function drain() {
    let idx;
    while ((idx = pending.indexOf("\n")) >= 0) {
      const rawLine = pending.slice(0, idx);
      pending = pending.slice(idx + 1);
      processLine(rawLine);
    }
  }
  return {
    push(chunk) {
      pending += decoder.write(chunk);
      drain();
    },
    end() {
      pending += decoder.end();
      drain();
      if (pending) {
        processLine(pending);
        pending = "";
      }
    },
  };
}

function extractUsage(evt, state) {
  if (evt.usageMetadata && typeof evt.usageMetadata === "object") {
    state.usage = evt.usageMetadata; // last-one-wins (kumulatif)
  }
  if (!state.model && typeof evt.modelVersion === "string") state.model = evt.modelVersion;
}

function detectSource(req, strippedPrefix) {
  if (strippedPrefix) return strippedPrefix;
  const h = req.headers["x-agy-source"];
  if (h === "cli" || h === "acp") return h;
  return "agy";
}

function createProxyServer(cfgOverrides = {}) {
  const cfg = loadConfig(cfgOverrides);
  const auth = createAuth(cfg);
  metrics.init(cfg.metricsFile);

  const agentOpts = {
    keepAlive: true,
    keepAliveMsecs: 15000,
    maxSockets: 32,
    maxFreeSockets: 8,
  };
  const upstreamAgent = cfg.upstreamTls ? new https.Agent(agentOpts) : new http.Agent(agentOpts);
  const upstreamProto = cfg.upstreamTls ? https : http;

  const server = http.createServer((clientReq, clientRes) => {
    if (clientReq.url.startsWith("/_proxy/")) {
      metrics.handleAdmin(clientReq, clientRes);
      return;
    }
    handle(clientReq, clientRes).catch((err) => {
      console.error("[proxy] unhandled:", err.message);
      if (!clientRes.headersSent) clientRes.writeHead(500);
      clientRes.end("Internal Error");
    });
  });

  async function handle(clientReq, clientRes) {
    const startTime = Date.now();
    const rawUrl = clientReq.url;

    // source marker: strip prefix /cli atau /acp.
    // Opsional segment kedua = agentId tag: /cli/<tag>/v1beta/... -> agentId="<tag>".
    // Segment dianggap tag kalau BUKAN path API (tidak diawali v<digit>: v1beta, v1, ...).
    // Kontrak metrics: entry.agentId (string) HANYA ada saat tag hadir (backward compat).
    let source = null;
    let agentId = null;
    let fwdUrl = rawUrl;
    const m = rawUrl.match(/^\/(cli|acp)(\/|$)/);
    if (m) {
      source = m[1];
      let rest = rawUrl.slice(m[1].length + 1) || "/";
      const seg = rest.match(/^\/([^/?]+)(\/|\?|$)/);
      if (seg && !/^v\d/.test(seg[1])) {
        agentId = decodeURIComponent(seg[1]);
        rest = rest.slice(seg[1].length + 1) || "/";
        if (!rest.startsWith("/")) rest = "/" + rest; // jaga query-only: /<tag>?q -> /?q
      }
      fwdUrl = rest;
    } else {
      source = detectSource(clientReq, null);
    }

    // Model discovery statis (agy >=1.3.2). Tidak diforward, tidak dicatat.
    if (
      clientReq.method === "GET" &&
      /^\/v1beta?\/models(\?|$)|^\/v1\/models(\?|$)/.test(fwdUrl) &&
      !MODEL_PATH_RE.test(fwdUrl)
    ) {
      clientRes.writeHead(200, { "content-type": "application/json" });
      clientRes.end(modelListJson());
      return;
    }

    const isIdempotent = ["GET", "HEAD"].includes(clientReq.method);
    const modelMatch = fwdUrl.match(MODEL_PATH_RE);
    const isModelCall = !!modelMatch;
    const reqModel = modelMatch ? decodeURIComponent(modelMatch[1]) : null;
    const action = modelMatch ? modelMatch[2] : null;
    const queryIdx = fwdUrl.indexOf("?");
    const query = queryIdx >= 0 ? fwdUrl.slice(queryIdx) : "";

    let accessToken;
    try {
      accessToken = await auth.getAccessToken();
    } catch (err) {
      console.error(`[auth] ${err.message}`);
      clientRes.writeHead(502, { "Content-Type": "application/json" });
      clientRes.end(JSON.stringify({ error: `agy-proxy auth: ${err.message}` }));
      return;
    }

    // Buffer body client (perlu full untuk wrap; ukuran prompt wajar < MB)
    const bodyChunks = [];
    clientReq.on("data", (c) => bodyChunks.push(c));
    await new Promise((resolve) => clientReq.on("end", resolve));
    const reqBodyRaw = Buffer.concat(bodyChunks);

    function buildUpstream(modelUsed) {
      let upstreamPath = fwdUrl;
      let outBody = reqBodyRaw;
      if (isModelCall) {
        // rewrite: /v1beta/models/{m}:{action} -> /v1internal:{action}
        // body: {model, project, request: <original JSON>}
        upstreamPath = `/v1internal:${action}${query}`;
        let inner = {};
        if (reqBodyRaw.length) {
          try {
            inner = JSON.parse(reqBodyRaw.toString("utf8"));
          } catch (e) {
            inner = {};
          }
        }
        const outer = { model: modelUsed, request: inner };
        const proj = auth.projectId();
        if (proj) outer.project = proj;
        outBody = Buffer.from(JSON.stringify(outer)); // stringify ONCE
      }
      const headers = { ...clientReq.headers };
      delete headers["connection"];
      delete headers["proxy-connection"];
      delete headers["transfer-encoding"];
      delete headers["host"];
      delete headers["accept-encoding"];
      delete headers["content-length"];
      headers["authorization"] = `Bearer ${accessToken}`;
      if (!headers["client-metadata"]) headers["client-metadata"] = CLIENT_METADATA;
      headers["host"] = cfg.upstreamHost;
      if (outBody.length) headers["content-length"] = outBody.length;
      else delete headers["content-length"];
      return {
        options: {
          hostname: cfg.upstreamHost,
          port: cfg.upstreamPort,
          path: upstreamPath,
          method: clientReq.method,
          agent: upstreamAgent,
          headers,
        },
        outBody,
      };
    }

    const usageState = { usage: null, model: null };
    let responseReceived = false;
    let retriedSocket = false;
    let servedModel = reqModel; // model aktual yang menyerve (berubah saat fallback)
    let fallbackQueue = null; // dibangun lazy saat 404 pertama
    let sessionId = null; // body.sessionId (per CLI session)
    let conversationId = null; // body.labels.cascade_id (per conversation)
    let thinkingBudget = null; // body.generationConfig.thinkingConfig.thinkingBudget

    // Identitas + tier observer-only: body yang di-forward TIDAK diubah.
    if (isModelCall && reqBodyRaw.length) {
      try {
        const peek = JSON.parse(reqBodyRaw.toString("utf8"));
        if (typeof peek.sessionId === "string" || typeof peek.sessionId === "number") {
          sessionId = String(peek.sessionId);
        }
        const cid = peek.labels && (peek.labels.cascade_id || peek.labels.root_cascade_id);
        if (typeof cid === "string") conversationId = cid;
        const tb = peek.generationConfig && peek.generationConfig.thinkingConfig;
        if (tb && typeof tb.thinkingBudget === "number") thinkingBudget = tb.thinkingBudget;
      } catch (e) {
        /* body non-JSON: skip */
      }
    }

    // Kontrak metrics: entry.model = model AKTUAL yang menyerve; modelRequested
    // (string) HANYA ada saat fallback nendang (nama asli yang diminta client).
    // entry.agentId HANYA ada saat path tag hadir. entry.sessionId /
    // entry.conversationId HANYA ada saat ada di body request (observer-only).
    function record(status, stream) {
      if (!isModelCall) return; // non-model tidak dicatat
      const u = metrics.normUsage(usageState.usage);
      const entry = {
        ts: new Date().toISOString(),
        source,
        model: servedModel || usageState.model || "unknown",
        status,
        stream: !!stream,
        ...u,
        durationMs: Date.now() - startTime,
      };
      if (servedModel && reqModel && servedModel !== reqModel) {
        entry.modelRequested = reqModel;
      }
      if (agentId) entry.agentId = agentId;
      if (sessionId) entry.sessionId = sessionId;
      if (conversationId) entry.conversationId = conversationId;
      metrics.track(entry);
      console.log(
        `${entry.ts} ${source} ${entry.model}${entry.modelRequested ? ` (req:${entry.modelRequested})` : ""} ${status} in=${u.promptTokens} out=${u.outputTokens} think=${u.thinkingTokens} cache=${u.cacheReadTokens} total=${u.totalTokens} ${entry.durationMs}ms`
      );
    }

    function onProxyResponse(proxyRes) {
      responseReceived = true;
      const status = proxyRes.statusCode || 0;

      // fallback model saat 404 (model tidak ada di channel upstream).
      // Prioritas: variant suffix (mis. X -> X-low) -> map manual.
      if (isModelCall && status === 404 && reqModel) {
        if (!fallbackQueue) {
          const intended = intendedSuffix(thinkingBudget);
          const suffixes = intended
            ? [intended, ...MODEL_SUFFIX_VARIANTS.filter((s) => s !== intended)]
            : MODEL_SUFFIX_VARIANTS;
          fallbackQueue = suffixes.map((s) => reqModel + s).filter((m) => m !== servedModel);
          const mapped = cfg.modelMap[reqModel];
          if (mapped && !fallbackQueue.includes(mapped)) fallbackQueue.push(mapped);
        }
        const next = fallbackQueue.shift();
        if (next) {
          console.log(`[model-fallback] ${servedModel} 404 -> retry sebagai ${next}`);
          servedModel = next;
          proxyRes.resume(); // drain
          proxyRes.on("end", () => {
            const r = forward(next);
            r.req.write(r.outBody);
            r.req.end();
          });
          return;
        }
      }

      const contentType = String(proxyRes.headers["content-type"] || "").toLowerCase();
      const isSSE = contentType.includes("text/event-stream");

      if (isModelCall && isSSE) {
        // SSE unwrap transform + usage track
        const outHeaders = { ...proxyRes.headers };
        delete outHeaders["content-length"]; // panjang berubah -> chunked
        clientRes.writeHead(status, outHeaders);
        const stream = createUnwrapStream(
          (line) => clientRes.write(line),
          (evt) => extractUsage(evt, usageState)
        );
        proxyRes.on("data", (chunk) => stream.push(chunk));
        proxyRes.on("end", () => {
          stream.end();
          clientRes.end();
          record(status, true);
        });
        proxyRes.on("error", () => {
          clientRes.end();
        });
        return;
      }

      if (isModelCall && contentType.includes("application/json") && status < 400) {
        // non-stream JSON: buffer, unwrap {"response":X} bila ada, track usage
        const chunks = [];
        proxyRes.on("data", (c) => chunks.push(c));
        proxyRes.on("end", () => {
          const raw = Buffer.concat(chunks);
          let outBuf = raw;
          try {
            const parsed = JSON.parse(raw.toString("utf8"));
            const inner =
              parsed && typeof parsed === "object" && parsed.response && typeof parsed.response === "object"
                ? parsed.response
                : parsed;
            if (inner && typeof inner === "object") extractUsage(inner, usageState);
            if (inner !== parsed) outBuf = Buffer.from(JSON.stringify(inner));
          } catch (e) {
            /* bukan JSON: passthrough raw */
          }
          const outHeaders = { ...proxyRes.headers };
          delete outHeaders["content-length"];
          outHeaders["content-length"] = outBuf.length;
          clientRes.writeHead(status, outHeaders);
          clientRes.end(outBuf);
          record(status, false);
        });
        return;
      }

      if (status >= 400) {
        // error path: forward live (byte-identical), tetap dicatat untuk model call
        clientRes.writeHead(status, proxyRes.headers);
        proxyRes.pipe(clientRes, { end: true });
        proxyRes.on("end", () => record(status, isSSE));
        return;
      }

      // default: passthrough byte-identical
      if (status) clientRes.writeHead(status, proxyRes.headers);
      proxyRes.pipe(clientRes, { end: true });
    }

    function forward(modelUsed) {
      const { options, outBody } = buildUpstream(modelUsed || reqModel);
      const req = upstreamProto.request(options, onProxyResponse);
      req.on("error", (err) => {
        // Node tidak auto-retry ECONNRESET di reused keep-alive socket; retry manual
        // sekali, hanya idempotent (POST tidak pernah di-retry: risiko double-spend).
        if (
          !retriedSocket &&
          !responseReceived &&
          isIdempotent &&
          req.reusedSocket &&
          (err.code === "ECONNRESET" || err.code === "EPIPE")
        ) {
          retriedSocket = true;
          const r = forward(modelUsed || reqModel);
          r.req.write(r.outBody);
          r.req.end();
          return;
        }
        console.error("Proxy error:", err.message);
        if (!clientRes.headersSent) clientRes.writeHead(502);
        clientRes.end("Bad Gateway");
      });
      return { req, outBody };
    }

    const first = forward(reqModel);
    if (first.outBody.length) first.req.write(first.outBody);
    first.req.end();
  }

  return { server, cfg, auth };
}

// Startup smoke: 1 request streamGenerateContent minimal ke upstream untuk
// memastikan upstream + token store hidup. Non-fatal. (fetchAvailableModels
// flappy 403 — jangan dipakai sebagai smoke.)
async function startupSmoke(auth, cfg) {
  try {
    const token = await auth.getAccessToken();
    const proj = auth.projectId();
    const body = JSON.stringify({
      model: "gemini-3.1-flash-lite",
      ...(proj ? { project: proj } : {}),
      request: { contents: [{ role: "user", parts: [{ text: "ok" }] }] },
    });
    const status = await new Promise((resolve) => {
      const proto = cfg.upstreamTls ? https : http;
      const req = proto.request(
        {
          hostname: cfg.upstreamHost,
          port: cfg.upstreamPort,
          path: "/v1internal:streamGenerateContent?alt=sse",
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Content-Length": body.length,
            Authorization: `Bearer ${token}`,
            "Client-Metadata": CLIENT_METADATA,
            "User-Agent": "antigravity/1.15.8 linux/amd64",
          },
        },
        (res) => {
          res.resume();
          res.on("end", () => resolve(res.statusCode));
        }
      );
      req.on("error", () => resolve(0));
      req.write(body);
      req.end();
    });
    if (status === 200) console.log(`[smoke] upstream ${cfg.upstreamHost} OK (200)`);
    else console.warn(`[smoke] upstream ${cfg.upstreamHost} status=${status} — cek AGY_PROXY_UPSTREAM`);
  } catch (e) {
    console.warn(`[smoke] gagal: ${e.message}`);
  }
}

if (require.main === module) {
  const { server, cfg, auth } = createProxyServer();
  server.listen(cfg.port, cfg.host, async () => {
    console.log(`agy-usage-proxy on http://${cfg.host}:${cfg.port} -> https://${cfg.upstreamHost}/v1internal`);
    console.log(`metrics: ${cfg.metricsFile}`);
    await startupSmoke(auth, cfg);
  });
}

module.exports = { createProxyServer, loadConfig, parseModelMap, createUnwrapStream };
