// metrics.js: token-usage tracking for agy-usage-proxy. Built-ins only.
// Pola: kimi-proxy metrics.js — SSE incremental tracker (StringDecoder + line
// buffer), append-only jsonl, aggregate rebuild dari replay file.
const fs = require("node:fs");
const path = require("node:path");
const { StringDecoder } = require("node:string_decoder");

let metricsFile = null;
let since = new Date().toISOString();
let requests = 0;
let errors = 0;
let totals = freshTotals();
let byModel = {};
let bySource = {};

function freshTotals() {
  return {
    promptTokens: 0,
    outputTokens: 0,
    thinkingTokens: 0,
    cacheReadTokens: 0,
    totalTokens: 0,
  };
}

function num(v) {
  return typeof v === "number" && Number.isFinite(v) ? v : null;
}

// Normalize Gemini usageMetadata -> our entry token fields.
// cachedContentTokenCount preferred; fallback sum(cacheTokensDetails[].tokenCount).
function normUsage(u) {
  if (!u || typeof u !== "object") {
    return {
      promptTokens: null,
      outputTokens: null,
      thinkingTokens: null,
      cacheReadTokens: null,
      cacheWriteTokens: null,
      totalTokens: null,
    };
  }
  let cacheRead = num(u.cachedContentTokenCount);
  if (cacheRead == null && Array.isArray(u.cacheTokensDetails)) {
    let s = 0;
    let found = false;
    for (const d of u.cacheTokensDetails) {
      const t = num(d && d.tokenCount);
      if (t != null) {
        s += t;
        found = true;
      }
    }
    if (found) cacheRead = s;
  }
  return {
    promptTokens: num(u.promptTokenCount),
    outputTokens: num(u.candidatesTokenCount),
    thinkingTokens: num(u.thoughtsTokenCount),
    cacheReadTokens: cacheRead,
    cacheWriteTokens: null, // Gemini usageMetadata tidak expose cache-write
    totalTokens: num(u.totalTokenCount),
  };
}

// SSE merge semantics (single source of truth): payload boleh dibungkus
// {"response": {...}} (cloudcode v1internal) atau raw Gemini. usageMetadata
// last-one-wins (Gemini mengirim kumulatif di akhir stream).
function mergeSSELine(line, state) {
  const t = line.trim();
  if (!t.startsWith("data:")) return;
  const payload = t.slice(5).trim();
  if (!payload || payload === "[DONE]") return;
  let evt;
  try {
    evt = JSON.parse(payload);
  } catch (e) {
    return;
  }
  if (evt && typeof evt === "object" && evt.response && typeof evt.response === "object") {
    evt = evt.response; // unwrap cloudcode wrapper
  }
  if (evt && typeof evt === "object") {
    if (evt.usageMetadata && typeof evt.usageMetadata === "object") {
      state.usage = evt.usageMetadata;
    }
    if (!state.model && typeof evt.modelVersion === "string") state.model = evt.modelVersion;
    if (!state.model && typeof evt.model === "string") state.model = evt.model;
  }
}

// Reference parser (kept for equivalence/fuzz tests).
function parseSSEUsage(text) {
  const state = { usage: null, model: null };
  for (const line of text.split("\n")) mergeSSELine(line, state);
  return { usage: state.usage, model: state.model };
}

// Incremental SSE usage tracker: StringDecoder (multi-byte UTF-8 bisa split
// antar-chunk) + line buffer. Observer-only.
function createUsageTracker() {
  const decoder = new StringDecoder("utf8");
  const state = { usage: null, model: null };
  let pending = "";
  function drain() {
    let idx;
    while ((idx = pending.indexOf("\n")) >= 0) {
      const line = pending.slice(0, idx);
      pending = pending.slice(idx + 1);
      mergeSSELine(line, state);
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
        mergeSSELine(pending, state);
        pending = "";
      }
      return { usage: state.usage, model: state.model };
    },
  };
}

function init(file) {
  metricsFile = file;
  since = new Date().toISOString();
  requests = 0;
  errors = 0;
  totals = freshTotals();
  byModel = {};
  bySource = {};
  fs.mkdirSync(path.dirname(file), { recursive: true });
  try {
    if (fs.existsSync(file)) {
      const lines = fs.readFileSync(file, "utf8").split("\n");
      let firstTs = null;
      for (const line of lines) {
        if (!line.trim()) continue;
        try {
          const entry = JSON.parse(line);
          if (!firstTs && typeof entry.ts === "string") firstTs = entry.ts; // append-only: oldest first
          accumulate(entry);
        } catch (e) {
          /* tolerate corrupt lines */
        }
      }
      if (firstTs) since = firstTs;
      if (requests > 0) console.log(`[metrics] rebuilt aggregate: ${requests} requests from ${file}`);
    }
  } catch (e) {
    console.error("[metrics] aggregate rebuild failed:", e.message);
  }
}

function addTokens(bucket, entry) {
  bucket.promptTokens += entry.promptTokens || 0;
  bucket.outputTokens += entry.outputTokens || 0;
  bucket.thinkingTokens += entry.thinkingTokens || 0;
  bucket.cacheReadTokens += entry.cacheReadTokens || 0;
  bucket.totalTokens += entry.totalTokens || 0;
}

function freshBucket() {
  return { requests: 0, errors: 0, ...freshTotals() };
}

function accumulate(entry) {
  requests++;
  if (entry.status >= 400) errors++;
  addTokens(totals, entry);
  const m = entry.model || "unknown";
  if (!byModel[m]) byModel[m] = freshBucket();
  byModel[m].requests++;
  if (entry.status >= 400) byModel[m].errors++;
  addTokens(byModel[m], entry);
  const s = entry.source || "agy";
  if (!bySource[s]) bySource[s] = freshBucket();
  bySource[s].requests++;
  if (entry.status >= 400) bySource[s].errors++;
  addTokens(bySource[s], entry);
}

// Called once per completed upstream response for model calls. Never throws.
function track(entry) {
  try {
    if (metricsFile) fs.appendFileSync(metricsFile, JSON.stringify(entry) + "\n");
  } catch (e) {
    console.error("[metrics] append failed:", e.message);
  }
  try {
    accumulate(entry);
  } catch (e) {
    console.error("[metrics] accumulate failed:", e.message);
  }
}

function sendJson(res, code, obj) {
  const b = JSON.stringify(obj);
  res.writeHead(code, { "content-type": "application/json" });
  res.end(b);
}

function stripTotals(t) {
  return {
    promptTokens: t.promptTokens,
    outputTokens: t.outputTokens,
    thinkingTokens: t.thinkingTokens,
    cacheReadTokens: t.cacheReadTokens,
    totalTokens: t.totalTokens,
  };
}

// Handles only GET /_proxy/stats.
function handleAdmin(req, res) {
  try {
    const u = new URL(req.url, "http://localhost");
    if (req.method === "GET" && u.pathname === "/_proxy/stats") {
      sendJson(res, 200, {
        since,
        requests,
        errors,
        totals: stripTotals(totals),
        byModel,
        bySource,
      });
    } else {
      sendJson(res, 404, { error: "not found" });
    }
  } catch (e) {
    console.error("[metrics] admin failed:", e.message);
    try {
      sendJson(res, 500, { error: "metrics error" });
    } catch (e2) {
      /* ignore */
    }
  }
  return true;
}

module.exports = { init, track, handleAdmin, parseSSEUsage, createUsageTracker, normUsage };
