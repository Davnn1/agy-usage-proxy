# agy-usage-proxy

Zero-dependency local telemetry & reverse-proxy for Antigravity (CLI & ACP) to track real-time token metrics, cache hits, and generation speed (TPS).

Designed to seamlessly pair with the [paseo-monitoring](https://github.com/Davnn1/paseo-usage-plugin) plugin for Paseo.

## Features

- **Zero dependencies:** Pure Node.js built-ins (`http`, `https`, `node:test`).
- **Telemetry logging:** Captures Gemini SSE streaming chunks and records `promptTokens`, `cacheReadTokens`, and duration into JSONL without altering payloads.
- **Token management:** Attaches local auth tokens transparently.
- **Model fallbacks:** Automatic suffix/tier routing (`-low`, `-medium`, `-high`) if models 404 upstream.

## Installation & Running

```bash
# Clone
git clone https://github.com/Davnn1/agy-usage-proxy.git
cd agy-usage-proxy

# Run
npm start
# Proxy listens on http://127.0.0.1:20129 by default
```

## Pointing Antigravity

Point Antigravity gateway to this proxy:

```bash
export AGY_LLM_GATEWAY_URL="http://127.0.0.1:20129/cli"
```

## Running Tests

```bash
npm test
```

## License

MIT
