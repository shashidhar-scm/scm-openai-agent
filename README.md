# openai-agent-service

Simple OpenAI-powered chat API that the dashboard can call.

## Env

- `PORT` (default: `8091`)
- `AGENT_API_KEY` or `AGENT_API_KEYS` (comma-separated) - required. Used as `X-API-Key` when calling this service.
- `OPENAI_API_KEY` - required
- `OPENAI_MODEL` (default: `gpt-4o-mini`)
- `TOOL_GATEWAY_BASE_URL` (default: `https://tool-gateway.citypost.us`)
- `TOOL_GATEWAY_API_KEY` - required (used to call scm-agent-tool)
- `MOCK_MODE` (default: `false`) - if set to `true` or `1`, the service will not call OpenAI and will return a deterministic mock response (still attempts tool-gateway fetches for impressions)
- `DATABASE_URL` - required (Postgres). Used for conversation/session history storage.
- `AUTO_CREATE_DB` (default: `false`) - if set to `true` or `1`, attempts to create the database in `DATABASE_URL` if it does not exist (requires DB privileges).
- `MAINTENANCE_DB` (default: `postgres`) - database to connect to when creating the target database.
- `CORS_ALLOWED_ORIGINS` (default: empty) - comma-separated list of allowed browser origins for CORS (e.g. `http://localhost:4200`).

Do not place secrets in repo files. Set them as environment variables (or Kubernetes secrets) at runtime.

## Run

```bash
go run ./cmd/api
```

## Deploy

### Docker

Build:
```bash
docker buildx build --platform linux/amd64 -t smartcitymedia/scm-openai-api:0.0.3 --push .
```

Run:
```bash
docker run --rm -p 8091:8091 \
  -e PORT=8091 \
  -e DATABASE_URL=... \
  -e TOOL_GATEWAY_BASE_URL=http://localhost:7070 \
  -e TOOL_GATEWAY_API_KEY=... \
  -e OPENAI_API_KEY=... \
  -e AGENT_API_KEYS=dev-local-key \
  openai-agent-service:latest
```

### Kubernetes

Manifests live in `k8s/`:
- `k8s/deployment.yaml`
- `k8s/service.yaml`
- `k8s/ingress.yaml`
- `k8s/secret.example.yaml`

Create secret (example):
```bash
kubectl apply -f k8s/secret.example.yaml
```

Apply resources:
```bash
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml
kubectl apply -f k8s/ingress.yaml
```

## API

### POST /conversations

Creates a new conversation and returns a `conversation_id`.

### GET /conversations/{id}

Fetch conversation metadata.

### GET /conversations/{id}/messages?limit=20

Fetch recent chat messages for the conversation.

### POST /chat
Header:
- `X-API-Key: <AGENT_API_KEY>`

Body:
```json
{ "message": "...", "conversation_id": "..." }
```

## Tool access (via scm-agent-tool)

When `MOCK_MODE=false`, the service can call internal SCM APIs through `scm-agent-tool` using a generic tool function (`scm_request`).

Only endpoints that exist in the tool gateway OpenAPI spec (`GET /openapi.json`) are allowed.

Gateway base URL:
- Prod: `https://tool-gateway.citypost.us`
- Local: `http://localhost:7070`

Safety limits:
- Max tool calls per user request: `6`
- Max tool response bytes per call: `1_000_000` (responses are truncated if larger)

Response:
```json
{
  "answer": "...",
  "data": {
    "campaign_impressions": {
      "campaign_id": "...",
      "impressions": 0,
      "posters": [
        {
          "poster_id": "...",
          "poster_name": "...",
          "impressions": 0,
          "play_time": 0
        }
      ]
    }
  },
  "steps": []
}
```

### POST /chat/stream

Streams responses via Server-Sent Events (SSE).

Headers:
- `X-API-Key: <AGENT_API_KEY>`
- `Content-Type: application/json`

Body:
```json
{ "message": "..." }
```

Events:
- `event: token` -> `{"text":"..."}`
- `event: final` -> full `ChatResponse` JSON (same shape as `/chat`)
- `event: error` -> `{"error":"...","message":"..."}`


export PORT=8091
export DATABASE_URL="postgres://postgres:asterisk@localhost:5432/openai_agent_service?sslmode=disable"
export TOOL_GATEWAY_BASE_URL="https://tool-gateway.citypost.us"
export TOOL_GATEWAY_API_KEY="<tool-gateway-api-key>"
export OPENAI_API_KEY="<openai-api-key>"
export AGENT_API_KEYS="dev-local-key"
