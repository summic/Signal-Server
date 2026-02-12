# OAuth Phase 1 (Bootstrap)

This phase adds optional OAuth Bearer-token authentication to `signal-server` while keeping existing Basic auth working by default.

## Config

Add an `oauth` section in server config:

```yaml
oauth:
  enabled: false
  allowBasicAuth: true
  issuer: "https://auth.example.com/"
  audience: "signal-api"
  jwksUrl: "https://auth.example.com/.well-known/jwks.json"
  algorithm: "RS256"
  clockSkewSeconds: 60
```

## Token claim contract

Access token must be JWT (RS256) and include:

- `sub` (string): account UUID
- `device_id` (number): device id (1..127)
- `primary_device_last_seen_epoch_seconds` (number, optional): epoch seconds

Standard JWT validation:

- signature via configured `jwksUrl`
- issuer (if configured)
- audience (if configured)
- `exp/nbf/iat` with `clockSkewSeconds` leeway

## Behavior

- `oauth.enabled = false`: existing behavior (Basic auth only)
- `oauth.enabled = true` + valid OAuth config:
  - REST accepts Bearer; Basic remains accepted when `allowBasicAuth=true`
  - gRPC interceptor accepts Bearer and Basic fallback
- `oauth.enabled = true` but invalid/missing OAuth config:
  - server logs warning and keeps Basic auth path
