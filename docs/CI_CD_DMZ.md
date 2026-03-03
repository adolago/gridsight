# CI/CD to Isolated Edge VM

This workflow deploys directly to an edge VM endpoint and does **not** use any internal jump host.

Workflow file: [.github/workflows/deploy-dmz.yml](../.github/workflows/deploy-dmz.yml)

## 1. Required GitHub Secrets

- `DMZ_SSH_HOST`: public SSH endpoint for the isolated edge VM
- `DMZ_SSH_USER`: deploy account on the edge VM
- `DMZ_SSH_PRIVATE_KEY`: private key for that user

Optional:

- `DMZ_SSH_PORT`: defaults to `22`
- `DMZ_SSH_HOST_KEY`: pinned host key line (recommended)
- `CF_ACCESS_CLIENT_ID`: Cloudflare Access service token id (preferred)
- `CF_ACCESS_CLIENT_SECRET`: Cloudflare Access service token secret (preferred)

If service token secrets are set, workflow uses `cloudflared access tcp` and connects through Access.
If not, workflow falls back to direct SSH.

## 2. Edge VM requirements

- App directory exists: `/opt/docker/gridsight`
- `.env` exists in `/opt/docker/gridsight/.env`
- User can run Docker commands (`docker-compose` or `docker compose`)

## 3. Deploy behavior

On push to `main` (or manual dispatch), workflow:

1. syncs repo to `/opt/docker/gridsight` (excluding `data/`)
2. runs [scripts/deploy_dmz.sh](../scripts/deploy_dmz.sh) remotely
3. script does:
   - `up -d --build`
   - if artifacts exist: `gridsight db-load --truncate`
   - else: full `bootstrap` + `db-bootstrap`
   - checks `http://127.0.0.1:<api_port>/health`

## 4. Isolation controls in workflow

- SSH target guardrails reject internal hostnames/IP ranges.
- SSH command disables jump/proxy options:
  - `-o ProxyJump=none`
  - `-o ProxyCommand=none`
- Sync step protects runtime-sensitive files (`.env`, `data/`, `cloudflared/`) from accidental deletion.

## 5. Recommended hardening

- Use a dedicated `deploy` user (not personal admin user).
- Restrict deploy key in `authorized_keys` with `from=` and forced command wrappers.
- Pin `DMZ_SSH_HOST_KEY` in GitHub secrets instead of relying on `ssh-keyscan`.
- Keep edge VM firewall policy deny-by-default toward LAN/internal segments.
