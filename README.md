# Login S3

Self-hosted S3-compatible object storage for Login, powered by RustFS.

## Endpoints

- S3 API: `https://s3.login.no`
- Local API port: `9100`
- Console: `https://spaces.login.no` behind the NTNU/VPN nginx gate
- Local console port: `9101`

## First Run

Create a local `.env` file before starting the service:

```sh
cp .env.example .env
openssl rand -base64 48
```

Put the generated value in `RUSTFS_SECRET_KEY`, then start RustFS:

```sh
docker compose up -d
```

## Programmatic Access

Use a normal AWS S3 SDK with path-style endpoints:

```ts
import { S3Client } from '@aws-sdk/client-s3'

export const s3 = new S3Client({
  region: 'us-east-1',
  endpoint: 'https://s3.login.no',
  forcePathStyle: true,
  credentials: {
    accessKeyId: process.env.S3_ACCESS_KEY!,
    secretAccessKey: process.env.S3_SECRET_KEY!,
  },
})
```

## Operations

```sh
docker compose pull
docker compose up -d
docker compose logs -f rustfs
```

## Authentik Sync

The RustFS OIDC configuration is generated from the Authentik `s3` application and provider:

```sh
scripts/sync-authentik-oidc.py
docker compose up -d
```

The sync script is idempotent. It keeps the Authentik `s3` app restricted to the `s3` group and maps accepted OIDC users to RustFS `consoleAdmin`.

Keep `.env` and object data out of git.
