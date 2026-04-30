#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT / ".env"


def read_env(path: Path) -> tuple[list[str], dict[str, str]]:
    lines = path.read_text().splitlines() if path.exists() else []
    values: dict[str, str] = {}
    for line in lines:
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key] = value
    return lines, values


def write_env(path: Path, original_lines: list[str], updates: dict[str, str], remove: set[str]) -> None:
    seen: set[str] = set()
    output: list[str] = []
    for line in original_lines:
        if not line or line.startswith("#") or "=" not in line:
            output.append(line)
            continue
        key, _ = line.split("=", 1)
        if key in remove:
            continue
        if key in updates:
            output.append(f"{key}={updates[key]}")
            seen.add(key)
        else:
            output.append(line)

    if updates.keys() - seen:
        if output and output[-1] != "":
            output.append("")
        for key in sorted(updates.keys() - seen):
            output.append(f"{key}={updates[key]}")

    path.write_text("\n".join(output).rstrip() + "\n")


class Authentik:
    def __init__(self, base_url: str, token: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token

    def request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> Any:
        data = None
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.token}",
        }
        if payload is not None:
            data = json.dumps(payload).encode()
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(self.base_url + path, data=data, headers=headers, method=method)
        with urllib.request.urlopen(req, timeout=30) as response:
            body = response.read()
            if not body:
                return None
            return json.loads(body.decode())

    def get(self, path: str) -> Any:
        return self.request("GET", path)

    def patch(self, path: str, payload: dict[str, Any]) -> Any:
        return self.request("PATCH", path, payload)

    def post(self, path: str, payload: dict[str, Any]) -> Any:
        return self.request("POST", path, payload)


def first_result(page: dict[str, Any], label: str) -> dict[str, Any]:
    results = page.get("results", [])
    if not results:
        raise RuntimeError(f"Could not find Authentik {label}")
    return results[0]


def main() -> int:
    lines, values = read_env(ENV_PATH)
    token = values.get("AUTHENTIK_API_TOKEN") or values.get("AUTHENTIK_API_KEY")
    if not token:
        raise RuntimeError("Missing AUTHENTIK_API_TOKEN in .env")

    base_url = values.get("AUTHENTIK_URL", "https://authentik.login.no")
    app_slug = values.get("AUTHENTIK_APPLICATION_SLUG", "s3")
    allowed_group = values.get("AUTHENTIK_ALLOWED_GROUP", "s3")
    console_url = values.get("RUSTFS_CONSOLE_PUBLIC_URL", "https://spaces.login.no").rstrip("/")
    redirect_uri = f"{console_url}/rustfs/admin/v3/oidc/callback/default"

    authentik = Authentik(base_url, token)
    app = authentik.get(f"/api/v3/core/applications/{urllib.parse.quote(app_slug)}/")
    provider_id = app["provider"]
    provider = authentik.get(f"/api/v3/providers/oauth2/{provider_id}/")
    group = first_result(
        authentik.get("/api/v3/core/groups/?" + urllib.parse.urlencode({"search": allowed_group})),
        f"group {allowed_group}",
    )

    redirect_uris = provider.get("redirect_uris") or []
    redirect_entry = {"matching_mode": "strict", "url": redirect_uri}
    if redirect_entry not in redirect_uris:
        redirect_uris.append(redirect_entry)

    provider_updates = {
        "redirect_uris": redirect_uris,
        "include_claims_in_id_token": True,
    }
    authentik.patch(f"/api/v3/providers/oauth2/{provider_id}/", provider_updates)

    bindings = authentik.get(
        "/api/v3/policies/bindings/?"
        + urllib.parse.urlencode({"page_size": 100, "target": app["pk"]})
    ).get("results", [])
    has_group_binding = any(binding.get("group") == group["pk"] and binding.get("enabled", True) for binding in bindings)
    if not has_group_binding:
        authentik.post(
            "/api/v3/policies/bindings/",
            {
                "target": app["pk"],
                "group": group["pk"],
                "order": 0,
                "enabled": True,
                "negate": False,
                "failure_result": False,
            },
        )

    updates = {
        "AUTHENTIK_URL": base_url.rstrip("/"),
        "AUTHENTIK_APPLICATION_SLUG": app_slug,
        "AUTHENTIK_ALLOWED_GROUP": allowed_group,
        "AUTHENTIK_API_TOKEN": token,
        "RUSTFS_IDENTITY_OPENID_ENABLE": "on",
        "RUSTFS_IDENTITY_OPENID_CONFIG_URL": f"{base_url.rstrip('/')}/application/o/{app_slug}/",
        "RUSTFS_IDENTITY_OPENID_CLIENT_ID": provider["client_id"],
        "RUSTFS_IDENTITY_OPENID_CLIENT_SECRET": provider["client_secret"],
        "RUSTFS_IDENTITY_OPENID_SCOPES": "openid,profile,email",
        "RUSTFS_IDENTITY_OPENID_REDIRECT_URI": redirect_uri,
        "RUSTFS_IDENTITY_OPENID_REDIRECT_URI_DYNAMIC": "off",
        "RUSTFS_IDENTITY_OPENID_CLAIM_NAME": "groups",
        "RUSTFS_IDENTITY_OPENID_GROUPS_CLAIM": "groups",
        "RUSTFS_IDENTITY_OPENID_ROLE_POLICY": "consoleAdmin",
        "RUSTFS_IDENTITY_OPENID_DISPLAY_NAME": "Authentik",
    }
    remove = {"AUTHENTIK_API_KEY", "AUTHENTIK_CLIENT_ID", "AUTHENTIK_CLIENT_SECRET"}
    write_env(ENV_PATH, lines, updates, remove)

    print(f"synced app={app_slug} provider={provider_id} group={allowed_group} redirect={redirect_uri}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"authentik sync failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
