from __future__ import annotations

import ipaddress
import os
import re
from dataclasses import dataclass
from urllib.parse import urlparse

from curl_cffi import requests


HTTPS_HOST_ORIGIN_RE = re.compile(r"https://[A-Za-z0-9.-]+(?::\d+)?")
HTTPS_IPV6_ORIGIN_RE = re.compile(r"https://\[(?P<host>[0-9A-Fa-f:.]+)\](?::\d+)?")


@dataclass(frozen=True)
class CredentialConfig:
    host: str
    auth_token: str
    login_uuid: str
    cookie: str


def validate_https_origin(value: str) -> str:
    normalized = value[:-1] if value.endswith("/") else value
    parsed = urlparse(normalized)

    if parsed.scheme != "https":
        raise ValueError("PAN_HOST must use https")
    if not parsed.netloc:
        raise ValueError("PAN_HOST must include a host")
    if "@" in parsed.netloc:
        raise ValueError("PAN_HOST must not include userinfo")
    if parsed.path or parsed.params or parsed.query or parsed.fragment:
        raise ValueError(
            "PAN_HOST must be an absolute https origin without path, query, or fragment"
        )

    if HTTPS_HOST_ORIGIN_RE.fullmatch(normalized):
        return normalized

    ipv6_match = HTTPS_IPV6_ORIGIN_RE.fullmatch(normalized)
    if not ipv6_match:
        raise ValueError("PAN_HOST must match https://<host> or https://<host>:<port>")

    try:
        ipaddress.IPv6Address(ipv6_match.group("host"))
    except ValueError as exc:
        raise ValueError("PAN_HOST must match https://<host> or https://<host>:<port>") from exc

    return normalized


def load_credentials() -> CredentialConfig:
    host = validate_https_origin(os.environ.get("PAN_HOST", "https://www.123pan.com"))
    auth_token = os.environ.get("PAN_AUTH_TOKEN", "")
    login_uuid = os.environ.get("PAN_LOGIN_UUID", "")
    cookie = os.environ.get("PAN_COOKIE", "")

    if not auth_token or not login_uuid or not cookie:
        raise ValueError("PAN_AUTH_TOKEN, PAN_LOGIN_UUID, and PAN_COOKIE are required")

    return CredentialConfig(
        host=host,
        auth_token=auth_token,
        login_uuid=login_uuid,
        cookie=cookie,
    )


def build_session(creds: CredentialConfig) -> requests.Session:
    session = requests.Session(impersonate="chrome124")
    session.headers.update(
        {
            "Authorization": f"Bearer {creds.auth_token}",
            "LoginUuid": creds.login_uuid,
            "platform": "web",
            "App-Version": "3",
            "Origin": creds.host,
            "Referer": f"{creds.host}/",
            "Content-Type": "application/json;charset=UTF-8",
            "Cookie": creds.cookie,
        }
    )
    return session
