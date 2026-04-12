import pytest

from fastlink_transfer.auth import CredentialConfig, build_session, load_credentials


def test_load_credentials_reads_required_environment(monkeypatch):
    monkeypatch.setenv("PAN_AUTH_TOKEN", "token")
    monkeypatch.setenv("PAN_LOGIN_UUID", "uuid")
    monkeypatch.setenv("PAN_COOKIE", "cookie=value")

    creds = load_credentials()

    assert creds == CredentialConfig(
        host="https://www.123pan.com",
        auth_token="token",
        login_uuid="uuid",
        cookie="cookie=value",
    )


@pytest.mark.parametrize(
    "value",
    [
        "http://www.123pan.com",
        "https://www.123pan.com/path",
        "https://www.123pan.com/path/",
        "https://www.123pan.com?x=1",
        "https://www.123pan.com/#x",
        "www.123pan.com",
        "https://exa mple.com",
        "https:// example.com",
        "https://www.123pan.com ",
        "https://www.123pan.com\\path",
        "https://www.123pan.com;param",
        "https://www.123pan.com%20",
    ],
)
def test_load_credentials_rejects_invalid_pan_host(monkeypatch, value):
    monkeypatch.setenv("PAN_AUTH_TOKEN", "token")
    monkeypatch.setenv("PAN_LOGIN_UUID", "uuid")
    monkeypatch.setenv("PAN_COOKIE", "cookie=value")
    monkeypatch.setenv("PAN_HOST", value)

    with pytest.raises(ValueError):
        load_credentials()


def test_load_credentials_accepts_single_trailing_slash(monkeypatch):
    monkeypatch.setenv("PAN_AUTH_TOKEN", "token")
    monkeypatch.setenv("PAN_LOGIN_UUID", "uuid")
    monkeypatch.setenv("PAN_COOKIE", "cookie=value")
    monkeypatch.setenv("PAN_HOST", "https://www.123pan.com/")

    assert load_credentials().host == "https://www.123pan.com"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("https://[::1]", "https://[::1]"),
        ("https://[::1]:8443", "https://[::1]:8443"),
    ],
)
def test_load_credentials_accepts_valid_ipv6_pan_host(monkeypatch, value, expected):
    monkeypatch.setenv("PAN_AUTH_TOKEN", "token")
    monkeypatch.setenv("PAN_LOGIN_UUID", "uuid")
    monkeypatch.setenv("PAN_COOKIE", "cookie=value")
    monkeypatch.setenv("PAN_HOST", value)

    assert load_credentials().host == expected


def test_build_session_applies_frozen_headers(monkeypatch):
    monkeypatch.setenv("PAN_AUTH_TOKEN", "token")
    monkeypatch.setenv("PAN_LOGIN_UUID", "uuid")
    monkeypatch.setenv("PAN_COOKIE", "cookie=value")

    session = build_session(load_credentials())

    assert session.headers["Authorization"] == "Bearer token"
    assert session.headers["LoginUuid"] == "uuid"
    assert session.headers["platform"] == "web"
    assert session.headers["App-Version"] == "3"
    assert session.headers["Origin"] == "https://www.123pan.com"
    assert session.headers["Referer"] == "https://www.123pan.com/"
    assert session.headers["Content-Type"] == "application/json;charset=UTF-8"
    assert session.headers["Cookie"] == "cookie=value"
