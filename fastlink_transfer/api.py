from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from curl_cffi.requests import exceptions as request_exceptions


CREDENTIAL_MESSAGES = {"login expired", "Please Login", "not login"}
RETRYABLE_TRANSPORT_NAMES = {
    "DNSError",
    "SSLError",
    "ConnectError",
    "ReadTimeout",
    "ConnectionResetError",
}
RETRYABLE_TRANSPORT_TYPES = (
    TimeoutError,
    ConnectionError,
    request_exceptions.Timeout,
    request_exceptions.ConnectionError,
)


class DecisionKind(Enum):
    RETRYABLE = "retryable"
    CREDENTIAL_FATAL = "credential_fatal"
    FAILED = "failed"
    NOT_REUSABLE = "not_reusable"
    COMPLETED = "completed"
    DIRECTORY_CREATED = "directory_created"


@dataclass(frozen=True)
class Decision:
    kind: DecisionKind
    error: str | None = None
    file_id: str | None = None
    payload: dict | None = None


def classify_transport_error(exc: Exception) -> Decision:
    if isinstance(exc, RETRYABLE_TRANSPORT_TYPES) or exc.__class__.__name__ in RETRYABLE_TRANSPORT_NAMES:
        return Decision(kind=DecisionKind.RETRYABLE, error=str(exc))

    return Decision(kind=DecisionKind.FAILED, error=str(exc))


def classify_json_response(response, *, operation: str) -> Decision:
    status_code = response.status_code
    if status_code in {401, 403}:
        return Decision(kind=DecisionKind.CREDENTIAL_FATAL, error=f"HTTP {status_code}")
    if status_code == 429 or 500 <= status_code <= 599:
        return Decision(kind=DecisionKind.RETRYABLE, error=f"HTTP {status_code}")
    if status_code < 200 or status_code >= 300:
        return Decision(kind=DecisionKind.FAILED, error=f"HTTP {status_code}")

    try:
        payload = response.json()
    except Exception as exc:
        return Decision(kind=DecisionKind.FAILED, error=f"invalid json: {exc}")

    if not isinstance(payload, dict) or "code" not in payload:
        return Decision(kind=DecisionKind.FAILED, error="missing code")

    code = payload["code"]
    message = payload.get("message", "")
    data = payload.get("data")

    if code != 0:
        if isinstance(message, str) and message in CREDENTIAL_MESSAGES:
            return Decision(kind=DecisionKind.CREDENTIAL_FATAL, error=message, payload=payload)
        return Decision(
            kind=DecisionKind.FAILED,
            error=message if isinstance(message, str) and message else f"api code {code}",
            payload=payload,
        )

    if operation == "mkdir":
        if not isinstance(data, dict):
            return Decision(kind=DecisionKind.FAILED, error="missing directory data", payload=payload)
        info = data.get("Info")
        if not isinstance(info, dict):
            return Decision(kind=DecisionKind.FAILED, error="missing directory file id", payload=payload)
        file_id = info.get("FileId")
        if file_id is None:
            return Decision(kind=DecisionKind.FAILED, error="missing directory file id", payload=payload)
        return Decision(
            kind=DecisionKind.DIRECTORY_CREATED,
            file_id=str(file_id),
            payload=payload,
        )

    if not isinstance(data, dict) or "Reuse" not in data:
        return Decision(kind=DecisionKind.FAILED, error="missing reuse", payload=payload)
    if data["Reuse"] is False:
        return Decision(kind=DecisionKind.NOT_REUSABLE, error="Reuse=false", payload=payload)
    if data["Reuse"] is not True:
        return Decision(kind=DecisionKind.FAILED, error="invalid reuse value", payload=payload)

    info = data.get("Info")
    if not isinstance(info, dict):
        return Decision(kind=DecisionKind.FAILED, error="missing file id", payload=payload)
    file_id = info.get("FileId")
    if file_id is None:
        return Decision(kind=DecisionKind.FAILED, error="missing file id", payload=payload)

    return Decision(kind=DecisionKind.COMPLETED, file_id=str(file_id), payload=payload)


class PanApiClient:
    def __init__(self, host: str, session):
        self.host = host.rstrip("/")
        self.session = session

    def _url(self, path: str) -> str:
        return f"{self.host}{path}"

    def mkdir(self, *, parent_file_id: str, folder_name: str) -> Decision:
        payload = {
            "driveId": 0,
            "etag": "",
            "fileName": folder_name,
            "parentFileId": str(parent_file_id),
            "size": 0,
            "type": 1,
            "duplicate": 1,
            "NotReuse": True,
            "event": "newCreateFolder",
            "operateType": 1,
            "RequestSource": None,
        }
        try:
            response = self.session.post(self._url("/b/api/file/upload_request"), json=payload)
        except Exception as exc:
            return classify_transport_error(exc)

        return classify_json_response(response, operation="mkdir")

    def rapid_upload(
        self,
        *,
        etag: str,
        size: int,
        file_name: str,
        parent_file_id: str,
        before_request=None,
    ) -> Decision:
        payload = {
            "driveId": 0,
            "etag": etag,
            "fileName": file_name,
            "parentFileId": str(parent_file_id),
            "size": int(size),
            "type": 0,
            "duplicate": 1,
            "RequestSource": None,
        }
        if before_request is not None:
            before_request()
        try:
            response = self.session.post(self._url("/b/api/file/upload_request"), json=payload)
        except Exception as exc:
            return classify_transport_error(exc)

        return classify_json_response(response, operation="file")
