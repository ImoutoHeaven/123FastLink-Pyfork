from types import SimpleNamespace

from curl_cffi.requests import exceptions as request_exceptions

from fastlink_transfer.api import (
    DecisionKind,
    PanApiClient,
    classify_json_response,
    classify_transport_error,
)


def test_classify_transport_error_marks_timeout_retryable():
    decision = classify_transport_error(TimeoutError("boom"))

    assert decision.kind is DecisionKind.RETRYABLE


def test_classify_transport_error_marks_generic_runtime_error_failed():
    decision = classify_transport_error(RuntimeError("boom"))

    assert decision.kind is DecisionKind.FAILED


def test_classify_transport_error_marks_curl_cffi_timeout_retryable():
    decision = classify_transport_error(request_exceptions.Timeout("boom"))

    assert decision.kind is DecisionKind.RETRYABLE


def test_classify_transport_error_marks_curl_cffi_connect_timeout_retryable():
    decision = classify_transport_error(request_exceptions.ConnectTimeout("boom"))

    assert decision.kind is DecisionKind.RETRYABLE


def test_classify_transport_error_marks_curl_cffi_connection_error_retryable():
    decision = classify_transport_error(request_exceptions.ConnectionError("boom"))

    assert decision.kind is DecisionKind.RETRYABLE


def test_classify_response_marks_401_as_credential_fatal():
    response = SimpleNamespace(status_code=401, json=lambda: {"code": 401, "message": "bad"})

    decision = classify_json_response(response, operation="file")

    assert decision.kind is DecisionKind.CREDENTIAL_FATAL


def test_classify_response_marks_429_as_retryable():
    response = SimpleNamespace(
        status_code=429,
        json=lambda: {"code": 429, "message": "slow down"},
    )

    decision = classify_json_response(response, operation="file")

    assert decision.kind is DecisionKind.RETRYABLE


def test_classify_response_marks_invalid_json_as_failed():
    def raise_value_error():
        raise ValueError("bad json")

    response = SimpleNamespace(status_code=200, json=raise_value_error)

    decision = classify_json_response(response, operation="file")

    assert decision.kind is DecisionKind.FAILED


def test_classify_response_marks_missing_code_as_failed():
    response = SimpleNamespace(status_code=200, json=lambda: {"message": "missing code"})

    decision = classify_json_response(response, operation="file")

    assert decision.kind is DecisionKind.FAILED


def test_classify_response_marks_login_expired_body_as_credential_fatal():
    response = SimpleNamespace(
        status_code=200,
        json=lambda: {"code": 1001, "message": "login expired"},
    )

    decision = classify_json_response(response, operation="file")

    assert decision.kind is DecisionKind.CREDENTIAL_FATAL


def test_classify_response_marks_nonzero_code_as_failed_when_not_credential_message():
    response = SimpleNamespace(
        status_code=200,
        json=lambda: {"code": 1002, "message": "bad payload"},
    )

    decision = classify_json_response(response, operation="file")

    assert decision.kind is DecisionKind.FAILED


def test_classify_response_marks_unhashable_list_message_as_failed():
    response = SimpleNamespace(
        status_code=200,
        json=lambda: {"code": 1002, "message": ["bad payload"]},
    )

    decision = classify_json_response(response, operation="file")

    assert decision.kind is DecisionKind.FAILED


def test_classify_response_marks_unhashable_dict_message_as_failed():
    response = SimpleNamespace(
        status_code=200,
        json=lambda: {"code": 1002, "message": {"bad": True}},
    )

    decision = classify_json_response(response, operation="file")

    assert decision.kind is DecisionKind.FAILED


def test_classify_response_marks_missing_reuse_as_failed():
    response = SimpleNamespace(status_code=200, json=lambda: {"code": 0, "data": {}})

    decision = classify_json_response(response, operation="file")

    assert decision.kind is DecisionKind.FAILED


def test_classify_response_marks_reuse_false_as_not_reusable():
    response = SimpleNamespace(
        status_code=200,
        json=lambda: {"code": 0, "data": {"Reuse": False}},
    )

    decision = classify_json_response(response, operation="file")

    assert decision.kind is DecisionKind.NOT_REUSABLE


def test_classify_response_requires_info_file_id_for_successful_file():
    response = SimpleNamespace(
        status_code=200,
        json=lambda: {"code": 0, "data": {"Reuse": True, "Info": {}}},
    )

    decision = classify_json_response(response, operation="file")

    assert decision.kind is DecisionKind.FAILED


def test_classify_response_returns_completed_with_file_id():
    response = SimpleNamespace(
        status_code=200,
        json=lambda: {"code": 0, "data": {"Reuse": True, "Info": {"FileId": 9001}}},
    )

    decision = classify_json_response(response, operation="file")

    assert decision.kind is DecisionKind.COMPLETED
    assert decision.file_id == "9001"


def test_classify_response_requires_info_file_id_for_directory_creation():
    response = SimpleNamespace(status_code=200, json=lambda: {"code": 0, "data": {}})

    decision = classify_json_response(response, operation="mkdir")

    assert decision.kind is DecisionKind.FAILED


def test_classify_response_returns_directory_created():
    response = SimpleNamespace(
        status_code=200,
        json=lambda: {"code": 0, "data": {"Info": {"FileId": 7001}}},
    )

    decision = classify_json_response(response, operation="mkdir")

    assert decision.kind is DecisionKind.DIRECTORY_CREATED
    assert decision.file_id == "7001"


class FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def post(self, url, json):
        self.calls.append((url, json))
        return self.response


def test_mkdir_posts_frozen_payload():
    session = FakeSession(FakeResponse(200, {"code": 0, "data": {"Info": {"FileId": 7}}}))
    client = PanApiClient(host="https://www.123pan.com", session=session)

    decision = client.mkdir(parent_file_id="12345678", folder_name="Demo")

    assert decision.kind is DecisionKind.DIRECTORY_CREATED
    assert session.calls == [
        (
            "https://www.123pan.com/b/api/file/upload_request",
            {
                "driveId": 0,
                "etag": "",
                "fileName": "Demo",
                "parentFileId": "12345678",
                "size": 0,
                "type": 1,
                "duplicate": 1,
                "NotReuse": True,
                "RequestSource": None,
            },
        )
    ]


def test_rapid_upload_posts_frozen_payload():
    session = FakeSession(
        FakeResponse(200, {"code": 0, "data": {"Reuse": True, "Info": {"FileId": 9}}})
    )
    client = PanApiClient(host="https://www.123pan.com", session=session)

    decision = client.rapid_upload(
        etag="0123456789abcdef0123456789abcdef",
        size=219448,
        file_name="example.rar",
        parent_file_id="12345678",
    )

    assert decision.kind is DecisionKind.COMPLETED
    assert session.calls == [
        (
            "https://www.123pan.com/b/api/file/upload_request",
            {
                "driveId": 0,
                "etag": "0123456789abcdef0123456789abcdef",
                "fileName": "example.rar",
                "parentFileId": "12345678",
                "size": 219448,
                "type": 0,
                "duplicate": 1,
                "RequestSource": None,
            },
        )
    ]


def test_rapid_upload_per_call_before_request_hook_runs_before_post():
    session = FakeSession(
        FakeResponse(200, {"code": 0, "data": {"Reuse": True, "Info": {"FileId": 9}}})
    )
    events = []

    def before_request():
        events.append("before_request")

    client = PanApiClient(host="https://www.123pan.com", session=session)

    decision = client.rapid_upload(
        etag="0123456789abcdef0123456789abcdef",
        size=219448,
        file_name="example.rar",
        parent_file_id="12345678",
        before_request=before_request,
    )

    assert decision.kind is DecisionKind.COMPLETED
    assert events == ["before_request"]
    assert session.calls == [
        (
            "https://www.123pan.com/b/api/file/upload_request",
            {
                "driveId": 0,
                "etag": "0123456789abcdef0123456789abcdef",
                "fileName": "example.rar",
                "parentFileId": "12345678",
                "size": 219448,
                "type": 0,
                "duplicate": 1,
                "RequestSource": None,
            },
        )
    ]
