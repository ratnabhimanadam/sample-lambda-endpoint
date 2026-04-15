import json
import os
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("GCP_SA_SECRET_NAME", "gcp-sa-key")
os.environ.setdefault("ASSUME_ROLE_ARN", "arn:aws:iam::123456789012:role/GcpStsRole")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("PUBSUB_TOPIC_ID", "projects/my-gcp-project/topics/transfer-status")
os.environ.setdefault("PUBSUB_SUBSCRIPTION_ID", "projects/my-gcp-project/subscriptions/transfer-status-sub")

from google.cloud import storage_transfer_v1

from src.handler import handler  # noqa: E402

GCP_SA_INFO = {
    "type": "service_account",
    "project_id": "my-gcp-project",
    "private_key_id": "key123",
    "private_key": "-----BEGIN RSA PRIVATE KEY-----\nMIIBogIBAAJBANjA\n-----END RSA PRIVATE KEY-----\n",
    "client_email": "sa@my-gcp-project.iam.gserviceaccount.com",
    "client_id": "1234567890",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
}


def _event(method, path, body=None):
    e = {"requestContext": {"http": {"method": method, "path": path}}}
    if body is not None:
        e["body"] = json.dumps(body)
    return e


def _ctx():
    ctx = MagicMock()
    ctx.aws_request_id = "test-request-id"
    return ctx


# ---------------------------------------------------------------------------
# /hello tests
# ---------------------------------------------------------------------------

class TestGetHello:
    def test_returns_greeting(self):
        res = handler(_event("GET", "/hello"), _ctx())
        assert res["statusCode"] == 200
        body = json.loads(res["body"])
        assert body["message"] == "Hello from Lambda!"

class TestPostHello:
    def test_returns_personalised_greeting(self):
        res = handler(_event("POST", "/hello", {"name": "Alice"}), _ctx())
        assert res["statusCode"] == 200
        body = json.loads(res["body"])
        assert body["message"] == "Hello, Alice!"

class TestUnknownRoute:
    def test_returns_404(self):
        res = handler(_event("GET", "/unknown"), _ctx())
        assert res["statusCode"] == 404


# ---------------------------------------------------------------------------
# /transfer tests
# ---------------------------------------------------------------------------

def _transfer_event(body):
    return _event("POST", "/transfer", body)


def _mock_gcp_credentials():
    """Patch Secrets Manager + google credentials construction."""
    creds_mock = MagicMock()
    creds_mock.project_id = "my-gcp-project"
    return creds_mock


@pytest.fixture(autouse=True)
def _env_vars():
    with mock.patch.dict(os.environ, {
        "GCP_SA_SECRET_NAME": "gcp-sa-key",
        "ASSUME_ROLE_ARN": "arn:aws:iam::123456789012:role/GcpStsRole",
        "PUBSUB_TOPIC_ID": "projects/my-gcp-project/topics/transfer-status",
        "PUBSUB_SUBSCRIPTION_ID": "projects/my-gcp-project/subscriptions/transfer-status-sub",
    }):
        yield


@pytest.fixture(autouse=True)
def _reset_clients():
    """Reset cached pub/sub clients between tests."""
    import src.handler as h
    h._publisher_client = None
    h._subscriber_client = None
    yield
    h._publisher_client = None
    h._subscriber_client = None


class TestTransferValidation:
    def test_400_when_source_missing(self):
        res = handler(_transfer_event({"destinationBucket": "gcs-bucket"}), _ctx())
        assert res["statusCode"] == 400
        assert "source" in json.loads(res["body"])["error"]

    def test_400_when_source_empty(self):
        res = handler(_transfer_event({"source": [], "destinationBucket": "b"}), _ctx())
        assert res["statusCode"] == 400

    def test_400_when_destination_bucket_missing(self):
        res = handler(_transfer_event({"source": ["s3://b/key.csv"]}), _ctx())
        assert res["statusCode"] == 400
        assert "destinationBucket" in json.loads(res["body"])["error"]


@patch("src.handler._get_publisher_client")
@patch("src.handler._get_secrets_client")
@patch("src.handler.service_account.Credentials.from_service_account_info")
@patch("src.handler.storage_transfer_v1.StorageTransferServiceClient")
class TestTransferSingleBucket:
    def test_creates_and_runs_one_job(self, MockStsClient, mock_creds_factory, mock_get_secrets, mock_get_pub):
        mock_secrets = MagicMock()
        mock_get_secrets.return_value = mock_secrets
        mock_secrets.get_secret_value.return_value = {
            "SecretString": json.dumps(GCP_SA_INFO),
        }
        creds = _mock_gcp_credentials()
        mock_creds_factory.return_value = creds

        mock_client = MagicMock()
        MockStsClient.return_value = mock_client

        mock_result = MagicMock()
        mock_result.name = "transferJobs/111"
        mock_client.create_transfer_job.return_value = mock_result

        res = handler(_transfer_event({
            "source": [
                "s3://my-bucket/data/file1.csv",
                "s3://my-bucket/data/file2.csv",
            ],
            "destinationBucket": "gcs-bucket",
            "destinationPrefix": "imported",
        }), _ctx())

        assert res["statusCode"] == 200
        body = json.loads(res["body"])
        assert body["jobNames"] == ["transferJobs/111"]

        mock_client.create_transfer_job.assert_called_once()
        call_kwargs = mock_client.create_transfer_job.call_args
        job = call_kwargs.kwargs.get("request") or call_kwargs[1].get("request") or call_kwargs[0][0]
        transfer_job = job.transfer_job

        assert transfer_job.transfer_spec.aws_s3_data_source.bucket_name == "my-bucket"
        assert transfer_job.transfer_spec.aws_s3_data_source.role_arn == "arn:aws:iam::123456789012:role/GcpStsRole"
        assert list(transfer_job.transfer_spec.object_conditions.include_prefixes) == [
            "data/file1.csv", "data/file2.csv",
        ]
        assert transfer_job.transfer_spec.gcs_data_sink.bucket_name == "gcs-bucket"
        dest_path = transfer_job.transfer_spec.gcs_data_sink.path
        assert dest_path.startswith("imported/")
        assert dest_path.endswith("Z/")
        assert transfer_job.transfer_spec.transfer_options.overwrite_objects_already_existing_in_sink is True

        notif = transfer_job.notification_config
        assert notif.pubsub_topic == "projects/my-gcp-project/topics/transfer-status"
        assert storage_transfer_v1.NotificationConfig.EventType.TRANSFER_OPERATION_SUCCESS in notif.event_types
        assert storage_transfer_v1.NotificationConfig.EventType.TRANSFER_OPERATION_FAILED in notif.event_types
        assert storage_transfer_v1.NotificationConfig.EventType.TRANSFER_OPERATION_ABORTED in notif.event_types
        assert notif.payload_format == storage_transfer_v1.NotificationConfig.PayloadFormat.JSON

        mock_client.run_transfer_job.assert_called_once()


@patch("src.handler._get_publisher_client")
@patch("src.handler._get_secrets_client")
@patch("src.handler.service_account.Credentials.from_service_account_info")
@patch("src.handler.storage_transfer_v1.StorageTransferServiceClient")
class TestTransferMultiBucket:
    def test_creates_one_job_per_bucket(self, MockStsClient, mock_creds_factory, mock_get_secrets, mock_get_pub):
        mock_secrets = MagicMock()
        mock_get_secrets.return_value = mock_secrets
        mock_secrets.get_secret_value.return_value = {
            "SecretString": json.dumps(GCP_SA_INFO),
        }
        mock_creds_factory.return_value = _mock_gcp_credentials()

        mock_client = MagicMock()
        MockStsClient.return_value = mock_client

        result_a = MagicMock()
        result_a.name = "transferJobs/aaa"
        result_b = MagicMock()
        result_b.name = "transferJobs/bbb"
        mock_client.create_transfer_job.side_effect = [result_a, result_b]

        res = handler(_transfer_event({
            "source": [
                "s3://bucket-a/dir/file1.csv",
                "s3://bucket-b/other/file2.csv",
            ],
            "destinationBucket": "gcs-bucket",
        }), _ctx())

        assert res["statusCode"] == 200
        body = json.loads(res["body"])
        assert len(body["jobNames"]) == 2
        assert "transferJobs/aaa" in body["jobNames"]
        assert "transferJobs/bbb" in body["jobNames"]
        assert mock_client.create_transfer_job.call_count == 2
        assert mock_client.run_transfer_job.call_count == 2


@patch("src.handler._get_publisher_client")
@patch("src.handler._get_secrets_client")
@patch("src.handler.service_account.Credentials.from_service_account_info")
@patch("src.handler.storage_transfer_v1.StorageTransferServiceClient")
class TestTransferObjectSource:
    def test_accepts_source_as_objects_with_uri(self, MockStsClient, mock_creds_factory, mock_get_secrets, mock_get_pub):
        mock_secrets = MagicMock()
        mock_get_secrets.return_value = mock_secrets
        mock_secrets.get_secret_value.return_value = {
            "SecretString": json.dumps(GCP_SA_INFO),
        }
        mock_creds_factory.return_value = _mock_gcp_credentials()

        mock_client = MagicMock()
        MockStsClient.return_value = mock_client
        mock_result = MagicMock()
        mock_result.name = "transferJobs/222"
        mock_client.create_transfer_job.return_value = mock_result

        res = handler(_transfer_event({
            "source": [{"uri": "s3://mybucket/path/to/file.parquet"}],
            "destinationBucket": "gcs-bucket",
        }), _ctx())

        assert res["statusCode"] == 200
        call_kwargs = mock_client.create_transfer_job.call_args
        job = call_kwargs.kwargs.get("request") or call_kwargs[1].get("request") or call_kwargs[0][0]
        spec = job.transfer_job.transfer_spec
        assert spec.aws_s3_data_source.bucket_name == "mybucket"
        assert list(spec.object_conditions.include_prefixes) == ["path/to/file.parquet"]


@patch("src.handler._get_publisher_client")
@patch("src.handler._get_secrets_client")
@patch("src.handler.service_account.Credentials.from_service_account_info")
@patch("src.handler.storage_transfer_v1.StorageTransferServiceClient")
class TestTransferCustomProject:
    def test_uses_gcp_project_id_from_body(self, MockStsClient, mock_creds_factory, mock_get_secrets, mock_get_pub):
        mock_secrets = MagicMock()
        mock_get_secrets.return_value = mock_secrets
        mock_secrets.get_secret_value.return_value = {
            "SecretString": json.dumps(GCP_SA_INFO),
        }
        mock_creds_factory.return_value = _mock_gcp_credentials()

        mock_client = MagicMock()
        MockStsClient.return_value = mock_client
        mock_result = MagicMock()
        mock_result.name = "transferJobs/333"
        mock_client.create_transfer_job.return_value = mock_result

        handler(_transfer_event({
            "source": ["s3://b/key.csv"],
            "destinationBucket": "gcs-bucket",
            "gcpProjectId": "custom-project",
        }), _ctx())

        call_kwargs = mock_client.create_transfer_job.call_args
        job = call_kwargs.kwargs.get("request") or call_kwargs[1].get("request") or call_kwargs[0][0]
        assert job.transfer_job.project_id == "custom-project"


@patch("src.handler._get_publisher_client")
@patch("src.handler._get_secrets_client")
@patch("src.handler.service_account.Credentials.from_service_account_info")
@patch("src.handler.storage_transfer_v1.StorageTransferServiceClient")
class TestTransferNoAssumeRole:
    def test_omits_role_arn_when_env_unset(self, MockStsClient, mock_creds_factory, mock_get_secrets, mock_get_pub):
        mock_secrets = MagicMock()
        mock_get_secrets.return_value = mock_secrets
        mock_secrets.get_secret_value.return_value = {
            "SecretString": json.dumps(GCP_SA_INFO),
        }
        mock_creds_factory.return_value = _mock_gcp_credentials()

        mock_client = MagicMock()
        MockStsClient.return_value = mock_client
        mock_result = MagicMock()
        mock_result.name = "transferJobs/444"
        mock_client.create_transfer_job.return_value = mock_result

        with mock.patch.dict(os.environ, {"ASSUME_ROLE_ARN": ""}):
            handler(_transfer_event({
                "source": ["s3://b/key.csv"],
                "destinationBucket": "gcs-bucket",
            }), _ctx())

        call_kwargs = mock_client.create_transfer_job.call_args
        job = call_kwargs.kwargs.get("request") or call_kwargs[1].get("request") or call_kwargs[0][0]
        assert job.transfer_job.transfer_spec.aws_s3_data_source.role_arn == ""


@patch("src.handler._get_publisher_client")
@patch("src.handler._get_secrets_client")
@patch("src.handler.service_account.Credentials.from_service_account_info")
@patch("src.handler.storage_transfer_v1.StorageTransferServiceClient")
class TestTransferErrors:
    def test_500_when_create_fails(self, MockStsClient, mock_creds_factory, mock_get_secrets, mock_get_pub):
        mock_secrets = MagicMock()
        mock_get_secrets.return_value = mock_secrets
        mock_secrets.get_secret_value.return_value = {
            "SecretString": json.dumps(GCP_SA_INFO),
        }
        mock_creds_factory.return_value = _mock_gcp_credentials()

        mock_client = MagicMock()
        MockStsClient.return_value = mock_client
        mock_client.create_transfer_job.side_effect = RuntimeError("GCP API error")

        res = handler(_transfer_event({
            "source": ["s3://b/key.csv"],
            "destinationBucket": "gcs-bucket",
        }), _ctx())

        assert res["statusCode"] == 500
        assert "GCP API error" in json.loads(res["body"])["error"]

    def test_500_for_invalid_s3_uri(self, MockStsClient, mock_creds_factory, mock_get_secrets, mock_get_pub):
        mock_secrets = MagicMock()
        mock_get_secrets.return_value = mock_secrets
        mock_secrets.get_secret_value.return_value = {
            "SecretString": json.dumps(GCP_SA_INFO),
        }
        mock_creds_factory.return_value = _mock_gcp_credentials()

        res = handler(_transfer_event({
            "source": ["https://not-s3/file.csv"],
            "destinationBucket": "gcs-bucket",
        }), _ctx())

        assert res["statusCode"] == 500
        assert "Invalid S3 URI" in json.loads(res["body"])["error"]


# ---------------------------------------------------------------------------
# Pub/Sub publishing tests
# ---------------------------------------------------------------------------

def _setup_transfer_mocks(mock_get_secrets, mock_creds_factory, MockStsClient):
    """Wire up the standard mock chain for transfer tests and return the STS mock client."""
    mock_secrets = MagicMock()
    mock_get_secrets.return_value = mock_secrets
    mock_secrets.get_secret_value.return_value = {
        "SecretString": json.dumps(GCP_SA_INFO),
    }
    mock_creds_factory.return_value = _mock_gcp_credentials()

    mock_client = MagicMock()
    MockStsClient.return_value = mock_client
    mock_result = MagicMock()
    mock_result.name = "transferJobs/pub111"
    mock_client.create_transfer_job.return_value = mock_result
    return mock_client


@patch("src.handler._get_publisher_client")
@patch("src.handler._get_secrets_client")
@patch("src.handler.service_account.Credentials.from_service_account_info")
@patch("src.handler.storage_transfer_v1.StorageTransferServiceClient")
class TestPubSubStartedEvent:
    def test_publishes_started_on_job_run(self, MockStsClient, mock_creds_factory, mock_get_secrets, mock_get_pub):
        mock_publisher = MagicMock()
        mock_publisher.publish.return_value.result.return_value = "msg-1"
        mock_get_pub.return_value = mock_publisher
        _setup_transfer_mocks(mock_get_secrets, mock_creds_factory, MockStsClient)

        res = handler(_transfer_event({
            "source": ["s3://my-bucket/file.csv"],
            "destinationBucket": "gcs-bucket",
        }), _ctx())

        assert res["statusCode"] == 200
        mock_publisher.publish.assert_called_once()
        call_args = mock_publisher.publish.call_args
        topic = call_args[0][0]
        data = json.loads(call_args[1]["data"])
        assert topic == "projects/my-gcp-project/topics/transfer-status"
        assert data["status"] == "STARTED"
        assert data["jobName"] == "transferJobs/pub111"
        assert data["sourceBucket"] == "my-bucket"
        assert data["destinationBucket"] == "gcs-bucket"
        assert data["requestId"] == "test-request-id"

    def test_publishes_started_per_bucket(self, MockStsClient, mock_creds_factory, mock_get_secrets, mock_get_pub):
        mock_publisher = MagicMock()
        mock_publisher.publish.return_value.result.return_value = "msg-1"
        mock_get_pub.return_value = mock_publisher

        mock_secrets = MagicMock()
        mock_get_secrets.return_value = mock_secrets
        mock_secrets.get_secret_value.return_value = {
            "SecretString": json.dumps(GCP_SA_INFO),
        }
        mock_creds_factory.return_value = _mock_gcp_credentials()

        mock_client = MagicMock()
        MockStsClient.return_value = mock_client
        result_a = MagicMock()
        result_a.name = "transferJobs/aaa"
        result_b = MagicMock()
        result_b.name = "transferJobs/bbb"
        mock_client.create_transfer_job.side_effect = [result_a, result_b]

        res = handler(_transfer_event({
            "source": [
                "s3://bucket-a/file1.csv",
                "s3://bucket-b/file2.csv",
            ],
            "destinationBucket": "gcs-bucket",
        }), _ctx())

        assert res["statusCode"] == 200
        assert mock_publisher.publish.call_count == 2
        statuses = [json.loads(c[1]["data"])["status"] for c in mock_publisher.publish.call_args_list]
        assert statuses == ["STARTED", "STARTED"]


@patch("src.handler._get_publisher_client")
@patch("src.handler._get_secrets_client")
@patch("src.handler.service_account.Credentials.from_service_account_info")
@patch("src.handler.storage_transfer_v1.StorageTransferServiceClient")
class TestPubSubSkipsWhenTopicUnset:
    def test_no_publish_when_topic_empty(self, MockStsClient, mock_creds_factory, mock_get_secrets, mock_get_pub):
        mock_publisher = MagicMock()
        mock_get_pub.return_value = mock_publisher
        mock_client = _setup_transfer_mocks(mock_get_secrets, mock_creds_factory, MockStsClient)

        with mock.patch.dict(os.environ, {"PUBSUB_TOPIC_ID": ""}):
            res = handler(_transfer_event({
                "source": ["s3://b/key.csv"],
                "destinationBucket": "gcs-bucket",
            }), _ctx())

        assert res["statusCode"] == 200
        mock_publisher.publish.assert_not_called()

        call_kwargs = mock_client.create_transfer_job.call_args
        job = call_kwargs.kwargs.get("request") or call_kwargs[1].get("request") or call_kwargs[0][0]
        assert job.transfer_job.notification_config.pubsub_topic == ""


@patch("src.handler._get_publisher_client")
@patch("src.handler._get_secrets_client")
@patch("src.handler.service_account.Credentials.from_service_account_info")
@patch("src.handler.storage_transfer_v1.StorageTransferServiceClient")
class TestPubSubFailedEvent:
    def test_publishes_failed_on_error(self, MockStsClient, mock_creds_factory, mock_get_secrets, mock_get_pub):
        mock_publisher = MagicMock()
        mock_publisher.publish.return_value.result.return_value = "msg-err"
        mock_get_pub.return_value = mock_publisher

        mock_secrets = MagicMock()
        mock_get_secrets.return_value = mock_secrets
        mock_secrets.get_secret_value.return_value = {
            "SecretString": json.dumps(GCP_SA_INFO),
        }
        mock_creds_factory.return_value = _mock_gcp_credentials()

        mock_client = MagicMock()
        MockStsClient.return_value = mock_client
        mock_client.create_transfer_job.side_effect = RuntimeError("GCP API error")

        res = handler(_transfer_event({
            "source": ["s3://b/key.csv"],
            "destinationBucket": "gcs-bucket",
        }), _ctx())

        assert res["statusCode"] == 500
        mock_publisher.publish.assert_called_once()
        data = json.loads(mock_publisher.publish.call_args[1]["data"])
        assert data["status"] == "FAILED"
        assert "GCP API error" in data["error"]

    def test_error_response_even_if_publish_fails(self, MockStsClient, mock_creds_factory, mock_get_secrets, mock_get_pub):
        mock_publisher = MagicMock()
        mock_publisher.publish.side_effect = Exception("Pub/Sub down")
        mock_get_pub.return_value = mock_publisher

        mock_secrets = MagicMock()
        mock_get_secrets.return_value = mock_secrets
        mock_secrets.get_secret_value.return_value = {
            "SecretString": json.dumps(GCP_SA_INFO),
        }
        mock_creds_factory.return_value = _mock_gcp_credentials()

        mock_client = MagicMock()
        MockStsClient.return_value = mock_client
        mock_client.create_transfer_job.side_effect = RuntimeError("GCP API error")

        res = handler(_transfer_event({
            "source": ["s3://b/key.csv"],
            "destinationBucket": "gcs-bucket",
        }), _ctx())

        assert res["statusCode"] == 500
        assert "GCP API error" in json.loads(res["body"])["error"]


# ---------------------------------------------------------------------------
# /notifications tests
# ---------------------------------------------------------------------------

def _make_received_message(message_id, data_dict, attributes=None, ack_id="ack-1"):
    """Build a mock ReceivedMessage for pull response."""
    msg = MagicMock()
    msg.ack_id = ack_id
    msg.message.message_id = message_id
    msg.message.data = json.dumps(data_dict).encode("utf-8")
    msg.message.publish_time.isoformat.return_value = "2026-04-15T12:00:00+00:00"
    msg.message.attributes = attributes or {}
    return msg


@patch("src.handler._get_subscriber_client")
@patch("src.handler._get_secrets_client")
@patch("src.handler.service_account.Credentials.from_service_account_info")
class TestNotifications:
    def test_pulls_and_acknowledges_messages(self, mock_creds_factory, mock_get_secrets, mock_get_sub):
        mock_secrets = MagicMock()
        mock_get_secrets.return_value = mock_secrets
        mock_secrets.get_secret_value.return_value = {
            "SecretString": json.dumps(GCP_SA_INFO),
        }
        mock_creds_factory.return_value = _mock_gcp_credentials()

        mock_subscriber = MagicMock()
        mock_get_sub.return_value = mock_subscriber

        msg1 = _make_received_message("msg-1", {"status": "STARTED", "jobName": "transferJobs/111"}, ack_id="ack-1")
        msg2 = _make_received_message("msg-2", {"status": "COMPLETED", "jobName": "transferJobs/111"}, ack_id="ack-2")
        mock_subscriber.pull.return_value.received_messages = [msg1, msg2]

        res = handler(_event("GET", "/notifications"), _ctx())

        assert res["statusCode"] == 200
        body = json.loads(res["body"])
        assert body["count"] == 2
        assert body["messages"][0]["data"]["status"] == "STARTED"
        assert body["messages"][1]["data"]["status"] == "COMPLETED"

        mock_subscriber.pull.assert_called_once()
        pull_req = mock_subscriber.pull.call_args[1]["request"]
        assert pull_req["subscription"] == "projects/my-gcp-project/subscriptions/transfer-status-sub"
        assert pull_req["max_messages"] == 20

        mock_subscriber.acknowledge.assert_called_once()
        ack_req = mock_subscriber.acknowledge.call_args[1]["request"]
        assert set(ack_req["ack_ids"]) == {"ack-1", "ack-2"}

    def test_returns_empty_when_no_messages(self, mock_creds_factory, mock_get_secrets, mock_get_sub):
        mock_secrets = MagicMock()
        mock_get_secrets.return_value = mock_secrets
        mock_secrets.get_secret_value.return_value = {
            "SecretString": json.dumps(GCP_SA_INFO),
        }
        mock_creds_factory.return_value = _mock_gcp_credentials()

        mock_subscriber = MagicMock()
        mock_get_sub.return_value = mock_subscriber
        mock_subscriber.pull.return_value.received_messages = []

        res = handler(_event("GET", "/notifications"), _ctx())

        assert res["statusCode"] == 200
        body = json.loads(res["body"])
        assert body["count"] == 0
        assert body["messages"] == []
        mock_subscriber.acknowledge.assert_not_called()

    def test_500_on_pull_error(self, mock_creds_factory, mock_get_secrets, mock_get_sub):
        mock_secrets = MagicMock()
        mock_get_secrets.return_value = mock_secrets
        mock_secrets.get_secret_value.return_value = {
            "SecretString": json.dumps(GCP_SA_INFO),
        }
        mock_creds_factory.return_value = _mock_gcp_credentials()

        mock_subscriber = MagicMock()
        mock_get_sub.return_value = mock_subscriber
        mock_subscriber.pull.side_effect = RuntimeError("Pub/Sub unavailable")

        res = handler(_event("GET", "/notifications"), _ctx())

        assert res["statusCode"] == 500
        assert "Pub/Sub unavailable" in json.loads(res["body"])["error"]


class TestNotificationsNoSubscription:
    def test_400_when_subscription_not_configured(self):
        with mock.patch.dict(os.environ, {"PUBSUB_SUBSCRIPTION_ID": ""}):
            res = handler(_event("GET", "/notifications"), _ctx())

        assert res["statusCode"] == 400
        assert "PUBSUB_SUBSCRIPTION_ID" in json.loads(res["body"])["error"]
