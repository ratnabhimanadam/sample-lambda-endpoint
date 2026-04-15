import json
import os
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("GCP_SA_SECRET_NAME", "gcp-sa-key")
os.environ.setdefault("ASSUME_ROLE_ARN", "arn:aws:iam::123456789012:role/GcpStsRole")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

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
    }):
        yield


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


@patch("src.handler._get_secrets_client")
@patch("src.handler.service_account.Credentials.from_service_account_info")
@patch("src.handler.storage_transfer_v1.StorageTransferServiceClient")
class TestTransferSingleBucket:
    def test_creates_and_runs_one_job(self, MockStsClient, mock_creds_factory, mock_get_secrets):
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

        mock_client.run_transfer_job.assert_called_once()


@patch("src.handler._get_secrets_client")
@patch("src.handler.service_account.Credentials.from_service_account_info")
@patch("src.handler.storage_transfer_v1.StorageTransferServiceClient")
class TestTransferMultiBucket:
    def test_creates_one_job_per_bucket(self, MockStsClient, mock_creds_factory, mock_get_secrets):
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


@patch("src.handler._get_secrets_client")
@patch("src.handler.service_account.Credentials.from_service_account_info")
@patch("src.handler.storage_transfer_v1.StorageTransferServiceClient")
class TestTransferObjectSource:
    def test_accepts_source_as_objects_with_uri(self, MockStsClient, mock_creds_factory, mock_get_secrets):
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


@patch("src.handler._get_secrets_client")
@patch("src.handler.service_account.Credentials.from_service_account_info")
@patch("src.handler.storage_transfer_v1.StorageTransferServiceClient")
class TestTransferCustomProject:
    def test_uses_gcp_project_id_from_body(self, MockStsClient, mock_creds_factory, mock_get_secrets):
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


@patch("src.handler._get_secrets_client")
@patch("src.handler.service_account.Credentials.from_service_account_info")
@patch("src.handler.storage_transfer_v1.StorageTransferServiceClient")
class TestTransferNoAssumeRole:
    def test_omits_role_arn_when_env_unset(self, MockStsClient, mock_creds_factory, mock_get_secrets):
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


@patch("src.handler._get_secrets_client")
@patch("src.handler.service_account.Credentials.from_service_account_info")
@patch("src.handler.storage_transfer_v1.StorageTransferServiceClient")
class TestTransferErrors:
    def test_500_when_create_fails(self, MockStsClient, mock_creds_factory, mock_get_secrets):
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

    def test_500_for_invalid_s3_uri(self, MockStsClient, mock_creds_factory, mock_get_secrets):
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
