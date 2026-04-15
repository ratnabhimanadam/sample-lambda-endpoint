import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

import boto3
from google.cloud import storage_transfer_v1
from google.oauth2 import service_account

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_secrets_client = None


def _get_secrets_client():
    global _secrets_client
    if _secrets_client is None:
        _secrets_client = boto3.client("secretsmanager")
    return _secrets_client


def handler(event, context):
    rc = event.get("requestContext", {})
    http = rc.get("http", {})
    method = http.get("method") or event.get("httpMethod", "")
    path = http.get("path") or event.get("path", "")

    if method == "GET" and path == "/hello":
        return _response(200, {
            "message": "Hello from Lambda!",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    if method == "POST" and path == "/hello":
        body = json.loads(event.get("body") or "{}")
        name = body.get("name", "World")
        return _response(200, {
            "message": f"Hello, {name}!",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    if method == "POST" and path == "/transfer":
        return _handle_transfer(event)

    return _response(404, {"error": "Not Found"})


# ---------------------------------------------------------------------------
# POST /transfer
# ---------------------------------------------------------------------------

def _handle_transfer(event):
    body = json.loads(event.get("body") or "{}")
    source = body.get("source")
    destination_bucket = body.get("destinationBucket")
    destination_prefix = body.get("destinationPrefix", "")
    gcp_project_id = body.get("gcpProjectId")
    description = body.get("description")
    wait_for_completion = body.get("waitForCompletion", False)
    poll_interval_seconds = body.get("pollIntervalSeconds", 20)

    if not source or not isinstance(source, list) or len(source) == 0:
        return _response(400, {
            "error": "source is required and must be a non-empty array of S3 URIs",
        })
    if not destination_bucket:
        return _response(400, {"error": "destinationBucket is required"})

    try:
        gcp_credentials = _get_gcp_credentials()
        project_id = gcp_project_id or gcp_credentials.project_id
        if not project_id:
            return _response(400, {
                "error": "GCP project_id is required but not found in config or parameters",
            })

        sts_client = storage_transfer_v1.StorageTransferServiceClient(
            credentials=gcp_credentials,
        )

        bucket_key_pairs = _parse_sources(source)
        pairs_by_bucket = _group_by_bucket(bucket_key_pairs)
        assume_role_arn = os.environ.get("ASSUME_ROLE_ARN")

        job_names = []

        for source_bucket, keys in pairs_by_bucket.items():
            s3_source_path, include_prefixes = _compute_path_and_prefixes(keys)

            logger.info(
                "Creating transfer job from s3://%s to gs://%s",
                source_bucket, destination_bucket,
            )
            logger.info("Files to transfer: %s", include_prefixes)

            dest_path = _normalise_path(destination_prefix)

            aws_s3_data_source = storage_transfer_v1.AwsS3Data(
                bucket_name=source_bucket,
                path=s3_source_path,
            )
            if assume_role_arn:
                aws_s3_data_source.role_arn = assume_role_arn

            transfer_spec = storage_transfer_v1.TransferSpec(
                aws_s3_data_source=aws_s3_data_source,
                gcs_data_sink=storage_transfer_v1.GcsData(
                    bucket_name=destination_bucket,
                    path=dest_path,
                ),
                object_conditions=storage_transfer_v1.ObjectConditions(
                    include_prefixes=include_prefixes,
                ),
                transfer_options=storage_transfer_v1.TransferOptions(
                    overwrite_objects_already_existing_in_sink=True,
                ),
            )

            transfer_job = storage_transfer_v1.TransferJob(
                project_id=project_id,
                description=description or f"S3→GCS transfer {datetime.now(timezone.utc).isoformat()}",
                status=storage_transfer_v1.TransferJob.Status.ENABLED,
                transfer_spec=transfer_spec,
            )

            result = sts_client.create_transfer_job(
                request=storage_transfer_v1.CreateTransferJobRequest(
                    transfer_job=transfer_job,
                ),
            )

            job_name = result.name
            job_names.append(job_name)
            logger.info("Transfer job created: %s", job_name)

            sts_client.run_transfer_job(
                request=storage_transfer_v1.RunTransferJobRequest(
                    job_name=job_name,
                    project_id=project_id,
                ),
            )
            logger.info("Transfer job started: %s", job_name)

            if wait_for_completion:
                _wait_for_transfer_completion(
                    sts_client, job_name, project_id, poll_interval_seconds,
                )

        logger.info("Files uploaded to GCP successfully via Storage Transfer Service")
        return _response(200, {
            "message": "Transfer jobs created and started successfully",
            "jobNames": job_names,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    except Exception as exc:
        logger.error("Storage Transfer Service error: %s", exc)
        return _response(500, {
            "error": f"Failed to transfer files using Storage Transfer Service: {exc}",
        })


# ---------------------------------------------------------------------------
# Source parsing & grouping
# ---------------------------------------------------------------------------

def _parse_sources(source):
    pairs = []
    for item in source:
        uri = item if isinstance(item, str) else item.get("uri", "")
        if not uri or not uri.startswith("s3://"):
            raise ValueError(f"Invalid S3 URI: {uri}")
        without_scheme = uri[5:]
        slash_idx = without_scheme.find("/")
        if slash_idx == -1:
            pairs.append({"bucket": without_scheme, "key": ""})
        else:
            pairs.append({
                "bucket": without_scheme[:slash_idx],
                "key": without_scheme[slash_idx + 1:],
            })
    return pairs


def _group_by_bucket(pairs):
    grouped = defaultdict(list)
    for p in pairs:
        grouped[p["bucket"]].append(p["key"])
    return grouped


def _compute_path_and_prefixes(keys):
    """Compute the longest common directory prefix and relative include_prefixes."""
    if not keys:
        return "", []

    dirs = []
    for k in keys:
        last_slash = k.rfind("/")
        dirs.append("" if last_slash == -1 else k[: last_slash + 1])

    common_prefix = dirs[0]
    for d in dirs[1:]:
        while not d.startswith(common_prefix):
            trim_idx = common_prefix.rfind("/", 0, len(common_prefix) - 1)
            common_prefix = "" if trim_idx == -1 else common_prefix[: trim_idx + 1]

    include_prefixes = [k[len(common_prefix):] for k in keys]
    return common_prefix, include_prefixes


def _normalise_path(p):
    if not p:
        return ""
    return p if p.endswith("/") else p + "/"


# ---------------------------------------------------------------------------
# Completion polling
# ---------------------------------------------------------------------------

def _wait_for_transfer_completion(sts_client, job_name, project_id, poll_interval_seconds):
    max_wait = 3600
    elapsed = 0
    logger.info("Waiting for transfer job %s to complete...", job_name)

    while elapsed < max_wait:
        response = sts_client.transport.operations_client.list_operations(
            name="transferOperations",
            filter=json.dumps({"projectId": project_id, "jobNames": [job_name]}),
        )

        for op in response:
            metadata = storage_transfer_v1.TransferOperation.deserialize(
                op.metadata.value,
            )
            status = metadata.status
            logger.info("Transfer status: %s", status)

            if status == storage_transfer_v1.TransferOperation.Status.SUCCESS:
                counters = metadata.counters
                logger.info(
                    "Transfer completed. Objects copied: %s, Bytes copied: %s",
                    counters.objects_copied_to_sink,
                    counters.bytes_copied_to_sink,
                )
                return
            if status == storage_transfer_v1.TransferOperation.Status.FAILED:
                raise RuntimeError(
                    f"Transfer failed. Errors: {metadata.error_breakdowns}"
                )
            if status == storage_transfer_v1.TransferOperation.Status.ABORTED:
                raise RuntimeError("Transfer was aborted")
            break

        time.sleep(poll_interval_seconds)
        elapsed += poll_interval_seconds

    raise RuntimeError(f"Transfer job timed out after {max_wait} seconds")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_gcp_credentials():
    secret_name = os.environ["GCP_SA_SECRET_NAME"]
    resp = _get_secrets_client().get_secret_value(SecretId=secret_name)
    sa_info = json.loads(resp["SecretString"])
    return service_account.Credentials.from_service_account_info(sa_info)


def _response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }
