import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timezone

import boto3
from google.cloud import storage_transfer_v1
from google.cloud.pubsub_v1 import PublisherClient
from google.oauth2 import service_account

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_secrets_client = None
_publisher_client = None


def _get_secrets_client():
    global _secrets_client
    if _secrets_client is None:
        _secrets_client = boto3.client("secretsmanager")
    return _secrets_client


def _get_publisher_client(credentials):
    global _publisher_client
    if _publisher_client is None:
        _publisher_client = PublisherClient(credentials=credentials)
    return _publisher_client


def _publish_status(credentials, project_id, status, request_id="", **extra):
    topic_id = os.environ.get("PUBSUB_TOPIC_ID", "")
    if not topic_id:
        logger.debug("PUBSUB_TOPIC_ID not set; skipping publish")
        return

    publisher = _get_publisher_client(credentials)
    message = {
        "status": status,
        "requestId": request_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        **extra,
    }
    data = json.dumps(message).encode("utf-8")
    future = publisher.publish(topic_id, data=data)
    message_id = future.result()
    logger.info(
        "Published %s event to %s (message_id=%s)",
        status, topic_id, message_id,
    )


def handler(event, context):
    rc = event.get("requestContext", {})
    http = rc.get("http", {})
    method = http.get("method") or event.get("httpMethod", "")
    path = http.get("path") or event.get("path", "")
    request_id = (context.aws_request_id if context else "local")

    logger.info(
        "Incoming request | request_id=%s method=%s path=%s",
        request_id, method, path,
    )

    if method == "GET" and path == "/hello":
        logger.info("Serving GET /hello | request_id=%s", request_id)
        return _response(200, {
            "message": "Hello from Lambda!",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    if method == "POST" and path == "/hello":
        body = json.loads(event.get("body") or "{}")
        name = body.get("name", "World")
        logger.info("Serving POST /hello | request_id=%s name=%s", request_id, name)
        return _response(200, {
            "message": f"Hello, {name}!",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    if method == "POST" and path == "/transfer":
        logger.info("Serving POST /transfer | request_id=%s", request_id)
        return _handle_transfer(event, request_id)

    logger.warning("Route not found | request_id=%s method=%s path=%s", request_id, method, path)
    return _response(404, {"error": "Not Found"})


# ---------------------------------------------------------------------------
# POST /transfer
# ---------------------------------------------------------------------------

def _handle_transfer(event, request_id=""):
    body = json.loads(event.get("body") or "{}")
    source = body.get("source")
    destination_bucket = body.get("destinationBucket")
    destination_prefix = body.get("destinationPrefix", "")
    gcp_project_id = body.get("gcpProjectId")
    description = body.get("description")

    if not source or not isinstance(source, list) or len(source) == 0:
        logger.warning("Validation failed: source missing or empty | request_id=%s", request_id)
        return _response(400, {
            "error": "source is required and must be a non-empty array of S3 URIs",
        })
    if not destination_bucket:
        logger.warning("Validation failed: destinationBucket missing | request_id=%s", request_id)
        return _response(400, {"error": "destinationBucket is required"})

    logger.info(
        "Transfer request | request_id=%s source_count=%d destination=gs://%s/%s",
        request_id, len(source), destination_bucket, destination_prefix,
    )

    gcp_credentials = None
    project_id = None

    try:
        logger.info("Fetching GCP credentials from Secrets Manager | request_id=%s", request_id)
        gcp_credentials = _get_gcp_credentials()
        project_id = gcp_project_id or gcp_credentials.project_id
        if not project_id:
            logger.error("GCP project_id not found | request_id=%s", request_id)
            return _response(400, {
                "error": "GCP project_id is required but not found in config or parameters",
            })

        logger.info("Initialising GCP STS client | request_id=%s project=%s", request_id, project_id)
        sts_client = storage_transfer_v1.StorageTransferServiceClient(
            credentials=gcp_credentials,
        )

        bucket_key_pairs = _parse_sources(source)
        pairs_by_bucket = _group_by_bucket(bucket_key_pairs)
        assume_role_arn = os.environ.get("ASSUME_ROLE_ARN")
        logger.info(
            "Parsed %d source URIs across %d bucket(s) | request_id=%s assume_role=%s",
            len(bucket_key_pairs), len(pairs_by_bucket), request_id,
            bool(assume_role_arn),
        )

        job_names = []

        for source_bucket, keys in pairs_by_bucket.items():
            logger.info(
                "Creating transfer job from s3://%s to gs://%s",
                source_bucket, destination_bucket,
            )
            logger.info("Files to transfer: %s", keys)

            timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            dest_path = _normalise_path(f"{destination_prefix}/{timestamp}" if destination_prefix else timestamp)

            aws_s3_data_source = storage_transfer_v1.AwsS3Data(
                bucket_name=source_bucket,
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
                    include_prefixes=keys,
                ),
                transfer_options=storage_transfer_v1.TransferOptions(
                    overwrite_objects_already_existing_in_sink=True,
                ),
            )

            notification_config = None
            pubsub_topic = os.environ.get("PUBSUB_TOPIC_ID", "")
            if pubsub_topic:
                notification_config = storage_transfer_v1.NotificationConfig(
                    pubsub_topic=pubsub_topic,
                    event_types=[
                        storage_transfer_v1.NotificationConfig.EventType.TRANSFER_OPERATION_SUCCESS,
                        storage_transfer_v1.NotificationConfig.EventType.TRANSFER_OPERATION_FAILED,
                        storage_transfer_v1.NotificationConfig.EventType.TRANSFER_OPERATION_ABORTED,
                    ],
                    payload_format=storage_transfer_v1.NotificationConfig.PayloadFormat.JSON,
                )

            transfer_job = storage_transfer_v1.TransferJob(
                project_id=project_id,
                description=description or f"S3→GCS transfer {datetime.now(timezone.utc).isoformat()}",
                status=storage_transfer_v1.TransferJob.Status.ENABLED,
                transfer_spec=transfer_spec,
                notification_config=notification_config,
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

            _publish_status(
                gcp_credentials, project_id, "STARTED",
                request_id=request_id,
                jobName=job_name,
                sourceBucket=source_bucket,
                destinationBucket=destination_bucket,
            )

        logger.info(
            "All transfer jobs created successfully | request_id=%s job_count=%d job_names=%s",
            request_id, len(job_names), job_names,
        )
        return _response(200, {
            "message": "Transfer jobs created and started successfully",
            "jobNames": job_names,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    except Exception as exc:
        logger.exception("Storage Transfer Service error | request_id=%s", request_id)
        if gcp_credentials and project_id:
            try:
                _publish_status(
                    gcp_credentials, project_id, "FAILED",
                    request_id=request_id,
                    error=str(exc),
                    destinationBucket=destination_bucket,
                )
            except Exception:
                logger.warning("Failed to publish FAILED status to Pub/Sub", exc_info=True)
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



def _normalise_path(p):
    if not p:
        return ""
    return p if p.endswith("/") else p + "/"


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
