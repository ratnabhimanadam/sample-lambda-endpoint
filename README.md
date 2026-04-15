# sample-lambda-endpoint

A sample AWS Lambda project (Python) with HTTP API endpoints, including an S3-to-GCP Storage Transfer Service integration using IAM AssumeRole. Deployed automatically via GitHub Actions using AWS SAM.

## API Endpoints

| Method | Path        | Description                                                    |
|--------|-------------|----------------------------------------------------------------|
| GET    | `/hello`    | Returns a greeting                                             |
| POST   | `/hello`    | Returns a personalised greeting (send `{"name": "..."}`)       |
| POST   | `/transfer` | Creates GCP Storage Transfer Service jobs to copy S3 files→GCS |

### POST /transfer

Creates one GCP Storage Transfer Service job per source S3 bucket. Uses an IAM AssumeRole trust policy for S3 access (no static AWS keys). Files are transferred server-side — data flows directly from S3 to GCS without passing through Lambda.

**Request body:**

```json
{
  "source": [
    "s3://my-bucket/data/file1.csv",
    "s3://my-bucket/data/file2.csv",
    "s3://other-bucket/reports/summary.parquet"
  ],
  "destinationBucket": "my-gcs-bucket",
  "destinationPrefix": "imported/",
  "gcpProjectId": "my-gcp-project",
  "description": "optional job description",
  "waitForCompletion": false,
  "pollIntervalSeconds": 20
}
```

| Field                | Required | Description                                                              |
|----------------------|----------|--------------------------------------------------------------------------|
| `source`             | Yes      | Array of S3 URIs (strings) or objects with a `uri` field                 |
| `destinationBucket`  | Yes      | Target GCS bucket                                                        |
| `destinationPrefix`  | No       | Path prefix inside the GCS bucket                                        |
| `gcpProjectId`       | No       | GCP project ID (defaults to the one in the service account key)          |
| `description`        | No       | Human-readable job description                                           |
| `waitForCompletion`  | No       | If `true`, polls STS until each job finishes (default: `false`)          |
| `pollIntervalSeconds`| No       | Polling interval in seconds when `waitForCompletion` is true (default: 20) |

**Response:**

```json
{
  "message": "Transfer jobs created and started successfully",
  "jobNames": [
    "transferJobs/OPI1234567890",
    "transferJobs/OPI0987654321"
  ],
  "timestamp": "2026-04-15T12:00:00.000Z"
}
```

One job is created per distinct source S3 bucket. Each job uses `include_prefixes` to transfer only the specified files.

## Prerequisites

- [AWS CLI](https://aws.amazon.com/cli/) configured with your account
- [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html)
- [Python 3.12+](https://www.python.org/)

## Local Development

```bash
pip install -r requirements-dev.txt
python -m pytest tests/ -v

# Start a local API (requires Docker)
sam build
sam local start-api
```

Then visit `http://127.0.0.1:3000/hello`.

## Manual Deploy

```bash
sam build
sam deploy --guided   # first time — saves defaults to samconfig.toml
sam deploy            # subsequent deploys
```

## CI/CD Setup (GitHub Actions)

The workflow at `.github/workflows/deploy.yml` runs on every push to `main`:

1. **test** — installs dependencies and runs `pytest`
2. **deploy** — builds and deploys the SAM stack to AWS

### One-time AWS Setup

The pipeline authenticates using **GitHub OIDC** (no long-lived keys required).

1. **Create an IAM OIDC identity provider** in your AWS account for `token.actions.githubusercontent.com`.

2. **Create an IAM Role** with the following trust policy (replace the repo value):

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Principal": {
           "Federated": "arn:aws:iam::<ACCOUNT_ID>:oidc-provider/token.actions.githubusercontent.com"
         },
         "Action": "sts:AssumeRoleWithWebIdentity",
         "Condition": {
           "StringEquals": {
             "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
           },
           "StringLike": {
             "token.actions.githubusercontent.com:sub": "repo:ratnabhimanadam/sample-lambda-endpoint:*"
           }
         }
       }
     ]
   }
   ```

3. **Attach policies** to the role: `AWSLambda_FullAccess`, `AmazonAPIGatewayAdministrator`, `AWSCloudFormationFullAccess`, `IAMFullAccess`, and `AmazonS3FullAccess` (or a scoped-down custom policy).

4. **Add the role ARN** as a GitHub repository secret named `AWS_ROLE_ARN`:
   - Go to your repo → Settings → Secrets and variables → Actions → New repository secret
   - Name: `AWS_ROLE_ARN`
   - Value: `arn:aws:iam::<ACCOUNT_ID>:role/<ROLE_NAME>`

## S3-to-GCP Transfer Setup

The `/transfer` endpoint uses an **IAM AssumeRole trust policy** for S3 access (no static AWS keys) and a GCP service account stored in Secrets Manager.

### 1. AWS IAM Role for GCP STS (AssumeRole)

Create an IAM role that the GCP Storage Transfer Service will assume to read from your S3 buckets.

**Trust policy** (allows GCP STS to assume the role):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::123456789012:root"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "<YOUR_GCP_PROJECT_NUMBER>"
        }
      }
    }
  ]
}
```

> Replace the principal with the GCP Storage Transfer Service's AWS account
> (see [GCP docs](https://cloud.google.com/storage-transfer/docs/reference/rest/v1/TransferSpec#AwsS3Data)
> for the current account ID). The external ID is your GCP project number.

**Permissions policy:**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-source-bucket",
        "arn:aws:s3:::your-source-bucket/*"
      ]
    }
  ]
}
```

Pass this role's ARN as the `AssumeRoleArn` parameter when deploying the SAM stack, or set it in `samconfig.toml`:

```toml
parameter_overrides = "AssumeRoleArn=arn:aws:iam::123456789012:role/GcpStsS3ReadRole"
```

### 2. GCP Service Account Key (Secrets Manager)

Store the GCP service account JSON key in Secrets Manager as `gcp-sa-key`:

```bash
aws secretsmanager create-secret \
  --name gcp-sa-key \
  --secret-string file://gcp-sa-key.json \
  --region us-east-1
```

### 3. GCP Setup

1. **Enable the Storage Transfer API:**

   ```bash
   gcloud services enable storagetransfer.googleapis.com
   ```

2. **Service account permissions** — the service account whose key is stored in `gcp-sa-key` must have:
   - `roles/storagetransfer.admin` on the GCP project
   - `roles/storage.admin` on the destination GCS bucket

3. **Destination GCS bucket** must already exist.

## Project Structure

```
├── .github/workflows/deploy.yml   # CI/CD pipeline
├── src/
│   └── handler.py                 # Lambda function code
├── tests/
│   └── test_handler.py            # Unit tests (pytest)
├── template.yaml                  # SAM infrastructure template
├── samconfig.toml                 # SAM deployment defaults
├── requirements.txt               # Runtime dependencies
└── requirements-dev.txt           # Dev/test dependencies
```
