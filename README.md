# sample-lambda-endpoint

A sample AWS Lambda project with an HTTP API endpoint, deployed automatically via GitHub Actions using AWS SAM.

## API Endpoints

| Method | Path     | Description                  |
|--------|----------|------------------------------|
| GET    | `/hello` | Returns a greeting           |
| POST   | `/hello` | Returns a personalised greeting (send `{"name": "Alice"}`) |

## Prerequisites

- [AWS CLI](https://aws.amazon.com/cli/) configured with your account
- [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html)
- [Node.js 20+](https://nodejs.org/)

## Local Development

```bash
npm install
npm test

# Start a local API (requires Docker)
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

1. **test** — installs dependencies and runs `npm test`
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

Once configured, every push to `main` will automatically build and deploy the Lambda to your AWS account.

## Project Structure

```
├── .github/workflows/deploy.yml   # CI/CD pipeline
├── src/
│   ├── handler.js                 # Lambda function code
│   └── handler.test.js            # Unit tests
├── template.yaml                  # SAM infrastructure template
├── samconfig.toml                 # SAM deployment defaults
└── package.json
```
