## Repack Parquet files on S3 using PyArrow (parallel, streaming)

This tool rewrites Parquet files in S3 using PyArrow in a streaming, parallel fashion. It preserves schema and content while "repacking" each file so they load cleanly in Snowflake when original files were produced by `arrow-go (pqarrow)`.

### Why this works
- The rewrite uses `pyarrow.parquet.ParquetWriter` and iterates record batches from the source file, writing them back out to S3. This normalizes Parquet metadata/encodings causing Snowflake compatibility issues to disappear in practice.
- It streams directly between S3 and PyArrow (no full local copies), and runs across many files concurrently using threads for high throughput.

---

## Installation

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Requirements:
- Python 3.9+
- `pyarrow` (with S3 support, included in official wheels)
- `boto3`

---

## AWS credentials

Provide credentials via any standard AWS method (env vars, shared config, or EC2 instance profile):
- Env vars: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`
- Or use `aws configure`
- Or run on EC2 with an IAM role attached

Required S3 permissions on source and destination buckets:
- `s3:ListBucket`
- `s3:GetObject`
- `s3:PutObject`

---

## Usage

Run the CLI as a module:

```bash
python -m repack_s3 \
  --src s3://SOURCE_BUCKET/some/prefix \
  --dst s3://DEST_BUCKET/some/prefix \
  --workers 32 \
  --batch-size 65536
```

Flags:
- `--src`: Source S3 prefix (bucket and optional prefix)
- `--dst`: Destination S3 prefix (bucket and optional prefix)
- `--workers`: Parallel threads. Defaults to `min(32, 4*CPU)` if 0 or omitted.
- `--batch-size`: Arrow record batch size during rewrite (default 65536)
- `--no-skip-existing`: By default, existing destination objects are skipped. Add this flag to always overwrite.

Notes:
- The destination prefix should be distinct from the source. In-place rewrite on S3 is not supported (there is no true rename on S3).
- File filtering: only keys ending with `.parquet` or `.parq` are processed.

---

## Recommended: Run on AWS EC2 for speed

1) Launch an EC2 instance in the same region as your S3 buckets. Choose an instance size with good network (e.g., c7i.large or bigger). Attach an IAM role with S3 permissions above.

2) Install dependencies and run:
```bash
sudo yum update -y  # or apt-get update -y
sudo yum install -y python3-pip  # or apt-get install -y python3-pip
cd /path/to/repack_pyarrow
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python -m repack_s3 \
  --src s3://SOURCE_BUCKET/prefix \
  --dst s3://DEST_BUCKET/prefix \
  --workers 32
```

3) Monitor progress: the tool prints a final summary of completed vs. errors. Rerun to retry any failures (with `--no-skip-existing` if you want to overwrite).

---

## Deploy via AWS CLI

Use the AWS CLI to stage the code artifact and start an EC2 runner without touching the console.

1. **Package and upload the repo**
```bash
zip -r repack_pyarrow.zip repack_pyarrow
aws s3 cp repack_pyarrow.zip s3://MY_BUCKET/artifacts/repack_pyarrow.zip
```

2. **Launch an EC2 instance and run the tool**
Create a simple user-data script (e.g., `deploy/run-repack.sh`) that accepts the source bucket/prefix and derives a `_repack` destination in the same bucket:
```bash
#!/bin/bash
set -euo pipefail
yum update -y
yum install -y python3 unzip
export SRC_BUCKET="${SRC_BUCKET:-my-source-bucket}"
export SRC_PREFIX="${SRC_PREFIX:-original-prefix}"
export DEST_PREFIX="${DEST_PREFIX:-${SRC_PREFIX%/}_repack}"
cd /tmp
aws s3 cp s3://MY_BUCKET/artifacts/repack_pyarrow.zip .
unzip repack_pyarrow.zip
cd repack_pyarrow
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

The script leaves the repo installed on the host; manually run the repack command after connecting:

```bash
python -m repack_s3 \
  --src "s3://${SRC_BUCKET}/${SRC_PREFIX}" \
  --dst "s3://${SRC_BUCKET}/${DEST_PREFIX}" \
  --workers 32
```


Then launch the instance with that script, passing bucket/prefix overrides via `--user-data` or exported env vars:
Note: the script runs once via user-data when the instance launches. To rerun processing later you can SSH/SSM into the same instance and rerun the script with new `SRC_BUCKET`/`SRC_PREFIX` values (no need to redeploy unless you want a fresh host).
```bash
aws ec2 run-instances \
  --image-id ami-0abcdef1234567890 \
  --instance-type c7i.large \
  --iam-instance-profile Name=RepackRunnerRole \
  --key-name MyKeyPair \
  --user-data file://deploy/run-repack.sh \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=repack-runner}]'
```

### Re-run with different source/prefix

Once the instance is running, you can rerun the repack script with new parameters via SSH or SSM without redeploying:

```bash
ssh ec2-user@<instance>
cd /tmp/repack_pyarrow
source .venv/bin/activate
export SRC_BUCKET="another-bucket"
export SRC_PREFIX="new/prefix"
export DEST_PREFIX="${SRC_PREFIX%/}_repack"
python -m repack_s3 \
  --src "s3://${SRC_BUCKET}/${SRC_PREFIX}" \
  --dst "s3://${SRC_BUCKET}/${DEST_PREFIX}" \
  --workers 32
```

The script reuses the same repository copy; adjust `SRC_BUCKET`/`SRC_PREFIX` for new runs and rerun the command as needed.

3. **Follow up**
- Watch instance output via `aws ec2 get-console-output` or via the CloudWatch logs produced by your user-data (if enabled).
- Terminate the instance after the job by `aws ec2 terminate-instances --instance-ids <id>`.

Replace placeholders with your bucket names, IAM role, AMI, and key pair. Ensure the IAM role allows `s3:GetObject` and `s3:PutObject` before running.

---

## Implementation details

- Uses `pyarrow.fs.S3FileSystem` to open S3 input/output streams.
- For each file:
  - Read via `pyarrow.parquet.ParquetFile`
  - Iterate batches and write via `pyarrow.parquet.ParquetWriter`
- Parallelized with `ThreadPoolExecutor` for high S3 throughput.
- Optionally skips destination keys that already exist to make re-runs fast and idempotent.

---

## Example IAM policy (attach to role or user)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::SOURCE_BUCKET",
        "arn:aws:s3:::DEST_BUCKET"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject"
      ],
      "Resource": "arn:aws:s3:::SOURCE_BUCKET/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject"
      ],
      "Resource": "arn:aws:s3:::DEST_BUCKET/*"
    }
  ]
}
```

### Create the policy via AWS CLI

Save the JSON above as `repack-policy.json` and run:

```bash
aws iam create-policy \
  --policy-name RepackParquetOnS3 \
  --policy-document file://repack-policy.json
```

Attach it to the role or user that the EC2 instance will assume:

```bash
aws iam attach-role-policy \
  --role-name RepackRunnerRole \
  --policy-arn arn:aws:iam::123456789012:policy/RepackParquetOnS3
```

---

## Run in Kubernetes (e.g., via Helm)

Build and push the Docker image:

```bash
docker build -t YOUR_REGISTRY/repack-s3:latest .
docker push YOUR_REGISTRY/repack-s3:latest
```

The container entrypoint reads configuration from environment variables:

- `SRC_BUCKET` (required): source S3 bucket
- `SRC_PREFIX` (required): source prefix within the bucket
- `DEST_PREFIX` (optional): destination prefix; defaults to `${SRC_PREFIX%/}_repack`
- `WORKERS` (optional): parallel workers, default `32`
- `BATCH_SIZE` (optional): batch size, default `65536`

Example `Job` manifest (Helm values would typically just set the env vars):

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: repack-job
spec:
  template:
    spec:
      restartPolicy: Never
      serviceAccountName: repack-sa
      containers:
        - name: repack
          image: YOUR_REGISTRY/repack-s3:latest
          env:
            - name: SRC_BUCKET
              value: your-bucket
            - name: SRC_PREFIX
              value: path/to/original
            # optional override:
            # - name: DEST_PREFIX
            #   value: path/to/original_repack
            - name: WORKERS
              value: "32"
```

You can deploy multiple Helm releases or Jobs with different `SRC_PREFIX` (and optionally `DEST_PREFIX`) values to fan out work across your Kubernetes cluster. Make sure the pod’s IAM (IRSA or node role) has the S3 permissions from the policy above.


