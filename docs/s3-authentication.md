# S3 authentication

`napari-bfs-nd2` uses `s3fs` and `fsspec` with familiar AWS credential sources.
Configure access in the shell or operating-system account that launches
napari. Credentials are never entered into the plugin widget.

## Named AWS profile

Create or select a profile with the AWS CLI:

```bash
aws configure --profile microscopy
export AWS_PROFILE=microscopy
napari
```

PowerShell:

```powershell
$env:AWS_PROFILE = "microscopy"
napari
```

The shared credential file is normally `~/.aws/credentials` on Linux/macOS and
`%USERPROFILE%\.aws\credentials` on Windows.

## Environment credentials

```bash
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
export AWS_SESSION_TOKEN="..."  # required for temporary credentials
napari
```

Do not store these values in shell history, notebooks, issue reports, or the
repository. Prefer profiles, a secrets manager, or short-lived role
credentials.

## Web identity role credentials

The AWS provider chain can obtain credentials from a web identity
configuration. Common environment variables include:

```bash
export AWS_ROLE_ARN="arn:aws:iam::123456789012:role/MicroscopyReadOnly"
export AWS_WEB_IDENTITY_TOKEN_FILE="/path/to/token"
```

This is a preferred pattern for managed compute environments because it does
not require long-lived access keys. If a compute platform exposes credentials
through another provider mechanism, set `AWS_PROFILE` or the relevant AWS
credential environment variables before launching napari.

## Public buckets

If the plugin detects no configured credential source, it uses anonymous S3
access. The object and any required bucket listing or metadata operation must
be publicly readable.

## Minimum permission

Opening a known object requires `s3:GetObject` for that object. A typical
read-only statement is:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::my-bucket/experiment/*"
    }
  ]
}
```

Bucket browsing is not part of this plugin; users paste a complete object URI.
Consequently, `s3:ListBucket` is not normally needed by the widget itself.

## Verify access before napari

With AWS CLI:

```bash
aws s3api head-object \
  --bucket my-bucket \
  --key experiment/acquisition.nd2
```

With Python in napari's environment:

```python
import s3fs

fs = s3fs.S3FileSystem()
print(fs.info("s3://my-bucket/experiment/acquisition.nd2"))
```

For public data:

```python
fs = s3fs.S3FileSystem(anon=True)
```

## Format-specific behavior

### ND2

The ND2 reader opens `s3://` directly with `fsspec` and performs byte-range
reads against the object.

### CZI

libCZI understands local paths and HTTP(S), but not the `s3://` scheme. The
plugin therefore asks `s3fs` for a one-hour HTTPS URL and refreshes the cached
URL before expiry. Signed URLs are not included in napari layer metadata or S3
error messages; the original `s3://` URI remains the reported source.

## Non-AWS S3-compatible storage

The current widget intentionally exposes only a path field and uses the normal
AWS/s3fs configuration. Custom endpoints are not yet a public plugin setting.
Open a feature request if endpoint, region, or requester-pays controls are
needed.
