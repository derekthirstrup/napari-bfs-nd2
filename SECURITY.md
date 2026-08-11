# Security policy

## Supported version

Security fixes are applied to the latest released version.

## Reporting a vulnerability

Do not open a public issue for a vulnerability or include credentials in an
issue. Use GitHub's private vulnerability-reporting flow:

https://github.com/derekthirstrup/napari-bfs-nd2/security/advisories/new

Include the affected version, impact, reproduction steps, and any proposed
mitigation. Remove or rotate exposed credentials before submitting a report.

## Credential handling

The plugin relies on the standard AWS credential chain. It does not request or
store AWS credentials in its widget. Presigned HTTPS query strings are treated
as sensitive and are removed from layer source metadata and S3-specific error
messages.
