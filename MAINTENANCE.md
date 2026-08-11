# Maintenance

Maintainer notes for releasing Sluice. Consumers should use the [README](README.md); this file is for cutting releases.

## Publish the server image

The workflow [`.github/workflows/release-image.yml`](.github/workflows/release-image.yml) builds, tests, and pushes `ghcr.io/<owner>/sluice` with `GITHUB_TOKEN` (no PAT).

**Trigger** — push a semver tag, or run the workflow manually (publishes `edge` + `sha-*` only):

```bash
git tag v0.1.0
git push origin v0.1.0
```

**Tags produced** from `v0.1.0`:

- `0.1.0`, `0.1`, and `latest` (stable semver)
- `sha-<short>`
- bare major (`1`, …) only when major ≥ 1 — not for `v0.*`

The same version string is stamped into the binary (`sluice -version`).

**After the first push**, open the package in GitHub → Packages and set visibility to **public** if anonymous pulls should work. Until then the package stays private to the repo.

The runtime image is only the `sluice` binary (plus CA certs). It does not include Compose, Postgres, GoTrue, or the harness. How operators run it is documented in the README.

## Not covered here yet

- CI on every push (vet, `-race`, harness smoke)
- SDK publish (`packages/sluice-js`)