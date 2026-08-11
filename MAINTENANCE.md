# Maintenance

Maintainer notes for releasing Sluice. Consumers should use the [README](README.md); this file is for cutting releases.

Pushing a semver tag (`v0.1.0`, `v1.2.3`, …) runs **both** release workflows: the server image to GHCR and the JS SDK to npm.

```bash
git tag v0.1.1
git push origin v0.1.1
```

## Publish the server image

Workflow: [`.github/workflows/release-image.yml`](.github/workflows/release-image.yml).

Auth: `GITHUB_TOKEN` only (no PAT). You can also run the workflow manually; that publishes `edge` + `sha-*` only (no semver tags).

**Tags produced** from `v0.1.0`:

- `ghcr.io/<owner>/sluice:0.1.0`, `:0.1`, and `:latest` (stable semver)
- `:sha-<short>`
- bare major (`:1`, …) only when major ≥ 1 — not for `v0.*`

The same version string is stamped into the binary (`sluice -version`). The runtime image is only the `sluice` binary (plus CA certs). How operators run it is in the README.

## Publish the JS SDK

Workflow: [`.github/workflows/release-sdk.yml`](.github/workflows/release-sdk.yml).

Publishes [`@pauserratgutierrez/sluice-js`](https://www.npmjs.com/package/@pauserratgutierrez/sluice-js) to the **public npm registry** (not GitHub Packages). The package version is taken from the git tag (`v0.1.1` → `0.1.1`).

Auth: [npm trusted publishing](https://docs.npmjs.com/trusted-publishers/) over OIDC — **no `NPM_TOKEN`**. Pattern matches [scraply](https://github.com/pauserratgutierrez/scraply/blob/main/.github/workflows/npm-publish.yml).

### One-time npm setup

1. Ensure the `@pauserratgutierrez` scope exists on [npmjs.com](https://www.npmjs.com/).
2. For `@pauserratgutierrez/sluice-js` → **Settings → Trusted Publisher → GitHub Actions**:
   - Organization or user: `pauserratgutierrez`
   - Repository: `sluice`
   - Workflow filename: `release-sdk.yml` (filename only)
   - Allowed action: `npm publish`
3. If the package does not exist yet, create it with a first publish (trusted publisher can be attached once the package page exists; npm only validates the config when you publish). A one-off granular publish token is fine for that first create, then rely on OIDC afterward.

The workflow runs `npm test` in `packages/sluice-js` before `npm publish --access public`. Provenance is generated automatically for this public repository.

## Not covered here yet

- CI on every push (vet, `-race`, harness smoke) — release workflows only run on tags
- Choosing an open-source license (SDK is still `UNLICENSED`)
