# Maintenance

Maintainer notes for releasing Sluice. Consumers should use the [README](README.md); this file is for cutting releases.

Pushing a semver tag (`v0.1.0`, `v1.2.3`, …) runs **both** release workflows: the server image to GHCR and the JS SDK to npm.

```bash
git tag v0.1.3
git push origin v0.1.3
```

Leave `packages/sluice-js/package.json` at `0.0.0` in git. The SDK workflow sets the published version from the tag (`v0.1.3` → `0.1.3`).

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

Publishes [`@pauserratgutierrez/sluice-js`](https://www.npmjs.com/package/@pauserratgutierrez/sluice-js) to the **public npm registry** (not GitHub Packages). The package version is taken from the git tag.

Auth: [npm trusted publishing](https://docs.npmjs.com/trusted-publishers/) over OIDC — **no `NPM_TOKEN`** (same idea as [scraply](https://github.com/pauserratgutierrez/scraply/blob/main/.github/workflows/npm-publish.yml)). Granular tokens hit `EOTP` under account 2FA and cannot publish from Actions.

The workflow runs `npm test` in `packages/sluice-js` before `npm publish --access public`. Provenance is generated automatically for this public repository.

### Trusted Publisher (required once)

The package page must already exist (bootstrap was a local `npm publish` of `0.0.0`). Then:

1. Open [package Access](https://www.npmjs.com/package/@pauserratgutierrez/sluice-js/access) → **Trusted Publisher → GitHub Actions**:
   - Organization or user: `pauserratgutierrez`
   - Repository: `sluice`
   - Workflow filename: `release-sdk.yml` (filename only)
   - Allowed action: `npm publish`
2. Do **not** keep an `NPM_TOKEN` Actions secret.

After that, each new `v*.*.*` tag publishes via OIDC.

### Bootstrap a brand-new package name again

If you ever create a different package name: publish once from a machine with `npm login` (browser/2FA), then attach Trusted Publisher as above. Do not rely on CI tokens for the first create.

## Not covered here yet

- CI on every push (vet, `-race`, harness smoke) — release workflows only run on tags
- Choosing an open-source license (SDK is still `UNLICENSED`)
- The issuer overlay smoke (`cmd/smoke-issuer`) does not replace the 36 RLS assertions
