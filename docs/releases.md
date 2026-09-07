# Releases and artifact verification

Versioned Stream Quilt source distributions and wheels are published on the repository's
[GitHub Releases](https://github.com/appleweiping/stream-quilt/releases) page. The project is not
claiming a package index release unless that index is linked from this repository.

The release workflow runs only for a `vX.Y.Z` tag on `main` whose cross-platform `CI gate` and
CodeQL analysis already succeeded. It refuses to publish when the tag, `pyproject.toml`, and the package's exported version
disagree. A pinned `uv` binary synchronizes the frozen `uv.lock`, including the exact build backend
and its transitive dependencies. The workflow reruns lint, formatting, and tests, builds a source
distribution and wheel without a second dependency resolution, checks package metadata, installs
the wheel in a fresh environment, and publishes `SHA256SUMS`.

Download and verify one release with:

```bash
gh release download v0.3.0 --repo appleweiping/stream-quilt --dir stream-quilt-v0.3.0
cd stream-quilt-v0.3.0
sha256sum --check SHA256SUMS
gh attestation verify ./*.whl --repo appleweiping/stream-quilt
gh attestation verify ./*.tar.gz --repo appleweiping/stream-quilt
```

GitHub's provenance attestation links each distribution to the public repository, commit, and
release workflow that built it. Verification establishes provenance and integrity; it does not by
itself establish that the software is safe or suitable for a particular deployment. Review the
security policy, changelog, research limitations, and exact configuration used for an evaluation.

Tags and released version numbers are not reused. A correction is published as a new semantic
version so an existing checksum and attestation retain one meaning.
