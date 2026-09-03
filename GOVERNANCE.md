# Governance

Stream Quilt is maintainer-led. The current release maintainer is
[@appleweiping](https://github.com/appleweiping). Design discussion, compatibility changes, and
roadmap work happen in public issues and pull requests; vulnerabilities follow the private process
in [SECURITY.md](SECURITY.md).

Changes to event-time semantics, watermark state, lateness policy, CloudEvents mapping, resource
limits, or result schemas require tests, a compatibility note, and an updated changelog. Benchmark
claims must retain the workload, environment, full machine-readable result, and the limitations in
[cloudevents-and-benchmarks.md](docs/cloudevents-and-benchmarks.md).

A release requires green CI, regenerated deterministic examples, wheel and source-distribution
checks, an isolated installation smoke test, checksums, and build provenance. Maintainer roles and
decision rules may evolve through an explicit pull request as the contributor base grows.

