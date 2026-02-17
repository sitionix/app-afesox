# AGENTS

BE service rules are located in `../AGENTS.md`.

## API-first Pre-Implementation Rules

- Every PR that changes a service contract in `apis/<service>/api-first/` must increment the service version versus `develop`.
- The version in `apis/<service>/api-first/metadata.yml` (`api.version`) must match the version in `apis/<service>/api-first/openapi-rest.yml` (`info.version`).
- After every push to a PR, trigger unstable generation for the required artifact type (`API` or `CLIENT`) using `/generate` with a valid name from `apis/metadata.yml`.
- After unstable artifact is generated, update the dependent service to that unstable version and run its build to verify dependency resolution.
- If Maven reuses cached artifacts, remove the exact local version from `~/.m2/repository/com/afesox/...` and rebuild to force a fresh download.
- Start implementation work only after unstable artifact generation and consumer build verification are successful.
