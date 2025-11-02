# Contribution Workflow Guidelines

This project follows a standardized workflow for preparing and shipping changes. Use the checklist below whenever you start new work.

## Branch Naming

1. Sync your local `develop` branch before branching off (`git checkout develop && git pull`).
2. Create feature branches with the pattern `feature/SITIONIX-<ticket-number>`. Replace `<ticket-number>` with the numeric identifier provided for the task—for example, ticket 42 becomes `feature/SITIONIX-42`.

## Pull Request Naming

Open pull requests using the format `[SITIONIX-<ticket-number>] - <short-description>`. The short description should summarize the change in a few words (for instance, `[SITIONIX-42] - Add payment endpoint`). Always keep the ticket number in the PR title aligned with the branch name.

## Generating Unstable JAR Versions

Use the `/generate` pull-request comment workflow to build unstable artifacts while the PR is under review:

1. Confirm the PR title uses `[SITIONIX-<ticket-number>] - <short-description>` so the workflow can capture the ticket ID.
2. Inspect [`apis/metadata.yml`](apis/metadata.yml) and reuse one of the predefined `name` values exactly as written. Each metadata record maps the `name` to a concrete API definition. For example, to build the Authentication API artifact comment:

   ```text
   /generate --name "API Authentication SOX"
   ```

   Providing any name that is not listed in the metadata file will be ignored by the workflow because it cannot resolve the API definition.
3. A GitHub Actions job responds to the comment, builds the requested API in unstable mode, and replies with the Maven coordinates of the generated artifact.
4. Consume the dependency snippet from the success comment in downstream projects as needed.

Every new `/generate` command replaces the previously published unstable artifact for that ticket and posts the refreshed coordinates.

## Stable Releases after Merge

When the pull request merges into `develop`, the CI pipeline automatically removes all unstable artifacts associated with that ticket and publishes the stable release coordinates. To keep both unstable and stable builds succeeding:

- Bump the project version in every pull request so the pipelines can produce distinct artifacts.
- Keep quality gates and release checks passing before merging.

## Resolve Merge Conflicts Immediately

Maintain every pull request in a mergeable state. If GitHub reports conflicts with the target branch, fetch the latest `develop`, resolve the conflicts locally, and push the updated feature branch before requesting reviewers or triggering `/generate`.
