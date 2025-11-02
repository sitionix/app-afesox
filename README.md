# Contribution Workflow Guidelines

This project follows a standardized workflow for preparing changes. Use the rules below whenever you start new work.

## Branch Naming

Create feature branches with the pattern `feature/SITIONIX-<ticket-number>`. Replace `<ticket-number>` with the numeric identifier provided for the task. For example, ticket 42 becomes `feature/SITIONIX-42`.

## Pull Request Naming

When opening a pull request, use the format `[SITIONIX-<ticket-number>] - <short-description>`. The description should summarize the change in a few words. For instance, for ticket 42 the PR title could be `[SITIONIX-42] - Add payment endpoint`.

Always ensure that the ticket number in the PR title matches the number in the branch name.

## Generating Unstable Versions for JARs

Use the `/generate` workflow to produce an unstable artifact while your pull request is under review:

1. Confirm that the PR title follows `[SITIONIX-<ticket-number>] - <short-description>` so the workflow can capture the ticket ID.
2. Look up the available API entries in [`apis/metadata.yml`](apis/metadata.yml). Each record lists a `name` that maps to a `definition-path`, and the `/generate` workflow only accepts those predefined `name` values. Reuse one of them verbatim when invoking the command—for instance, to build the Authentication API you would comment:

   ```text
   /generate --name "API Authentication SOX"
   ```

   Using any value that is not present in the metadata will cause the workflow to ignore the request because it cannot resolve the API definition.

3. A GitHub Actions workflow responds to that command, builds the specified API in unstable mode, and posts a follow-up comment with the Maven coordinates for the generated artifact.
4. Consume the dependency snippet from the success comment in downstream projects as needed.

Every new `/generate` comment replaces the previously published unstable artifact for that ticket and then produces the refreshed build.

## Releasing Stable Versions after Merge

Once the pull request is merged into the `develop` branch, the CI pipeline automatically removes any remaining unstable artifacts for that ticket and produces a stable release. The workflow publishes the stable Maven coordinates for long-term consumption.

To keep this automation working, ensure that each pull request bumps the project version to a new value. The fresh version number lets the pipeline create both unstable and stable builds without conflicts and keeps quality gates and release checks passing.

