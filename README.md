# Contribution Workflow Guidelines

This project follows a standardized workflow for preparing changes. Use the rules below whenever you start new work.

## Branch Naming

Create feature branches with the pattern `feature/SITIONIX-<ticket-number>`. Replace `<ticket-number>` with the numeric identifier provided for the task. For example, ticket 42 becomes `feature/SITIONIX-42`.

## Pull Request Naming

When opening a pull request, use the format `[SITIONIX-<ticket-number>] - <short-description>`. The description should summarize the change in a few words. For instance, for ticket 42 the PR title could be `[SITIONIX-42] - Add payment endpoint`.

Always ensure that the ticket number in the PR title matches the number in the branch name.

## Generating Unstable Versions for JARs

To build an unstable artifact for a pull request, trigger the `/generate` workflow directly from the PR conversation. Follow the steps below:

1. Make sure your pull request title uses the required format `[SITIONIX-<ticket-number>] - <short-description>` so the workflow can read the ticket ID.
2. Leave a new comment on the pull request that starts with the slash command and the API name, for example:

   ```text
   /generate --name "athssox"
   ```

3. The GitHub Actions workflow listens for that command, builds the specified API in unstable mode, and posts a follow-up comment with the generated Maven coordinates once deployment succeeds.
4. Use the dependency snippet from the success comment to consume the unstable JAR in your downstream project.

Each new `/generate` comment replaces any previous unstable artifact for the same ticket before producing an updated build.

