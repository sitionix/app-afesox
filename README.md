# Sitionix Platform Service Specifications

This repository captures the API-first contracts that power the customer lifecycle within the Sitionix platform. The specifications describe how platform services collaborate to onboard new clients, maintain their profiles, and coordinate the secure interactions required by downstream applications. By keeping these definitions in one place, the repo ensures every team integrates against the same source of truth when building features for Sitionix users.

## Contribution Workflow Guidelines

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

## Event Generation (Kafka + Avro)

This repository can generate Kafka producer or consumer libraries from Avro schemas.

### Folder layout

Place your schema here:

```
apis/<service>/event/<event-name>/<version>/envelope.avsc
apis/<service>/event/<event-name>/<version>/imports/<EventName>.avsc
```

Common metadata schema:

```
apis/common/event/Metadata.avsc
```

Register event builds for `/generate` in:

```
apis/metadata.yml
```

Event entries use:
- `definition-path: /<service>/event/<event-name>/<version>`


Example dummy event:

```
apis/athssox/event/dummy/v1/envelope.avsc
apis/athssox/event/dummy/v1/imports/DummyEvent.avsc
apis/common/event/Metadata.avsc
```

### Generate a producer library

```
EVENT_SERVICE=athssox \
EVENT_NAME=dummy \
EVENT_VERSION=v1 \
EVENT_PACKAGE=com.app_afesox.athssox.events.dummy \
EVENT_CLASS=DummyEvent \
EVENT_WRAPPER_PACKAGE=com.app_afesox.athssox.events.dummy.kafka \
mvn clean package -Pevent-producer
```

### Generate a consumer library

```
EVENT_SERVICE=athssox \
EVENT_NAME=dummy \
EVENT_VERSION=v1 \
EVENT_PACKAGE=com.app_afesox.athssox.events.dummy \
EVENT_CLASS=DummyEvent \
EVENT_WRAPPER_PACKAGE=com.app_afesox.athssox.events.dummy.kafka \
mvn clean package -Pevent-consumer
```

Notes:
- Avro generates the event POJOs; the Kafka producer/consumer wrappers are created from templates in `src/main/resources/event-templates`.
- The wrappers accept a value serializer/deserializer, so you can plug in your Kafka Avro serialization strategy.

## Resolve Merge Conflicts Immediately

Keep every pull request in a mergeable state. If GitHub reports conflicts with the target branch, fetch the latest changes, resolve the conflicts locally, and push the updated branch before requesting reviewers or triggering `/generate`. This guarantees that automation can run without interruption and that reviewers only see conflict-free diffs.
