# Frontend Contract Pipeline Design (BFF)

## Current pipeline architecture
- Source of truth for generation entries is `apis/metadata.yml`.
- Type routing is driven by `api-spec-type`:
  - `api-first` -> Maven profile `api-first` (OpenAPI generator `spring`)
  - `client` -> Maven profile `client` (OpenAPI generator `java` / `resttemplate`)
  - `event-*` -> dedicated event workflows + event Maven profiles
- Comment flow:
  - `trigger-message.yml` handles non-event entries and delegates to `build-and-deploy.yml`.
  - `trigger-message-event.yml` handles event entries and delegates to `build-and-deploy-event.yml`.
- Develop flow:
  - `detect-metadata-changes.yml` builds matrices from changed metadata.
  - `develop-build.yml` runs API and Event deploy workflows from those matrices.

## Extension point
The least-risk extension point is adding a new explicit `api-spec-type` and mapping it to a new Maven profile, while keeping existing `api-first`/`client` and event behavior unchanged.

## New type decision
Chosen new type: `frontend`.

Why:
- Matches current hyphenated type style (`event-producer`, `event-consumer`).
- Explicitly frontend-oriented.
- Generic for all future BFF APIs, not Automation-specific.

## Generation strategy
- Add Maven profile `frontend`.
- Input spec: `apis/<name>/rest/openapi.yml` (same metadata-driven resolution).
- Generator: `typescript-fetch` via OpenAPI Generator.
- Output shape: generated TypeScript contract package (`apis/*`, `models/*`, `runtime.ts`) published as a dedicated artifact.

Rationale:
- Frontend teams can consume generated contracts directly or wrap generated APIs in a thin local adapter.
- Keeps frontend contract boundary at BFF while preserving additive, low-risk integration in existing Maven publication flow.

## Artifact strategy
- Keep BE publication (`api-first`/`client`) in Maven unchanged.
- Publish frontend contracts as a dedicated npm package:
  - `@sitionix/app-afesox-<api-name>-frontend-<variant>`
- Frontend package remains separate from Java artifacts and aligns with frontend consumption tooling.

## Workflow extension strategy
- Add dedicated reusable workflow for frontend contracts:
  - `build-and-deploy-frontend-contract.yml`
- Comment flow (`trigger-message.yml`):
  - route `frontend` entries to new reusable workflow
  - keep existing non-event Java path untouched
- Develop flow:
  - extend detection with dedicated `frontend` matrix output
  - run separate frontend deploy job

## BFF boundary decision
Frontend-oriented contracts are generated from BFF specs (`bffssox`) and registered in metadata as BFF entries. Internal service specs are not used as frontend boundary in this pipeline.

## Regression safety
- No behavior changes to existing type semantics.
- Existing workflows, metadata entries, and artifact naming for `api-first`/`client` remain unchanged.
- New behavior is additive and type-gated by `frontend`.
