# Experiment Queue App

## Retired

This repository is retired and should not be used for new development or
deployment. The Experiment Queue interface is deprecated and is no longer an
active Olio product surface.

The production dashboard entry was removed in [OLI-3164], and current
`web-app-infra` no longer deploys `experiment-queue`.

## Current Workflows

- Use Experiment Scheduling for planning and scheduling workflows.
- Use Processing Queue only for processing-status visibility.

Those surfaces are separate from this retired FastAPI/React application.

## Repository Status

This repository is being tombstoned as part of [OLI-3167] and
[Olio-Labs/olio#4194].
The previous ECS deploy workflow has been removed so pushes to `main` cannot
redeploy this application.

If historical behavior or implementation details are needed, use the git
history. Do not revive this repository without opening a new Linear issue that
explains the replacement need and the deployment plan.

[OLI-3164]: https://linear.app/olio-labs/issue/OLI-3164
[OLI-3167]: https://linear.app/olio-labs/issue/OLI-3167
[Olio-Labs/olio#4194]: https://github.com/Olio-Labs/olio/issues/4194
