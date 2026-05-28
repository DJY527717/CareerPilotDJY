# CareerPilot Production Baseline

This document records production-readiness boundaries to reserve during the Web/API productization phase.

## Deployment Target

CareerPilot is expected to deploy to Alibaba Cloud ECS. Local development can keep lightweight defaults, but production settings must be injected through environment or deployment configuration outside the repository.

## Persistence

- Formal multi-user persistence should reserve Alibaba Cloud RDS MySQL or PostgreSQL.
- SQLite is acceptable only for local development, smoke tests, and temporary single-user experiments.
- Do not treat repository-local `.db`, `.sqlite`, or `.sqlite3` files as production data.

## File Storage

- Uploaded resumes, JD files, reports, and derived artifacts should reserve an OSS or object-storage boundary.
- Long-lived uploads should not be written into the project directory.
- Local upload directories are development scratch space and must stay ignored by Git.

## Product Boundaries To Reserve

The API and data model should leave room for these production concepts:

- `users` for authenticated account records.
- `workspaces` for team, tenant, or project-level isolation.
- `user_roles` for role assignment, including admin capabilities.
- `uploaded_files` for file metadata, storage keys, ownership, retention, and deletion state.
- `usage_events` for auditable feature usage.
- `daily_metrics` for aggregated operational reporting.
- `user_id` for authenticated user ownership.
- `workspace_id` for team, tenant, or project scoping.
- `admin` role for site-owner and operations-only functions.

API interfaces should reserve `user_id`, `workspace_id`, and admin-role checks even before the full production persistence layer is connected.

## Authentication And Workspace Isolation

The current authentication and workspace implementation is a mock contract only. Production deployment must validate server-side sessions or tokens on every protected request, store only secure password hashes, and scope business reads and writes by both `user_id` and `workspace_id`.

Frontend hiding of admin navigation is not an authorization boundary. Administrator permissions must be checked by backend code before returning admin data or accepting admin mutations.

## Admin Surface

The operations/admin backend is visible only to the site owner or administrator roles. It must not appear in normal user navigation, and regular user flows should not depend on admin-only views.

## External Services

Do not connect production RDS, OSS, Alibaba Cloud AccessKey, or paid model APIs during local boundary work. Use placeholders, mocks, or explicit adapters until deployment configuration is intentionally introduced.

## Sensitive Data Handling

Do not expose personal sensitive information, real resumes, real JD uploads, server IPs, domains, Alibaba Cloud AccessKey values, or paid API credentials in frontend code, logs, fixtures, mock data, or documentation examples.
