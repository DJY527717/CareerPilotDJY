# Auth And Workspace Contract Plan

This document records the third-round boundary work for CareerPilot authentication and multi-user workspace isolation. The current implementation is contract-only and mock-backed.

## Current Scope

- Authentication endpoints define request and response shapes for the Web/API product.
- Workspace endpoints define ownership and role fields for future isolation.
- The mock implementation does not connect RDS, OSS, Alibaba Cloud AccessKey, paid model APIs, third-party login, or email verification.
- Default data is generic demo data and must not contain personal information.

## Production Requirements

- Production persistence should use Alibaba Cloud RDS MySQL or PostgreSQL.
- Passwords must be stored only as secure password hashes with an appropriate password hashing algorithm and per-password salt.
- Session or token state must be validated by the server on every protected request.
- Admin authorization must be enforced by backend checks; frontend navigation hiding is only a usability layer.
- Every business record should carry `user_id` and `workspace_id` so data access can be scoped by account and workspace.

## Reserved API Boundaries

Authentication responses reserve:

- `user_id`
- `email`
- `display_name`
- `role`
- `is_admin`
- `default_workspace_id`

Workspace responses reserve:

- `workspace_id`
- `owner_user_id`
- `name`
- `role`
- `created_at`
- `updated_at`

Future business APIs should accept or derive `workspace_id` from the validated session, then apply both `user_id` and `workspace_id` filters before reading or mutating data.

## Admin Surface

The administrator operations surface is reserved for site owners and administrator roles. It must not appear in normal user navigation, and production access must never rely only on frontend checks.
