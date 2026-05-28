# CareerPilot Migration Boundary

This document defines the first-round boundary between the legacy Streamlit flow and the productized Web/API direction.

## Current Roles

- `legacy/streamlit_app.py` is the legacy Streamlit main entry and business-flow reference. It can be used to understand existing user journeys, page logic, field semantics, matching behavior, and report generation intent.
- `web/` is the forward React frontend surface for productized user experience.
- `careerpilot_api/` is the forward API layer for productized business capabilities and frontend integration.
- The root `app.py` should not be restored as the active product entry. Avoid continuing complex UI polishing in Streamlit.
- The current productized direction is `web/` plus `careerpilot_api/`, not a restored root-level Streamlit app.

## Migration Direction

Future work should move capability by capability:

1. Identify the business capability in the legacy flow, such as resume parsing, JD parsing, match scoring, evidence mapping, report generation, or batch screening.
2. Extract or reuse the underlying domain logic behind a stable Python interface.
3. Define the API contract in `careerpilot_api/` before connecting the React frontend.
4. Connect `web/` to the API through adapters and typed contracts.
5. Keep compatibility tests around legacy output semantics where they protect user-facing behavior.

## What Not To Migrate Directly

- Do not copy Streamlit session-state logic into the API layer.
- Do not reproduce Streamlit UI layout decisions in React.
- Do not place frontend-only state transitions inside business services.
- Do not introduce real external infrastructure credentials during migration.

## Boundary Principles

- Business logic belongs in domain modules or API services, not UI components.
- UI state belongs in `web/` and should call explicit API contracts.
- API schemas should be stable enough for frontend adapters and tests.
- `web/` pages must not directly depend on backend `snake_case` response fields. The API adapter layer should translate backend contracts into frontend-facing `camelCase` view models.
- Legacy behavior can guide correctness, but productized code should have clean ownership boundaries.

## Legacy File Placement

The old Streamlit app has been moved from the root into `legacy/streamlit_app.py` to make the project boundary clearer. Treat this file as a reference for old business and page behavior, not as the place for new product UI work.
