# CareerPilot API Layer

This package is a standalone HTTP API contract prototype for the future React web frontend.
The legacy Streamlit entrypoint remains `app.py`; this package does not import Streamlit UI code or the root Streamlit app.

## Run

```powershell
.\.venv\Scripts\python.exe -m careerpilot_api.app
```

The mock server listens on `http://127.0.0.1:8765`.

## Current Scope

- Uses generic mock data only.
- Uses Python standard library HTTP server only.
- Does not install or require new dependencies.
- Does not change database schema.
- Does not call the existing matching algorithms yet.
- Keeps API response structures close to the React prototype data model.

## Endpoints

- `GET /api/health`
- `GET /api/bootstrap`
- `GET /api/me`
- `GET /api/workspaces`
- `GET /api/settings/summary`
- `GET /api/product-roles`
- `GET /api/profile`
- `GET /api/resumes`
- `GET /api/preferences`
- `POST /api/jd/analyze`
- `POST /api/jd/batch-screen`
- `GET /api/jd/trends`
- `POST /api/resume/parse`
- `POST /api/resume/match`
- `POST /api/resume/rewrite`
- `POST /api/resume/gap`
- `GET /api/jobs/ranked`
- `POST /api/jobs/evaluate`
- `POST /api/jobs/compare`
- `GET /api/applications`
- `GET /api/interviews`
- `POST /api/interviews/record`
- `POST /api/interviews/report`
- `GET /api/reports/dashboard`

## Integration Notes

React defaults to `http://127.0.0.1:8765` and can override it with `VITE_API_BASE_URL`.
If the API is not running, the frontend falls back to `web/src/mockData.ts`.
