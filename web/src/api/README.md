# Web API Boundary

`web/src/api` is the only boundary where raw backend, offline fixture, or future database-shaped responses should enter the React app.

- `contracts.ts` describes raw API responses. Raw fields may use `snake_case`.
- `adapters.ts` converts raw responses into frontend-facing `camelCase` types from `web/src/types.ts`.
- `adapterFixtures.ts` is offline demo data for adapter verification only.
- `index.ts` is the facade consumed by pages and components.
- Pages must not import `adapterFixtures.ts` or consume `Raw*` contracts directly.
- Future live API clients should fetch raw responses, pass them through adapters, and return only camelCase view models.
- Adapters may normalize missing fields, arrays, nulls, and unusual structures. They must not make product decisions, invent personal data, or connect to real paid/external services.
