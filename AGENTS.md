# Agent Conventions

This repository is a single Node/Express server (`server.js`) with a vanilla JS frontend in `web/`. Follow these conventions when changing the codebase:

- Client API calls:
  - Most client fetches use the client-side queue (`enqueueClientFetch` via `fetchJson`/`fetchServerJson`) to avoid flooding.
  - Some local-only DB endpoints bypass the queue using `fetchJson(..., { immediate: true })` for responsiveness.
  - Any endpoints that do not call RetroAchievements should bypass the queue with `{ immediate: true }`.
- Find Games player counts:
  - `/api/game-players-batch` is DB-only and used to hydrate player counts quickly.
  - `/api/game-players-refresh` hits the RA API and updates the DB; it runs lazily in the background.
  - The UI keeps stale DB values visible until refresh completes.
  - Batch requests are chunked to <=100 ids per call.
  - Refresh calls are cancellable/aborted when list changes.
- Sorting:
  - Find Games supports sorting by name, players, and points.
  - When sorting by players, the list re-sorts as counts stream in.
- Letter/console/search changes:
  - These cancel in-flight player-count requests to keep UI responsive.
- Static frontend:
  - The app is a single-page frontend served from `web/` (`web/index.html`, `web/style.css`, `web/app.js`).
  - `server.js` exposes `/api/editor/content` and `/api/editor/save` for editing the HTML/CSS.
- Environment and deploy:
  - `.env` is loaded from the same folder as `server.js` (not the CWD).
  - `DATABASE_URL` is optional; without it, sessions and user data are memory-only.
  - Render deploy config lives in `render.yaml` and sets `DATABASE_SSL=true`.
- Ops scripts:
  - `scripts/recompute-levels.cjs` recalculates user levels from RA totals; requires `RA_API_KEY` and `DATABASE_URL`.
- Git hygiene:
  - `commit-and-push.bat` is ignored via `.gitignore`.
.
- Logging:
  - When making commits for testing, record the commit hash in chat for traceability.
  - Always add changes to the commit log when asked to push.

If you add new local DB endpoints, consider whether they should bypass the client queue. If you add RA-backed endpoints, keep them queued and cached to avoid rate limits. 
