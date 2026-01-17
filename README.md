# Competitive RetroAchievements

A lightweight dashboard for comparing RetroAchievements friends: monthly points, recent activity, and profile/game comparisons.

## Features
- Monthly leaderboard with live "now playing" status.
- Profile view with shared games, full recent games, and achievement counts for you vs them.
- Per-game comparison (achievements + leaderboard times).
- Friends activity feeds with incremental "show more."

## Setup
1) Install dependencies:
   npm install

2) Create `.env` next to `server.js` and set:
   RA_API_KEY=YOUR_KEY
   DATABASE_URL=YOUR_POSTGRES_URL
   SESSION_SECRET=YOUR_RANDOM_SECRET
   # Optional (Render often needs SSL)
   DATABASE_SSL=true

3) Run:
   npm start

4) Open:
   http://localhost:5179

## API Endpoints
- /api/monthly/:username
- /api/recent-achievements/:username (query: ?m=minutes&limit=n)
- /api/recent-times/:username (query: ?games=n&limit=n)
- /api/recent-games/:username (query: ?count=n)
- /api/user-summary/:username
- /api/game-achievements/:username/:gameId
- /api/game-achievements-basic/:gameId
- /api/game-times/:username/:gameId
- /api/consoles
- /api/game-list (query: ?consoleId=1&letter=0-9|A-Z)
- /api/now-playing/:username (query: ?window=seconds)
- /api/auth/me
- /api/auth/login
- /api/auth/logout
- /api/friends

## Notes
- Uses RetroAchievements API. Provide your own API key in `.env` or via the UI settings.
- Recent times and other endpoints are cached for a few minutes to reduce 429s.
- Friends are stored per-account in Postgres and loaded via cookie sessions.
