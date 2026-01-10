# Competitive RetroAchievements

A lightweight dashboard for comparing RetroAchievements friends: monthly points, recent activity, and profile/game comparisons.

## Features
- Monthly leaderboard with live "now playing" status.
- Profile view with shared games, full recent games, and achievement counts for you vs them.
- Per-game comparison (achievements + leaderboard times).
- Friends activity feeds with incremental "show more."
- Built-in caching and retry/backoff for RA API rate limits.

## Setup
1) Install dependencies:
   npm install

2) Create `.env` next to `server.js` and set:
   RA_API_KEY=YOUR_KEY

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
- /api/game-times/:username/:gameId
- /api/now-playing/:username (query: ?window=seconds)

## Notes
- Uses RetroAchievements API. Provide your own API key in `.env` or via the UI settings.
- Recent times and other endpoints are cached for a few minutes to reduce 429s.
