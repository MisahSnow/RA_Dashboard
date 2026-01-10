# RA Ultra-Minimal Friends Dashboard (Points + Activity)

## Setup
1) Install dependencies:
   npm install

2) Create `.env` next to server.js (copy from .env.example) and set:
   RA_API_KEY=YOUR_KEY

3) Run:
   npm start

4) Open:
   http://localhost:5179

## Endpoints
- /api/monthly/:username               (this month's points gained)
- /api/recent-achievements/:username   (recent unlocks; query ?m=minutes&limit=n)
- /api/recent-times/:username          (recent leaderboard entries; query ?games=n&limit=n)

Recent achievements uses API_GetUserRecentAchievements.
Recently played games uses API_GetUserRecentlyPlayedGames.
User game leaderboards uses API_GetUserGameLeaderboards.

## Rate limiting
This version caches recent-times for ~3 minutes and limits leaderboard requests to 2 at a time to avoid RA 429 errors.
