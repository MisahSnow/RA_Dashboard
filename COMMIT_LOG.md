# Commit Log
- 2026-01-25 19:20: Add profile social pagination and scrollable profile feeds.
- 2026-01-25 19:25: Restrict profile social feed to only show the viewed user's posts.
- 2026-01-25 19:49: Load friends list immediately for the friends dropdown to avoid empty state after refresh.
- 2026-01-25 20:52: Add profile add-friend action and remove profile header buttons.
- 2026-01-25 21:06: Close other dropdowns when opening friends, notifications, or avatar menus.
- 2026-01-25 22:40: Add in-page game compare mode and link profile/leaderboard game clicks to the new find-game view.
- 2026-01-25 23:24: Remove legacy compare/self-game panels and normalize main page card widths.
- 2026-02-18: Fix missing closing modal container in `web/index.html` so Add Friend modal is no longer hidden by Leaderboard History modal nesting.
- 2026-02-18: Switch `/api/game-players-refresh` to use server `RA_API_KEY` instead of user request API key.
