import express from "express";
import session from "express-session";
import connectPgSimple from "connect-pg-simple";
import pg from "pg";
import path from "path";
import fs from "fs";
import { fileURLToPath } from "url";
import dotenv from "dotenv";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Load .env from the same folder as server.js (NOT the working directory)
const envPath = path.join(__dirname, ".env");
if (fs.existsSync(envPath)) {
  dotenv.config({ path: envPath });
} else {
  console.warn("WARNING: .env not found next to server.js");
}

const app = express();
app.set("trust proxy", 1);
app.use(express.json());

const PORT = Number(process.env.PORT || 5179);
const RA_API_KEY = process.env.RA_API_KEY;
const DATABASE_URL = process.env.DATABASE_URL;
const SESSION_SECRET = process.env.SESSION_SECRET || "dev-session-secret";
const SNAPSHOT_SECRET = process.env.SNAPSHOT_SECRET || "";

if (!RA_API_KEY) {
  console.warn("WARNING: RA_API_KEY is not set. Check .env next to server.js.");
}
if (!DATABASE_URL) {
  console.warn("WARNING: DATABASE_URL is not set. User accounts will not persist.");
}

const { Pool } = pg;
const useSsl = process.env.DATABASE_SSL === "true";
const pool = DATABASE_URL
  ? new Pool({ connectionString: DATABASE_URL, ssl: useSsl ? { rejectUnauthorized: false } : false })
  : null;

if (pool) {
  const PgSession = connectPgSimple(session);
  app.use(session({
    store: new PgSession({ pool, createTableIfMissing: true }),
    secret: SESSION_SECRET,
    resave: false,
    saveUninitialized: false,
    cookie: {
      sameSite: "lax",
      secure: process.env.NODE_ENV === "production"
    }
  }));
}


// --- tiny in-memory cache (avoid 429s) ---
const _cache = new Map();
/**
 * cacheGet(key): returns { value, expiresAt } or null
 */
function cacheGet(key) {
  const hit = _cache.get(key);
  if (!hit) return null;
  if (Date.now() > hit.expiresAt) { _cache.delete(key); return null; }
  return hit.value;
}
function cacheSet(key, value, ttlMs) {
  _cache.set(key, { value, expiresAt: Date.now() + ttlMs });
}
const DEFAULT_CACHE_TTL_MS = 180 * 1000; // 3 minutes
const PRESENCE_TTL_MS = 15 * 1000;
const presence = new Map(); // Map<username, Map<sessionId, lastSeen>>

async function initDb() {
  if (!pool) return;
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      username TEXT UNIQUE NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS friends (
      id SERIAL PRIMARY KEY,
      user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      friend_username TEXT NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(user_id, friend_username)
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS daily_points (
      id SERIAL PRIMARY KEY,
      username TEXT NOT NULL,
      day DATE NOT NULL,
      mode TEXT NOT NULL DEFAULT 'hc',
      points INTEGER NOT NULL DEFAULT 0,
      updated_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(username, day, mode)
    );
  `);
}

// simple concurrency limiter
function createLimiter(max) {
  let active = 0;
  const queue = [];
  const runNext = () => {
    if (active >= max) return;
    const job = queue.shift();
    if (!job) return;
    active++;
    job().finally(() => {
      active--;
      runNext();
    });
  };
  return (fn) => new Promise((resolve, reject) => {
    queue.push(() => fn().then(resolve, reject));
    runNext();
  });
}
const limitLb = createLimiter(2); // only 2 leaderboard requests at a time

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// --- helpers ---
function toEpochSeconds(date) {
  return Math.floor(date.getTime() / 1000);
}

function getMonthRange(now = new Date()) {
  const start = new Date(now.getFullYear(), now.getMonth(), 1, 0, 0, 0, 0);
  const end = new Date(now);
  return { start, end };
}

function getDayRange(now = new Date()) {
  const start = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0, 0);
  const end = new Date(now);
  return { start, end };
}

async function computeDailyPoints(username, includeSoftcore, apiKey) {
  const { start, end } = getDayRange();
  const unlocks = await raGetAchievementsEarnedBetween(username, start, end, apiKey);

  const isHardcoreUnlock = (u) => Boolean(
    u?.HardcoreMode ?? u?.hardcoreMode ??
    u?.Hardcore ?? u?.hardcore ??
    u?.IsHardcore ?? u?.isHardcore ??
    u?.HardcoreModeActive ?? u?.hardcoreModeActive ??
    false
  );

  const considered = includeSoftcore ? unlocks : unlocks.filter(isHardcoreUnlock);

  let points = 0;
  for (const u of considered) {
    points += Number(u.points ?? u.Points ?? 0);
  }

  return {
    start,
    end,
    points,
    unlockCount: considered.length,
    unlockCountAll: unlocks.length
  };
}

async function upsertDailyPoints(username, dayKey, mode, points) {
  if (!pool) return;
  await pool.query(
    `
      INSERT INTO daily_points (username, day, mode, points, updated_at)
      VALUES ($1, $2, $3, $4, NOW())
      ON CONFLICT (username, day, mode)
      DO UPDATE SET points = EXCLUDED.points, updated_at = NOW()
    `,
    [username, dayKey, mode, points]
  );
}

async function raFetchJson(url, { retries = 2 } = {}) {
  let attempt = 0;
  while (true) {
    const res = await fetch(url, { headers: { Accept: "application/json" } });

    if (res.status === 429 && attempt < retries) {
      // RetroAchievements rate limit: back off a bit and retry
      const delay = 750 * Math.pow(2, attempt); // 750ms, 1500ms, ...
      await sleep(delay);
      attempt++;
      continue;
    }

    if (!res.ok) {
      const text = await res.text().catch(() => "");
      throw new Error(`RA API error ${res.status}: ${text || res.statusText}`);
    }
    return res.json();
  }
}

async function raGetAchievementsEarnedBetween(username, fromDate, toDate, apiKey) {
  if (!apiKey) throw new Error("Missing RA API key.");

  const f = toEpochSeconds(fromDate);
  const t = toEpochSeconds(toDate);

  const url = new URL("https://retroachievements.org/API/API_GetAchievementsEarnedBetween.php");
  url.searchParams.set("u", username);
  url.searchParams.set("f", String(f));
  url.searchParams.set("t", String(t));
  url.searchParams.set("y", apiKey);

  const data = await raFetchJson(url.toString());
  if (!Array.isArray(data)) throw new Error("Unexpected RA response (expected an array).");
  return data;
}

async function raGetUserRecentAchievements(username, minutes = 10080, apiKey) {
  if (!apiKey) throw new Error("Missing RA API key.");

  const url = new URL("https://retroachievements.org/API/API_GetUserRecentAchievements.php");
  url.searchParams.set("u", username);
  url.searchParams.set("m", String(minutes));
  url.searchParams.set("y", apiKey);

  const data = await raFetchJson(url.toString());
  if (!Array.isArray(data)) throw new Error("Unexpected RA response (expected an array).");
  return data;
}

async function raGetUserRecentlyPlayedGames(username, count = 10, offset = 0, apiKey) {
  if (!apiKey) throw new Error("Missing RA API key.");

  const url = new URL("https://retroachievements.org/API/API_GetUserRecentlyPlayedGames.php");
  url.searchParams.set("u", username);
  url.searchParams.set("c", String(count));
  url.searchParams.set("o", String(offset));
  url.searchParams.set("y", apiKey);

  const MAX_TRIES = 4; // total attempts (initial + retries)
  for (let attempt = 0; attempt < MAX_TRIES; attempt++) {
    try {
      const data = await raFetchJson(url.toString(), { retries: 3 });
      if (!Array.isArray(data)) throw new Error("Unexpected RA response (expected an array).");
      return data;
    } catch (err) {
      const msg = String(err?.message || err || "");
      const isTransient =
        msg.includes("fetch failed") ||
        msg.includes("ETIMEDOUT") ||
        msg.includes("ECONNRESET") ||
        msg.includes("EAI_AGAIN") ||
        msg.includes("RA API error 5") ||   // 500-599
        msg.includes("RA API error 429");

      if (!isTransient || attempt === MAX_TRIES - 1) throw err;

      const delay = 500 * Math.pow(2, attempt); // 500ms, 1s, 2s...
      await sleep(delay);
    }
  }

  return [];
}

async function raGetUserGameLeaderboards(username, gameId, count = 200, apiKey) {
  if (!apiKey) throw new Error("Missing RA API key.");

  const url = new URL("https://retroachievements.org/API/API_GetUserGameLeaderboards.php");
  url.searchParams.set("u", username);
  url.searchParams.set("i", String(gameId));
  url.searchParams.set("c", String(count));
  url.searchParams.set("y", apiKey);

  // This endpoint returns 422 for games with no leaderboards.
  // Treat that as "no results" instead of failing the whole feed.
  const res = await fetch(url.toString(), { headers: { Accept: "application/json" } });

  if (res.status === 429) {
    // reuse our backoff logic by delegating to raFetchJson
    const data = await raFetchJson(url.toString());
    const results = data?.Results || data?.results;
    return Array.isArray(results) ? results : [];
  }

  if (res.status === 422) {
    // Typical bodies include:
    // ["Game has no leaderboards"]
    // ["User has no leaderboards on this game"]
    // Treat any 422 here as "no results" so the feed can continue.
    return [];
  }

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`RA API error ${res.status}: ${text || res.statusText}`);
  }

  const data = await res.json();
  const results = data?.Results || data?.results;
  return Array.isArray(results) ? results : [];
}

async function raGetGameInfoAndUserProgress(username, gameId, apiKey) {
  if (!apiKey) throw new Error("Missing RA API key.");

  const url = new URL("https://retroachievements.org/API/API_GetGameInfoAndUserProgress.php");
  url.searchParams.set("u", username);
  url.searchParams.set("g", String(gameId));
  url.searchParams.set("y", apiKey);

  return raFetchJson(url.toString());
}

async function raGetUserSummary(username, apiKey) {
  if (!apiKey) throw new Error("Missing RA API key.");

  const url = new URL("https://retroachievements.org/API/API_GetUserSummary.php");
  url.searchParams.set("u", username);
  url.searchParams.set("y", apiKey);

  return raFetchJson(url.toString());
}

// --- API routes ---
app.get("/api/health", (_req, res) => res.json({ ok: true }));

function normalizeUsername(u) {
  return String(u || "").trim().replace(/\s+/g, "").toLowerCase();
}

function requireAuth(req, res, next) {
  if (!req.session || !req.session.user) {
    return res.status(401).json({ error: "Not authenticated" });
  }
  return next();
}

function requireSnapshotSecret(req, res, next) {
  if (!SNAPSHOT_SECRET) return next();
  const token = String(req.headers["x-snapshot-secret"] || req.query.secret || "").trim();
  if (token !== SNAPSHOT_SECRET) return res.status(401).json({ error: "Unauthorized" });
  return next();
}

app.get("/api/auth/me", (req, res) => {
  const user = req.session?.user;
  if (!user) return res.json({ username: "" });
  res.json({ username: user.username });
});

app.post("/api/auth/login", async (req, res) => {
  if (!pool) return res.status(500).json({ error: "Database unavailable" });
  const raw = req.body?.username;
  const username = normalizeUsername(raw);
  if (!username) return res.status(400).json({ error: "Missing username" });

  const result = await pool.query(
    "INSERT INTO users (username) VALUES ($1) ON CONFLICT (username) DO UPDATE SET username = EXCLUDED.username RETURNING id, username",
    [username]
  );
  const user = result.rows[0];
  req.session.user = { id: user.id, username: user.username };
  res.json({ username: user.username });
});

app.post("/api/auth/logout", (req, res) => {
  if (req.session) req.session.destroy(() => {});
  res.json({ ok: true });
});

app.get("/api/friends", requireAuth, async (req, res) => {
  if (!pool) return res.status(500).json({ error: "Database unavailable" });
  const userId = req.session.user.id;
  const result = await pool.query(
    "SELECT friend_username FROM friends WHERE user_id = $1 ORDER BY friend_username",
    [userId]
  );
  res.json({ results: result.rows.map(r => r.friend_username) });
});

app.post("/api/friends", requireAuth, async (req, res) => {
  if (!pool) return res.status(500).json({ error: "Database unavailable" });
  const userId = req.session.user.id;
  const friend = normalizeUsername(req.body?.username);
  if (!friend) return res.status(400).json({ error: "Missing friend username" });

  if (RA_API_KEY) {
    try {
      await raGetUserSummary(friend, RA_API_KEY);
    } catch (err) {
      const msg = String(err?.message || "");
      const notFound = msg.includes("404") || msg.toLowerCase().includes("not found");
      if (notFound) return res.status(404).json({ error: `User not found: ${friend}` });
      return res.status(502).json({ error: `Unable to verify user: ${friend}` });
    }
  }

  await pool.query(
    "INSERT INTO friends (user_id, friend_username) VALUES ($1, $2) ON CONFLICT (user_id, friend_username) DO NOTHING",
    [userId, friend]
  );
  res.json({ ok: true });
});

app.delete("/api/friends/:username", requireAuth, async (req, res) => {
  if (!pool) return res.status(500).json({ error: "Database unavailable" });
  const userId = req.session.user.id;
  const friend = normalizeUsername(req.params.username);
  if (!friend) return res.status(400).json({ error: "Missing friend username" });
  await pool.query(
    "DELETE FROM friends WHERE user_id = $1 AND friend_username = $2",
    [userId, friend]
  );
  res.json({ ok: true });
});

// Online presence (best-effort)
app.get("/api/presence", (_req, res) => {
  const now = Date.now();
  const active = [];
  for (const [username, sessions] of presence.entries()) {
    for (const [sessionId, lastSeen] of sessions.entries()) {
      if (now - lastSeen > PRESENCE_TTL_MS) {
        sessions.delete(sessionId);
      }
    }
    if (sessions.size === 0) {
      presence.delete(username);
      continue;
    }
    const latest = Math.max(...Array.from(sessions.values()));
    active.push({ username, lastSeen: latest });
  }
  active.sort((a, b) => b.lastSeen - a.lastSeen || a.username.localeCompare(b.username));
  res.json({ count: active.length, results: active.map(a => a.username) });
});

app.post("/api/presence", (req, res) => {
  const username = String(req.body?.username || "").trim();
  const sessionId = String(req.body?.sessionId || "").trim();
  if (!username || !sessionId) return res.status(400).json({ error: "Missing username or sessionId" });
  if (!presence.has(username)) presence.set(username, new Map());
  presence.get(username).set(sessionId, Date.now());
  res.json({ ok: true });
});

app.post("/api/presence/remove", (req, res) => {
  const username = String(req.body?.username || "").trim();
  const sessionId = String(req.body?.sessionId || "").trim();
  if (!username || !sessionId) return res.status(400).json({ error: "Missing username or sessionId" });
  const sessions = presence.get(username);
  if (sessions) {
    sessions.delete(sessionId);
    if (sessions.size === 0) presence.delete(username);
  }
  res.json({ ok: true });
});

// Monthly points gained (this month)
//
// NOTE about "points":
// RetroAchievements can award points in Hardcore or Softcore.
// The website's "monthly points" is commonly interpreted as Hardcore-only.
// This endpoint defaults to Hardcore-only to match that expectation.
// Use ?mode=all to include both HC + SC.
app.get("/api/monthly/:username", async (req, res) => {
  try {
    const username = String(req.params.username || "").trim();
    if (!username) return res.status(400).json({ error: "Missing username" });
    const apiKey = String(req.headers["x-ra-api-key"] || RA_API_KEY || "").trim();
    if (!apiKey) return res.status(400).json({ error: "Missing RA API key" });

    const { start, end } = getMonthRange();

    // optional overrides: ?from=YYYY-MM-DD&to=YYYY-MM-DD
    const fromQ = typeof req.query.from === "string" ? req.query.from : null;
    const toQ = typeof req.query.to === "string" ? req.query.to : null;

    const fromDate = fromQ ? new Date(fromQ + "T00:00:00") : start;
    const toDate = toQ ? new Date(toQ + "T23:59:59") : end;

    // mode: "hc" (default) or "all"
    const modeQ = typeof req.query.mode === "string" ? req.query.mode.toLowerCase() : "hc";
    const includeSoftcore = modeQ === "all";

    const unlocks = await raGetAchievementsEarnedBetween(username, fromDate, toDate, apiKey);

    const isHardcoreUnlock = (u) => Boolean(
      u?.HardcoreMode ?? u?.hardcoreMode ??
      u?.Hardcore ?? u?.hardcore ??
      u?.IsHardcore ?? u?.isHardcore ??
      u?.HardcoreModeActive ?? u?.hardcoreModeActive ??
      false
    );

    const considered = includeSoftcore ? unlocks : unlocks.filter(isHardcoreUnlock);

    let retroPoints = 0;
    let points = 0;

    for (const u of considered) {
      retroPoints += Number(u.trueRatio ?? u.TrueRatio ?? 0);
      points += Number(u.points ?? u.Points ?? 0);
    }

    res.json({
      username,
      mode: includeSoftcore ? "all" : "hc",
      fromDate: fromDate.toISOString(),
      toDate: toDate.toISOString(),
      retroPoints,
      points,
      unlockCount: considered.length,
      unlockCountAll: unlocks.length
    });
  } catch (err) {
    res.status(500).json({ error: err?.message || "Failed to fetch monthly data" });
  }
});

// Daily points gained (today)
app.get("/api/daily/:username", async (req, res) => {
  try {
    const rawUsername = String(req.params.username || "").trim();
    if (!rawUsername) return res.status(400).json({ error: "Missing username" });
    const username = normalizeUsername(rawUsername);
    const apiKey = String(req.headers["x-ra-api-key"] || RA_API_KEY || "").trim();
    if (!apiKey) return res.status(400).json({ error: "Missing RA API key" });

    const modeQ = typeof req.query.mode === "string" ? req.query.mode.toLowerCase() : "hc";
    const includeSoftcore = modeQ === "all";
    const mode = includeSoftcore ? "all" : "hc";

    const cacheKey = `daily:${username}:${mode}:${apiKey}`;
    const cached = cacheGet(cacheKey);
    if (cached) return res.json({ ...cached, cached: true });

    const { start, end, points, unlockCount, unlockCountAll } =
      await computeDailyPoints(username, includeSoftcore, apiKey);

    const payload = {
      username,
      mode,
      fromDate: start.toISOString(),
      toDate: end.toISOString(),
      points,
      unlockCount,
      unlockCountAll
    };

    cacheSet(cacheKey, payload, DEFAULT_CACHE_TTL_MS);

    const dayKey = start.toISOString().slice(0, 10);
    await upsertDailyPoints(username, dayKey, mode, points);

    res.json(payload);
  } catch (err) {
    res.status(500).json({ error: err?.message || "Failed to fetch daily data" });
  }
});

// Daily points history from DB (default last 7 days)
app.get("/api/daily-history", async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ error: "Database unavailable" });
    const usersParam = typeof req.query.users === "string" ? req.query.users : "";
    const users = usersParam
      .split(",")
      .map(normalizeUsername)
      .filter(Boolean);
    if (!users.length) return res.status(400).json({ error: "Missing users" });

    const daysParam = typeof req.query.days === "string" ? Number(req.query.days) : 7;
    const days = Math.max(1, Math.min(30, Number.isFinite(daysParam) ? daysParam : 7));
    const modeQ = typeof req.query.mode === "string" ? req.query.mode.toLowerCase() : "hc";
    const mode = modeQ === "all" ? "all" : "hc";

    const start = new Date();
    start.setDate(start.getDate() - (days - 1));
    const startKey = start.toISOString().slice(0, 10);

    const result = await pool.query(
      `
        SELECT username, day, points
        FROM daily_points
        WHERE username = ANY($1) AND mode = $2 AND day >= $3
        ORDER BY day ASC
      `,
      [users, mode, startKey]
    );

    const map = {};
    for (const row of result.rows) {
      const user = row.username;
      const dayKey = new Date(row.day).toISOString().slice(0, 10);
      if (!map[user]) map[user] = {};
      map[user][dayKey] = Number(row.points || 0);
    }

    res.json({ days, mode, results: map });
  } catch (err) {
    res.status(500).json({ error: err?.message || "Failed to fetch daily history" });
  }
});

// Daily points snapshot for all known users (intended for cron)
app.post("/api/daily-snapshot", requireSnapshotSecret, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ error: "Database unavailable" });
    if (!RA_API_KEY) return res.status(400).json({ error: "Missing RA API key" });

    const userRows = await pool.query("SELECT username FROM users");
    const friendRows = await pool.query("SELECT DISTINCT friend_username AS username FROM friends");
    const usernames = Array.from(new Set([
      ...userRows.rows.map(r => normalizeUsername(r.username)),
      ...friendRows.rows.map(r => normalizeUsername(r.username))
    ])).filter(Boolean);

    if (!usernames.length) return res.json({ ok: true, count: 0 });

    const modeQ = typeof req.query.mode === "string" ? req.query.mode.toLowerCase() : "hc";
    const includeSoftcore = modeQ === "all";
    const mode = includeSoftcore ? "all" : "hc";
    const dayKey = getDayRange().start.toISOString().slice(0, 10);
    const limitDaily = createLimiter(2);

    await Promise.all(usernames.map((username) => limitDaily(async () => {
      try {
        const { points } = await computeDailyPoints(username, includeSoftcore, RA_API_KEY);
        await upsertDailyPoints(username, dayKey, mode, points);
      } catch {
        // ignore snapshot failures per user
      }
    })));

    res.json({ ok: true, count: usernames.length });
  } catch (err) {
    res.status(500).json({ error: err?.message || "Failed to snapshot daily points" });
  }
});

// Recent achievements for a user
app.get("/api/recent-achievements/:username", async (req, res) => {
  try {
    const username = String(req.params.username || "").trim();
    if (!username) return res.status(400).json({ error: "Missing username" });
    const apiKey = String(req.headers["x-ra-api-key"] || RA_API_KEY || "").trim();
    if (!apiKey) return res.status(400).json({ error: "Missing RA API key" });

    const minutes = typeof req.query.m === "string" ? Number(req.query.m) : 10080; // default 7 days
    const limit = typeof req.query.limit === "string" ? Number(req.query.limit) : 20;

    const items = await raGetUserRecentAchievements(username, Number.isFinite(minutes) ? minutes : 10080, apiKey);

    // normalize + trim
    const normalized = items.slice(0, Math.max(0, limit)).map(a => ({
      username,
      date: a.Date ?? a.date,
      hardcore: Boolean(a.HardcoreMode ?? a.hardcoreMode),
      achievementId: a.AchievementID ?? a.achievementId,
      title: a.Title ?? a.title,
      description: a.Description ?? a.description,
      points: a.Points ?? a.points,
      trueRatio: a.TrueRatio ?? a.trueRatio,
      gameId: a.GameID ?? a.gameId,
      gameTitle: a.GameTitle ?? a.gameTitle,
      consoleName: a.ConsoleName ?? a.consoleName,
      badgeUrl: a.BadgeURL ?? a.badgeUrl,
      gameIcon: a.GameIcon ?? a.gameIcon
    }));

    res.json({ username, minutes, count: normalized.length, results: normalized });
  } catch (err) {
    res.status(500).json({ error: err?.message || "Failed to fetch recent achievements" });
  }
});

// Recent leaderboard "times/scores" for a user (derived from recently played games + user game leaderboards)
app.get("/api/recent-times/:username", async (req, res) => {
  try {
    const username = String(req.params.username || "").trim();
    if (!username) return res.status(400).json({ error: "Missing username" });
    const apiKey = String(req.headers["x-ra-api-key"] || RA_API_KEY || "").trim();
    if (!apiKey) return res.status(400).json({ error: "Missing RA API key" });

    const gamesWanted = typeof req.query.games === "string" ? Number(req.query.games) : 5;
    const limit = typeof req.query.limit === "string" ? Number(req.query.limit) : 20;

    const targetGames = Math.max(1, Math.min(200, Number.isFinite(gamesWanted) ? gamesWanted : 5));
    const safeLimit = Math.max(1, Math.min(50, Number.isFinite(limit) ? limit : 20));

    // cache key includes params
    const cacheKey = `recent-times:${username}:${targetGames}:${safeLimit}:${apiKey}`;
    const cached = cacheGet(cacheKey);
    if (cached) return res.json({ ...cached, cached: true });

    const chunkSize = 50; // API max per page
    const pages = Math.ceil(targetGames / chunkSize);

    const recentlyPlayedPages = await Promise.all(
      Array.from({ length: pages }, async (_v, idx) => {
        const offset = idx * chunkSize;
        const count = Math.min(chunkSize, targetGames - offset);
        return raGetUserRecentlyPlayedGames(username, count, offset, apiKey);
      })
    );

    const recentlyPlayed = recentlyPlayedPages.flat();

    const gameIds = recentlyPlayed
      .map(g => g.GameID ?? g.gameId)
      .filter(Boolean);

    // fetch leaderboards with a limiter to avoid bursts
    const perGame = await Promise.all(gameIds.map(async (gid) => limitLb(async () => {
      try {
        const lbs = await raGetUserGameLeaderboards(username, gid, 200, apiKey);
        const gameMeta = recentlyPlayed.find(g => (g.GameID ?? g.gameId) == gid) || {};
        const gameTitle = gameMeta.Title ?? gameMeta.title ?? `Game ${gid}`;
        const consoleName = gameMeta.ConsoleName ?? gameMeta.consoleName ?? "";
        const imageIcon = gameMeta.ImageIcon ?? gameMeta.imageIcon ?? "";

        return lbs
          .map(lb => {
            const ue = lb.UserEntry ?? lb.userEntry;
            if (!ue) return null;
            return {
              username,
              gameId: gid,
              gameTitle,
              consoleName,
              imageIcon,
              leaderboardId: lb.ID ?? lb.id,
              leaderboardTitle: lb.Title ?? lb.title,
              format: lb.Format ?? lb.format,
              rankAsc: lb.RankAsc ?? lb.rankAsc,
              score: ue.Score ?? ue.score,
              formattedScore: ue.FormattedScore ?? ue.formattedScore,
              rank: ue.Rank ?? ue.rank,
              dateUpdated: ue.DateUpdated ?? ue.dateUpdated
            };
          })
          .filter(Boolean);
      } catch (e) {
        // If any single game fails (including 422 variants), ignore and continue.
        return [];
      }
    })));
    const flattened = perGame.flat();

    flattened.sort((a, b) => {
      const da = Date.parse(a.dateUpdated || "") || 0;
      const db = Date.parse(b.dateUpdated || "") || 0;
      return db - da;
    });

    const payload = {
      username,
      gamesRequested: targetGames,
      gamesChecked: gameIds.length,
      count: Math.min(flattened.length, safeLimit),
      results: flattened.slice(0, safeLimit)
    };

    cacheSet(cacheKey, payload, DEFAULT_CACHE_TTL_MS);
    res.json(payload);
  } catch (err) {
    res.status(500).json({ error: err?.message || "Failed to fetch recent times" });
  }
});

// Recent games for a user (from recently played games)
app.get("/api/recent-games/:username", async (req, res) => {
  try {
    const username = String(req.params.username || "").trim();
    if (!username) return res.status(400).json({ error: "Missing username" });
    const apiKey = String(req.headers["x-ra-api-key"] || RA_API_KEY || "").trim();
    if (!apiKey) return res.status(400).json({ error: "Missing RA API key" });

    const countQ = typeof req.query.count === "string" ? Number(req.query.count) : 50;
    const count = Math.max(1, Math.min(200, Number.isFinite(countQ) ? countQ : 50));

    const cacheKey = `recent-games:${username}:${count}:${apiKey}`;
    const cached = cacheGet(cacheKey);
    if (cached) return res.json({ ...cached, cached: true });

    const items = await raGetUserRecentlyPlayedGames(username, count, 0, apiKey);

    const normalized = items.map(g => ({
      username,
      gameId: g.GameID ?? g.gameId,
      title: g.Title ?? g.title,
      consoleName: g.ConsoleName ?? g.consoleName,
      imageIcon: g.ImageIcon ?? g.imageIcon,
      lastPlayed: g.LastPlayed ?? g.lastPlayed
    }));

    const payload = { username, count: normalized.length, results: normalized };
    cacheSet(cacheKey, payload, DEFAULT_CACHE_TTL_MS);
    res.json(payload);
  } catch (err) {
    res.status(500).json({ error: err?.message || "Failed to fetch recent games" });
  }
});

// User summary/profile
app.get("/api/user-summary/:username", async (req, res) => {
  try {
    const username = String(req.params.username || "").trim();
    if (!username) return res.status(400).json({ error: "Missing username" });
    const apiKey = String(req.headers["x-ra-api-key"] || RA_API_KEY || "").trim();
    if (!apiKey) return res.status(400).json({ error: "Missing RA API key" });

    const cacheKey = `user-summary:${username}:${apiKey}`;
    const cached = cacheGet(cacheKey);
    if (cached) return res.json({ ...cached, cached: true });

    const data = await raGetUserSummary(username, apiKey);
    const user =
      (data && typeof data.User === "object" ? data.User : null) ||
      (data && typeof data.user === "object" ? data.user : null) ||
      data;

    const payload = {
      username,
      totalPoints:
        user?.TotalPoints ?? data?.TotalPoints ?? user?.totalPoints ?? data?.totalPoints ??
        user?.Points ?? data?.Points ?? user?.points ?? data?.points,
      retroPoints:
        user?.TotalRetroPoints ?? data?.TotalRetroPoints ?? user?.totalRetroPoints ?? data?.totalRetroPoints ??
        user?.TotalTruePoints ?? data?.TotalTruePoints ?? user?.totalTruePoints ?? data?.totalTruePoints ??
        user?.TruePoints ?? data?.TruePoints ?? user?.truePoints ?? data?.truePoints,
      totalPointsHardcore:
        user?.TotalPointsHardcore ?? data?.TotalPointsHardcore ?? user?.totalPointsHardcore ?? data?.totalPointsHardcore ??
        user?.TotalHardcorePoints ?? data?.TotalHardcorePoints ?? user?.totalHardcorePoints ?? data?.totalHardcorePoints ??
        user?.HardcorePoints ?? data?.HardcorePoints ?? user?.hardcorePoints ?? data?.hardcorePoints,
      totalPointsSoftcore:
        user?.TotalPointsSoftcore ?? data?.TotalPointsSoftcore ?? user?.totalPointsSoftcore ?? data?.totalPointsSoftcore ??
        user?.TotalSoftcorePoints ?? data?.TotalSoftcorePoints ?? user?.totalSoftcorePoints ?? data?.totalSoftcorePoints ??
        user?.SoftcorePoints ?? data?.SoftcorePoints ?? user?.softcorePoints ?? data?.softcorePoints,
      rank: user?.Rank ?? data?.Rank ?? user?.rank ?? data?.rank,
      memberSince: user?.MemberSince ?? data?.MemberSince ?? user?.memberSince ?? data?.memberSince,
      lastActivity: user?.LastActivity ?? data?.LastActivity ?? user?.lastActivity ?? data?.lastActivity,
      completedGames: user?.CompletedGames ?? data?.CompletedGames ?? user?.completedGames ?? data?.completedGames
    };

    cacheSet(cacheKey, payload, DEFAULT_CACHE_TTL_MS);
    res.json(payload);
  } catch (err) {
    res.status(500).json({ error: err?.message || "Failed to fetch user summary" });
  }
});

// Achievements for a user + game (for comparison)
app.get("/api/game-achievements/:username/:gameId", async (req, res) => {
  try {
    const username = String(req.params.username || "").trim();
    const gameId = String(req.params.gameId || "").trim();
    if (!username || !gameId) return res.status(400).json({ error: "Missing username or gameId" });
    const apiKey = String(req.headers["x-ra-api-key"] || RA_API_KEY || "").trim();
    if (!apiKey) return res.status(400).json({ error: "Missing RA API key" });

    const cacheKey = `game-achievements:${username}:${gameId}:${apiKey}`;
    const cached = cacheGet(cacheKey);
    if (cached) return res.json({ ...cached, cached: true });

    const data = await raGetGameInfoAndUserProgress(username, gameId, apiKey);
    const rawAchievements = data?.Achievements ?? data?.achievements ?? {};
    const achievements = Object.values(rawAchievements).map(a => ({
      id: a.ID ?? a.id,
      title: a.Title ?? a.title,
      description: a.Description ?? a.description,
      points: a.Points ?? a.points,
      badgeUrl: a.BadgeName ? `/Badge/${a.BadgeName}.png` : (a.BadgeURL ?? a.badgeUrl),
      earned: Boolean(
        a.DateEarned || a.dateEarned ||
        a.DateEarnedHardcore || a.dateEarnedHardcore ||
        a.Earned || a.earned
      ),
      earnedHardcore: Boolean(a.DateEarnedHardcore || a.dateEarnedHardcore)
    }));

    const payload = {
      username,
      gameId,
      gameTitle: data?.Title ?? data?.title,
      consoleName: data?.ConsoleName ?? data?.consoleName,
      imageIcon: data?.ImageIcon ?? data?.imageIcon,
      achievements
    };

    cacheSet(cacheKey, payload, DEFAULT_CACHE_TTL_MS);
    res.json(payload);
  } catch (err) {
    res.status(500).json({ error: err?.message || "Failed to fetch game achievements" });
  }
});

// Leaderboard times/scores for a user + game
app.get("/api/game-times/:username/:gameId", async (req, res) => {
  try {
    const username = String(req.params.username || "").trim();
    const gameId = String(req.params.gameId || "").trim();
    if (!username || !gameId) return res.status(400).json({ error: "Missing username or gameId" });
    const apiKey = String(req.headers["x-ra-api-key"] || RA_API_KEY || "").trim();
    if (!apiKey) return res.status(400).json({ error: "Missing RA API key" });

    const cacheKey = `game-times:${username}:${gameId}:${apiKey}`;
    const cached = cacheGet(cacheKey);
    if (cached) return res.json({ ...cached, cached: true });

    const lbs = await raGetUserGameLeaderboards(username, gameId, 200, apiKey);
    const normalized = lbs.map(lb => {
      const ue = lb.UserEntry ?? lb.userEntry ?? {};
      return {
        leaderboardId: lb.ID ?? lb.id,
        leaderboardTitle: lb.Title ?? lb.title,
        format: lb.Format ?? lb.format,
        rankAsc: lb.RankAsc ?? lb.rankAsc,
        score: ue.Score ?? ue.score,
        formattedScore: ue.FormattedScore ?? ue.formattedScore,
        rank: ue.Rank ?? ue.rank
      };
    });

    const payload = { username, gameId, count: normalized.length, results: normalized };
    cacheSet(cacheKey, payload, DEFAULT_CACHE_TTL_MS);
    res.json(payload);
  } catch (err) {
    res.status(500).json({ error: err?.message || "Failed to fetch game times" });
  }
});


// "Now playing" (best-effort): use most recent LastPlayed from Recently Played Games.
// If LastPlayed is within `windowSeconds` (default 60), treat as "currently playing".
app.get("/api/now-playing/:username", async (req, res) => {
  try {
    const username = String(req.params.username || "").trim();
    if (!username) return res.status(400).json({ error: "Missing username" });
    const apiKey = String(req.headers["x-ra-api-key"] || RA_API_KEY || "").trim();
    if (!apiKey) return res.status(400).json({ error: "Missing RA API key" });

    const windowSeconds = typeof req.query.window === "string" ? Number(req.query.window) : 120;
    const win = Number.isFinite(windowSeconds) ? Math.max(5, Math.min(600, windowSeconds)) : 60;

    const cacheKey = `now-playing:${username}:${win}:${apiKey}`;
    const cached = cacheGet(cacheKey);
    if (cached) return res.json({ ...cached, cached: true });

    // only need the most recent game
    const [latest] = await raGetUserRecentlyPlayedGames(username, 1, 0, apiKey);

    if (!latest) {
      const payload = { username, nowPlaying: false, reason: "no recent games" };
      cacheSet(cacheKey, payload, 30 * 1000);
      return res.json(payload);
    }

    const lastPlayedRaw = latest.LastPlayed ?? latest.lastPlayed;
    const lastPlayed = lastPlayedRaw ? String(lastPlayedRaw) : "";
    const ts = Date.parse(lastPlayed.replace(" ", "T") + "Z"); // best effort
    const ageSeconds = ts ? Math.floor((Date.now() - ts) / 1000) : null;

    const payload = {
      username,
      nowPlaying: ageSeconds !== null ? ageSeconds <= win : false,
      ageSeconds,
      windowSeconds: win,
      gameId: latest.GameID ?? latest.gameId,
      consoleName: latest.ConsoleName ?? latest.consoleName,
      title: latest.Title ?? latest.title,
      richPresence:
        latest.RichPresenceMsg ??
        latest.RichPresence ??
        latest.RichPresenceText ??
        latest.richPresence ??
        latest.richPresenceMsg,
      imageIcon: latest.ImageIcon ?? latest.imageIcon,
      lastPlayed
    };

    cacheSet(cacheKey, payload, 30 * 1000); // refreshable "presence" cache
    res.json(payload);
  } catch (err) {
    res.status(500).json({ error: err?.message || "Failed to fetch now playing" });
  }
});

// --- static site ---
const webPath = path.join(__dirname, "web");
app.use(express.static(webPath));
app.get("/", (_req, res) => res.sendFile(path.join(webPath, "index.html")));

const startServer = () => {
  app.listen(PORT, () => {
    console.log(`Server running: http://localhost:${PORT}`);
    console.log(`Health check : http://localhost:${PORT}/api/health`);
  });
};

if (pool) {
  initDb()
    .then(startServer)
    .catch((err) => {
      console.error("Failed to initialize database:", err);
      startServer();
    });
} else {
  startServer();
}
