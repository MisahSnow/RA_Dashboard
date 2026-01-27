// RetroRivals server: serves the static web app and exposes API endpoints
// for RetroAchievements data, social features, and group/friends features.
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

// Load .env from the same folder as server.js (NOT the working directory).
const envPath = path.join(__dirname, ".env");
if (fs.existsSync(envPath)) {
  dotenv.config({ path: envPath });
} else {
  console.warn("WARNING: .env not found next to server.js");
}

// Express app setup.
const app = express();
app.set("trust proxy", 1);
app.use(express.json({ limit: "3mb" }));

// Core config pulled from environment variables.
const PORT = Number(process.env.PORT || 5179);
const RA_API_KEY = process.env.RA_API_KEY;
const DATABASE_URL = process.env.DATABASE_URL;
const SESSION_SECRET = process.env.SESSION_SECRET || "dev-session-secret";
const SNAPSHOT_SECRET = process.env.SNAPSHOT_SECRET || "";
const SOCIAL_MAX_IMAGE_BYTES = 2 * 1024 * 1024;
const SOCIAL_MAX_POSTS = 40;

if (!RA_API_KEY) {
  console.warn("WARNING: RA_API_KEY is not set. Check .env next to server.js.");
}
if (!DATABASE_URL) {
  console.warn("WARNING: DATABASE_URL is not set. User accounts will not persist.");
}

// Database connection (optional). When missing, the app still runs but
// user accounts and sessions won't persist across restarts.
const { Pool } = pg;
const useSsl = process.env.DATABASE_SSL === "true";
const pool = DATABASE_URL
  ? new Pool({ connectionString: DATABASE_URL, ssl: useSsl ? { rejectUnauthorized: false } : false })
  : null;

// Session config. If no database is present, the default memory store is used.
const sessionOptions = {
  secret: SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  cookie: {
    sameSite: "lax",
    secure: process.env.NODE_ENV === "production",
    maxAge: 30 * 24 * 60 * 60 * 1000
  }
};

if (pool) {
  const PgSession = connectPgSimple(session);
  sessionOptions.store = new PgSession({ pool, createTableIfMissing: true });
}

app.use(session(sessionOptions));


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
const GAME_INDEX_TTL_MS = 12 * 60 * 60 * 1000;
const GAME_META_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const GENRE_CACHE_TTL_MS = 10 * 365 * 24 * 60 * 60 * 1000;
const CONSOLE_LIST_TTL_MS = 24 * 60 * 60 * 1000;
const ALL_CONSOLES_WARMUP_INTERVAL_MS = Number(process.env.ALL_CONSOLES_WARMUP_INTERVAL_MS) || 6 * 60 * 60 * 1000;
const ALL_CONSOLES_WARMUP_START_DELAY_MS = Number(process.env.ALL_CONSOLES_WARMUP_START_DELAY_MS) || 15 * 1000;
const ALL_CONSOLES_WARMUP_PREEMPTIVE_MS = Number(process.env.ALL_CONSOLES_WARMUP_PREEMPTIVE_MS) || 60 * 60 * 1000;
const presence = new Map(); // Map<username, Map<sessionId, lastSeen>>
let consoleListCache = { builtAt: 0, list: null, inFlight: null };
const gameListCache = new Map(); // Map<consoleId, { builtAt, list, inFlight }>
let allGameListCache = { builtAt: 0, list: null, inFlight: null };
let allConsolesWarmupStatus = {
  status: "idle",
  startedAt: null,
  finishedAt: null,
  total: 0,
  completed: 0,
  lastError: ""
};
const gameMetaRefreshInFlight = new Map(); // Map<gameId, Promise>
const gameGenreCacheKey = (gameId) => `game-genre:${gameId}`;
const notificationStreams = new Map(); // Map<username, Set<res>>

// Create tables and columns if they don't exist yet.
async function initDb() {
  if (!pool) return;
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      username TEXT UNIQUE NOT NULL,
      total_points INTEGER DEFAULT 0,
      level INTEGER DEFAULT 1,
      level_updated_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);
  await pool.query(`
    ALTER TABLE users
      ADD COLUMN IF NOT EXISTS total_points INTEGER DEFAULT 0,
      ADD COLUMN IF NOT EXISTS level INTEGER DEFAULT 1,
      ADD COLUMN IF NOT EXISTS level_updated_at TIMESTAMPTZ
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
    CREATE TABLE IF NOT EXISTS groups (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      owner_user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS group_members (
      id SERIAL PRIMARY KEY,
      group_id INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
      user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      role TEXT NOT NULL DEFAULT 'member',
      joined_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(group_id, user_id)
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS group_invites (
      id SERIAL PRIMARY KEY,
      group_id INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
      invited_user TEXT NOT NULL,
      invited_by_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
      status TEXT NOT NULL DEFAULT 'pending',
      created_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(group_id, invited_user)
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS notifications (
      id SERIAL PRIMARY KEY,
      username TEXT NOT NULL,
      type TEXT NOT NULL,
      message TEXT NOT NULL,
      meta JSONB,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      read_at TIMESTAMPTZ
    );
  `);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS notifications_user_read_idx
      ON notifications (username, read_at);
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
  await pool.query(`
    CREATE TABLE IF NOT EXISTS hourly_points (
      id SERIAL PRIMARY KEY,
      username TEXT NOT NULL,
      hour_start TIMESTAMPTZ NOT NULL,
      mode TEXT NOT NULL DEFAULT 'hc',
      points INTEGER NOT NULL DEFAULT 0,
      updated_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(username, hour_start, mode)
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS challenges (
      id SERIAL PRIMARY KEY,
      creator_username TEXT NOT NULL,
      opponent_username TEXT NOT NULL,
      duration_hours INTEGER NOT NULL DEFAULT 24,
      status TEXT NOT NULL DEFAULT 'pending',
      created_at TIMESTAMPTZ DEFAULT NOW(),
      accepted_at TIMESTAMPTZ,
      start_at TIMESTAMPTZ,
      end_at TIMESTAMPTZ
    );
  `);
  await pool.query(`
    ALTER TABLE challenges
    ADD COLUMN IF NOT EXISTS creator_points INTEGER,
    ADD COLUMN IF NOT EXISTS opponent_points INTEGER,
    ADD COLUMN IF NOT EXISTS points_updated_at TIMESTAMPTZ
  `);
  await pool.query(`
    ALTER TABLE challenges
    ADD COLUMN IF NOT EXISTS challenge_type TEXT DEFAULT 'points',
    ADD COLUMN IF NOT EXISTS game_id INTEGER,
    ADD COLUMN IF NOT EXISTS leaderboard_id INTEGER,
    ADD COLUMN IF NOT EXISTS game_title TEXT,
    ADD COLUMN IF NOT EXISTS leaderboard_title TEXT,
    ADD COLUMN IF NOT EXISTS creator_start_score TEXT,
    ADD COLUMN IF NOT EXISTS opponent_start_score TEXT,
    ADD COLUMN IF NOT EXISTS leaderboard_format TEXT,
    ADD COLUMN IF NOT EXISTS leaderboard_lower_is_better BOOLEAN,
    ADD COLUMN IF NOT EXISTS creator_final_score TEXT,
    ADD COLUMN IF NOT EXISTS opponent_final_score TEXT,
    ADD COLUMN IF NOT EXISTS creator_final_score_value BIGINT,
    ADD COLUMN IF NOT EXISTS opponent_final_score_value BIGINT,
    ADD COLUMN IF NOT EXISTS creator_current_score TEXT,
    ADD COLUMN IF NOT EXISTS opponent_current_score TEXT,
    ADD COLUMN IF NOT EXISTS creator_current_score_value BIGINT,
    ADD COLUMN IF NOT EXISTS opponent_current_score_value BIGINT
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS backlog_items (
      id SERIAL PRIMARY KEY,
      user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      game_id INTEGER NOT NULL,
      title TEXT NOT NULL,
      console_name TEXT,
      image_icon TEXT,
      num_achievements INTEGER NOT NULL DEFAULT 0,
      points INTEGER NOT NULL DEFAULT 0,
      started_awarded INTEGER NOT NULL DEFAULT 0,
      started_total INTEGER NOT NULL DEFAULT 0,
      started_checked_at TIMESTAMPTZ,
      added_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(user_id, game_id)
    );
  `);
  await pool.query(`
    ALTER TABLE backlog_items
    ADD COLUMN IF NOT EXISTS started_awarded INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS started_total INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS started_checked_at TIMESTAMPTZ
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS social_posts (
      id SERIAL PRIMARY KEY,
      user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
      username TEXT NOT NULL,
      game_title TEXT,
      caption TEXT,
      image_data TEXT NOT NULL,
      image_url TEXT,
      is_auto BOOLEAN NOT NULL DEFAULT false,
      post_type TEXT NOT NULL DEFAULT 'screenshot',
      achievement_title TEXT,
      achievement_id INTEGER,
      achievement_description TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);
  await pool.query(`
    ALTER TABLE social_posts
    ADD COLUMN IF NOT EXISTS image_url TEXT,
    ADD COLUMN IF NOT EXISTS is_auto BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS post_type TEXT NOT NULL DEFAULT 'screenshot',
    ADD COLUMN IF NOT EXISTS achievement_title TEXT,
    ADD COLUMN IF NOT EXISTS achievement_id INTEGER,
    ADD COLUMN IF NOT EXISTS achievement_description TEXT
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS social_comments (
      id SERIAL PRIMARY KEY,
      post_id INTEGER NOT NULL REFERENCES social_posts(id) ON DELETE CASCADE,
      user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
      username TEXT NOT NULL,
      body TEXT NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS social_reactions (
      id SERIAL PRIMARY KEY,
      post_id INTEGER NOT NULL REFERENCES social_posts(id) ON DELETE CASCADE,
      user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
      username TEXT NOT NULL,
      reaction TEXT NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(post_id, username)
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS social_completion_events (
      id SERIAL PRIMARY KEY,
      username TEXT NOT NULL,
      game_id INTEGER NOT NULL,
      award_kind TEXT NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(username, game_id, award_kind)
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS game_metadata (
      game_id INTEGER PRIMARY KEY,
      num_distinct_players INTEGER,
      genre TEXT,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);
  await pool.query(`
    ALTER TABLE game_metadata
      ADD COLUMN IF NOT EXISTS num_distinct_players INTEGER,
      ADD COLUMN IF NOT EXISTS genre TEXT,
      ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()
  `);
}

// Simple concurrency limiter to avoid flooding RA endpoints.
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
const limitLb = createLimiter(1); // only 1 leaderboard request at a time

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// RetroAchievements API throttling (slow queue).
const RA_REQUEST_INTERVAL_MS = 200;
const RA_MAX_CONCURRENT = 1;
const raQueue = [];
let raQueueActive = 0;
let raQueueTimer = null;
let lastRaRequestAt = 0;

// Fast queue for lightweight RA requests.
const RA_FAST_MAX_CONCURRENT = 1;
const raFastQueue = [];
let raFastActive = 0;

function enqueueRaRequest(url, retries = 0) {
  return new Promise((resolve, reject) => {
    raQueue.push({ url, resolve, reject, retries });
    processRaQueue();
  });
}

function processRaQueue() {
  if (raQueueActive >= RA_MAX_CONCURRENT || raQueue.length === 0) return;
  const now = Date.now();
  const waitMs = Math.max(0, RA_REQUEST_INTERVAL_MS - (now - lastRaRequestAt));
  if (waitMs > 0) {
    if (!raQueueTimer) {
      raQueueTimer = setTimeout(() => {
        raQueueTimer = null;
        processRaQueue();
      }, waitMs);
    }
    return;
  }
  const job = raQueue.shift();
  raQueueActive += 1;
  lastRaRequestAt = Date.now();
  (async () => {
    const res = await fetch(job.url, { headers: { Accept: "application/json" } });
    if (res.status === 429 && (job.retries || 0) < 3) {
      raQueue.unshift({ ...job, retries: (job.retries || 0) + 1 });
    } else {
      job.resolve(res);
    }
  })().catch((err) => {
    job.reject(err);
  }).finally(() => {
    raQueueActive -= 1;
    processRaQueue();
  });
  processRaQueue();
}

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

function enqueueRaFastRequest(url, retries = 0) {
  return new Promise((resolve, reject) => {
    raFastQueue.push({ url, resolve, reject, retries });
    processRaFastQueue();
  });
}

function processRaFastQueue() {
  if (raFastActive >= RA_FAST_MAX_CONCURRENT || raFastQueue.length === 0) return;
  const job = raFastQueue.shift();
  raFastActive += 1;
  (async () => {
    const res = await fetch(job.url, { headers: { Accept: "application/json" } });
    if (res.status === 429 && (job.retries || 0) < 3) {
      raFastQueue.push({ ...job, retries: (job.retries || 0) + 1 });
    } else {
      job.resolve(res);
    }
  })().catch((err) => {
    job.reject(err);
  }).finally(() => {
    raFastActive -= 1;
    processRaFastQueue();
  });
  processRaFastQueue();
}

function getHourRange(now = new Date()) {
  const start = new Date(
    now.getFullYear(),
    now.getMonth(),
    now.getDate(),
    now.getHours(),
    0,
    0,
    0
  );
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

async function computeHourlyPoints(username, includeSoftcore, apiKey) {
  const { start, end } = getHourRange();
  const points = await computePointsBetween(username, start, end, includeSoftcore, apiKey);
  return { start, end, points };
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

async function upsertHourlyPoints(username, hourStart, mode, points) {
  if (!pool) return;
  await pool.query(
    `
      INSERT INTO hourly_points (username, hour_start, mode, points, updated_at)
      VALUES ($1, $2, $3, $4, NOW())
      ON CONFLICT (username, hour_start, mode)
      DO UPDATE SET points = EXCLUDED.points, updated_at = NOW()
    `,
    [username, hourStart, mode, points]
  );
}

  async function computeDailyPointsWithRetry(username, includeSoftcore, apiKey, retries = 10) {
    for (let attempt = 0; attempt <= retries; attempt++) {
      try {
        return await computeDailyPoints(username, includeSoftcore, apiKey);
      } catch (err) {
        const msg = String(err?.message || err || "");
        const is429 = msg.includes("429") || msg.includes("Too Many Attempts");
        if (!is429 || attempt >= retries) throw err;
      }
    }
    throw new Error("Failed to fetch daily points after retries.");
  }

async function computePointsBetween(username, fromDate, toDate, includeSoftcore, apiKey) {
  const unlocks = await raGetAchievementsEarnedBetween(username, fromDate, toDate, apiKey);

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

  return points;
}

  async function computePointsBetweenWithRetry(username, fromDate, toDate, includeSoftcore, apiKey, retries = 10) {
    for (let attempt = 0; attempt <= retries; attempt++) {
      try {
        return await computePointsBetween(username, fromDate, toDate, includeSoftcore, apiKey);
      } catch (err) {
        const msg = String(err?.message || err || "");
        const is429 = msg.includes("429") || msg.includes("Too Many Attempts");
        if (!is429 || attempt >= retries) throw err;
      }
    }
    throw new Error("Failed to fetch points after retries.");
  }

  async function computeHourlyPointsWithRetry(username, includeSoftcore, apiKey, retries = 10) {
    for (let attempt = 0; attempt <= retries; attempt++) {
      try {
        return await computeHourlyPoints(username, includeSoftcore, apiKey);
      } catch (err) {
        const msg = String(err?.message || err || "");
        const is429 = msg.includes("429") || msg.includes("Too Many Attempts");
        if (!is429 || attempt >= retries) throw err;
      }
    }
    throw new Error("Failed to fetch hourly points after retries.");
  }

  async function raFetchJson(url, { retries = 2, fast = false } = {}) {
    let attempt = 0;
    while (true) {
      const res = await (fast ? enqueueRaFastRequest(url) : enqueueRaRequest(url));

      if (res.status === 429 && attempt < retries) {
        attempt++;
        continue;
      }

      if (!res.ok) {
        const text = await res.text().catch(() => "");
        const err = new Error(`RA API error ${res.status}: ${text || res.statusText}`);
        err.status = res.status;
        throw err;
      }
      return res.json();
    }
  }

// --- RetroAchievements API wrappers ---
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
  const res = await enqueueRaRequest(url.toString());

  if (res.status === 429) {
    // reuse our queue + retry logic by delegating to raFetchJson
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

  return raFetchJson(url.toString(), { fast: true });
}

async function raGetUserSummary(username, apiKey) {
  if (!apiKey) throw new Error("Missing RA API key.");

  const url = new URL("https://retroachievements.org/API/API_GetUserSummary.php");
  url.searchParams.set("u", username);
  url.searchParams.set("y", apiKey);

  return raFetchJson(url.toString());
}

async function raGetUserCompletionProgress(username, count = 100, offset = 0, apiKey) {
  if (!apiKey) throw new Error("Missing RA API key.");

  const url = new URL("https://retroachievements.org/API/API_GetUserCompletionProgress.php");
  url.searchParams.set("u", username);
  url.searchParams.set("c", String(count));
  url.searchParams.set("o", String(offset));
  url.searchParams.set("y", apiKey);

  return raFetchJson(url.toString());
}

function normalizeGameLetter(letter) {
  const raw = String(letter || "").trim().toUpperCase();
  if (!raw || raw === "NUMBERS" || raw === "0-9" || raw === "0") return "0-9";
  const first = raw[0];
  if (first >= "A" && first <= "Z") return first;
  return "0-9";
}

function letterKeyFromTitle(title) {
  const trimmed = String(title || "").trim();
  if (!trimmed) return "0-9";
  const firstChar = trimmed[0];
  if (/[0-9]/.test(firstChar)) return "0-9";
  const upFirst = firstChar.toUpperCase();
  if (upFirst >= "A" && upFirst <= "Z") return upFirst;
  return "0-9";
}

async function raGetConsoleIds(apiKey) {
  if (!apiKey) throw new Error("Missing RA API key.");
  const url = new URL("https://retroachievements.org/API/API_GetConsoleIDs.php");
  url.searchParams.set("y", apiKey);
  const data = await raFetchJson(url.toString(), { fast: true });
  return Array.isArray(data) ? data : [];
}

async function raGetGameList(consoleId, apiKey) {
  if (!apiKey) throw new Error("Missing RA API key.");
  const url = new URL("https://retroachievements.org/API/API_GetGameList.php");
  url.searchParams.set("i", String(consoleId));
  url.searchParams.set("f", "1");
  url.searchParams.set("y", apiKey);
  const data = await raFetchJson(url.toString(), { fast: true });
  return Array.isArray(data) ? data : [];
}

async function raGetGameInfo(gameId, apiKey) {
  if (!apiKey) throw new Error("Missing RA API key.");
  const url = new URL("https://retroachievements.org/API/API_GetGameExtended.php");
  url.searchParams.set("i", String(gameId));
  url.searchParams.set("y", apiKey);
  return raFetchJson(url.toString(), { fast: true });
}

async function getConsoleList(apiKey) {
  const now = Date.now();
  if (consoleListCache.list && (now - consoleListCache.builtAt) < CONSOLE_LIST_TTL_MS) {
    return consoleListCache.list;
  }
  if (consoleListCache.inFlight) return consoleListCache.inFlight;
  consoleListCache.inFlight = (async () => {
    const list = await raGetConsoleIds(apiKey);
    const normalized = list
      .map(c => ({
        id: c?.ID ?? c?.id ?? c?.ConsoleID ?? c?.consoleId,
        name: c?.Name ?? c?.name ?? c?.ConsoleName ?? c?.consoleName
      }))
      .filter(c => c.id && c.name);
    consoleListCache = { builtAt: Date.now(), list: normalized, inFlight: null };
    return normalized;
  })().finally(() => {
    consoleListCache.inFlight = null;
  });
  return consoleListCache.inFlight;
}

async function getGameListForConsole(consoleId, apiKey) {
  const key = String(consoleId);
  const cached = gameListCache.get(key);
  const now = Date.now();
  if (cached?.list && (now - cached.builtAt) < GAME_INDEX_TTL_MS) {
    const hasIcons = cached.list.some(g => g && g.imageIcon);
    if (hasIcons) {
      return cached.list;
    }
  }
  if (cached?.inFlight) return cached.inFlight;
  const inFlight = (async () => {
    const list = await raGetGameList(consoleId, apiKey);
    const normalized = list
      .map(g => ({
        gameId: g?.ID ?? g?.id ?? g?.GameID ?? g?.gameId,
        title: g?.Title ?? g?.title ?? g?.GameTitle ?? g?.gameTitle ?? "",
        imageIcon: g?.ImageIcon ?? g?.imageIcon,
        numAchievements: g?.NumAchievements ?? g?.numAchievements ?? 0,
        numDistinctPlayers: (() => {
          const raw = g?.NumDistinctPlayers ?? g?.numDistinctPlayers ?? g?.Players ?? g?.players;
          const parsed = Number(raw);
          return Number.isFinite(parsed) ? parsed : null;
        })(),
        points: g?.Points ?? g?.points ?? 0,
        consoleId: g?.ConsoleID ?? g?.consoleId ?? g?.ConsoleId ?? g?.consoleId,
        consoleName: g?.ConsoleName ?? g?.consoleName ?? g?.Console ?? g?.console ?? ""
      }))
      .filter(g => g.gameId && g.title);
    gameListCache.set(key, { builtAt: Date.now(), list: normalized, inFlight: null });
    return normalized;
  })().finally(() => {
    const existing = gameListCache.get(key) || {};
    gameListCache.set(key, { ...existing, inFlight: null });
  });
  gameListCache.set(key, { builtAt: now, list: cached?.list || null, inFlight });
  return inFlight;
}

async function getGameListForAllConsoles(apiKey, options = {}) {
  const forceRefresh = Boolean(options.forceRefresh);
  const onProgress = typeof options.onProgress === "function" ? options.onProgress : null;
  const now = Date.now();
  if (!forceRefresh && allGameListCache.list && (now - allGameListCache.builtAt) < GAME_INDEX_TTL_MS) {
    return allGameListCache.list;
  }
  if (allGameListCache.inFlight) return allGameListCache.inFlight;
  allGameListCache.inFlight = (async () => {
    const consoles = await getConsoleList(apiKey);
    const combined = [];
    let completedConsoles = 0;
    if (onProgress) onProgress({ total: consoles.length, completed: completedConsoles });
    for (const consoleInfo of consoles) {
      const consoleId = consoleInfo?.id ?? consoleInfo?.consoleId;
      if (!consoleId) continue;
      const list = await getGameListForConsole(consoleId, apiKey);
      combined.push(...list);
      completedConsoles += 1;
      if (onProgress) onProgress({ total: consoles.length, completed: completedConsoles });
    }
    allGameListCache = { builtAt: Date.now(), list: combined, inFlight: null };
    return combined;
  })().finally(() => {
    allGameListCache.inFlight = null;
  });
  return allGameListCache.inFlight;
}

let allConsolesWarmupTimer = null;
let allConsolesWarmupInFlight = null;

function scheduleAllConsolesWarmup() {
  if (!RA_API_KEY || allConsolesWarmupTimer) return;
  const runWarmup = async (reason) => {
    if (allConsolesWarmupInFlight) return allConsolesWarmupInFlight;
    const now = Date.now();
    const age = now - (allGameListCache.builtAt || 0);
    const shouldRefresh = !allGameListCache.list || age >= (GAME_INDEX_TTL_MS - ALL_CONSOLES_WARMUP_PREEMPTIVE_MS);
    if (!shouldRefresh) return null;
    const task = (async () => {
      try {
        console.log(`Warmup: refreshing all-console game list (${reason}).`);
        allConsolesWarmupStatus = {
          status: "running",
          startedAt: new Date().toISOString(),
          finishedAt: null,
          total: 0,
          completed: 0,
          lastError: ""
        };
        await getGameListForAllConsoles(RA_API_KEY, {
          forceRefresh: true,
          onProgress: ({ total, completed }) => {
            if (Number.isFinite(total)) allConsolesWarmupStatus.total = total;
            if (Number.isFinite(completed)) allConsolesWarmupStatus.completed = completed;
          }
        });
        allConsolesWarmupStatus = {
          ...allConsolesWarmupStatus,
          status: "idle",
          finishedAt: new Date().toISOString()
        };
      } catch (err) {
        console.warn("Warmup: failed to refresh all-console game list.", err?.message || err);
        allConsolesWarmupStatus = {
          ...allConsolesWarmupStatus,
          status: "error",
          finishedAt: new Date().toISOString(),
          lastError: String(err?.message || err || "")
        };
      }
    })().finally(() => {
      allConsolesWarmupInFlight = null;
    });
    allConsolesWarmupInFlight = task;
    return task;
  };

  setTimeout(() => runWarmup("startup"), ALL_CONSOLES_WARMUP_START_DELAY_MS);
  allConsolesWarmupTimer = setInterval(() => runWarmup("interval"), ALL_CONSOLES_WARMUP_INTERVAL_MS);
}

async function readGameMeta(gameId) {
  if (!pool) return null;
  const result = await pool.query(
    `SELECT num_distinct_players, genre, updated_at FROM game_metadata WHERE game_id = $1`,
    [gameId]
  );
  return result.rows[0] || null;
}

async function upsertGameMeta(gameId, numDistinctPlayers, genre) {
  if (!pool) return;
  await pool.query(
    `INSERT INTO game_metadata (game_id, num_distinct_players, genre, updated_at)
     VALUES ($1, $2, $3, NOW())
     ON CONFLICT (game_id)
     DO UPDATE SET
       num_distinct_players = COALESCE(EXCLUDED.num_distinct_players, game_metadata.num_distinct_players),
       genre = COALESCE(EXCLUDED.genre, game_metadata.genre),
       updated_at = NOW()`,
    [gameId, numDistinctPlayers, genre]
  );
}

async function refreshGameMeta(gameId, apiKey) {
  const key = String(gameId);
  if (gameMetaRefreshInFlight.has(key)) return gameMetaRefreshInFlight.get(key);
  const task = (async () => {
    const data = await raGetGameInfo(gameId, apiKey);
    const rawPlayers = data?.NumDistinctPlayers ?? data?.numDistinctPlayers;
    const players = Number(rawPlayers);
    const rawGenre = data?.Genre ?? data?.genre ?? "";
    const genre = String(rawGenre || "").trim();
    const safePlayers = Number.isFinite(players) ? players : null;
    const safeGenre = genre ? genre : null;
    if (safePlayers !== null || safeGenre !== null) {
      await upsertGameMeta(gameId, safePlayers, safeGenre);
      if (safePlayers !== null) {
        cacheSet(`game-players:${gameId}`, safePlayers, GAME_META_TTL_MS);
      }
      if (safeGenre !== null) {
        cacheSet(gameGenreCacheKey(gameId), safeGenre, GAME_META_TTL_MS);
      }
    }
    return { numDistinctPlayers: safePlayers, genre: safeGenre };
  })().finally(() => {
    gameMetaRefreshInFlight.delete(key);
  });
  gameMetaRefreshInFlight.set(key, task);
  return task;
}

// --- API routes ---
app.get("/api/health", (_req, res) => res.json({ ok: true }));

app.get("/api/debug/warmup-status", requireAuth, (req, res) => {
  const apiKey = requireApiKey(req, res);
  if (!apiKey) return;
  res.json({
    ...allConsolesWarmupStatus,
    cacheBuiltAt: allGameListCache.builtAt || null,
    cacheHasData: !!(allGameListCache.list && allGameListCache.list.length)
  });
});

function normalizeUsername(u) {
  return String(u || "").trim().replace(/\s+/g, "").toLowerCase();
}

function computeLevelFromPoints(pointsRaw) {
  const points = Number(pointsRaw);
  if (!Number.isFinite(points) || points <= 0) return 1;
  return Math.max(1, Math.floor(Math.sqrt(points / 10) * 3));
}

async function upsertUserLevel(username, totalPoints) {
  if (!pool) return null;
  const normalized = normalizeUsername(username);
  if (!normalized) return null;
  const points = Number.isFinite(Number(totalPoints)) ? Number(totalPoints) : 0;
  const level = computeLevelFromPoints(points);
  const result = await pool.query(
    `INSERT INTO users (username, total_points, level, level_updated_at)
     VALUES ($1, $2, $3, NOW())
     ON CONFLICT (username)
     DO UPDATE SET total_points = EXCLUDED.total_points, level = EXCLUDED.level, level_updated_at = NOW()
     RETURNING level, total_points`,
    [normalized, points, level]
  );
  return result.rows[0] || { level, total_points: points };
}

function estimateDataUrlBytes(dataUrl) {
  const raw = String(dataUrl || "");
  if (!raw) return 0;
  const commaIndex = raw.indexOf(",");
  if (commaIndex === -1) return Buffer.byteLength(raw, "utf8");
  const base64 = raw.slice(commaIndex + 1);
  const padding = base64.endsWith("==") ? 2 : base64.endsWith("=") ? 1 : 0;
  return Math.max(0, Math.floor((base64.length * 3) / 4) - padding);
}

function raAssetUrl(rel) {
  if (!rel) return "";
  if (String(rel).startsWith("http")) return rel;
  if (String(rel).startsWith("//")) return `https:${rel}`;
  return `https://retroachievements.org${rel}`;
}

function completionAwardKind(raw) {
  const value = String(raw || "").toLowerCase();
  if (value.includes("master")) return "mastered";
  if (value.includes("beaten")) return "beaten";
  return "";
}

async function createNotification({ username, type, message, meta = null }) {
  if (!pool) return;
  const normalized = normalizeUsername(username);
  if (!normalized || !type || !message) return;
  await pool.query(
    `
      INSERT INTO notifications (username, type, message, meta, created_at)
      VALUES ($1, $2, $3, $4, NOW())
    `,
    [normalized, type, message, meta ? JSON.stringify(meta) : null]
  );
  await broadcastUnreadCount(normalized);
}

async function notifyFriendsOfSocialPost({
  authorUsername,
  displayName,
  postId,
  postType,
  gameTitle,
  isAuto = false
}) {
  if (!pool) return;
  const author = normalizeUsername(authorUsername);
  if (!author || !postId) return;
  const name = String(displayName || authorUsername || author || "").trim() || author;
  const friendsRes = await pool.query(
    `
      SELECT DISTINCT u.username AS username
      FROM friends f
      JOIN users u ON f.user_id = u.id
      WHERE LOWER(f.friend_username) = LOWER($1)
    `,
    [author]
  );
  const recipients = friendsRes.rows
    .map(r => normalizeUsername(r.username))
    .filter(u => u && u.toLowerCase() !== author.toLowerCase());
  if (!recipients.length) return;

  let message = `${name} posted to social.`;
  if (postType === "achievement" && gameTitle) {
    message = `${name} earned an achievement in ${gameTitle}.`;
  } else if (postType === "screenshot") {
    message = `${name} posted a screenshot.`;
  } else if (postType === "completion" && gameTitle) {
    message = `${name} completed ${gameTitle}.`;
  } else if (isAuto && gameTitle) {
    message = `${name} completed ${gameTitle}.`;
  }

  for (const recipient of recipients) {
    await createNotification({
      username: recipient,
      type: "social_post",
      message,
      meta: { from: name, postId, postType: postType || "text" }
    });
  }
}

async function deleteChallengeNotifications(username, challengeId) {
  if (!pool) return;
  const normalized = normalizeUsername(username);
  const id = Number(challengeId);
  if (!normalized || !Number.isFinite(id)) return;
  await pool.query(
    `
      DELETE FROM notifications
      WHERE username = $1
        AND type = 'challenge_pending'
        AND (meta->>'challengeId')::int = $2
    `,
    [normalized, id]
  );
  await broadcastUnreadCount(normalized);
}

async function getUnreadNotificationCount(username) {
  if (!pool) return 0;
  const result = await pool.query(
    "SELECT COUNT(*) FROM notifications WHERE username = $1 AND read_at IS NULL",
    [username]
  );
  return Number(result.rows[0]?.count || 0);
}

async function broadcastUnreadCount(username) {
  const normalized = normalizeUsername(username);
  const set = notificationStreams.get(normalized);
  if (!set || !set.size) return;
  const count = await getUnreadNotificationCount(normalized);
  const payload = `data: ${JSON.stringify({ unreadCount: count })}\n\n`;
  for (const res of set) {
    res.write(payload);
  }
}

async function recordCompletionSocialPost({ username, gameId, gameTitle, imageIcon, awardKind }) {
  if (!pool) return;
  if (!username || !gameId || !awardKind) return;
  const normalized = normalizeUsername(username);
  const imageUrl = raAssetUrl(imageIcon);
  const label = awardKind === "mastered" ? "Mastered" : "Completed";
  const caption = `${label} ${gameTitle || "a game"}`;

  const userRes = await pool.query(
    `SELECT id, username FROM users WHERE LOWER(username) = LOWER($1)`,
    [normalized]
  );
  const userId = userRes.rows[0]?.id ?? null;
  const displayName = userRes.rows[0]?.username ?? username;

  const insertEvent = await pool.query(
    `
      INSERT INTO social_completion_events (username, game_id, award_kind, created_at)
      VALUES ($1, $2, $3, NOW())
      ON CONFLICT (username, game_id, award_kind) DO NOTHING
      RETURNING id
    `,
    [normalized, Number(gameId), awardKind]
  );

  if (!insertEvent.rows.length) return;

  const insertPost = await pool.query(
    `
      INSERT INTO social_posts
        (user_id, username, game_title, caption, image_data, image_url, is_auto, post_type, created_at)
      VALUES ($1, $2, $3, $4, '', $5, true, 'completion', NOW())
      RETURNING id
    `,
    [userId, displayName, gameTitle || null, caption, imageUrl || null]
  );
  const postId = insertPost.rows[0]?.id;
  if (postId) {
    await notifyFriendsOfSocialPost({
      authorUsername: displayName,
      displayName,
      postId,
      postType: "completion",
      gameTitle,
      isAuto: true
    });
  }
}

// Simple session-based auth guard.
function requireAuth(req, res, next) {
  if (!req.session || !req.session.user) {
    return res.status(401).json({ error: "Not authenticated" });
  }
  return next();
}

function getRequestApiKey(req) {
  return String(req.headers["x-ra-api-key"] || "").trim();
}

function requireApiKey(req, res) {
  const apiKey = getRequestApiKey(req);
  if (!apiKey) {
    res.status(401).json({ error: "Missing RA API key" });
    return null;
  }
  const sessionKey = req.session?.raApiKey;
  if (sessionKey && sessionKey !== apiKey) {
    res.status(403).json({ error: "API key does not match logged in user" });
    return null;
  }
  return apiKey;
}

function extractSummaryUsername(data) {
  const userObj =
    (data && typeof data.User === "object" ? data.User : null) ||
    (data && typeof data.user === "object" ? data.user : null) ||
    null;
  const candidates = [
    userObj?.User,
    userObj?.Username,
    userObj?.UserName,
    userObj?.user,
    userObj?.username,
    data?.User,
    data?.Username,
    data?.UserName,
    data?.user,
    data?.username
  ];
  for (const entry of candidates) {
    if (typeof entry === "string" && entry.trim()) return entry.trim();
  }
  return "";
}

async function raGetGameLeaderboards(gameId, count = 200, apiKey) {
  if (!apiKey) throw new Error("Missing RA API key.");

  const url = new URL("https://retroachievements.org/API/API_GetGameLeaderboards.php");
  url.searchParams.set("i", String(gameId));
  url.searchParams.set("c", String(count));
  url.searchParams.set("y", apiKey);

  const data = await raFetchJson(url.toString());
  const results = data?.Results || data?.results || data;
  return Array.isArray(results) ? results : [];
}

async function getUserLeaderboardBest(username, gameId, leaderboardId, apiKey, leaderboardTitle = "") {
  const results = await raGetUserGameLeaderboards(username, gameId, 200, apiKey);
  let target = results.find((row) => {
    const id = row.LeaderboardID ?? row.leaderboardId ?? row.ID ?? row.id;
    return Number(id) === Number(leaderboardId);
  });
  if (!target && leaderboardTitle) {
    const titleLower = String(leaderboardTitle).toLowerCase();
    target = results.find((row) => {
      const title = row.Title ?? row.title ?? "";
      return String(title).toLowerCase() === titleLower;
    });
  }
  if (!target) return null;
  const userEntry = target.UserEntry ?? target.userEntry ?? {};
  const rawScore =
    userEntry.Score ?? userEntry.score ??
    target.Score ?? target.score ??
    target.Value ?? target.value ??
    null;
  const scoreValue = rawScore === null || rawScore === undefined || rawScore === ""
    ? null
    : Number(rawScore);
  const formatted =
    userEntry.FormattedScore ?? userEntry.formattedScore ??
    userEntry.Score ?? userEntry.score ??
    target.FormattedScore ?? target.formattedScore ??
    target.Score ?? target.score ??
    target.Value ?? target.value ??
    null;
  return {
    scoreText: formatted !== null ? String(formatted) : null,
    scoreValue: Number.isFinite(scoreValue) ? scoreValue : null,
    format: target.Format ?? target.format ?? null,
    lowerIsBetter: target.LowerIsBetter ?? target.lowerIsBetter ?? null
  };
}

async function getUserLeaderboardBestWithRetry(username, gameId, leaderboardId, apiKey, leaderboardTitle = "", retries = 6) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      return await getUserLeaderboardBest(username, gameId, leaderboardId, apiKey, leaderboardTitle);
    } catch (err) {
      const msg = String(err?.message || err || "");
      if (msg.includes("Missing RA API key") || attempt >= retries) throw err;
    }
  }
  throw new Error("Failed to fetch leaderboard best after retries.");
}

function requireSnapshotSecret(req, res, next) {
  if (!SNAPSHOT_SECRET) return next();
  const token = String(req.headers["x-snapshot-secret"] || req.query.secret || "").trim();
  if (token !== SNAPSHOT_SECRET) return res.status(401).json({ error: "Unauthorized" });
  return next();
}

function withChallengeWinner(row) {
  const creatorPoints = row.creator_points ?? null;
  const opponentPoints = row.opponent_points ?? null;
  const isScore = row.challenge_type === "score";
  const creatorFinal = row.creator_final_score_value ?? null;
  const opponentFinal = row.opponent_final_score_value ?? null;
  const lowerIsBetter = row.leaderboard_lower_is_better === true;
  let winner = "";
  let lead = null;

  if (isScore && creatorFinal !== null && opponentFinal !== null) {
    if (creatorFinal === opponentFinal) {
      winner = "tie";
      lead = 0;
    } else if (lowerIsBetter ? creatorFinal < opponentFinal : creatorFinal > opponentFinal) {
      winner = row.creator_username;
      lead = Math.abs(creatorFinal - opponentFinal);
    } else {
      winner = row.opponent_username;
      lead = Math.abs(creatorFinal - opponentFinal);
    }
    return { ...row, winner, lead };
  }

  if (creatorPoints !== null && opponentPoints !== null) {
    if (creatorPoints > opponentPoints) {
      winner = row.creator_username;
      lead = creatorPoints - opponentPoints;
    } else if (opponentPoints > creatorPoints) {
      winner = row.opponent_username;
      lead = opponentPoints - creatorPoints;
    } else {
      winner = "tie";
      lead = 0;
    }
  }
  return { ...row, winner, lead };
}

// --- Auth endpoints ---
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
  const apiKey = String(req.body?.apiKey || getRequestApiKey(req) || "").trim();
  if (!apiKey) return res.status(400).json({ error: "Missing RA API key" });

  try {
    const summary = await raGetUserSummary(username, apiKey);
    const summaryName = normalizeUsername(extractSummaryUsername(summary));
    if (summaryName && summaryName !== username) {
      return res.status(403).json({ error: "API key does not match username" });
    }
  } catch (err) {
    const msg = String(err?.message || "");
    const notFound = msg.includes("404") || msg.toLowerCase().includes("not found");
    if (notFound) return res.status(404).json({ error: `User not found: ${username}` });
    return res.status(401).json({ error: "Invalid RA API key" });
  }

  const result = await pool.query(
    "INSERT INTO users (username) VALUES ($1) ON CONFLICT (username) DO UPDATE SET username = EXCLUDED.username RETURNING id, username",
    [username]
  );
  const user = result.rows[0];
  req.session.user = { id: user.id, username: user.username };
  req.session.raApiKey = apiKey;
  res.json({ username: user.username });
});

app.post("/api/auth/logout", (req, res) => {
  if (req.session) req.session.destroy(() => {});
  res.json({ ok: true });
});

// --- Friends ---
app.get("/api/friends", requireAuth, async (req, res) => {
  if (!pool) return res.status(500).json({ error: "Database unavailable" });
  const userId = req.session.user.id;
  const result = await pool.query(
    "SELECT friend_username FROM friends WHERE user_id = $1 ORDER BY friend_username",
    [userId]
  );
  res.json({ results: result.rows.map(r => r.friend_username) });
});

app.get("/api/friends/suggestions", requireAuth, async (req, res) => {
  if (!pool) return res.status(500).json({ error: "Database unavailable" });
  const userId = req.session.user.id;
  const me = normalizeUsername(req.session.user.username);
  const limitQ = typeof req.query.limit === "string" ? Number(req.query.limit) : 6;
  const limit = Math.max(1, Math.min(20, Number.isFinite(limitQ) ? limitQ : 6));

  const friendsRes = await pool.query(
    "SELECT friend_username FROM friends WHERE user_id = $1",
    [userId]
  );
  const myFriends = friendsRes.rows.map(r => normalizeUsername(r.friend_username)).filter(Boolean);
  if (!myFriends.length) return res.json({ results: [] });

  const friendIdsRes = await pool.query(
    "SELECT id, username FROM users WHERE username = ANY($1)",
    [myFriends]
  );
  const friendIds = friendIdsRes.rows.map(r => r.id);
  if (!friendIds.length) return res.json({ results: [] });

  const suggestionsRes = await pool.query(
    `
      SELECT DISTINCT f.friend_username AS username
      FROM friends f
      WHERE f.user_id = ANY($1)
    `,
    [friendIds]
  );
  const suggestions = suggestionsRes.rows
    .map(r => normalizeUsername(r.username))
    .filter(u => u && u !== me && !myFriends.includes(u))
    .slice(0, limit);
  res.json({ results: suggestions });
});

app.post("/api/friends", requireAuth, async (req, res) => {
  if (!pool) return res.status(500).json({ error: "Database unavailable" });
  const userId = req.session.user.id;
  const friend = normalizeUsername(req.body?.username);
  if (!friend) return res.status(400).json({ error: "Missing friend username" });

  const apiKey = requireApiKey(req, res);
  if (!apiKey) return;
  try {
    await raGetUserSummary(friend, apiKey);
  } catch (err) {
    const msg = String(err?.message || "");
    const notFound = msg.includes("404") || msg.toLowerCase().includes("not found");
    if (notFound) return res.status(404).json({ error: `User not found: ${friend}` });
    return res.status(502).json({ error: `Unable to verify user: ${friend}` });
  }

  await pool.query(
    "INSERT INTO friends (user_id, friend_username) VALUES ($1, $2) ON CONFLICT (user_id, friend_username) DO NOTHING",
    [userId, friend]
  );
  await createNotification({
    username: friend,
    type: "friend_added",
    message: `${req.session.user.username} added you as a friend.`,
    meta: { from: req.session.user.username }
  });
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

// Groups
// --- Groups ---
app.post("/api/groups", requireAuth, async (req, res) => {
  if (!pool) return res.status(500).json({ error: "Database unavailable" });
  const userId = req.session.user.id;
  const name = String(req.body?.name || "").trim();
  if (!name) return res.status(400).json({ error: "Missing group name" });

  const result = await pool.query(
    "INSERT INTO groups (name, owner_user_id) VALUES ($1, $2) RETURNING id, name, created_at",
    [name, userId]
  );
  const group = result.rows[0];
  await pool.query(
    "INSERT INTO group_members (group_id, user_id, role) VALUES ($1, $2, 'owner') ON CONFLICT DO NOTHING",
    [group.id, userId]
  );
  res.json({ group });
});

app.get("/api/groups", requireAuth, async (req, res) => {
  if (!pool) return res.status(500).json({ error: "Database unavailable" });
  const userId = req.session.user.id;
  const result = await pool.query(
    `SELECT g.id, g.name, g.created_at, gm.role,
            (SELECT COUNT(*) FROM group_members gm2 WHERE gm2.group_id = g.id) AS member_count
       FROM groups g
       JOIN group_members gm ON gm.group_id = g.id
      WHERE gm.user_id = $1
      ORDER BY g.created_at DESC`,
    [userId]
  );
  res.json({ results: result.rows });
});

app.get("/api/groups/browse", requireAuth, async (req, res) => {
  if (!pool) return res.status(500).json({ error: "Database unavailable" });
  const userId = req.session.user.id;
  const result = await pool.query(
    `SELECT g.id, g.name, g.created_at,
            (SELECT COUNT(*) FROM group_members gm2 WHERE gm2.group_id = g.id) AS member_count,
            EXISTS(
              SELECT 1 FROM group_members gm3 WHERE gm3.group_id = g.id AND gm3.user_id = $1
            ) AS is_member
       FROM groups g
      ORDER BY g.created_at DESC`,
    [userId]
  );
  res.json({ results: result.rows });
});

app.post("/api/groups/:groupId/join", requireAuth, async (req, res) => {
  if (!pool) return res.status(500).json({ error: "Database unavailable" });
  const userId = req.session.user.id;
  const groupId = Number(req.params.groupId);
  if (!Number.isFinite(groupId)) return res.status(400).json({ error: "Invalid group id" });

  await pool.query(
    "INSERT INTO group_members (group_id, user_id, role) VALUES ($1, $2, 'member') ON CONFLICT DO NOTHING",
    [groupId, userId]
  );
  res.json({ ok: true });
});

app.get("/api/groups/:groupId/members", requireAuth, async (req, res) => {
  if (!pool) return res.status(500).json({ error: "Database unavailable" });
  const groupId = Number(req.params.groupId);
  if (!Number.isFinite(groupId)) return res.status(400).json({ error: "Invalid group id" });

  const result = await pool.query(
    `SELECT u.username, gm.role
       FROM group_members gm
       JOIN users u ON u.id = gm.user_id
      WHERE gm.group_id = $1
      ORDER BY u.username`,
    [groupId]
  );
  res.json({ results: result.rows });
});

app.post("/api/groups/:groupId/invite", requireAuth, async (req, res) => {
  if (!pool) return res.status(500).json({ error: "Database unavailable" });
  const userId = req.session.user.id;
  const groupId = Number(req.params.groupId);
  if (!Number.isFinite(groupId)) return res.status(400).json({ error: "Invalid group id" });
  const invited = normalizeUsername(req.body?.username);
  if (!invited) return res.status(400).json({ error: "Missing username" });

  const memberCheck = await pool.query(
    "SELECT 1 FROM group_members WHERE group_id = $1 AND user_id = $2",
    [groupId, userId]
  );
  if (!memberCheck.rows.length) return res.status(403).json({ error: "Not a group member" });

  await pool.query(
    `INSERT INTO group_invites (group_id, invited_user, invited_by_id)
     VALUES ($1, $2, $3)
     ON CONFLICT (group_id, invited_user)
     DO UPDATE SET status = 'pending', invited_by_id = EXCLUDED.invited_by_id, created_at = NOW()`,
    [groupId, invited, userId]
  );
  const groupRow = await pool.query("SELECT name FROM groups WHERE id = $1", [groupId]);
  const groupName = groupRow.rows[0]?.name || "a group";
  await createNotification({
    username: invited,
    type: "group_invite",
    message: `${req.session.user.username} invited you to join ${groupName}.`,
    meta: { groupId, groupName, invitedBy: req.session.user.username }
  });
  res.json({ ok: true });
});

app.get("/api/groups/invites", requireAuth, async (req, res) => {
  if (!pool) return res.status(500).json({ error: "Database unavailable" });
  const username = normalizeUsername(req.session.user.username);
  const result = await pool.query(
    `SELECT gi.id, gi.group_id, gi.created_at, g.name AS group_name,
            u.username AS invited_by
       FROM group_invites gi
       JOIN groups g ON g.id = gi.group_id
       LEFT JOIN users u ON u.id = gi.invited_by_id
      WHERE gi.invited_user = $1 AND gi.status = 'pending'
      ORDER BY gi.created_at DESC`,
    [username]
  );
  res.json({ results: result.rows });
});

app.post("/api/groups/invites/:inviteId/accept", requireAuth, async (req, res) => {
  if (!pool) return res.status(500).json({ error: "Database unavailable" });
  const inviteId = Number(req.params.inviteId);
  if (!Number.isFinite(inviteId)) return res.status(400).json({ error: "Invalid invite id" });
  const userId = req.session.user.id;
  const username = normalizeUsername(req.session.user.username);

  const inviteRes = await pool.query(
    "SELECT group_id FROM group_invites WHERE id = $1 AND invited_user = $2 AND status = 'pending'",
    [inviteId, username]
  );
  if (!inviteRes.rows.length) return res.status(404).json({ error: "Invite not found" });
  const groupId = inviteRes.rows[0].group_id;

  await pool.query(
    "INSERT INTO group_members (group_id, user_id, role) VALUES ($1, $2, 'member') ON CONFLICT DO NOTHING",
    [groupId, userId]
  );
  await pool.query(
    "UPDATE group_invites SET status = 'accepted' WHERE id = $1",
    [inviteId]
  );
  res.json({ ok: true, groupId });
});

app.post("/api/groups/invites/:inviteId/decline", requireAuth, async (req, res) => {
  if (!pool) return res.status(500).json({ error: "Database unavailable" });
  const inviteId = Number(req.params.inviteId);
  if (!Number.isFinite(inviteId)) return res.status(400).json({ error: "Invalid invite id" });
  const username = normalizeUsername(req.session.user.username);
  await pool.query(
    "UPDATE group_invites SET status = 'declined' WHERE id = $1 AND invited_user = $2",
    [inviteId, username]
  );
  res.json({ ok: true });
});

// Challenges
// --- Challenges ---
app.get("/api/challenges", requireAuth, async (req, res) => {
  if (!pool) return res.status(500).json({ error: "Database unavailable" });
  const me = normalizeUsername(req.session.user.username);
  const totalsQ = typeof req.query.totals === "string" ? req.query.totals.toLowerCase() : "1";
  const includeTotals = !(totalsQ === "0" || totalsQ === "false");
  const apiKey = includeTotals ? requireApiKey(req, res) : null;
  if (includeTotals && !apiKey) return;
  const incoming = await pool.query(
    `
      SELECT id, creator_username, opponent_username, duration_hours, status,
             created_at, accepted_at, start_at, end_at,
             creator_points, opponent_points, points_updated_at,
             challenge_type, game_id, leaderboard_id, game_title, leaderboard_title,
             creator_start_score, opponent_start_score, leaderboard_format, leaderboard_lower_is_better,
             creator_current_score, opponent_current_score, creator_current_score_value, opponent_current_score_value
      FROM challenges
      WHERE opponent_username = $1 AND status = 'pending'
      ORDER BY created_at DESC
    `,
    [me]
  );
  const outgoing = await pool.query(
    `
      SELECT id, creator_username, opponent_username, duration_hours, status,
             created_at, accepted_at, start_at, end_at,
             creator_points, opponent_points, points_updated_at,
             challenge_type, game_id, leaderboard_id, game_title, leaderboard_title,
             creator_start_score, opponent_start_score, leaderboard_format, leaderboard_lower_is_better,
             creator_current_score, opponent_current_score, creator_current_score_value, opponent_current_score_value
      FROM challenges
      WHERE creator_username = $1 AND status = 'pending'
      ORDER BY created_at DESC
    `,
    [me]
  );
  const active = await pool.query(
    `
      SELECT id, creator_username, opponent_username, duration_hours, status,
             created_at, accepted_at, start_at, end_at,
             creator_points, opponent_points, points_updated_at,
             challenge_type, game_id, leaderboard_id, game_title, leaderboard_title,
             creator_start_score, opponent_start_score, leaderboard_format, leaderboard_lower_is_better,
             creator_current_score, opponent_current_score, creator_current_score_value, opponent_current_score_value
      FROM challenges
      WHERE status = 'active' AND (creator_username = $1 OR opponent_username = $1)
      ORDER BY start_at DESC
    `,
    [me]
  );

  let activeRows = active.rows;
  const warnings = [];
    if (includeTotals && apiKey && activeRows.length) {
      const limitChallenges = createLimiter(1);
    const now = new Date();
      activeRows = await Promise.all(activeRows.map((row) => limitChallenges(async () => {
        try {
          const startAt = row.start_at ? new Date(row.start_at) : null;
          let startScoreDebug = "";
          const baseRow = {
            ...row,
            creator_points: row.creator_points ?? null,
            opponent_points: row.opponent_points ?? null
          };
          if (!startAt) return baseRow;
          let creatorStart = row.creator_start_score ?? null;
          let opponentStart = row.opponent_start_score ?? null;
          let leaderboardFormat = row.leaderboard_format ?? null;
          let leaderboardLower = row.leaderboard_lower_is_better ?? null;
          let creatorCurrent = null;
          let opponentCurrent = null;
          let creatorCurrentValue = null;
          let opponentCurrentValue = null;
          if (row.challenge_type === "score" && row.game_id && row.leaderboard_id &&
              (creatorStart === null || opponentStart === null)) {
            try {
              const [creatorBest, opponentBest] = await Promise.all([
                getUserLeaderboardBestWithRetry(row.creator_username, row.game_id, row.leaderboard_id, apiKey, row.leaderboard_title, 6),
                getUserLeaderboardBestWithRetry(row.opponent_username, row.game_id, row.leaderboard_id, apiKey, row.leaderboard_title, 6)
              ]);
              creatorStart = creatorBest?.scoreText ?? creatorStart;
              opponentStart = opponentBest?.scoreText ?? opponentStart;
              creatorCurrent = creatorBest?.scoreText ?? creatorCurrent;
              opponentCurrent = opponentBest?.scoreText ?? opponentCurrent;
              creatorCurrentValue = creatorBest?.scoreValue ?? creatorCurrentValue;
              opponentCurrentValue = opponentBest?.scoreValue ?? opponentCurrentValue;
              leaderboardFormat = creatorBest?.format ?? opponentBest?.format ?? leaderboardFormat;
              leaderboardLower = creatorBest?.lowerIsBetter ?? opponentBest?.lowerIsBetter ?? leaderboardLower;
              await pool.query(
                `
                  UPDATE challenges
                  SET creator_start_score = $1,
                      opponent_start_score = $2,
                      leaderboard_format = $3,
                      leaderboard_lower_is_better = $4
                  WHERE id = $5
                `,
                [creatorStart, opponentStart, leaderboardFormat, leaderboardLower, row.id]
              );
              if (creatorStart === null || opponentStart === null) {
                startScoreDebug = "Start score missing after fetch.";
              } else {
                startScoreDebug = "Start score fetched.";
              }
            } catch (err) {
              startScoreDebug = `Start score error: ${String(err?.message || "unknown")}`;
            }
          }
          if (row.challenge_type === "score" && row.game_id && row.leaderboard_id &&
              (creatorCurrent === null || opponentCurrent === null || creatorCurrentValue === null || opponentCurrentValue === null)) {
            try {
              const [creatorBest, opponentBest] = await Promise.all([
                getUserLeaderboardBestWithRetry(row.creator_username, row.game_id, row.leaderboard_id, apiKey, row.leaderboard_title, 6),
                getUserLeaderboardBestWithRetry(row.opponent_username, row.game_id, row.leaderboard_id, apiKey, row.leaderboard_title, 6)
              ]);
              creatorCurrent = creatorBest?.scoreText ?? creatorCurrent;
              opponentCurrent = opponentBest?.scoreText ?? opponentCurrent;
              creatorCurrentValue = creatorBest?.scoreValue ?? creatorCurrentValue;
              opponentCurrentValue = opponentBest?.scoreValue ?? opponentCurrentValue;
              await pool.query(
                `
                  UPDATE challenges
                  SET creator_current_score = COALESCE($1, creator_current_score),
                      opponent_current_score = COALESCE($2, opponent_current_score),
                      creator_current_score_value = COALESCE($3, creator_current_score_value),
                      opponent_current_score_value = COALESCE($4, opponent_current_score_value),
                      points_updated_at = NOW()
                  WHERE id = $5
                `,
                [creatorCurrent, opponentCurrent, creatorCurrentValue, opponentCurrentValue, row.id]
              );
            } catch (err) {
              startScoreDebug = startScoreDebug || `Current score error: ${String(err?.message || "unknown")}`;
            }
          }
          const creatorPoints = await computePointsBetweenWithRetry(row.creator_username, startAt, now, false, apiKey, 10);
          const opponentPoints = await computePointsBetweenWithRetry(row.opponent_username, startAt, now, false, apiKey, 10);
          await pool.query(
            `
              UPDATE challenges
              SET creator_points = $1,
                  opponent_points = $2,
                  points_updated_at = NOW()
              WHERE id = $3
            `,
            [creatorPoints, opponentPoints, row.id]
          );
          if (row.end_at && new Date(row.end_at) <= now) {
            let finalCreator = null;
            let finalOpponent = null;
            let finalCreatorValue = null;
            let finalOpponentValue = null;
            if (row.challenge_type === "score" && row.game_id && row.leaderboard_id && apiKey) {
              try {
                const [creatorBest, opponentBest] = await Promise.all([
                  getUserLeaderboardBestWithRetry(row.creator_username, row.game_id, row.leaderboard_id, apiKey, row.leaderboard_title, 6),
                  getUserLeaderboardBestWithRetry(row.opponent_username, row.game_id, row.leaderboard_id, apiKey, row.leaderboard_title, 6)
                ]);
                finalCreator = creatorBest?.scoreText ?? null;
                finalOpponent = opponentBest?.scoreText ?? null;
                finalCreatorValue = creatorBest?.scoreValue ?? null;
                finalOpponentValue = opponentBest?.scoreValue ?? null;
              } catch {
                // ignore final score failures
              }
            }
            await pool.query(
              `
                UPDATE challenges
                SET status = 'completed',
                    creator_final_score = COALESCE($2, creator_final_score),
                    opponent_final_score = COALESCE($3, opponent_final_score),
                    creator_final_score_value = COALESCE($4, creator_final_score_value),
                    opponent_final_score_value = COALESCE($5, opponent_final_score_value)
                WHERE id = $1 AND status = 'active'
              `,
              [row.id, finalCreator, finalOpponent, finalCreatorValue, finalOpponentValue]
            );
          }
          return {
            ...row,
            creator_points: creatorPoints,
            opponent_points: opponentPoints,
            creator_start_score: creatorStart,
            opponent_start_score: opponentStart,
            leaderboard_format: leaderboardFormat,
            leaderboard_lower_is_better: leaderboardLower,
            start_score_debug: startScoreDebug,
            creator_current_score: creatorCurrent,
            opponent_current_score: opponentCurrent,
            creator_current_score_value: creatorCurrentValue,
            opponent_current_score_value: opponentCurrentValue
          };
        } catch (err) {
          const msg = String(err?.message || "");
          if (msg.includes("429") || msg.includes("Too Many Attempts")) {
            warnings.push("Rate limited by RetroAchievements. Challenge totals may be delayed.");
          }
          return {
            ...row,
            creator_points: row.creator_points ?? null,
            opponent_points: row.opponent_points ?? null,
            creator_start_score: row.creator_start_score ?? null,
            opponent_start_score: row.opponent_start_score ?? null,
            leaderboard_format: row.leaderboard_format ?? null,
            leaderboard_lower_is_better: row.leaderboard_lower_is_better ?? null,
            start_score_debug: msg ? `Start score error: ${msg}` : "",
            creator_current_score: row.creator_current_score ?? null,
            opponent_current_score: row.opponent_current_score ?? null,
            creator_current_score_value: row.creator_current_score_value ?? null,
            opponent_current_score_value: row.opponent_current_score_value ?? null
          };
        }
      })));
    } else {
      activeRows = activeRows.map(row => ({
        ...row,
        creator_points: row.creator_points ?? null,
        opponent_points: row.opponent_points ?? null,
        creator_current_score: row.creator_current_score ?? null,
        opponent_current_score: row.opponent_current_score ?? null,
        creator_current_score_value: row.creator_current_score_value ?? null,
        opponent_current_score_value: row.opponent_current_score_value ?? null
      }));
    }

  res.json({
    incoming: incoming.rows,
    outgoing: outgoing.rows,
    active: activeRows,
    warnings
  });
});

app.post("/api/challenges", requireAuth, async (req, res) => {
  if (!pool) return res.status(500).json({ error: "Database unavailable" });
  const me = normalizeUsername(req.session.user.username);
  const opponent = normalizeUsername(req.body?.opponent);
  if (!opponent) return res.status(400).json({ error: "Missing opponent" });
  if (opponent === me) return res.status(400).json({ error: "Cannot challenge yourself" });

  const durationHours = Math.max(1, Math.min(168, Number(req.body?.hours || 24)));
  const challengeType = String(req.body?.type || "points").toLowerCase();
  const gameId = Number(req.body?.gameId || 0);
  const leaderboardId = Number(req.body?.leaderboardId || 0);
  const gameTitle = String(req.body?.gameTitle || "");
  const leaderboardTitle = String(req.body?.leaderboardTitle || "");
  const apiKey = requireApiKey(req, res);
  if (!apiKey) return;
  let leaderboardFormat = null;
  let leaderboardLower = null;
  let resolvedLeaderboardTitle = leaderboardTitle;

  if (challengeType === "score") {
    if (!Number.isFinite(gameId) || gameId <= 0) {
      return res.status(400).json({ error: "Missing game selection" });
    }
    if (!Number.isFinite(leaderboardId) || leaderboardId <= 0) {
      return res.status(400).json({ error: "Missing leaderboard selection" });
    }
    try {
      const boards = await raGetGameLeaderboards(gameId, 200, apiKey);
      const target = boards.find((lb) => Number(lb.ID ?? lb.id) === Number(leaderboardId));
      if (!target) {
        return res.status(400).json({ error: "Selected leaderboard not found." });
      }
      leaderboardFormat = target.Format ?? target.format ?? null;
      leaderboardLower = target.LowerIsBetter ?? target.lowerIsBetter ?? null;
      resolvedLeaderboardTitle = target.Title ?? target.title ?? leaderboardTitle;
    } catch (err) {
      return res.status(502).json({ error: String(err?.message || "Failed to validate leaderboard") });
    }
  }

  try {
    await raGetUserSummary(opponent, apiKey);
  } catch (err) {
    const msg = String(err?.message || "");
    const notFound = msg.includes("404") || msg.toLowerCase().includes("not found");
    if (notFound) return res.status(404).json({ error: `User not found: ${opponent}` });
    return res.status(502).json({ error: `Unable to verify user: ${opponent}` });
  }

  const result = await pool.query(
    `
      INSERT INTO challenges (
        creator_username,
        opponent_username,
        duration_hours,
        challenge_type,
        game_id,
        leaderboard_id,
        game_title,
        leaderboard_title,
        leaderboard_format,
        leaderboard_lower_is_better
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
      RETURNING id, creator_username, opponent_username, duration_hours, status,
                created_at, accepted_at, start_at, end_at,
                challenge_type, game_id, leaderboard_id, game_title, leaderboard_title,
                leaderboard_format, leaderboard_lower_is_better
    `,
    [
      me,
      opponent,
      durationHours,
      challengeType,
      gameId || null,
      leaderboardId || null,
      gameTitle || null,
      resolvedLeaderboardTitle || null,
      leaderboardFormat,
      leaderboardLower
    ]
  );
  await createNotification({
    username: opponent,
    type: "challenge_pending",
    message: `${req.session.user.username} sent you a challenge.`,
    meta: { from: req.session.user.username, challengeId: result.rows[0]?.id || null }
  });
  res.json(result.rows[0]);
});

app.post("/api/challenges/:id/accept", requireAuth, async (req, res) => {
  if (!pool) return res.status(500).json({ error: "Database unavailable" });
  const me = normalizeUsername(req.session.user.username);
  const id = Number(req.params.id);
  if (!Number.isFinite(id)) return res.status(400).json({ error: "Invalid challenge id" });
  const apiKey = requireApiKey(req, res);
  if (!apiKey) return;

  const existing = await pool.query(
    `
      SELECT id, creator_username, opponent_username, duration_hours, status,
             challenge_type, game_id, leaderboard_id, leaderboard_title
      FROM challenges
      WHERE id = $1 AND opponent_username = $2 AND status = 'pending'
    `,
    [id, me]
  );
  if (!existing.rows.length) {
    return res.status(404).json({ error: "Challenge not found or not pending" });
  }

  const row = existing.rows[0];
  let creatorStart = null;
  let opponentStart = null;
  let leaderboardFormat = null;
  let leaderboardLower = null;

  if (row.challenge_type === "score" && row.game_id && row.leaderboard_id && apiKey) {
    try {
      const [creatorBest, opponentBest] = await Promise.all([
        getUserLeaderboardBest(row.creator_username, row.game_id, row.leaderboard_id, apiKey, row.leaderboard_title),
        getUserLeaderboardBest(row.opponent_username, row.game_id, row.leaderboard_id, apiKey, row.leaderboard_title)
      ]);
      creatorStart = creatorBest?.scoreText ?? null;
      opponentStart = opponentBest?.scoreText ?? null;
      leaderboardFormat = creatorBest?.format ?? opponentBest?.format ?? null;
      leaderboardLower = creatorBest?.lowerIsBetter ?? opponentBest?.lowerIsBetter ?? null;
    } catch {
      // ignore start score failures
    }
  }

  const result = await pool.query(
    `
      UPDATE challenges
      SET status = 'active',
          accepted_at = NOW(),
          start_at = NOW(),
          end_at = NOW() + (duration_hours || ' hours')::interval,
          creator_start_score = $1,
          opponent_start_score = $2,
          leaderboard_format = $3,
          leaderboard_lower_is_better = $4
      WHERE id = $5 AND opponent_username = $6 AND status = 'pending'
      RETURNING id, creator_username, opponent_username, duration_hours, status,
                created_at, accepted_at, start_at, end_at,
                challenge_type, game_id, leaderboard_id, game_title, leaderboard_title,
                creator_start_score, opponent_start_score, leaderboard_format, leaderboard_lower_is_better
    `,
    [creatorStart, opponentStart, leaderboardFormat, leaderboardLower, id, me]
  );

  if (result.rows.length === 0) {
    return res.status(404).json({ error: "Challenge not found or not pending" });
  }
  await deleteChallengeNotifications(me, id);
  res.json(result.rows[0]);
});

app.post("/api/challenges/:id/decline", requireAuth, async (req, res) => {
  if (!pool) return res.status(500).json({ error: "Database unavailable" });
  const me = normalizeUsername(req.session.user.username);
  const id = Number(req.params.id);
  if (!Number.isFinite(id)) return res.status(400).json({ error: "Invalid challenge id" });

  const result = await pool.query(
    `
      UPDATE challenges
      SET status = 'declined'
      WHERE id = $1 AND opponent_username = $2 AND status = 'pending'
      RETURNING id
    `,
    [id, me]
  );

  if (result.rows.length === 0) {
    return res.status(404).json({ error: "Challenge not found or not pending" });
  }
  await deleteChallengeNotifications(me, id);
  res.json({ ok: true });
});

app.post("/api/challenges/:id/cancel", requireAuth, async (req, res) => {
  if (!pool) return res.status(500).json({ error: "Database unavailable" });
  const me = normalizeUsername(req.session.user.username);
  const id = Number(req.params.id);
  if (!Number.isFinite(id)) return res.status(400).json({ error: "Invalid challenge id" });

  const result = await pool.query(
    `
      UPDATE challenges
      SET status = 'cancelled'
      WHERE id = $1
        AND status IN ('pending', 'active')
        AND (creator_username = $2 OR opponent_username = $2)
      RETURNING id
    `,
    [id, me]
  );

  if (result.rows.length === 0) {
    return res.status(404).json({ error: "Challenge not found or not active" });
  }
  await deleteChallengeNotifications(me, id);
  res.json({ ok: true });
});

// Notifications
// --- Notifications ---
app.get("/api/notifications", requireAuth, async (req, res) => {
  if (!pool) return res.status(500).json({ error: "Database unavailable" });
  const username = normalizeUsername(req.session.user.username);
  const unreadOnly = String(req.query.unread || "").toLowerCase() === "true" || req.query.unread === "1";
  const limitQ = typeof req.query.limit === "string" ? Number(req.query.limit) : 50;
  const limit = Math.max(1, Math.min(100, Number.isFinite(limitQ) ? limitQ : 50));
  const where = unreadOnly ? "AND read_at IS NULL" : "";
  const rows = await pool.query(
    `
      SELECT id, type, message, meta, created_at, read_at
      FROM notifications
      WHERE username = $1 ${where}
      ORDER BY created_at DESC
      LIMIT $2
    `,
    [username, limit]
  );
  const countRes = await pool.query(
    "SELECT COUNT(*) FROM notifications WHERE username = $1 AND read_at IS NULL",
    [username]
  );
  res.json({
    results: rows.rows,
    unreadCount: Number(countRes.rows[0]?.count || 0)
  });
});

app.post("/api/notifications/mark-read", requireAuth, async (req, res) => {
  if (!pool) return res.status(500).json({ error: "Database unavailable" });
  const username = normalizeUsername(req.session.user.username);
  const idsRaw = Array.isArray(req.body?.ids) ? req.body.ids : [];
  const ids = idsRaw.map(Number).filter(Number.isFinite);
  if (ids.length) {
    await pool.query(
      `UPDATE notifications SET read_at = NOW() WHERE username = $1 AND id = ANY($2)`,
      [username, ids]
    );
  } else {
    await pool.query(
      `UPDATE notifications SET read_at = NOW() WHERE username = $1 AND read_at IS NULL`,
      [username]
    );
  }
  await broadcastUnreadCount(username);
  res.json({ ok: true });
});

app.delete("/api/notifications/:id", requireAuth, async (req, res) => {
  if (!pool) return res.status(500).json({ error: "Database unavailable" });
  const username = normalizeUsername(req.session.user.username);
  const id = Number(req.params.id);
  if (!Number.isFinite(id)) return res.status(400).json({ error: "Invalid notification id" });
  await pool.query(
    "DELETE FROM notifications WHERE username = $1 AND id = $2",
    [username, id]
  );
  await broadcastUnreadCount(username);
  res.json({ ok: true });
});

app.get("/api/notifications/stream", requireAuth, async (req, res) => {
  if (!pool) return res.status(500).json({ error: "Database unavailable" });
  const username = normalizeUsername(req.session.user.username);
  res.set({
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive"
  });
  res.flushHeaders();

  const set = notificationStreams.get(username) || new Set();
  set.add(res);
  notificationStreams.set(username, set);

  const count = await getUnreadNotificationCount(username);
  res.write(`data: ${JSON.stringify({ unreadCount: count })}\n\n`);

  const ping = setInterval(() => {
    res.write("event: ping\ndata: {}\n\n");
  }, 15000);

  req.on("close", () => {
    clearInterval(ping);
    const existing = notificationStreams.get(username);
    if (existing) {
      existing.delete(res);
      if (!existing.size) notificationStreams.delete(username);
    }
  });
});

app.get("/api/challenges-history", requireAuth, async (req, res) => {
  if (!pool) return res.status(500).json({ error: "Database unavailable" });
  const me = normalizeUsername(req.session.user.username);
    const result = await pool.query(
      `
        SELECT id, creator_username, opponent_username, duration_hours, status,
               created_at, accepted_at, start_at, end_at,
               creator_points, opponent_points, points_updated_at,
               challenge_type, game_id, leaderboard_id, game_title, leaderboard_title,
               creator_start_score, opponent_start_score, leaderboard_format, leaderboard_lower_is_better,
               creator_final_score, opponent_final_score, creator_final_score_value, opponent_final_score_value
        FROM challenges
        WHERE status = 'completed' AND (creator_username = $1 OR opponent_username = $1)
        ORDER BY end_at DESC
      `,
      [me]
    );
  const rows = result.rows.map(withChallengeWinner);
  res.json({ results: rows });
});

// Challenge totals snapshot (intended for cron)
app.post("/api/challenges-snapshot", requireSnapshotSecret, async (_req, res) => {
  try {
    if (!pool) return res.status(500).json({ error: "Database unavailable" });
    if (!RA_API_KEY) return res.status(400).json({ error: "Missing RA API key" });

      const active = await pool.query(
        `
          SELECT id, creator_username, opponent_username, start_at, end_at,
                 challenge_type, game_id, leaderboard_id, leaderboard_title
          FROM challenges
          WHERE status = 'active' AND start_at IS NOT NULL
        `
      );
    if (!active.rows.length) return res.json({ ok: true, count: 0 });

    const limitChallenges = createLimiter(1);
    const now = new Date();
    const skipped = [];
    await Promise.all(active.rows.map((row) => limitChallenges(async () => {
      try {
        const startAt = new Date(row.start_at);
        const creatorPoints = await computePointsBetweenWithRetry(row.creator_username, startAt, now, false, RA_API_KEY, 10);
        const opponentPoints = await computePointsBetweenWithRetry(row.opponent_username, startAt, now, false, RA_API_KEY, 10);
        await pool.query(
          `
            UPDATE challenges
            SET creator_points = $1,
                opponent_points = $2,
                points_updated_at = NOW()
            WHERE id = $3
          `,
          [creatorPoints, opponentPoints, row.id]
        );
          if (row.end_at && new Date(row.end_at) <= now) {
            let finalCreator = null;
            let finalOpponent = null;
            let finalCreatorValue = null;
            let finalOpponentValue = null;
            if (row.challenge_type === "score" && row.game_id && row.leaderboard_id) {
              try {
                const [creatorBest, opponentBest] = await Promise.all([
                  getUserLeaderboardBestWithRetry(row.creator_username, row.game_id, row.leaderboard_id, RA_API_KEY, row.leaderboard_title, 6),
                  getUserLeaderboardBestWithRetry(row.opponent_username, row.game_id, row.leaderboard_id, RA_API_KEY, row.leaderboard_title, 6)
                ]);
                finalCreator = creatorBest?.scoreText ?? null;
                finalOpponent = opponentBest?.scoreText ?? null;
                finalCreatorValue = creatorBest?.scoreValue ?? null;
                finalOpponentValue = opponentBest?.scoreValue ?? null;
              } catch {
                // ignore final score failures
              }
            }
            await pool.query(
              `
                UPDATE challenges
                SET status = 'completed',
                    creator_final_score = COALESCE($2, creator_final_score),
                    opponent_final_score = COALESCE($3, opponent_final_score),
                    creator_final_score_value = COALESCE($4, creator_final_score_value),
                    opponent_final_score_value = COALESCE($5, opponent_final_score_value)
                WHERE id = $1 AND status = 'active'
              `,
              [row.id, finalCreator, finalOpponent, finalCreatorValue, finalOpponentValue]
            );
          }
        } catch {
          skipped.push(row.id);
        }
      })));

    if (skipped.length) {
      console.warn(`Challenge snapshot skipped ${skipped.length} challenge(s): ${skipped.join(", ")}`);
    }
    res.json({ ok: true, count: active.rows.length, skipped });
  } catch (err) {
    res.status(500).json({ error: err?.message || "Failed to snapshot challenges" });
  }
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
    const apiKey = requireApiKey(req, res);
    if (!apiKey) return;

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

    let level = null;
    if (pool) {
      const normalized = normalizeUsername(username);
      const stored = await pool.query("SELECT level FROM users WHERE username = $1", [normalized]);
      if (stored.rows.length) {
        const val = stored.rows[0].level;
        if (val !== null && val !== undefined) level = Number(val);
      }
      if (level === null) {
        try {
          const summary = await raGetUserSummary(username, apiKey);
          const user =
            (summary && typeof summary.User === "object" ? summary.User : null) ||
            (summary && typeof summary.user === "object" ? summary.user : null) ||
            summary;
          const totalPoints =
            user?.TotalPoints ?? summary?.TotalPoints ?? user?.totalPoints ?? summary?.totalPoints ??
            user?.Points ?? summary?.Points ?? user?.points ?? summary?.points;
          const saved = await upsertUserLevel(username, totalPoints);
          level = saved?.level ?? computeLevelFromPoints(totalPoints);
        } catch {
          level = null;
        }
      }
    }

    res.json({
      username,
      mode: includeSoftcore ? "all" : "hc",
      fromDate: fromDate.toISOString(),
      toDate: toDate.toISOString(),
      retroPoints,
      points,
      unlockCount: considered.length,
      unlockCountAll: unlocks.length,
      level
    });
    } catch (err) {
      const status = err?.status || 500;
      res.status(status).json({ error: err?.message || "Failed to fetch monthly data" });
    }
  });

// Daily points gained (today)
app.get("/api/daily/:username", async (req, res) => {
  try {
    const rawUsername = String(req.params.username || "").trim();
    if (!rawUsername) return res.status(400).json({ error: "Missing username" });
    const username = normalizeUsername(rawUsername);
    const apiKey = requireApiKey(req, res);
    if (!apiKey) return;

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
      const status = err?.status || 500;
      res.status(status).json({ error: err?.message || "Failed to fetch daily data" });
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

// Hourly points history from DB (default last 24 hours)
app.get("/api/hourly-history", async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ error: "Database unavailable" });
    const usersParam = typeof req.query.users === "string" ? req.query.users : "";
    const users = usersParam
      .split(",")
      .map(normalizeUsername)
      .filter(Boolean);
    if (!users.length) return res.status(400).json({ error: "Missing users" });

    const hoursParam = typeof req.query.hours === "string" ? Number(req.query.hours) : 24;
    const hours = Math.max(1, Math.min(72, Number.isFinite(hoursParam) ? hoursParam : 24));
    const modeQ = typeof req.query.mode === "string" ? req.query.mode.toLowerCase() : "hc";
    const mode = modeQ === "all" ? "all" : "hc";

    const start = new Date();
    start.setMinutes(0, 0, 0);
    start.setHours(start.getHours() - (hours - 1));

    const result = await pool.query(
      `
        SELECT username, hour_start, points
        FROM hourly_points
        WHERE username = ANY($1) AND mode = $2 AND hour_start >= $3
        ORDER BY hour_start ASC
      `,
      [users, mode, start.toISOString()]
    );

    const map = {};
    for (const row of result.rows) {
      const user = row.username;
      const hourKey = new Date(row.hour_start).toISOString();
      if (!map[user]) map[user] = {};
      map[user][hourKey] = Number(row.points || 0);
    }

    res.json({ hours, mode, results: map });
  } catch (err) {
    res.status(500).json({ error: err?.message || "Failed to fetch hourly history" });
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
    const limitDaily = createLimiter(1);

    const skipped = [];
    await Promise.all(usernames.map((username) => limitDaily(async () => {
      try {
        const { points } = await computeDailyPointsWithRetry(username, includeSoftcore, RA_API_KEY, 10);
        await upsertDailyPoints(username, dayKey, mode, points);
      } catch {
        skipped.push(username);
      }
    })));

    if (skipped.length) {
      console.warn(`Daily snapshot skipped ${skipped.length} user(s): ${skipped.join(", ")}`);
    }
    res.json({ ok: true, count: usernames.length, skipped });
  } catch (err) {
    res.status(500).json({ error: err?.message || "Failed to snapshot daily points" });
  }
});

// Hourly points snapshot for all known users (intended for cron)
app.post("/api/hourly-snapshot", requireSnapshotSecret, async (req, res) => {
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
    const hourStart = getHourRange().start;
    const limitHourly = createLimiter(1);

    const skipped = [];
    await Promise.all(usernames.map((username) => limitHourly(async () => {
      try {
        const { points } = await computeHourlyPointsWithRetry(username, includeSoftcore, RA_API_KEY, 10);
        await upsertHourlyPoints(username, hourStart.toISOString(), mode, points);
      } catch {
        skipped.push(username);
      }
    })));

    if (skipped.length) {
      console.warn(`Hourly snapshot skipped ${skipped.length} user(s): ${skipped.join(", ")}`);
    }
    res.json({ ok: true, count: usernames.length, skipped });
  } catch (err) {
    res.status(500).json({ error: err?.message || "Failed to snapshot hourly points" });
  }
});

// Recent achievements for a user
app.get("/api/recent-achievements/:username", async (req, res) => {
  try {
    const username = String(req.params.username || "").trim();
    if (!username) return res.status(400).json({ error: "Missing username" });
    const apiKey = requireApiKey(req, res);
    if (!apiKey) return;

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
      const status = err?.status || 500;
      res.status(status).json({ error: err?.message || "Failed to fetch recent achievements" });
    }
  });

// Recent leaderboard "times/scores" for a user (derived from recently played games + user game leaderboards)
app.get("/api/recent-times/:username", async (req, res) => {
  try {
    const username = String(req.params.username || "").trim();
    if (!username) return res.status(400).json({ error: "Missing username" });
    const apiKey = requireApiKey(req, res);
    if (!apiKey) return;

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
      const status = err?.status || 500;
      res.status(status).json({ error: err?.message || "Failed to fetch recent times" });
    }
  });

// Recent games for a user (from recently played games)
app.get("/api/recent-games/:username", async (req, res) => {
  try {
    const username = String(req.params.username || "").trim();
    if (!username) return res.status(400).json({ error: "Missing username" });
    const apiKey = requireApiKey(req, res);
    if (!apiKey) return;

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
      consoleId: g.ConsoleID ?? g.consoleId,
      consoleName: g.ConsoleName ?? g.consoleName,
      imageIcon: g.ImageIcon ?? g.imageIcon,
      lastPlayed: g.LastPlayed ?? g.lastPlayed
    }));

    const payload = { username, count: normalized.length, results: normalized };
    cacheSet(cacheKey, payload, DEFAULT_CACHE_TTL_MS);
    res.json(payload);
    } catch (err) {
      const status = err?.status || 500;
      res.status(status).json({ error: err?.message || "Failed to fetch recent games" });
    }
  });

// User summary/profile
app.get("/api/user-summary/:username", async (req, res) => {
  try {
    const username = String(req.params.username || "").trim();
    if (!username) return res.status(400).json({ error: "Missing username" });
    const apiKey = requireApiKey(req, res);
    if (!apiKey) return;

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
      completedGames: user?.CompletedGames ?? data?.CompletedGames ?? user?.completedGames ?? data?.completedGames,
      userPic:
        user?.UserPic ?? data?.UserPic ?? user?.userPic ?? data?.userPic ??
        user?.UserPicURL ?? data?.UserPicURL ?? user?.userPicUrl ?? data?.userPicUrl
    };

    const levelRow = await upsertUserLevel(username, payload.totalPoints);
    payload.level = levelRow?.level ?? computeLevelFromPoints(payload.totalPoints);

    cacheSet(cacheKey, payload, DEFAULT_CACHE_TTL_MS);
    res.json(payload);
  } catch (err) {
    const status = err?.status || 500;
    res.status(status).json({ error: err?.message || "Failed to fetch user summary" });
  }
});

// User levels from DB only
app.get("/api/user-levels", async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ error: "Database unavailable" });
    const usersParam = typeof req.query.users === "string" ? req.query.users : "";
    const users = usersParam
      .split(",")
      .map(normalizeUsername)
      .filter(Boolean);
    if (!users.length) return res.status(400).json({ error: "Missing users" });

    const result = await pool.query(
      "SELECT username, level FROM users WHERE username = ANY($1)",
      [users]
    );
    res.json({ results: result.rows.map(r => ({ username: r.username, level: r.level })) });
  } catch (err) {
    const status = err?.status || 500;
    res.status(status).json({ error: err?.message || "Failed to fetch user levels" });
  }
});

// User completion progress (for beaten/mastered badges)
app.get("/api/user-completion-progress/:username", async (req, res) => {
  try {
    const username = String(req.params.username || "").trim();
    if (!username) return res.status(400).json({ error: "Missing username" });
    const apiKey = requireApiKey(req, res);
    if (!apiKey) return;

    const countQ = typeof req.query.count === "string" ? Number(req.query.count) : 100;
    const offsetQ = typeof req.query.offset === "string" ? Number(req.query.offset) : 0;
    const count = Math.max(1, Math.min(500, Number.isFinite(countQ) ? countQ : 100));
    const offset = Math.max(0, Number.isFinite(offsetQ) ? offsetQ : 0);

    const cacheKey = `user-completion:${username}:${count}:${offset}:${apiKey}`;
    const cached = cacheGet(cacheKey);
    if (cached) return res.json({ ...cached, cached: true });

    const data = await raGetUserCompletionProgress(username, count, offset, apiKey);
    const rawResults = data?.Results ?? data?.results ?? data ?? [];
    const results = Array.isArray(rawResults) ? rawResults : [];

    const normalized = results.map(r => ({
      gameId: r.GameID ?? r.gameId,
      title: r.Title ?? r.title,
      imageIcon: r.ImageIcon ?? r.imageIcon,
      consoleId: r.ConsoleID ?? r.consoleId,
      consoleName: r.ConsoleName ?? r.consoleName,
      maxPossible: r.MaxPossible ?? r.maxPossible,
      numAwarded: r.NumAwarded ?? r.numAwarded,
      numAwardedHardcore: r.NumAwardedHardcore ?? r.numAwardedHardcore,
      mostRecentAwardedDate: r.MostRecentAwardedDate ?? r.mostRecentAwardedDate,
      highestAwardKind: r.HighestAwardKind ?? r.highestAwardKind,
      highestAwardDate: r.HighestAwardDate ?? r.highestAwardDate
    }));

    const completionCandidates = normalized
      .map((row) => ({
        gameId: row.gameId,
        title: row.title,
        imageIcon: row.imageIcon,
        awardKind: completionAwardKind(row.highestAwardKind)
      }))
      .filter((row) => row.gameId && row.awardKind);

    if (completionCandidates.length) {
      const unique = new Map();
      completionCandidates.forEach((row) => {
        const key = `${row.gameId}:${row.awardKind}`;
        if (!unique.has(key)) unique.set(key, row);
      });
      const list = Array.from(unique.values()).slice(0, 30);
      for (const row of list) {
        try {
          await recordCompletionSocialPost({
            username,
            gameId: row.gameId,
            gameTitle: row.title,
            imageIcon: row.imageIcon,
            awardKind: row.awardKind
          });
        } catch (err) {
          // ignore social auto-post failures
        }
      }
    }

    const payload = {
      username,
      count: normalized.length,
      total: data?.Total ?? data?.total ?? normalized.length,
      results: normalized
    };

    cacheSet(cacheKey, payload, DEFAULT_CACHE_TTL_MS);
    res.json(payload);
  } catch (err) {
    const status = err?.status || 500;
    res.status(status).json({ error: err?.message || "Failed to fetch completion progress" });
  }
});

// Social posts (friends-only read)
// --- Social ---
app.get("/api/social/posts", requireAuth, async (req, res) => {
  try {
    if (!pool) return res.json({ count: 0, results: [] });
    const userId = req.session?.user?.id;
    const username = req.session?.user?.username;
    if (!userId || !username) return res.status(401).json({ error: "Not authenticated" });
    const limitQ = typeof req.query.limit === "string" ? Number(req.query.limit) : SOCIAL_MAX_POSTS;
    const offsetQ = typeof req.query.offset === "string" ? Number(req.query.offset) : 0;
    const limit = Math.max(1, Math.min(SOCIAL_MAX_POSTS, Number.isFinite(limitQ) ? limitQ : SOCIAL_MAX_POSTS));
    const offset = Math.max(0, Number.isFinite(offsetQ) ? offsetQ : 0);
    const userFilter = typeof req.query.user === "string" ? normalizeUsername(req.query.user) : "";
    const friendsRes = await pool.query(
      `SELECT friend_username FROM friends WHERE user_id = $1`,
      [userId]
    );
    const friendUsernames = friendsRes.rows.map(r => normalizeUsername(r.friend_username));
    const allowed = [normalizeUsername(username), ...friendUsernames];
    if (userFilter && !allowed.map(u => u.toLowerCase()).includes(userFilter.toLowerCase())) {
      return res.status(403).json({ error: "Not authorized to view this user's posts." });
    }
    const allowedList = userFilter ? [userFilter] : allowed;
    const postsRes = await pool.query(
      `
        SELECT id, username, game_title, caption, image_data, image_url, is_auto, post_type, achievement_title, achievement_id, achievement_description, created_at
        FROM social_posts
        WHERE LOWER(username) = ANY($1)
        ORDER BY created_at DESC
        LIMIT $2
        OFFSET $3
      `,
      [allowedList, limit + 1, offset]
    );
    const rawRows = postsRes.rows;
    const hasMore = rawRows.length > limit;
    const rows = hasMore ? rawRows.slice(0, limit) : rawRows;
    const posts = rows.map((row) => ({
      id: row.id,
      user: row.username,
      game: row.game_title || "",
      caption: row.caption || "",
      imageData: row.image_data,
      imageUrl: row.image_url,
      isAuto: row.is_auto,
      postType: row.post_type,
      achievementTitle: row.achievement_title,
      achievementId: row.achievement_id,
      achievementDescription: row.achievement_description,
      createdAt: row.created_at,
      reactions: { likes: 0, dislikes: 0, userReaction: null },
      comments: []
    }));

    if (!posts.length) {
      return res.json({ count: 0, results: [] });
    }

    const postIds = posts.map(p => p.id);
    const reactionsRes = await pool.query(
      `
        SELECT post_id, reaction, username
        FROM social_reactions
        WHERE post_id = ANY($1)
      `,
      [postIds]
    );
    const reactionsByPost = new Map();
    reactionsRes.rows.forEach((row) => {
      if (!reactionsByPost.has(row.post_id)) {
        reactionsByPost.set(row.post_id, { likes: 0, dislikes: 0, userReaction: null });
      }
      const bucket = reactionsByPost.get(row.post_id);
      if (row.reaction === "like") bucket.likes += 1;
      if (row.reaction === "dislike") bucket.dislikes += 1;
      if (normalizeUsername(row.username) === normalizeUsername(username)) {
        bucket.userReaction = row.reaction;
      }
    });
    const commentsRes = await pool.query(
      `
        SELECT id, post_id, username, body, created_at
        FROM social_comments
        WHERE post_id = ANY($1)
        ORDER BY created_at ASC
      `,
      [postIds]
    );
    const byPost = new Map();
    commentsRes.rows.forEach((row) => {
      if (!byPost.has(row.post_id)) byPost.set(row.post_id, []);
      byPost.get(row.post_id).push({
        id: row.id,
        user: row.username,
        text: row.body,
        createdAt: row.created_at
      });
    });
    posts.forEach((post) => {
      post.comments = byPost.get(post.id) || [];
      post.reactions = reactionsByPost.get(post.id) || { likes: 0, dislikes: 0, userReaction: null };
    });

    res.json({ count: posts.length, offset, hasMore, results: posts });
  } catch (err) {
    const status = err?.status || 500;
    res.status(status).json({ error: err?.message || "Failed to fetch social posts" });
  }
});

// Create social post (auth required)
app.post("/api/social/posts", requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ error: "Database unavailable" });
    const userId = req.session?.user?.id;
    const username = req.session?.user?.username;
    if (!userId || !username) return res.status(401).json({ error: "Not authenticated" });

    const postType = String(req.body?.postType || "text").trim().toLowerCase();
    const caption = String(req.body?.caption || "").trim();
    const gameTitle = String(req.body?.game || "").trim();
    const imageData = String(req.body?.imageData || "").trim();
    const imageUrl = String(req.body?.imageUrl || "").trim();
    const achievementTitle = String(req.body?.achievementTitle || "").trim();
    const achievementIdRaw = req.body?.achievementId;
    const achievementDescription = String(req.body?.achievementDescription || "").trim();
    const achievementId = Number.isFinite(Number(achievementIdRaw)) ? Number(achievementIdRaw) : null;

    if (!["text", "screenshot", "achievement"].includes(postType)) {
      return res.status(400).json({ error: "Invalid post type" });
    }
    if (postType === "screenshot") {
      if (!imageData || !imageData.startsWith("data:image/")) {
        return res.status(400).json({ error: "Invalid image data" });
      }
      const sizeBytes = estimateDataUrlBytes(imageData);
      if (sizeBytes <= 0 || sizeBytes > SOCIAL_MAX_IMAGE_BYTES) {
        return res.status(400).json({ error: "Image too large (max 2MB)" });
      }
    } else if (postType === "text") {
      if (!caption) return res.status(400).json({ error: "Text post requires content" });
    } else if (postType === "achievement") {
      if (!achievementTitle || !gameTitle || !imageUrl) {
        return res.status(400).json({ error: "Achievement post missing details" });
      }
    }

    const insertRes = await pool.query(
      `
        INSERT INTO social_posts
          (user_id, username, game_title, caption, image_data, image_url, is_auto, post_type, achievement_title, achievement_id, achievement_description, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, false, $7, $8, $9, $10, NOW())
        RETURNING id, created_at
      `,
      [
        userId,
        username,
        gameTitle || null,
        caption || null,
        imageData,
        imageUrl || null,
        postType,
        achievementTitle || null,
        achievementId,
        achievementDescription || null
      ]
    );

    res.json({
      id: insertRes.rows[0].id,
      user: username,
      game: gameTitle,
      caption,
      imageData,
      imageUrl: imageUrl || null,
      isAuto: false,
      postType,
      achievementTitle: achievementTitle || null,
      achievementId,
      achievementDescription: achievementDescription || null,
      createdAt: insertRes.rows[0].created_at,
      comments: []
    });

    await notifyFriendsOfSocialPost({
      authorUsername: username,
      displayName: username,
      postId: insertRes.rows[0].id,
      postType,
      gameTitle
    });
  } catch (err) {
    const status = err?.status || 500;
    res.status(status).json({ error: err?.message || "Failed to create social post" });
  }
});

// Add social comment (auth required)
app.post("/api/social/posts/:id/comments", requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ error: "Database unavailable" });
    const userId = req.session?.user?.id;
    const username = req.session?.user?.username;
    if (!userId || !username) return res.status(401).json({ error: "Not authenticated" });
    const postId = Number(req.params.id);
    if (!Number.isFinite(postId)) return res.status(400).json({ error: "Invalid post id" });
    const text = String(req.body?.text || "").trim();
    if (!text) return res.status(400).json({ error: "Missing comment text" });

    const postRes = await pool.query(`SELECT id FROM social_posts WHERE id = $1`, [postId]);
    if (!postRes.rows.length) return res.status(404).json({ error: "Post not found" });

    const insertRes = await pool.query(
      `
        INSERT INTO social_comments (post_id, user_id, username, body, created_at)
        VALUES ($1, $2, $3, $4, NOW())
        RETURNING id, created_at
      `,
      [postId, userId, username, text]
    );

  res.json({
    id: insertRes.rows[0].id,
    postId,
    user: username,
    text,
    createdAt: insertRes.rows[0].created_at
  });
} catch (err) {
  const status = err?.status || 500;
  res.status(status).json({ error: err?.message || "Failed to add comment" });
}
});

// Delete social post (auth required)
app.delete("/api/social/posts/:id", requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ error: "Database unavailable" });
    const userId = req.session?.user?.id;
    const username = req.session?.user?.username;
    if (!userId || !username) return res.status(401).json({ error: "Not authenticated" });
    const postId = Number(req.params.id);
    if (!Number.isFinite(postId)) return res.status(400).json({ error: "Invalid post id" });
    const result = await pool.query(
      `
        DELETE FROM social_posts
        WHERE id = $1 AND (user_id = $2 OR LOWER(username) = LOWER($3))
        RETURNING id
      `,
      [postId, userId, username]
    );
    if (!result.rows.length) return res.status(404).json({ error: "Post not found" });
    res.json({ ok: true });
  } catch (err) {
    const status = err?.status || 500;
    res.status(status).json({ error: err?.message || "Failed to delete post" });
  }
});

app.post("/api/social/posts/:id/reaction", requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ error: "Database unavailable" });
    const userId = req.session?.user?.id;
    const username = req.session?.user?.username;
    if (!userId || !username) return res.status(401).json({ error: "Not authenticated" });
    const postId = Number(req.params.id);
    if (!Number.isFinite(postId)) return res.status(400).json({ error: "Invalid post id" });
    const reactionRaw = String(req.body?.reaction || "").trim().toLowerCase();
    if (!["like", "dislike", "none"].includes(reactionRaw)) {
      return res.status(400).json({ error: "Invalid reaction" });
    }
    if (reactionRaw === "none") {
      await pool.query(
        "DELETE FROM social_reactions WHERE post_id = $1 AND username = $2",
        [postId, username]
      );
    } else {
      await pool.query(
        `
          INSERT INTO social_reactions (post_id, user_id, username, reaction, created_at)
          VALUES ($1, $2, $3, $4, NOW())
          ON CONFLICT (post_id, username)
          DO UPDATE SET reaction = EXCLUDED.reaction, created_at = NOW()
        `,
        [postId, userId, username, reactionRaw]
      );
    }
    const counts = await pool.query(
      `
        SELECT
          SUM(CASE WHEN reaction = 'like' THEN 1 ELSE 0 END) AS likes,
          SUM(CASE WHEN reaction = 'dislike' THEN 1 ELSE 0 END) AS dislikes
        FROM social_reactions
        WHERE post_id = $1
      `,
      [postId]
    );
    res.json({
      postId,
      likes: Number(counts.rows[0]?.likes || 0),
      dislikes: Number(counts.rows[0]?.dislikes || 0),
      userReaction: reactionRaw === "none" ? null : reactionRaw
    });
  } catch (err) {
    const status = err?.status || 500;
    res.status(status).json({ error: err?.message || "Failed to update reaction" });
  }
});

// Backlog for a user (public read)
// --- Backlog ---
app.get("/api/backlog/:username", async (req, res) => {
  try {
    if (!pool) return res.json({ username: "", count: 0, results: [] });
    const username = String(req.params.username || "").trim();
    if (!username) return res.status(400).json({ error: "Missing username" });
    const normalized = normalizeUsername(username);
    const userRes = await pool.query(`SELECT id, username FROM users WHERE LOWER(username) = LOWER($1)`, [normalized]);
    if (!userRes.rows.length) {
      return res.json({ username, count: 0, results: [] });
    }
    const userId = userRes.rows[0].id;
    const itemsRes = await pool.query(
      `
        SELECT game_id, title, console_name, image_icon, num_achievements, points,
               started_awarded, started_total, started_checked_at, added_at
        FROM backlog_items
        WHERE user_id = $1
        ORDER BY added_at DESC
      `,
      [userId]
    );
    const results = itemsRes.rows.map(r => ({
      gameId: r.game_id,
      title: r.title,
      consoleName: r.console_name,
      imageIcon: r.image_icon,
      numAchievements: r.num_achievements,
      points: r.points,
      startedAwarded: r.started_awarded,
      startedTotal: r.started_total,
      startedCheckedAt: r.started_checked_at,
      addedAt: r.added_at
    }));
    res.json({ username: userRes.rows[0].username, count: results.length, results });
  } catch (err) {
    const status = err?.status || 500;
    res.status(status).json({ error: err?.message || "Failed to fetch backlog" });
  }
});

// Add backlog item (auth required)
app.post("/api/backlog", requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ error: "Database unavailable" });
    const userId = req.session?.user?.id;
    if (!userId) return res.status(401).json({ error: "Not authenticated" });

    const {
      gameId,
      title,
      consoleName = "",
      imageIcon = "",
      numAchievements = 0,
      points = 0
    } = req.body || {};

    if (!gameId || !title) {
      return res.status(400).json({ error: "Missing gameId or title" });
    }

    await pool.query(
      `
        INSERT INTO backlog_items
          (user_id, game_id, title, console_name, image_icon, num_achievements, points,
           started_awarded, started_total, started_checked_at, added_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, 0, 0, NULL, NOW())
        ON CONFLICT (user_id, game_id)
        DO UPDATE SET
          title = EXCLUDED.title,
          console_name = EXCLUDED.console_name,
          image_icon = EXCLUDED.image_icon,
          num_achievements = EXCLUDED.num_achievements,
          points = EXCLUDED.points
      `,
      [userId, Number(gameId), title, consoleName, imageIcon, Number(numAchievements) || 0, Number(points) || 0]
    );

    res.json({ ok: true });
  } catch (err) {
    const status = err?.status || 500;
    res.status(status).json({ error: err?.message || "Failed to add backlog item" });
  }
});

// Remove backlog item (auth required)
app.delete("/api/backlog/:gameId", requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ error: "Database unavailable" });
    const userId = req.session?.user?.id;
    if (!userId) return res.status(401).json({ error: "Not authenticated" });
    const gameId = Number(req.params.gameId);
    if (!Number.isFinite(gameId)) return res.status(400).json({ error: "Invalid gameId" });

    await pool.query(
      `
        DELETE FROM backlog_items
        WHERE user_id = $1 AND game_id = $2
      `,
      [userId, gameId]
    );

    res.json({ ok: true });
  } catch (err) {
    const status = err?.status || 500;
    res.status(status).json({ error: err?.message || "Failed to remove backlog item" });
  }
});

// Update backlog progress snapshot (auth required)
app.put("/api/backlog/:gameId/progress", requireAuth, async (req, res) => {
  try {
    if (!pool) return res.status(500).json({ error: "Database unavailable" });
    const userId = req.session?.user?.id;
    if (!userId) return res.status(401).json({ error: "Not authenticated" });
    const gameId = Number(req.params.gameId);
    if (!Number.isFinite(gameId)) return res.status(400).json({ error: "Invalid gameId" });
    const awarded = Number(req.body?.awarded ?? 0);
    const total = Number(req.body?.total ?? 0);
    if (!Number.isFinite(awarded) || awarded < 0 || !Number.isFinite(total) || total < 0) {
      return res.status(400).json({ error: "Invalid progress values" });
    }

    await pool.query(
      `
        UPDATE backlog_items
        SET started_awarded = $3,
            started_total = $4,
            started_checked_at = NOW()
        WHERE user_id = $1 AND game_id = $2
      `,
      [userId, gameId, awarded, total]
    );

    res.json({ ok: true });
  } catch (err) {
    const status = err?.status || 500;
    res.status(status).json({ error: err?.message || "Failed to update backlog progress" });
  }
});

// Console list (cached)
app.get("/api/consoles", async (req, res) => {
  try {
    const apiKey = requireApiKey(req, res);
    if (!apiKey) return;

    const list = await getConsoleList(apiKey);
    res.json({ count: list.length, results: list });
  } catch (err) {
    const status = err?.status || 500;
    res.status(status).json({ error: err?.message || "Failed to fetch console list" });
  }
});

// All games list (cached) grouped by starting letter.
app.get("/api/game-list", async (req, res) => {
  try {
    const apiKey = requireApiKey(req, res);
    if (!apiKey) return;

    const rawConsoleId = String(req.query.consoleId || "").trim();
    if (!rawConsoleId) return res.status(400).json({ error: "Missing consoleId" });
    const isAllConsoles = rawConsoleId.toLowerCase() === "all";
    const consoleId = isAllConsoles ? "all" : Number(rawConsoleId);
    if (!isAllConsoles && (!Number.isFinite(consoleId) || consoleId <= 0)) {
      return res.status(400).json({ error: "Missing consoleId" });
    }
    const list = isAllConsoles
      ? await getGameListForAllConsoles(apiKey)
      : await getGameListForConsole(consoleId, apiKey);
    const rawLetter = String(req.query.letter || "0-9").trim();
    const isAll = rawLetter === "" || rawLetter.toLowerCase() === "all";
    const letter = isAll ? "all" : normalizeGameLetter(rawLetter);
    const results = isAll ? list : list.filter(g => letterKeyFromTitle(g.title) === letter);

    res.json({
      letter,
      consoleId,
      totalGames: list.length,
      count: results.length,
      results
    });
  } catch (err) {
    const status = err?.status || 500;
    res.status(status).json({ error: err?.message || "Failed to fetch game list" });
  }
});

app.get("/api/game-players", async (req, res) => {
  try {
    const apiKey = requireApiKey(req, res);
    if (!apiKey) return;

    const gameId = Number(req.query.gameId || 0);
    if (!Number.isFinite(gameId) || gameId <= 0) {
      return res.status(400).json({ error: "Missing gameId" });
    }

    const cacheKey = `game-players:${gameId}`;
    const cached = cacheGet(cacheKey);
    if (cached !== null && cached !== undefined) {
      return res.json({ gameId, numDistinctPlayers: cached, cached: true });
    }

    const meta = await readGameMeta(gameId);
    if (meta?.num_distinct_players !== null && meta?.num_distinct_players !== undefined) {
      const updatedAt = meta.updated_at ? new Date(meta.updated_at).getTime() : 0;
      const ageMs = updatedAt ? Date.now() - updatedAt : Number.POSITIVE_INFINITY;
      const players = Number(meta.num_distinct_players);
      if (Number.isFinite(players)) {
        cacheSet(cacheKey, players, GAME_META_TTL_MS);
        const stale = !Number.isFinite(ageMs) || ageMs >= GAME_META_TTL_MS;
        return res.json({ gameId, numDistinctPlayers: players, cached: true, stale });
      }
    }

    res.json({ gameId, numDistinctPlayers: null, cached: false, stale: true });
  } catch (err) {
    const status = err?.status || 500;
    res.status(status).json({ error: err?.message || "Failed to fetch game players" });
  }
});

app.get("/api/game-players-refresh", async (req, res) => {
  try {
    const apiKey = requireApiKey(req, res);
    if (!apiKey) return;

    const gameId = Number(req.query.gameId || 0);
    if (!Number.isFinite(gameId) || gameId <= 0) {
      return res.status(400).json({ error: "Missing gameId" });
    }

    const meta = await refreshGameMeta(gameId, apiKey);
    res.json({ gameId, numDistinctPlayers: meta?.numDistinctPlayers ?? null });
  } catch (err) {
    const status = err?.status || 500;
    res.status(status).json({ error: err?.message || "Failed to refresh game players" });
  }
});

app.get("/api/game-players-batch", async (req, res) => {
  try {
    const apiKey = requireApiKey(req, res);
    if (!apiKey) return;
    const raw = String(req.query.ids || "").trim();
    if (!raw) return res.json({ results: [] });
    const ids = raw
      .split(",")
      .map(id => Number(id))
      .filter(id => Number.isFinite(id) && id > 0);
    if (!ids.length) return res.json({ results: [] });
    if (ids.length > 100) return res.status(400).json({ error: "Too many ids" });

    if (!pool) {
      return res.json({
        results: ids.map(gameId => ({ gameId, numDistinctPlayers: null, stale: true }))
      });
    }

    const result = await pool.query(
      `SELECT game_id, num_distinct_players, updated_at
       FROM game_metadata
       WHERE game_id = ANY($1::int[])`,
      [ids]
    );
    const rows = result.rows || [];
    const byId = new Map();
    for (const row of rows) {
      const updatedAt = row.updated_at ? new Date(row.updated_at).getTime() : 0;
      const ageMs = updatedAt ? Date.now() - updatedAt : Number.POSITIVE_INFINITY;
      const stale = !Number.isFinite(ageMs) || ageMs >= GAME_META_TTL_MS;
      byId.set(Number(row.game_id), {
        gameId: Number(row.game_id),
        numDistinctPlayers: row.num_distinct_players,
        stale
      });
    }
    const results = ids.map(gameId => {
      if (byId.has(gameId)) return byId.get(gameId);
      return { gameId, numDistinctPlayers: null, stale: true };
    });
    res.json({ results });
  } catch (err) {
    const status = err?.status || 500;
    res.status(status).json({ error: err?.message || "Failed to fetch game players batch" });
  }
});

app.get("/api/game-genre", async (req, res) => {
  try {
    const gameId = Number(req.query.gameId || 0);
    if (!Number.isFinite(gameId) || gameId <= 0) {
      return res.status(400).json({ error: "Missing gameId" });
    }

    const cacheKey = gameGenreCacheKey(gameId);
    const cached = cacheGet(cacheKey);
    if (cached !== null && cached !== undefined) {
      return res.json({ gameId, genre: cached, cached: true });
    }

    const meta = await readGameMeta(gameId);
    if (meta?.genre) {
      cacheSet(cacheKey, meta.genre, GENRE_CACHE_TTL_MS);
      return res.json({ gameId, genre: meta.genre, cached: true, stale: false });
    }

    res.json({ gameId, genre: null, cached: false, stale: true });
  } catch (err) {
    const status = err?.status || 500;
    res.status(status).json({ error: err?.message || "Failed to fetch game genre" });
  }
});

app.get("/api/game-genre-refresh", async (req, res) => {
  try {
    if (!RA_API_KEY) return res.status(400).json({ error: "Missing RA API key" });

    const gameId = Number(req.query.gameId || 0);
    if (!Number.isFinite(gameId) || gameId <= 0) {
      return res.status(400).json({ error: "Missing gameId" });
    }

    const existing = await readGameMeta(gameId);
    if (existing?.genre) {
      cacheSet(gameGenreCacheKey(gameId), existing.genre, GENRE_CACHE_TTL_MS);
      return res.json({ gameId, genre: existing.genre, cached: true });
    }

    const meta = await refreshGameMeta(gameId, RA_API_KEY);
    res.json({ gameId, genre: meta?.genre ?? null });
  } catch (err) {
    const status = err?.status || 500;
    res.status(status).json({ error: err?.message || "Failed to refresh game genre" });
  }
});

app.get("/api/game-genres-batch", async (req, res) => {
  try {
    const raw = String(req.query.ids || "").trim();
    if (!raw) return res.json({ results: [] });
    const ids = raw
      .split(",")
      .map(id => Number(id))
      .filter(id => Number.isFinite(id) && id > 0);
    if (!ids.length) return res.json({ results: [] });
    if (ids.length > 100) return res.status(400).json({ error: "Too many ids" });

    if (!pool) {
      return res.json({
        results: ids.map(gameId => ({ gameId, genre: null, stale: true }))
      });
    }

    const result = await pool.query(
      `SELECT game_id, genre, updated_at
       FROM game_metadata
       WHERE game_id = ANY($1::int[])`,
      [ids]
    );
    const rows = result.rows || [];
    const byId = new Map();
    for (const row of rows) {
      byId.set(Number(row.game_id), {
        gameId: Number(row.game_id),
        genre: row.genre || null,
        stale: false
      });
    }
    const results = ids.map(gameId => {
      if (byId.has(gameId)) return byId.get(gameId);
      return { gameId, genre: null, stale: true };
    });
    res.json({ results });
  } catch (err) {
    const status = err?.status || 500;
    res.status(status).json({ error: err?.message || "Failed to fetch game genres batch" });
  }
});

// Game achievements (no user progress)
app.get("/api/game-achievements-basic/:gameId", async (req, res) => {
  try {
    const gameId = String(req.params.gameId || "").trim();
    if (!gameId) return res.status(400).json({ error: "Missing gameId" });
    const apiKey = requireApiKey(req, res);
    if (!apiKey) return;

    const cacheKey = `game-achievements-basic:${gameId}:${apiKey}`;
    const cached = cacheGet(cacheKey);
    if (cached) return res.json({ ...cached, cached: true });

    const data = await raGetGameInfo(gameId, apiKey);
    const rawAchievements = data?.Achievements ?? data?.achievements ?? {};
    const achievements = Object.values(rawAchievements).map(a => ({
      id: a.ID ?? a.id,
      title: a.Title ?? a.title,
      description: a.Description ?? a.description,
      points: a.Points ?? a.points,
      badgeUrl: a.BadgeName ? `/Badge/${a.BadgeName}.png` : (a.BadgeURL ?? a.badgeUrl)
    }));

    const payload = {
      gameId,
      gameTitle: data?.Title ?? data?.title,
      consoleName: data?.ConsoleName ?? data?.consoleName,
      imageIcon: data?.ImageIcon ?? data?.imageIcon,
      imageTitle: data?.ImageTitle ?? data?.imageTitle,
      imageIngame: data?.ImageIngame ?? data?.imageIngame,
      imageBoxArt: data?.ImageBoxArt ?? data?.imageBoxArt,
      forumTopicId: data?.ForumTopicID ?? data?.forumTopicId,
      parentGameId: data?.ParentGameID ?? data?.parentGameId,
      numAchievements: data?.NumAchievements ?? data?.numAchievements,
      numDistinctPlayers: data?.NumDistinctPlayers ?? data?.numDistinctPlayers,
      publisher: data?.Publisher ?? data?.publisher,
      developer: data?.Developer ?? data?.developer,
      genre: data?.Genre ?? data?.genre,
      released: data?.Released ?? data?.released,
      achievements
    };

    cacheSet(cacheKey, payload, DEFAULT_CACHE_TTL_MS);
    res.json(payload);
  } catch (err) {
    const status = err?.status || 500;
    res.status(status).json({ error: err?.message || "Failed to fetch game achievements" });
  }
});

// Achievements for a user + game (for comparison)
app.get("/api/game-achievements/:username/:gameId", async (req, res) => {
  try {
    const username = String(req.params.username || "").trim();
    const gameId = String(req.params.gameId || "").trim();
    if (!username || !gameId) return res.status(400).json({ error: "Missing username or gameId" });
    const apiKey = requireApiKey(req, res);
    if (!apiKey) return;

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
      earnedHardcore: Boolean(a.DateEarnedHardcore || a.dateEarnedHardcore),
      earnedDate: a.DateEarned ?? a.dateEarned ?? null,
      earnedHardcoreDate: a.DateEarnedHardcore ?? a.dateEarnedHardcore ?? null
    }));

    const payload = {
      username,
      gameId,
      gameTitle: data?.Title ?? data?.title,
      consoleName: data?.ConsoleName ?? data?.consoleName,
      imageIcon: data?.ImageIcon ?? data?.imageIcon,
      achievements,
      highestAwardKind: data?.HighestAwardKind ?? data?.highestAwardKind ?? data?.HighestAward ?? data?.highestAward ?? null,
      completionStatus:
        data?.CompletionStatus ?? data?.completionStatus ??
        data?.Completion ?? data?.completion ??
        data?.Beaten ?? data?.beaten ??
        null
    };

    cacheSet(cacheKey, payload, DEFAULT_CACHE_TTL_MS);
    res.json(payload);
    } catch (err) {
      const status = err?.status || 500;
      res.status(status).json({ error: err?.message || "Failed to fetch game achievements" });
    }
  });

// Leaderboard times/scores for a user + game
app.get("/api/game-times/:username/:gameId", async (req, res) => {
  try {
    const username = String(req.params.username || "").trim();
    const gameId = String(req.params.gameId || "").trim();
    if (!username || !gameId) return res.status(400).json({ error: "Missing username or gameId" });
    const apiKey = requireApiKey(req, res);
    if (!apiKey) return;

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
      const status = err?.status || 500;
      res.status(status).json({ error: err?.message || "Failed to fetch game times" });
    }
  });

// Game leaderboards (for score attack selection)
app.get("/api/game-leaderboards/:gameId", async (req, res) => {
  try {
    const gameId = String(req.params.gameId || "").trim();
    if (!gameId) return res.status(400).json({ error: "Missing gameId" });
    const apiKey = requireApiKey(req, res);
    if (!apiKey) return;

    const cacheKey = `game-leaderboards:${gameId}:${apiKey}`;
    const cached = cacheGet(cacheKey);
    if (cached) return res.json({ ...cached, cached: true });

    const items = await raGetGameLeaderboards(gameId, 200, apiKey);
    const normalized = items.map(lb => ({
      id: lb.ID ?? lb.id,
      title: lb.Title ?? lb.title,
      description: lb.Description ?? lb.description,
      format: lb.Format ?? lb.format,
      lowerIsBetter: lb.LowerIsBetter ?? lb.lowerIsBetter
    }));

    const payload = { gameId, count: normalized.length, results: normalized };
    cacheSet(cacheKey, payload, DEFAULT_CACHE_TTL_MS);
    res.json(payload);
    } catch (err) {
      const status = err?.status || 500;
      res.status(status).json({ error: err?.message || "Failed to fetch game leaderboards" });
    }
  });


// "Now playing" (best-effort): use most recent LastPlayed from Recently Played Games.
// If LastPlayed is within `windowSeconds` (default 60), treat as "currently playing".
app.get("/api/now-playing/:username", async (req, res) => {
  try {
    const username = String(req.params.username || "").trim();
    if (!username) return res.status(400).json({ error: "Missing username" });
    const apiKey = requireApiKey(req, res);
    if (!apiKey) return;

    const windowSeconds = typeof req.query.window === "string" ? Number(req.query.window) : 120;
    const win = Number.isFinite(windowSeconds) ? Math.max(5, Math.min(600, windowSeconds)) : 60;

    const cacheKey = `now-playing:${username}:${win}:${apiKey}`;
    const cached = cacheGet(cacheKey);
    if (cached) return res.json({ ...cached, cached: true });

    // only need the most recent game + summary for rich presence
    const [latestList, summary] = await Promise.all([
      raGetUserRecentlyPlayedGames(username, 1, 0, apiKey),
      raGetUserSummary(username, apiKey).catch(() => null)
    ]);
    const [latest] = latestList || [];

    if (!latest) {
      const payload = { username, nowPlaying: false, reason: "no recent games" };
      cacheSet(cacheKey, payload, 30 * 1000);
      return res.json(payload);
    }

    const lastPlayedRaw = latest.LastPlayed ?? latest.lastPlayed;
    const lastPlayed = lastPlayedRaw ? String(lastPlayedRaw) : "";
    const ts = Date.parse(lastPlayed.replace(" ", "T") + "Z"); // best effort
    const ageSeconds = ts ? Math.floor((Date.now() - ts) / 1000) : null;

    const summaryUser =
      (summary && typeof summary.User === "object" ? summary.User : null) ||
      (summary && typeof summary.user === "object" ? summary.user : null) ||
      summary;
    const summaryRichPresence =
      summaryUser?.RichPresenceMsg ??
      summaryUser?.RichPresence ??
      summaryUser?.RichPresenceText ??
      summary?.RichPresenceMsg ??
      summary?.RichPresence ??
      summary?.RichPresenceText;
    const summaryGameId = summaryUser?.LastGameID ?? summary?.LastGameID ?? summaryUser?.LastGameId ?? summary?.LastGameId;
    const summaryTitle = summaryUser?.LastGameTitle ?? summary?.LastGameTitle ?? summaryUser?.LastGame ?? summary?.LastGame;
    const summaryConsoleName =
      summaryUser?.LastGameConsoleName ?? summary?.LastGameConsoleName ?? summaryUser?.LastGameConsole ?? summary?.LastGameConsole;
    const summaryImageIcon = summaryUser?.LastGameIcon ?? summary?.LastGameIcon ?? summaryUser?.LastGameImageIcon ?? summary?.LastGameImageIcon;

    const payload = {
      username,
      nowPlaying: ageSeconds !== null ? ageSeconds <= win : false,
      ageSeconds,
      windowSeconds: win,
      gameId: latest.GameID ?? latest.gameId ?? summaryGameId,
      consoleName: latest.ConsoleName ?? latest.consoleName ?? summaryConsoleName,
      title: latest.Title ?? latest.title ?? summaryTitle,
      richPresence:
        summaryRichPresence ??
        latest.RichPresenceMsg ??
        latest.RichPresence ??
        latest.RichPresenceText ??
        latest.richPresence ??
        latest.richPresenceMsg,
      imageIcon: latest.ImageIcon ?? latest.imageIcon ?? summaryImageIcon,
      lastPlayed
    };

    cacheSet(cacheKey, payload, 30 * 1000); // refreshable "presence" cache
    res.json(payload);
    } catch (err) {
      const status = err?.status || 500;
      res.status(status).json({ error: err?.message || "Failed to fetch now playing" });
    }
  });

// --- static site ---
const webPath = path.join(__dirname, "web");
const indexPath = path.join(webPath, "index.html");
const stylePath = path.join(webPath, "style.css");
const editorPath = path.join(webPath, "editor.html");

app.get("/api/editor/content", (_req, res) => {
  try {
    const html = fs.readFileSync(indexPath, "utf8");
    const css = fs.readFileSync(stylePath, "utf8");
    res.json({ html, css });
  } catch (err) {
    res.status(500).json({ error: err?.message || "Failed to load editor content" });
  }
});

app.post("/api/editor/save", (req, res) => {
  try {
    const html = req.body?.html;
    const css = req.body?.css;
    if (typeof html !== "string" || typeof css !== "string") {
      return res.status(400).json({ error: "Invalid editor payload" });
    }
    fs.writeFileSync(indexPath, html, "utf8");
    fs.writeFileSync(stylePath, css, "utf8");
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err?.message || "Failed to save editor content" });
  }
});

app.use(express.static(webPath));
app.get("/", (_req, res) => res.sendFile(indexPath));
app.get("/editor", (_req, res) => res.sendFile(editorPath));

const startServer = () => {
  app.listen(PORT, () => {
    console.log(`Server running: http://localhost:${PORT}`);
    console.log(`Health check : http://localhost:${PORT}/api/health`);
    scheduleAllConsolesWarmup();
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


