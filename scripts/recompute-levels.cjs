const dotenv = require("dotenv");
const { Pool } = require("pg");

dotenv.config();

const RA_API_KEY = process.env.RA_API_KEY || "";
const DATABASE_URL = process.env.DATABASE_URL || "";

if (!RA_API_KEY) {
  console.error("Missing RA_API_KEY in environment.");
  process.exit(1);
}
if (!DATABASE_URL) {
  console.error("Missing DATABASE_URL in environment.");
  process.exit(1);
}

function computeLevelFromPoints(pointsRaw) {
  const points = Number(pointsRaw);
  if (!Number.isFinite(points) || points <= 0) return 1;
  return Math.max(1, Math.floor(Math.sqrt(points / 10) * 3));
}

async function raGetUserSummary(username) {
  const url = new URL("https://retroachievements.org/API/API_GetUserSummary.php");
  url.searchParams.set("u", username);
  url.searchParams.set("y", RA_API_KEY);
  const res = await fetch(url.toString());
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`RA API error ${res.status}: ${text || res.statusText}`);
  }
  return res.json();
}

async function run() {
  const pool = new Pool({
    connectionString: DATABASE_URL,
    ssl: { rejectUnauthorized: false }
  });
  try {
    const usersRes = await pool.query("SELECT username FROM users ORDER BY username");
    const users = usersRes.rows.map(r => r.username).filter(Boolean);
    if (!users.length) {
      console.log("No users found.");
      return;
    }

    let updated = 0;
    let failed = 0;

    for (const username of users) {
      try {
        const data = await raGetUserSummary(username);
        const user =
          (data && typeof data.User === "object" ? data.User : null) ||
          (data && typeof data.user === "object" ? data.user : null) ||
          data;
        const totalPoints =
          user?.TotalPoints ?? data?.TotalPoints ?? user?.totalPoints ?? data?.totalPoints ??
          user?.Points ?? data?.Points ?? user?.points ?? data?.points ?? 0;
        const level = computeLevelFromPoints(totalPoints);
        await pool.query(
          `UPDATE users
             SET total_points = $2,
                 level = $3,
                 level_updated_at = NOW()
           WHERE username = $1`,
          [String(username).toLowerCase(), Number(totalPoints), level]
        );
        updated += 1;
        console.log(`Updated ${username}: level ${level} (points ${totalPoints})`);
      } catch (err) {
        failed += 1;
        console.error(`Failed ${username}: ${err?.message || err}`);
      }
    }

    console.log(`Done. Updated ${updated}, failed ${failed}.`);
  } finally {
    await pool.end();
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
