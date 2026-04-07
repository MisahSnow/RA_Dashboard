import { useEffect, useMemo, useState } from 'react';
import { getDailyHistory, getFriends, getMe, getMonthly } from '../lib/api';

const RANGES = ['daily', 'weekly', 'monthly'];

function normalizeUsername(value) {
  return String(value || '').trim();
}

function extractUsername(friend) {
  if (typeof friend === 'string') return normalizeUsername(friend);
  if (friend && typeof friend === 'object') {
    return normalizeUsername(
      friend.friend_username ||
      friend.friendUsername ||
      friend.username ||
      friend.name
    );
  }
  return '';
}

function getTodayKey() {
  return new Date().toISOString().slice(0, 10);
}

function getLastSevenKeys() {
  const keys = [];
  const now = new Date();

  for (let i = 6; i >= 0; i -= 1) {
    const d = new Date(now);
    d.setDate(now.getDate() - i);
    keys.push(d.toISOString().slice(0, 10));
  }

  return keys;
}

function sumWeekly(historyForUser) {
  if (!historyForUser) return 0;
  return getLastSevenKeys().reduce((sum, key) => {
    return sum + Number(historyForUser[key] || 0);
  }, 0);
}

function getDaily(historyForUser) {
  if (!historyForUser) return 0;
  return Number(historyForUser[getTodayKey()] || 0);
}

function buildRows(monthlyRows, historyMap, me) {
  const myMonthly = monthlyRows.find(
    (row) => row.username.toLowerCase() === me.toLowerCase()
  );

  const myMonthlyPoints = Number(myMonthly?.points || 0);
  const myDailyPoints = getDaily(historyMap[me]);
  const myWeeklyPoints = sumWeekly(historyMap[me]);

  return monthlyRows.map((row) => {
    const username = row.username;
    const dailyPoints = getDaily(historyMap[username]);
    const weeklyPoints = sumWeekly(historyMap[username]);

    return {
      ...row,
      dailyPoints,
      weeklyPoints,
      monthlyPoints: Number(row.points || 0),
      dailyGap: dailyPoints - myDailyPoints,
      weeklyGap: weeklyPoints - myWeeklyPoints,
      monthlyGap: Number(row.points || 0) - myMonthlyPoints,
      isMe: username.toLowerCase() === me.toLowerCase(),
    };
  });
}

function sortRows(rows, range) {
  const copy = [...rows];

  copy.sort((a, b) => {
    const aPoints =
      range === 'daily'
        ? a.dailyPoints
        : range === 'weekly'
        ? a.weeklyPoints
        : a.monthlyPoints;

    const bPoints =
      range === 'daily'
        ? b.dailyPoints
        : range === 'weekly'
        ? b.weeklyPoints
        : b.monthlyPoints;

    return bPoints - aPoints || a.username.localeCompare(b.username);
  });

  return copy;
}

function formatPoints(row, range) {
  if (range === 'daily') return row.dailyPoints;
  if (range === 'weekly') return row.weeklyPoints;
  return row.monthlyPoints;
}

function formatGap(row, range) {
  if (range === 'daily') return row.dailyGap;
  if (range === 'weekly') return row.weeklyGap;
  return row.monthlyGap;
}

function rangeTitle(range) {
  if (range === 'daily') return 'Daily Leaderboard';
  if (range === 'weekly') return 'Weekly Leaderboard';
  return 'Monthly Leaderboard';
}

function rangeNote(range) {
  if (range === 'daily') return "Points column shows today's total.";
  if (range === 'weekly') return 'Points column shows the last 7 days total.';
  return 'Points column shows monthly total.';
}

export default function Leaderboard() {
  const [range, setRange] = useState('monthly');
  const [rows, setRows] = useState([]);
  const [me, setMe] = useState('');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    let cancelled = false;

    async function load() {
      setLoading(true);
      setError('');

      try {
        const meData = await getMe();
        const myUsername = normalizeUsername(
          meData?.username || meData?.user?.username || meData?.me?.username
        );

        if (!myUsername) {
          throw new Error('No logged-in user found.');
        }

        const friendsData = await getFriends();
        const friendUsernames = Array.isArray(friendsData)
          ? friendsData.map(extractUsername).filter(Boolean)
          : [];

        const usernames = Array.from(
          new Set([myUsername, ...friendUsernames])
        );

        const monthlyResults = await Promise.all(
          usernames.map(async (username) => {
            const monthly = await getMonthly(username);
            return {
              username,
              points: Number(monthly?.points || 0),
              unlocks: Number(
                monthly?.unlockCount ??
                  monthly?.unlocks ??
                  monthly?.unlockCountAll ??
                  0
              ),
              level: Number(monthly?.level || 1),
            };
          })
        );

        let historyMap = {};
        try {
          const history = await getDailyHistory(usernames, 7);
          historyMap = history?.results || {};
        } catch {
          historyMap = {};
        }

        const builtRows = buildRows(monthlyResults, historyMap, myUsername);

        if (!cancelled) {
          setMe(myUsername);
          setRows(builtRows);
        }
      } catch (err) {
        if (!cancelled) {
          setError(err.message || 'Failed to load leaderboard.');
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    load();
    return () => {
      cancelled = true;
    };
  }, []);

  const sortedRows = useMemo(() => sortRows(rows, range), [rows, range]);

  return (
    <section className="leaderboard-card">
      <div className="leaderboard-header">
        <div>
          <h2>{rangeTitle(range)}</h2>
          <p className="leaderboard-note">{rangeNote(range)}</p>
        </div>

        <div className="leaderboard-tabs" role="tablist" aria-label="Leaderboard range">
          {RANGES.map((item) => (
            <button
              key={item}
              type="button"
              className={item === range ? 'active' : ''}
              onClick={() => setRange(item)}
            >
              {item[0].toUpperCase() + item.slice(1)}
            </button>
          ))}
        </div>
      </div>

      {loading && <div className="leaderboard-state">Loading leaderboard...</div>}
      {error && <div className="leaderboard-state leaderboard-error">{error}</div>}

      {!loading && !error && (
        <div className="leaderboard-table-wrap">
          <table className="leaderboard-table">
            <thead>
              <tr>
                <th>#</th>
                <th>User</th>
                <th>Points</th>
                <th>Gap vs You</th>
                <th>Unlocks</th>
              </tr>
            </thead>
            <tbody>
              {sortedRows.map((row, index) => {
                const gap = formatGap(row, range);

                return (
                  <tr key={row.username}>
                    <td>{index + 1}</td>
                    <td className="leaderboard-user">
                      <span>{row.username}</span>
                      {row.isMe && <span className="leaderboard-you">(you)</span>}
                    </td>
                    <td>{formatPoints(row, range)}</td>
                    <td
                      className={
                        gap > 0
                          ? 'gap-positive'
                          : gap < 0
                          ? 'gap-negative'
                          : 'gap-zero'
                      }
                    >
                      {gap > 0 ? '+' : ''}
                      {gap}
                    </td>
                    <td>{row.unlocks}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}