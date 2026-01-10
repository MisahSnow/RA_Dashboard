const LS_ME = "ra.me";
const LS_FRIENDS = "ra.friends";

const meInput = document.getElementById("meInput");
const friendInput = document.getElementById("friendInput");
const addFriendBtn = document.getElementById("addFriendBtn");
const refreshBtn = document.getElementById("refreshBtn");
const tbody = document.querySelector("#leaderboard tbody");
const statusEl = document.getElementById("status");

const recentAchievementsEl = document.getElementById("recentAchievements");
const recentTimesEl = document.getElementById("recentTimes");
const recentHoursEl = document.getElementById("recentHours");
const recentGamesEl = document.getElementById("recentGames");
const refreshRecentAchBtn = document.getElementById("refreshRecentAchBtn");
const refreshTimesBtn = document.getElementById("refreshTimesBtn");

function clampUsername(s) {
  return (s || "").trim().replace(/\s+/g, "");
}

function loadState() {
  meInput.value = localStorage.getItem(LS_ME) || "";
  try {
    return JSON.parse(localStorage.getItem(LS_FRIENDS) || "[]");
  } catch {
    return [];
  }
}

let friends = loadState();

function saveState() {
  localStorage.setItem(LS_ME, clampUsername(meInput.value));
  localStorage.setItem(LS_FRIENDS, JSON.stringify(friends));
}

async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(text || `Request failed: ${res.status}`);
  }
  return res.json();
}

async function fetchMonthly(username) {
  return fetchJson(`/api/monthly/${encodeURIComponent(username)}`);
}

async function fetchRecentAchievements(username, minutes, limit) {
  const u = encodeURIComponent(username);
  return fetchJson(`/api/recent-achievements/${u}?m=${encodeURIComponent(minutes)}&limit=${encodeURIComponent(limit)}`);
}

async function fetchNowPlaying(username, windowSeconds) {
  const u = encodeURIComponent(username);
  return fetchJson(`/api/now-playing/${u}?window=${encodeURIComponent(windowSeconds)}`);
}

async function fetchRecentTimes(username, games, limit) {
  const u = encodeURIComponent(username);
  return fetchJson(`/api/recent-times/${u}?games=${encodeURIComponent(games)}&limit=${encodeURIComponent(limit)}`);
}

function setStatus(msg) {
  statusEl.textContent = msg || "";
}

function renderLeaderboard(rows, me) {
  tbody.innerHTML = "";
  for (const r of rows) {
    const tr = document.createElement("tr");

    const delta = r.deltaVsYou;
    const cls = delta > 0 ? "delta-pos" : delta < 0 ? "delta-neg" : "delta-zero";
    const isMe = r.username.toLowerCase() === me.toLowerCase();

    tr.innerHTML = `
      <td><strong>${r.username}</strong>${isMe ? ' <span class="note">(you)</span>' : ""}</td>
      <td><strong>${Math.round(r.points)}</strong></td>
      <td class="${cls}"><strong>${delta > 0 ? "+" : ""}${Math.round(delta)}</strong></td>
      <td>${r.unlocks}</td>
      <td>${r.nowPlayingText || ''}</td>
      <td style="text-align:right;">
        ${isMe ? "" : `<button class="smallBtn" data-remove="${r.username}">Remove</button>`}
      </td>
    `;

    tbody.appendChild(tr);
  }

  tbody.querySelectorAll("button[data-remove]").forEach(btn => {
    btn.addEventListener("click", () => {
      const u = btn.getAttribute("data-remove");
      friends = friends.filter(x => x !== u);
      saveState();
      refreshLeaderboard();
      refreshRecentAchievements();
      refreshRecentTimes();
    });
  });
}

function formatDate(d) {
  const t = Date.parse(d);
  if (!t) return d || "";
  return new Date(t).toLocaleString();
}

function iconUrl(rel) {
  if (!rel) return "";
  // RA returns paths like /Images/xxxx.png or /Badge/xxx.png
  return `https://retroachievements.org${rel}`;
}

function achievementUrl(id) {
  return `https://retroachievements.org/achievement/${encodeURIComponent(id)}`;
}


function renderRecentAchievements(items) {
  recentAchievementsEl.innerHTML = "";
  if (!items.length) {
    recentAchievementsEl.innerHTML = `<div class="meta">No recent achievements in this window.</div>`;
    return;
  }

  for (const a of items) {
    const div = document.createElement("div");
    div.className = "item";

    

    // Make the whole achievement row clickable
    if (a.achievementId) {
      div.classList.add("clickable");
      div.tabIndex = 0;
      div.setAttribute("role", "link");
      div.setAttribute("aria-label", `Open achievement: ${a.title || ""}`);
      const open = () => window.open(achievementUrl(a.achievementId), "_blank", "noopener,noreferrer");
      div.addEventListener("click", open);
      div.addEventListener("keydown", (e) => {
        if (e.key === "Enter" || e.key === " ") { e.preventDefault(); open(); }
      });
    }
const badge = document.createElement("img");
    badge.className = "badge";
    badge.alt = "badge";
    badge.src = iconUrl(a.badgeUrl);
    badge.loading = "lazy";

    const main = document.createElement("div");
    main.className = "itemMain";

    const title = document.createElement("div");
    title.className = "itemTitle";
    title.innerHTML = `
      <strong>${a.username}</strong>
      <span class="pill ${a.hardcore ? "hc" : ""}">${a.hardcore ? "HC" : "SC"}</span>
      <span class="meta">${formatDate(a.date)}</span>
    `;

    const body = document.createElement("div");
    body.innerHTML = `
      <div><strong>${a.title}</strong> <span class="meta">(${a.points} pts)</span>${a.achievementId ? ` <span class="meta">↗</span>` : ``}</div>
      <div class="meta">${a.gameTitle}${a.consoleName ? " • " + a.consoleName : ""}</div>
      <div class="meta">${a.description || ""}</div>
    `;

    main.appendChild(title);
    main.appendChild(body);

    div.appendChild(badge);
    div.appendChild(main);
    recentAchievementsEl.appendChild(div);
  }
}




function renderRecentTimes(items) {
  recentTimesEl.innerHTML = "";
  if (!items.length) {
    recentTimesEl.innerHTML = `<div class="meta">No recent leaderboard entries found (based on recently played games).</div>`;
    return;
  }

  for (const t of items) {
    const div = document.createElement("div");
    div.className = "item";

    const img = document.createElement("img");
    img.className = "badge";
    img.alt = "game";
    img.src = iconUrl(t.imageIcon);
    img.loading = "lazy";

    const main = document.createElement("div");
    main.className = "itemMain";

    const head = document.createElement("div");
    head.className = "itemTitle";
    head.innerHTML = `
      <strong>${t.username}</strong>
      <span class="pill mono">#${t.rank}</span>
      <span class="pill mono">${t.formattedScore ?? t.score}</span>
      <span class="meta">${formatDate(t.dateUpdated)}</span>
    `;

    const body = document.createElement("div");
    body.innerHTML = `
      <div><strong>${t.leaderboardTitle}</strong></div>
      <div class="meta">${t.gameTitle}${t.consoleName ? " • " + t.consoleName : ""}</div>
    `;

    main.appendChild(head);
    main.appendChild(body);

    div.appendChild(img);
    div.appendChild(main);

    recentTimesEl.appendChild(div);
  }
}

function getUsersIncludingMe() {
  const me = clampUsername(meInput.value);
  const users = Array.from(new Set([me, ...friends].map(clampUsername).filter(Boolean)));
  return { me, users };
}

async function refreshLeaderboard() {
  const { me, users } = getUsersIncludingMe();
  if (!me) return;

  try {
    // 1) Load monthly points first and render immediately.
    const results = await Promise.all(users.map(u => fetchMonthly(u).then(m => [u, m])));
    const map = Object.fromEntries(results);
    const myPoints = map[me]?.points ?? 0;

    // Initial rows: show placeholder in Now Playing while we fetch presence.
    const rows = users.map(u => ({
      username: u,
      points: map[u]?.points ?? 0,
      deltaVsYou: (map[u]?.points ?? 0) - myPoints,
      unlocks: map[u]?.unlockCount ?? 0,
      nowPlayingText: "…"
    }));

    rows.sort((a, b) => (b.points - a.points) || a.username.localeCompare(b.username));
    renderLeaderboard(rows);

    // 2) Fetch presence in the background. Retry a few times (best-effort) but never block the leaderboard.
    const win = 120; // 2 minutes
    const sleep = (ms) => new Promise(r => setTimeout(r, ms));

    async function fetchNowPlayingWithRetry(username, windowSeconds, {
  retries = 4,
  delayMs = 1000,
  onRetry = () => {}
} = {}) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      return await fetchNowPlaying(username, windowSeconds);
    } catch (e) {
      const msg = String(e?.message || e || "");
      const is429 = msg.includes("429") || msg.includes("Too Many Attempts");
      if (!is429 || attempt === retries) throw e;
      onRetry(attempt);
      await sleep(delayMs);
    }
  }
}




async function refreshRecentAchievements() {
  const { me, users } = getUsersIncludingMe();
  if (!me) return;

  const hours = Number(recentHoursEl.value || 72);
  const minutes = Math.max(1, Math.floor(hours * 60));
  const perUserLimit = 10;

  try {
    const payloads = await Promise.all(
      users.map(u => fetchRecentAchievements(u, minutes, perUserLimit).catch(() => ({ results: [] })))
    );

    const combined = payloads.flatMap(p => p.results || []);
    combined.sort((a, b) => (Date.parse(b.date || "") || 0) - (Date.parse(a.date || "") || 0));
    renderRecentAchievements(combined.slice(0, 30));
  } catch {
    // keep quiet; leaderboard is primary
  }
}

async function refreshRecentTimes() {
  const { me, users } = getUsersIncludingMe();
  if (!me) return;

    const games = Math.max(1, Number(recentGamesEl.value || 50));
  const perUserLimit = 10;

  try {
    const payloads = await Promise.all(
      users.map(async (u) => {
        try {
          return await fetchRecentTimes(u, games, perUserLimit);
        } catch (e) {
          return { __error: (e?.message || "error"), results: [], username: u };
        }
      })
    );

    const errors = payloads.filter(p => p.__error).map(p => `${p.username}: ${p.__error}`);
    const combined = payloads.flatMap(p => p.results || []);
    combined.sort((a, b) => (Date.parse(b.dateUpdated || "") || 0) - (Date.parse(a.dateUpdated || "") || 0));
    renderRecentTimes(combined.slice(0, 30));

    if (!combined.length && errors.length) {
      // show the first error so it doesn't look like "nothing happened"
      setStatus(`Times fetch issue: ${errors[0]}`);
    }
  } catch (e) {
    setStatus(e?.message || "Failed to load recent times.");
  }
}

// UI wiring
addFriendBtn.addEventListener("click", () => {
  const me = clampUsername(meInput.value);
  const u = clampUsername(friendInput.value);
  if (!me) return setStatus("Set your username first.");
  if (!u) return;
  if (u.toLowerCase() === me.toLowerCase()) return setStatus("That’s you 🙂 Add someone else.");

  if (!friends.includes(u)) friends.push(u);
  friendInput.value = "";
  saveState();

  refreshLeaderboard();
  refreshRecentAchievements();
  refreshRecentTimes();
});

refreshBtn.addEventListener("click", () => {
  refreshLeaderboard();
  refreshRecentAchievements();
  refreshRecentTimes();
});

refreshRecentAchBtn.addEventListener("click", refreshRecentAchievements);
refreshTimesBtn.addEventListener("click", refreshRecentTimes);

meInput.addEventListener("keydown", (e) => {
  if (e.key === "Enter") {
    refreshLeaderboard();
      refreshRecentAchievements();
    refreshRecentTimes();
  }
});

friendInput.addEventListener("keydown", (e) => {
  if (e.key === "Enter") addFriendBtn.click();
});

// initial load
if (meInput.value) {
  refreshLeaderboard();
  refreshRecentAchievements();
  refreshRecentTimes();
}