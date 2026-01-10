function safeText(v){ return (v === null || v === undefined) ? "" : String(v); }

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

const profilePanel = document.getElementById("profilePanel");
const profileTitleNameEl = document.getElementById("profileTitleName");
const profileSharedGamesEl = document.getElementById("profileSharedGames");
const profileCloseBtn = document.getElementById("profileCloseBtn");

const comparePanel = document.getElementById("comparePanel");
const compareTitleGameEl = document.getElementById("compareTitleGame");
const compareMetaEl = document.getElementById("compareMeta");
const compareAchievementsEl = document.getElementById("compareAchievements");
const compareBackBtn = document.getElementById("compareBackBtn");

const tabButtons = document.querySelectorAll(".tabBtn");
const tabPanels = document.querySelectorAll(".tabPanel");

let currentProfileUser = "";

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

async function fetchRecentGames(username, count) {
  const u = encodeURIComponent(username);
  return fetchJson(`/api/recent-games/${u}?count=${encodeURIComponent(count)}`);
}

async function fetchGameAchievements(username, gameId) {
  const u = encodeURIComponent(username);
  return fetchJson(`/api/game-achievements/${u}/${encodeURIComponent(gameId)}`);
}

function setStatus(msg) {
  statusEl.textContent = msg || "";
}

function setActiveTab(name) {
  tabButtons.forEach(btn => {
    const isActive = btn.dataset.tab === name;
    btn.classList.toggle("active", isActive);
    btn.setAttribute("aria-selected", isActive ? "true" : "false");
  });

  tabPanels.forEach(panel => {
    const isActive = panel.id === `tab-${name}`;
    panel.classList.toggle("active", isActive);
  });
}

function renderLeaderboard(rows, me) {
  tbody.innerHTML = "";
  for (const r of rows) {
    const tr = document.createElement("tr");

    const delta = r.deltaVsYou;
    const cls = delta > 0 ? "delta-pos" : delta < 0 ? "delta-neg" : "delta-zero";
    const isMe = (r.username && me) ? r.username.toLowerCase() === me.toLowerCase() : false;
tr.innerHTML = `
      <td><button class="linkBtn" type="button" data-profile="${safeText(r.username)}"><strong>${safeText(r.username)}</strong></button>${isMe ? ' <span class="note">(you)</span>' : ""}</td>
      <td><strong>${Math.round(r.points)}</strong></td>
      <td class="${cls}"><strong>${delta > 0 ? "+" : ""}${Math.round(delta)}</strong></td>
      <td>${r.unlocks}</td>
      <td>${r.nowPlayingText || ""}</td>
      <td style="text-align:right;">
        ${isMe ? "" : `<button class="smallBtn" data-remove="${safeText(r.username)}">Remove</button>`}
      </td>
    `;

    tbody.appendChild(tr);

    const nameBtn = tr.querySelector("button[data-profile]");
    if (nameBtn) {
      nameBtn.addEventListener("click", () => {
        openProfile(nameBtn.getAttribute("data-profile"));
      });
    }
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


function renderSharedGames(games) {
  profileSharedGamesEl.innerHTML = "";

  if (!games.length) {
    profileSharedGamesEl.innerHTML = `<div class="meta">No shared games found in recent play history.</div>`;
    return;
  }

  for (const g of games) {
    const tile = document.createElement("div");
    tile.className = "tile clickable";
    tile.setAttribute("role", "button");
    tile.tabIndex = 0;

    const img = document.createElement("img");
    img.src = iconUrl(g.imageIcon);
    img.alt = safeText(g.title || "game");
    img.loading = "lazy";

    const title = document.createElement("div");
    title.className = "tileTitle";
    title.textContent = g.title || `Game ${safeText(g.gameId)}`;

    tile.appendChild(img);
    tile.appendChild(title);
    profileSharedGamesEl.appendChild(tile);

    const open = () => openGameCompare(g);
    tile.addEventListener("click", open);
    tile.addEventListener("keydown", (e) => {
      if (e.key === "Enter" || e.key === " ") { e.preventDefault(); open(); }
    });
  }
}

async function openProfile(username) {
  const target = clampUsername(username);
  if (!target) return;

  const { me } = getUsersIncludingMe();
  if (!me) return setStatus("Set your username first.");

  profilePanel.hidden = false;
  comparePanel.hidden = true;
  profileTitleNameEl.textContent = target;
  profileSharedGamesEl.innerHTML = `<div class="meta">Loading shared games...</div>`;
  currentProfileUser = target;

  try {
    const count = 60;
    const [mine, theirs] = await Promise.all([
      fetchRecentGames(me, count),
      fetchRecentGames(target, count)
    ]);

    const myMap = new Map((mine.results || []).map(g => [g.gameId, g]));
    const shared = [];

    for (const g of (theirs.results || [])) {
      if (!myMap.has(g.gameId)) continue;
      const mineGame = myMap.get(g.gameId);
      shared.push({
        gameId: g.gameId,
        title: g.title || mineGame?.title,
        imageIcon: g.imageIcon || mineGame?.imageIcon
      });
    }

    const seen = new Set();
    const unique = shared.filter(g => {
      const key = String(g.gameId ?? "");
      if (!key || seen.has(key)) return false;
      seen.add(key);
      return true;
    });

    renderSharedGames(unique);
    profilePanel.scrollIntoView({ behavior: "smooth", block: "start" });
  } catch (e) {
    profileSharedGamesEl.innerHTML = `<div class="meta">Failed to load shared games.</div>`;
    setStatus(e?.message || "Failed to load profile.");
  }
}

function renderCompareList(items) {
  compareAchievementsEl.innerHTML = "";

  if (!items.length) {
    compareAchievementsEl.innerHTML = `<div class="meta">No achievements found for this game.</div>`;
    return;
  }

  for (const a of items) {
    const row = document.createElement("div");
    row.className = `compareItem${a.shared ? " shared" : ""}`;

    const img = document.createElement("img");
    img.className = "compareBadge";
    img.alt = safeText(a.title || "achievement");
    img.loading = "lazy";
    img.src = iconUrl(a.badgeUrl);

    const main = document.createElement("div");
    main.className = "compareMain";

    const title = document.createElement("div");
    title.className = "compareTitle";
    title.textContent = a.title || `Achievement ${safeText(a.id)}`;

    const desc = document.createElement("div");
    desc.className = "compareDesc";
    desc.textContent = a.description || "";

    main.appendChild(title);
    main.appendChild(desc);

    const status = document.createElement("div");
    status.className = `statusPill ${a.statusClass}`;
    status.textContent = a.statusLabel;

    row.appendChild(img);
    row.appendChild(main);
    row.appendChild(status);
    compareAchievementsEl.appendChild(row);
  }
}

async function openGameCompare(game) {
  const target = clampUsername(currentProfileUser);
  if (!target) return;

  const { me } = getUsersIncludingMe();
  if (!me) return setStatus("Set your username first.");

  profilePanel.hidden = true;
  comparePanel.hidden = false;
  compareTitleGameEl.textContent = game.title || `Game ${safeText(game.gameId)}`;
  compareMetaEl.textContent = `You: ${me} | Friend: ${target}`;
  compareAchievementsEl.innerHTML = `<div class="meta">Loading achievements...</div>`;

  try {
    const [mine, theirs] = await Promise.all([
      fetchGameAchievements(me, game.gameId),
      fetchGameAchievements(target, game.gameId)
    ]);

    const mineById = new Map((mine.achievements || []).map(a => [a.id, a]));
    const theirsById = new Map((theirs.achievements || []).map(a => [a.id, a]));
    const ids = new Set([...mineById.keys(), ...theirsById.keys()]);

    const combined = [];
    let sharedCount = 0;
    let mineCount = 0;
    let theirsCount = 0;

    for (const id of ids) {
      const ma = mineById.get(id);
      const ta = theirsById.get(id);
      const mineEarned = Boolean(ma?.earned);
      const theirsEarned = Boolean(ta?.earned);

      if (mineEarned) mineCount++;
      if (theirsEarned) theirsCount++;
      if (mineEarned && theirsEarned) sharedCount++;

      let statusLabel = "None";
      let statusClass = "none";
      if (mineEarned && theirsEarned) { statusLabel = "Both"; statusClass = ""; }
      else if (mineEarned) { statusLabel = "You"; statusClass = "me"; }
      else if (theirsEarned) { statusLabel = "Them"; statusClass = "them"; }

      combined.push({
        id,
        title: ma?.title || ta?.title,
        description: ma?.description || ta?.description,
        badgeUrl: ma?.badgeUrl || ta?.badgeUrl,
        shared: mineEarned && theirsEarned,
        statusLabel,
        statusClass
      });
    }

    combined.sort((a, b) => {
      const weight = (x) => x.shared ? 0 : (x.statusLabel === "You" || x.statusLabel === "Them") ? 1 : 2;
      const w = weight(a) - weight(b);
      if (w !== 0) return w;
      return String(a.title || "").localeCompare(String(b.title || ""));
    });

    compareMetaEl.textContent = `You: ${me} | Friend: ${target} | Shared: ${sharedCount} | You: ${mineCount} | Them: ${theirsCount}`;
    renderCompareList(combined);
    comparePanel.scrollIntoView({ behavior: "smooth", block: "start" });
  } catch (e) {
    compareAchievementsEl.innerHTML = `<div class="meta">Failed to load achievements.</div>`;
    setStatus(e?.message || "Failed to load game comparison.");
  }
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
      <div><strong>${a.title}</strong> <span class="meta">(${a.points} pts)</span>${a.achievementId ? ` <span class="meta">open</span>` : ``}</div>
      <div class="meta">${a.gameTitle}${a.consoleName ? " - " + a.consoleName : ""}</div>
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
      <div class="meta">${t.gameTitle}${t.consoleName ? " - " + t.consoleName : ""}</div>
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
      nowPlayingText: "Loading..."
    }));

    rows.sort((a, b) => (b.points - a.points) || a.username.localeCompare(b.username));
    renderLeaderboard(rows, me);

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
      return null;
    }

    (async () => {
      const presencePairs = await Promise.all(users.map(async (u) => {
        try {
          const p = await fetchNowPlayingWithRetry(u, win, {
            retries: 4,
            delayMs: 1000,
            onRetry: (attempt) => {
              const row = rows.find(r => r.username === u);
              if (row) {
                row.nowPlayingText = `Loading... (retry ${attempt}/4)`;
                renderLeaderboard(rows, me);
              }
            }
          });
          return [u, p];
        } catch {
          return [u, null];
        }
      }));
      const presence = Object.fromEntries(presencePairs);

      for (const r of rows) {
        const p = presence[r.username];
        if (p && p.title) {
          const age = (typeof p.ageSeconds === "number")
            ? (p.ageSeconds < 60 ? `${p.ageSeconds}s ago` : `${Math.floor(p.ageSeconds/60)}m ago`)
            : "";
          r.nowPlayingText = p.nowPlaying ? `Playing: ${p.title}` : `${p.title}${age ? ` (${age})` : ""}`;
        } else {
          r.nowPlayingText = "";
        }
      }

      renderLeaderboard(rows, me);
    })().catch(() => {
      // Ignore background errors; leaderboard already rendered.
    });

  } catch (e) {
    setStatus(e?.message || "Failed to load leaderboard.");
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
tabButtons.forEach(btn => {
  btn.addEventListener("click", () => setActiveTab(btn.dataset.tab));
});

if (tabButtons.length) {
  setActiveTab(tabButtons[0].dataset.tab);
}

addFriendBtn.addEventListener("click", () => {
  const me = clampUsername(meInput.value);
  const u = clampUsername(friendInput.value);
  if (!me) return setStatus("Set your username first.");
  if (!u) return;
  if (u.toLowerCase() === me.toLowerCase()) return setStatus("That's you. Add someone else.");

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

if (profileCloseBtn) {
  profileCloseBtn.addEventListener("click", () => {
    profilePanel.hidden = true;
    profileTitleNameEl.textContent = "";
    profileSharedGamesEl.innerHTML = "";
    comparePanel.hidden = true;
    compareTitleGameEl.textContent = "";
    compareMetaEl.textContent = "";
    compareAchievementsEl.innerHTML = "";
    currentProfileUser = "";
  });
}

if (compareBackBtn) {
  compareBackBtn.addEventListener("click", () => {
    comparePanel.hidden = true;
    profilePanel.hidden = false;
    compareTitleGameEl.textContent = "";
    compareMetaEl.textContent = "";
    compareAchievementsEl.innerHTML = "";
  });
}

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
