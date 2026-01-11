function safeText(v){ return (v === null || v === undefined) ? "" : String(v); }

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

const LS_ME = "ra.me";
const LS_FRIENDS = "ra.friends";
const LS_API_KEY = "ra.apiKey";
const LS_CACHE_PREFIX = "ra.cache.";
const LS_DAILY_HISTORY = "ra.dailyHistory";

const meInput = document.getElementById("meInput");
const apiKeyInput = document.getElementById("apiKeyInput");
const friendInput = document.getElementById("friendInput");
const addFriendOpenBtn = document.getElementById("addFriendOpenBtn");
const addFriendModal = document.getElementById("addFriendModal");
const addFriendConfirmBtn = document.getElementById("addFriendConfirmBtn");
const addFriendCloseBtn = document.getElementById("addFriendCloseBtn");
const addFriendCancelBtn = document.getElementById("addFriendCancelBtn");
const addFriendErrorEl = document.getElementById("addFriendError");
const addFriendLoadingEl = document.getElementById("addFriendLoading");
const usernameModal = document.getElementById("usernameModal");
const usernameModalInput = document.getElementById("usernameModalInput");
const usernameModalApiKeyInput = document.getElementById("usernameModalApiKeyInput");
const usernameModalConfirmBtn = document.getElementById("usernameModalConfirmBtn");
const usernameModalErrorEl = document.getElementById("usernameModalError");
const usernameModalLoadingEl = document.getElementById("usernameModalLoading");
const refreshBtn = document.getElementById("refreshBtn");
const tbody = document.querySelector("#leaderboard tbody");
const statusEl = document.getElementById("status");
const refreshCountdownEl = document.getElementById("refreshCountdown");
const onlineUsersEl = document.getElementById("onlineUsers");
const onlineHintEl = document.getElementById("onlineHint");
const leaderboardChartEl = document.getElementById("leaderboardChart");

const settingsBtn = document.getElementById("settingsBtn");
const settingsModal = document.getElementById("settingsModal");
const settingsSaveBtn = document.getElementById("settingsSaveBtn");
const settingsCloseBtn = document.getElementById("settingsCloseBtn");
const settingsCancelBtn = document.getElementById("settingsCancelBtn");

const recentAchievementsEl = document.getElementById("recentAchievements");
const recentTimesEl = document.getElementById("recentTimes");
const recentGamesEl = document.getElementById("recentGames");
const recentAchievementsToggleBtn = document.getElementById("recentAchievementsToggleBtn");
const recentAchievementsShowLessBtn = document.getElementById("recentAchievementsShowLessBtn");
const recentTimesToggleBtn = document.getElementById("recentTimesToggleBtn");
const leaderboardLoadingEl = document.getElementById("leaderboardLoading");
const profileLoadingEl = document.getElementById("profileLoading");
const compareLoadingEl = document.getElementById("compareLoading");
const recentAchievementsLoadingEl = document.getElementById("recentAchievementsLoading");
const recentTimesLoadingEl = document.getElementById("recentTimesLoading");

const profilePanel = document.getElementById("profilePanel");
const profileTitleNameEl = document.getElementById("profileTitleName");
const profileSummaryEl = document.getElementById("profileSummary");
const profileInsightsEl = document.getElementById("profileInsights");
const profileSharedGamesEl = document.getElementById("profileSharedGames");
const profileCloseBtn = document.getElementById("profileCloseBtn");
const profileShowMoreBtn = document.getElementById("profileShowMoreBtn");
const profileGamesNoteEl = document.getElementById("profileGamesNote");
const profileGameSearchEl = document.getElementById("profileGameSearch");
const profileLegendMeEl = document.getElementById("profileLegendMe");
const profileLegendThemEl = document.getElementById("profileLegendThem");

const comparePanel = document.getElementById("comparePanel");
const compareTitleGameEl = document.getElementById("compareTitleGame");
const compareMetaEl = document.getElementById("compareMeta");
const compareAchievementsEl = document.getElementById("compareAchievements");
const compareTimesEl = document.getElementById("compareTimes");
const compareBackBtn = document.getElementById("compareBackBtn");

const compareTabButtons = document.querySelectorAll(".compareTabBtn");
const compareTabPanels = document.querySelectorAll(".compareTabPanel");

const tabButtons = document.querySelectorAll(".tabBtn");
const tabPanels = document.querySelectorAll(".tabPanel");

let currentProfileUser = "";
let profileSharedGames = [];
let profileDisplayedGames = [];
let profileAllGamesLoaded = false;
let profileGamesEmptyMessage = "No shared games found in recent play history.";
let profileAutoLoadingAll = false;
let profileSkipAutoLoadOnce = false;
let profileGameAchievementCounts = new Map();
let profileGameAchievementPending = new Map();
const profileAchievementLimiter = createLimiter(2);
const RECENT_DEFAULT_ROWS = 6;
const RECENT_STEP_ROWS = 4;
let recentAchievementsItems = [];
let recentTimesItems = [];
let recentAchievementsVisible = RECENT_DEFAULT_ROWS;
let recentTimesVisible = RECENT_DEFAULT_ROWS;
let refreshCountdownTimer = null;
let nextRefreshAt = null;

const AUTO_REFRESH_MS = 120000;
const ACHIEVEMENTS_DEFAULT_HOURS = 72;
const ACHIEVEMENTS_STEP_HOURS = 24;
const ACHIEVEMENTS_MAX_HOURS = 720;
const ACHIEVEMENTS_DEFAULT_MAX = 30;
const ACHIEVEMENTS_STEP_MAX = 30;
const ACHIEVEMENTS_MAX_SHOW_MORE = 6;
let achievementsLookbackHours = ACHIEVEMENTS_DEFAULT_HOURS;
let achievementsMaxResults = ACHIEVEMENTS_DEFAULT_MAX;
const ACHIEVEMENTS_MIN_WAIT_MS = 5000;
const achievementsLoadStart = Date.now();
let achievementsShowMoreCount = 0;
const STAGGER_MS = 400;
const PRESENCE_PING_MS = 15000;
let presenceTimer = null;
const PRESENCE_SESSION_KEY = "ra.presence.session";
let presenceSessionId = sessionStorage.getItem(PRESENCE_SESSION_KEY);
if (!presenceSessionId) {
  presenceSessionId = Math.random().toString(36).slice(2);
  sessionStorage.setItem(PRESENCE_SESSION_KEY, presenceSessionId);
}

function clampUsername(s) {
  return (s || "").trim().replace(/\s+/g, "");
}

function loadState() {
  meInput.value = localStorage.getItem(LS_ME) || "";
  if (apiKeyInput) apiKeyInput.value = localStorage.getItem(LS_API_KEY) || "";
  try {
    return JSON.parse(localStorage.getItem(LS_FRIENDS) || "[]");
  } catch {
    return [];
  }
}

let friends = loadState();

function saveState() {
  localStorage.setItem(LS_ME, clampUsername(meInput.value));
  if (apiKeyInput) localStorage.setItem(LS_API_KEY, (apiKeyInput.value || "").trim());
  localStorage.setItem(LS_FRIENDS, JSON.stringify(friends));
}

function cacheSet(key, data) {
  try {
    localStorage.setItem(`${LS_CACHE_PREFIX}${key}`, JSON.stringify({ ts: Date.now(), data }));
  } catch {
    // Ignore cache failures.
  }
}

function cacheGet(key) {
  try {
    const raw = localStorage.getItem(`${LS_CACHE_PREFIX}${key}`);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    return parsed?.data ?? null;
  } catch {
    return null;
  }
}

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

async function fetchPresence() {
  return fetchJson("/api/presence", { silent: true });
}

function getLocalDateKey(d = new Date()) {
  const year = d.getFullYear();
  const month = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function loadDailyHistory() {
  try {
    return JSON.parse(localStorage.getItem(LS_DAILY_HISTORY) || "{}");
  } catch {
    return {};
  }
}

function saveDailyHistory(history) {
  try {
    localStorage.setItem(LS_DAILY_HISTORY, JSON.stringify(history));
  } catch {
    // ignore
  }
}

function updateDailyHistory(rows) {
  const history = loadDailyHistory();
  const today = getLocalDateKey();
  for (const r of rows) {
    if (r.dailyPoints === null || r.dailyPoints === undefined) continue;
    if (!history[r.username]) history[r.username] = {};
    history[r.username][today] = Math.round(r.dailyPoints);
  }
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - 30);
  const cutoffKey = getLocalDateKey(cutoff);
  for (const [user, days] of Object.entries(history)) {
    for (const day of Object.keys(days)) {
      if (day < cutoffKey) delete days[day];
    }
    if (Object.keys(days).length === 0) delete history[user];
  }
  saveDailyHistory(history);
}

function userColor(username) {
  let hash = 0;
  for (let i = 0; i < username.length; i++) {
    hash = ((hash << 5) - hash) + username.charCodeAt(i);
    hash |= 0;
  }
  const hue = Math.abs(hash) % 360;
  return `hsl(${hue}, 75%, 62%)`;
}

function renderLeaderboardChart(rows) {
  if (!leaderboardChartEl) return;
  const history = loadDailyHistory();
  const days = [];
  const today = new Date();
  for (let i = 6; i >= 0; i--) {
    const d = new Date(today);
    d.setDate(today.getDate() - i);
    days.push(getLocalDateKey(d));
  }

  const series = rows.map(r => ({
    username: r.username,
    values: days.map(day => Number(history[r.username]?.[day] || 0)),
    color: r.nameColor || userColor(r.username)
  }));

  const max = Math.max(1, ...series.flatMap(s => s.values));
  const width = 800;
  const height = 160;
  const pad = 12;
  const xStep = (width - pad * 2) / (days.length - 1);
  const yScale = (val) => height - pad - ((val / max) * (height - pad * 2));

  const axisY = `<line class="chartAxis" x1="${pad}" y1="${pad}" x2="${pad}" y2="${height - pad}" />`;
  const axisX = `<line class="chartAxis" x1="${pad}" y1="${height - pad}" x2="${width - pad}" y2="${height - pad}" />`;

  const paths = series.map(s => {
    const d = s.values.map((v, idx) => {
      const x = pad + xStep * idx;
      const y = yScale(v);
      return `${idx === 0 ? "M" : "L"} ${x} ${y}`;
    }).join(" ");
    return `<path class="chartLine" stroke="${s.color}" d="${d}" />`;
  }).join("");

  const legend = series.map(s => `
    <div class="chartLegendItem">
      <span class="chartLegendSwatch" style="background:${s.color}"></span>
      <span class="chartLegendLabel">${safeText(s.username)}</span>
    </div>
  `).join("");

  leaderboardChartEl.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" width="100%" height="${height}" role="img" aria-label="Daily points chart">
      ${axisY}
      ${axisX}
      ${paths}
    </svg>
    <div class="chartLegend">${legend}</div>
  `;
}

async function sendPresence(username) {
  try {
    await fetch("/api/presence", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, sessionId: presenceSessionId })
    });
  } catch {
    // Ignore presence failures.
  }
}

function sendPresenceRemove() {
  const me = clampUsername(meInput.value);
  if (!me) return;
  const payload = JSON.stringify({ username: me, sessionId: presenceSessionId });
  try {
    if (navigator.sendBeacon) {
      navigator.sendBeacon("/api/presence/remove", payload);
      return;
    }
  } catch {
    // fall through
  }
  fetch("/api/presence/remove", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: payload
  }).catch(() => {});
}

function renderPresence(users, me) {
  if (!onlineUsersEl) return;
  onlineUsersEl.innerHTML = "";
  if (!users.length) {
    onlineUsersEl.innerHTML = `<div class="meta">No active viewers yet.</div>`;
    return;
  }
  for (const u of users) {
    const div = document.createElement("div");
    div.className = "onlineUser";
    div.textContent = u === me ? `${u} (you)` : u;
    onlineUsersEl.appendChild(div);
  }
}

function startPresence() {
  const me = clampUsername(meInput.value);
  if (!me) {
    if (onlineHintEl) onlineHintEl.textContent = "Set your username to appear online.";
    return;
  }
  if (onlineHintEl) onlineHintEl.textContent = "Active viewers appear here.";
  if (presenceTimer) clearInterval(presenceTimer);

  const tick = async () => {
    await sendPresence(me);
    try {
      const data = await fetchPresence();
      renderPresence(data?.results || [], me);
    } catch {
      // ignore
    }
  };
  tick();
  presenceTimer = setInterval(tick, PRESENCE_PING_MS);
}

window.addEventListener("pagehide", sendPresenceRemove);

async function fetchJson(url, { silent = false } = {}) {
  const apiKey = (apiKeyInput?.value || "").trim();
  const headers = apiKey ? { "x-ra-api-key": apiKey } : {};
  const maxRetries = 2;
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      const res = await fetch(url, { headers });
      if (res.status === 429 && attempt < maxRetries) {
        const delayMs = 750 * Math.pow(2, attempt);
        if (!silent) setStatus(`Rate limited by RA API. Retrying in ${Math.round(delayMs / 1000)}s...`);
        await sleep(delayMs);
        continue;
      }
      if (!res.ok) {
        const text = await res.text().catch(() => "");
        throw new Error(text || `Request failed: ${res.status}`);
      }
      if (attempt > 0 && !silent) setStatus("");
      return res.json();
    } catch (e) {
      if (attempt >= maxRetries) throw e;
      const delayMs = 750 * Math.pow(2, attempt);
      if (!silent) setStatus(`Connection issue. Retrying in ${Math.round(delayMs / 1000)}s...`);
      await sleep(delayMs);
    }
  }
  throw new Error("Request failed after retries.");
}

async function fetchMonthly(username) {
  return fetchJson(`/api/monthly/${encodeURIComponent(username)}`);
}

async function fetchDaily(username) {
  return fetchJson(`/api/daily/${encodeURIComponent(username)}`);
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

async function fetchUserSummary(username, opts) {
  const u = encodeURIComponent(username);
  return fetchJson(`/api/user-summary/${u}`, opts);
}

async function fetchGameAchievements(username, gameId) {
  const u = encodeURIComponent(username);
  return fetchJson(`/api/game-achievements/${u}/${encodeURIComponent(gameId)}`);
}

async function fetchGameTimes(username, gameId) {
  const u = encodeURIComponent(username);
  return fetchJson(`/api/game-times/${u}/${encodeURIComponent(gameId)}`);
}

function setStatus(msg) {
  statusEl.textContent = msg || "";
}

function setLoading(el, isLoading) {
  if (!el) return;
  el.hidden = !isLoading;
}

function formatCountdown(ms) {
  const total = Math.max(0, Math.ceil(ms / 1000));
  const m = Math.floor(total / 60);
  const s = String(total % 60).padStart(2, "0");
  return `${m}:${s}`;
}

function setNextRefresh(delayMs = AUTO_REFRESH_MS) {
  nextRefreshAt = Date.now() + delayMs;
  if (refreshCountdownEl) {
    refreshCountdownEl.textContent = `Auto refresh in ${formatCountdown(delayMs)}`;
  }
}

function startRefreshCountdown() {
  if (refreshCountdownTimer) return;
  refreshCountdownTimer = setInterval(() => {
    if (!nextRefreshAt) return;
    const remaining = nextRefreshAt - Date.now();
    if (remaining <= 0) {
      refreshLeaderboard();
      refreshRecentAchievements();
      refreshRecentTimes();
      setNextRefresh();
      return;
    }
    if (refreshCountdownEl) {
      refreshCountdownEl.textContent = `Auto refresh in ${formatCountdown(remaining)}`;
    }
  }, 1000);
}

function resetRefreshCountdown() {
  setNextRefresh();
  startRefreshCountdown();
}

function openAddFriendModal() {
  if (!addFriendModal) return;
  addFriendModal.hidden = false;
  if (addFriendErrorEl) addFriendErrorEl.textContent = "";
  if (addFriendLoadingEl) addFriendLoadingEl.hidden = true;
  friendInput.focus();
}

function closeAddFriendModal() {
  if (!addFriendModal) return;
  addFriendModal.hidden = true;
  if (friendInput) friendInput.value = "";
  if (addFriendErrorEl) addFriendErrorEl.textContent = "";
  if (addFriendLoadingEl) addFriendLoadingEl.hidden = true;
}

async function addFriendFromModal() {
  const me = ensureUsername();
  const u = clampUsername(friendInput.value);
  if (!me) return setStatus("Set your username first.");
  if (!u) return;
  if (u.toLowerCase() === me.toLowerCase()) return setStatus("That's you. Add someone else.");

  try {
    if (addFriendLoadingEl) addFriendLoadingEl.hidden = false;
    await fetchUserSummary(u, { silent: true });
  } catch (e) {
    const msg = String(e?.message || "");
    const notFound = msg.includes("404") || msg.toLowerCase().includes("not found");
    const apiKeyMissing = msg.toLowerCase().includes("missing ra api key");
    const label = apiKeyMissing
      ? "API key required to validate this user."
      : notFound ? `User not found: ${u}` : `Could not verify user: ${u}`;
    if (addFriendErrorEl) addFriendErrorEl.textContent = label;
    if (addFriendLoadingEl) addFriendLoadingEl.hidden = true;
    return;
  }

  if (!friends.includes(u)) friends.push(u);
  friendInput.value = "";
  saveState();
  closeAddFriendModal();
  if (addFriendLoadingEl) addFriendLoadingEl.hidden = true;

  refreshLeaderboard();
  refreshRecentAchievements();
  refreshRecentTimes();
}

function openSettings() {
  if (!settingsModal) return;
  settingsModal.hidden = false;
  meInput.focus();
}

function closeSettings() {
  if (!settingsModal) return;
  settingsModal.hidden = true;
}

function ensureUsername() {
  const existing = clampUsername(meInput.value);
  if (existing) return existing;

  if (usernameModal) {
    usernameModal.hidden = false;
    if (usernameModalInput) {
      usernameModalInput.value = "";
      usernameModalInput.focus();
    }
    if (usernameModalApiKeyInput) {
      usernameModalApiKeyInput.value = "";
    }
    if (usernameModalErrorEl) usernameModalErrorEl.textContent = "";
    if (usernameModalLoadingEl) usernameModalLoadingEl.hidden = true;
  }
  return "";
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

function setActiveCompareTab(name) {
  compareTabButtons.forEach(btn => {
    const isActive = btn.dataset.tab === name;
    btn.classList.toggle("active", isActive);
    btn.setAttribute("aria-selected", isActive ? "true" : "false");
  });

  compareTabPanels.forEach(panel => {
    const isActive = panel.id === `compare-tab-${name}`;
    panel.classList.toggle("active", isActive);
  });
}

function renderLeaderboard(rows, me) {
  tbody.innerHTML = "";
  cacheSet("leaderboard", { rows, me });
  renderLeaderboardChart(rows);
  const total = rows.length;
  for (const r of rows) {
    const tr = document.createElement("tr");

    const delta = r.deltaVsYou;
    const cls = delta > 0 ? "delta-neg" : delta < 0 ? "delta-pos" : "delta-zero";
    const isMe = (r.username && me) ? r.username.toLowerCase() === me.toLowerCase() : false;
    tr.innerHTML = `
      <td><button class="linkBtn" type="button" data-profile="${safeText(r.username)}"><span class="nameRank">${rows.indexOf(r) + 1}.</span> <strong>${safeText(r.username)}</strong></button>${isMe ? ' <span class="note">(you)</span>' : ""}</td>
      <td>
        <strong>${Math.round(r.points)}</strong>
        ${r.dailyPoints !== null ? `<span class="dailyPoints">(+${Math.round(r.dailyPoints)})</span>` : ""}
      </td>
      <td class="${cls}"><strong>${delta > 0 ? "+" : ""}${Math.round(delta)}</strong></td>
      <td>${r.unlocks}</td>
      <td>${r.nowPlayingHtml || r.nowPlayingText || ""}</td>
      <td style="text-align:right;">
        ${isMe ? "" : `<button class="smallBtn" data-remove="${safeText(r.username)}">Remove</button>`}
      </td>
    `;

    tbody.appendChild(tr);

    const nameBtn = tr.querySelector("button[data-profile]");
    if (nameBtn) {
      if (total > 1) {
        const idx = rows.indexOf(r);
        const ratio = Math.min(1, Math.max(0, idx / (total - 1)));
        const startHue = 45; // gold
        const endHue = 215; // blue
        const hue = Math.round(startHue + (endHue - startHue) * ratio);
        r.nameColor = `hsl(${hue}, 70%, 60%)`;
        nameBtn.style.setProperty("--name-color", r.nameColor);
      }
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


function renderProfileGamesList(games, emptyMessage) {
  profileSharedGamesEl.innerHTML = "";

  if (!games.length) {
    profileSharedGamesEl.innerHTML = `<div class="meta">${safeText(emptyMessage)}</div>`;
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

    const meta = document.createElement("div");
    meta.className = "tileMeta";
    meta.textContent = "Achievements: --/--";

    tile.appendChild(img);
    tile.appendChild(title);
    tile.appendChild(meta);
    profileSharedGamesEl.appendChild(tile);

    const open = () => openGameCompare(g);
    tile.addEventListener("click", open);
    tile.addEventListener("keydown", (e) => {
      if (e.key === "Enter" || e.key === " ") { e.preventDefault(); open(); }
    });
    const cached = profileGameAchievementCounts.get(String(g.gameId ?? ""));
    if (cached) {
      setTileAchievementMeta(meta, cached);
    } else if (g.gameId) {
      loadProfileGameAchievements(g.gameId, meta);
    }
  }
}

function applyProfileGameFilter() {
  const query = (profileGameSearchEl?.value || "").trim().toLowerCase();
  const base = profileDisplayedGames || [];

  if (query && !profileAllGamesLoaded) {
    if (profileSkipAutoLoadOnce) {
      profileSkipAutoLoadOnce = false;
    } else {
    if (!profileAutoLoadingAll) {
      profileAutoLoadingAll = true;
      profileSharedGamesEl.innerHTML = `<div class="meta">Loading more games...</div>`;
      loadAllProfileGames().finally(() => {
        profileAutoLoadingAll = false;
      });
    }
    return;
    }
  }

  if (!query) {
    renderProfileGamesList(base, profileGamesEmptyMessage);
    return;
  }

  const filtered = base.filter(g => (g.title || "").toLowerCase().includes(query));
  renderProfileGamesList(filtered, "No games match your search.");
}

function formatAchievementValue(count) {
  if (!count || count.earned === null || count.total === null) return "--/--";
  return `${count.earned}/${count.total}`;
}

function setTileAchievementMeta(metaEl, counts) {
  const meText = formatAchievementValue(counts.me);
  const themText = formatAchievementValue(counts.them);
  metaEl.innerHTML = `Achievements: <span class="me">${meText}</span> <span class="sep">|</span> <span class="them">${themText}</span>`;
}

async function loadProfileGameAchievements(gameId, metaEl) {
  const target = clampUsername(currentProfileUser);
  const { me } = getUsersIncludingMe();
  if (!target || !me || !metaEl) return;

  const key = String(gameId ?? "");
  if (!key) return;

  const cached = profileGameAchievementCounts.get(key);
  if (cached) {
    setTileAchievementMeta(metaEl, cached);
    return;
  }

  const pending = profileGameAchievementPending.get(key);
  if (pending) {
    pending.then((counts) => {
      if (clampUsername(currentProfileUser) === target) {
        setTileAchievementMeta(metaEl, counts);
      }
    });
    return;
  }

  metaEl.textContent = "Achievements: Loading...";
  const targetAtRequest = target;
  const promise = profileAchievementLimiter(async () => {
    const [mine, theirs] = await Promise.all([
      fetchGameAchievements(me, gameId).catch(() => null),
      fetchGameAchievements(target, gameId).catch(() => null)
    ]);
    const mineList = mine?.achievements || null;
    const theirsList = theirs?.achievements || null;
    const mineCount = mineList
      ? { earned: mineList.filter(a => a.earned).length, total: mineList.length }
      : { earned: null, total: null };
    const theirCount = theirsList
      ? { earned: theirsList.filter(a => a.earned).length, total: theirsList.length }
      : { earned: null, total: null };
    const counts = { me: mineCount, them: theirCount };
    profileGameAchievementCounts.set(key, counts);
    return counts;
  }).finally(() => {
    profileGameAchievementPending.delete(key);
  });

  profileGameAchievementPending.set(key, promise);

  try {
    const counts = await promise;
    if (clampUsername(currentProfileUser) === targetAtRequest) {
      setTileAchievementMeta(metaEl, counts);
    }
  } catch {
    if (clampUsername(currentProfileUser) === targetAtRequest) {
      metaEl.textContent = "Achievements: --/--";
    }
  }
}

function renderSharedGames(games, emptyMessage = "No shared games found in recent play history.") {
  profileDisplayedGames = games;
  profileGamesEmptyMessage = emptyMessage;
  applyProfileGameFilter();
}

function mergeGameLists(primary, secondary) {
  const seen = new Set();
  const merged = [];
  for (const list of [primary, secondary]) {
    for (const g of list) {
      const key = String(g.gameId ?? "");
      if (!key || seen.has(key)) continue;
      seen.add(key);
      merged.push({
        gameId: g.gameId,
        title: g.title || `Game ${safeText(g.gameId)}`,
        imageIcon: g.imageIcon
      });
    }
  }
  return merged;
}

async function loadAllProfileGames() {
  const target = clampUsername(currentProfileUser);
  if (!target || !profileShowMoreBtn) return;

  profileShowMoreBtn.disabled = true;
  profileShowMoreBtn.textContent = "Loading...";
  setLoading(profileLoadingEl, true);

  try {
    const theirs = await fetchRecentGames(target, 200);
    const allGames = (theirs.results || []).map(g => ({
      gameId: g.gameId,
      title: g.title,
      imageIcon: g.imageIcon
    }));

    const combined = mergeGameLists(profileSharedGames, allGames);
    renderSharedGames(combined, "No games found for this user.");

    if (profileGamesNoteEl) {
      profileGamesNoteEl.textContent = "Shared games plus their full recent list.";
    }

    profileAllGamesLoaded = true;
    profileShowMoreBtn.disabled = false;
    profileShowMoreBtn.textContent = "Show less";
  } catch (e) {
    profileShowMoreBtn.disabled = false;
    profileShowMoreBtn.textContent = "Show more";
    setStatus(e?.message || "Failed to load more games.");
  } finally {
    setLoading(profileLoadingEl, false);
  }
}

function collapseProfileGames() {
  profileAllGamesLoaded = false;
  profileDisplayedGames = profileSharedGames;
  profileGamesEmptyMessage = "No shared games found in recent play history.";
  if (profileGamesNoteEl) {
    profileGamesNoteEl.textContent = "Shows recently played games you both have in common.";
  }
  if (profileShowMoreBtn) {
    profileShowMoreBtn.textContent = "Show more";
  }
  if ((profileGameSearchEl?.value || "").trim()) {
    profileSkipAutoLoadOnce = true;
  }
  renderSharedGames(profileSharedGames, profileGamesEmptyMessage);
}

function renderProfileSummary(summary) {
  profileSummaryEl.innerHTML = "";

  if (!summary) {
    profileSummaryEl.innerHTML = `<div class="meta">No profile summary available.</div>`;
    return;
  }

  function formatProfileValue(label, value) {
    if (label === "Last Activity") {
      if (value && typeof value === "object") {
        const candidate =
          value.Date ?? value.date ??
          value.LastActivity ?? value.lastActivity ??
          value.LastUpdated ?? value.lastUpdated ??
          value.timestamp ?? value.time ??
          value.LastPlayed ?? value.lastPlayed;
        if (candidate) return formatDate(candidate);
        for (const [k, v] of Object.entries(value)) {
          if (typeof v === "string" && k.toLowerCase().includes("date")) {
            return formatDate(v);
          }
        }
        return "";
      }
      return formatDate(value);
    }
    if (value instanceof Date) return formatDate(value);
    if (value && typeof value === "object") return "";
    return String(value);
  }

  const items = [
    ["Rank", summary.rank],
    ["Member Since", summary.memberSince],
    ["Last Activity", summary.lastActivity],
    ["Completed Games", summary.completedGames]
  ].filter(([, v]) => v !== undefined && v !== null && v !== "");

  if (!items.length) {
    profileSummaryEl.innerHTML = `<div class="meta">Profile summary unavailable.</div>`;
    return;
  }

  for (const [label, value] of items) {
    const displayValue = formatProfileValue(label, value);
    if (!displayValue && displayValue !== 0) continue;
    const card = document.createElement("div");
    card.className = "summaryCard";

    const l = document.createElement("div");
    l.className = "summaryLabel";
    l.textContent = label;

    const v = document.createElement("div");
    v.className = "summaryValue";
    v.textContent = displayValue;

    card.appendChild(l);
    card.appendChild(v);
    profileSummaryEl.appendChild(card);
  }
}

function renderProfileInsights({ sharedCount, meSummary, themSummary }) {
  if (!profileInsightsEl) return;
  profileInsightsEl.innerHTML = "";

  const formatNum = (v) => {
    if (v === null || v === undefined || v === "") return null;
    const n = Number(v);
    return Number.isNaN(n) ? null : n;
  };
  const fmt = (v) => (v === null ? "--" : v.toLocaleString());
  const formatLabel = (label, meVal, themVal) => {
    const me = formatNum(meVal);
    const them = formatNum(themVal);
    if (me === null && them === null) return null;
    return {
      label,
      valueHtml: `
        <span class="me">You ${fmt(me)}</span>
        <span class="sep">|</span>
        <span class="them">Them ${fmt(them)}</span>
      `
    };
  };

  const rows = [
    { label: "Shared Games", valueHtml: `<strong>${Number(sharedCount ?? 0).toLocaleString()}</strong>` },
    formatLabel("Total Points", meSummary?.totalPoints, themSummary?.totalPoints),
    formatLabel("Retro Points", meSummary?.retroPoints, themSummary?.retroPoints),
    formatLabel("Completed Games", meSummary?.completedGames, themSummary?.completedGames),
    formatLabel("Rank", meSummary?.rank, themSummary?.rank)
  ].filter(Boolean);

  if (!rows.length) {
    profileInsightsEl.innerHTML = `<div class="meta">Profile insights unavailable.</div>`;
    return;
  }

  for (const row of rows) {
    const card = document.createElement("div");
    card.className = "insightCard";

    const label = document.createElement("div");
    label.className = "insightLabel";
    label.textContent = row.label;

    const value = document.createElement("div");
    value.className = "insightValue";
    value.innerHTML = row.valueHtml;

    card.appendChild(label);
    card.appendChild(value);
    profileInsightsEl.appendChild(card);
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
  profileSummaryEl.innerHTML = `<div class="meta">Loading profile summary...</div>`;
  if (profileInsightsEl) profileInsightsEl.innerHTML = `<div class="meta">Loading profile insights...</div>`;
  profileSharedGamesEl.innerHTML = `<div class="meta">Loading shared games...</div>`;
  currentProfileUser = target;
  profileSharedGames = [];
  profileDisplayedGames = [];
  profileAllGamesLoaded = false;
  profileGamesEmptyMessage = "No shared games found in recent play history.";
  profileAutoLoadingAll = false;
  profileSkipAutoLoadOnce = false;
  profileGameAchievementCounts = new Map();
  profileGameAchievementPending = new Map();
  if (profileLegendMeEl) {
    profileLegendMeEl.textContent = me || "You";
  }
  if (profileLegendThemEl) {
    profileLegendThemEl.textContent = target || "Friend";
  }
  if (profileShowMoreBtn) {
    profileShowMoreBtn.disabled = false;
    profileShowMoreBtn.textContent = "Show more";
  }
  if (profileGamesNoteEl) {
    profileGamesNoteEl.textContent = "Shows recently played games you both have in common.";
  }
  if (profileGameSearchEl) {
    profileGameSearchEl.value = "";
  }

  setLoading(profileLoadingEl, true);

  try {
    const count = 60;
    const [mine, theirs, meSummary, themSummary] = await Promise.all([
      fetchRecentGames(me, count),
      fetchRecentGames(target, count),
      fetchUserSummary(me).catch(() => null),
      fetchUserSummary(target).catch(() => null)
    ]);

    if (themSummary) {
      renderProfileSummary(themSummary);
    } else {
      profileSummaryEl.innerHTML = `<div class="meta">Profile summary unavailable.</div>`;
    }

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

    profileSharedGames = unique;
    renderSharedGames(unique);
    renderProfileInsights({
      sharedCount: unique.length,
      meSummary,
      themSummary
    });
    profilePanel.scrollIntoView({ behavior: "smooth", block: "start" });
  } catch (e) {
    profileSummaryEl.innerHTML = `<div class="meta">Failed to load profile summary.</div>`;
    if (profileInsightsEl) profileInsightsEl.innerHTML = `<div class="meta">Failed to load profile insights.</div>`;
    profileSharedGamesEl.innerHTML = `<div class="meta">Failed to load shared games.</div>`;
    setStatus(e?.message || "Failed to load profile.");
  } finally {
    setLoading(profileLoadingEl, false);
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
    const points = (a.points !== undefined && a.points !== null) ? ` (${a.points} pts)` : "";
    desc.textContent = `${a.description || ""}${points}`;

    main.appendChild(title);
    main.appendChild(desc);

    const statusWrap = document.createElement("div");
    statusWrap.className = "statusWrap";

    const status = document.createElement("div");
    status.className = `statusPill ${a.statusClass}`;
    status.textContent = a.statusLabel;

    const pointsPill = document.createElement("div");
    pointsPill.className = "statusPill points";
    pointsPill.textContent = `+${a.points ?? 0} pts`;

    statusWrap.appendChild(status);
    statusWrap.appendChild(pointsPill);

    row.appendChild(img);
    row.appendChild(main);
    row.appendChild(statusWrap);
    compareAchievementsEl.appendChild(row);
  }
}

function renderCompareTimes(items) {
  compareTimesEl.innerHTML = "";

  if (!items.length) {
    compareTimesEl.innerHTML = `<div class="meta">No leaderboard scores found for this game.</div>`;
    return;
  }

  for (const t of items) {
    const row = document.createElement("div");
    row.className = "compareItem";

    const main = document.createElement("div");
    main.className = "compareMain";

    const title = document.createElement("div");
    title.className = "compareTitle";
    title.textContent = t.leaderboardTitle || "Leaderboard";

    const desc = document.createElement("div");
    desc.className = "compareDesc";
    desc.textContent = t.format ? `Format: ${t.format}` : "";

    main.appendChild(title);
    main.appendChild(desc);

    const cols = document.createElement("div");
    cols.className = "timeCols";

    const mineCol = document.createElement("div");
    mineCol.className = "timeCol";
    mineCol.innerHTML = `
      <div class="meta">You</div>
      <div class="statusPill ${t.me ? "me" : "none"}">${t.me || "No time"}</div>
    `;

    const theirsCol = document.createElement("div");
    theirsCol.className = "timeCol";
    theirsCol.innerHTML = `
      <div class="meta">Them</div>
      <div class="statusPill ${t.them ? "them" : "none"}">${t.them || "No time"}</div>
    `;

    cols.appendChild(mineCol);
    cols.appendChild(theirsCol);

    row.appendChild(main);
    row.appendChild(cols);
    compareTimesEl.appendChild(row);
  }
}

async function openGameCompare(game) {
  const target = clampUsername(currentProfileUser);
  if (!target) return;

  const { me } = getUsersIncludingMe();
  if (!me) return setStatus("Set your username first.");

  profilePanel.hidden = true;
  comparePanel.hidden = false;
  setActiveCompareTab("achievements");
  compareTitleGameEl.textContent = game.title || `Game ${safeText(game.gameId)}`;
  compareMetaEl.textContent = `You: ${me} | Friend: ${target}`;
  compareAchievementsEl.innerHTML = `<div class="meta">Loading achievements...</div>`;
  compareTimesEl.innerHTML = `<div class="meta">Loading scores...</div>`;
  setLoading(compareLoadingEl, true);

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
        points: ma?.points ?? ta?.points,
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

  try {
    const [mineTimes, theirsTimes] = await Promise.all([
      fetchGameTimes(me, game.gameId),
      fetchGameTimes(target, game.gameId)
    ]);

    const mineMap = new Map((mineTimes.results || []).map(lb => [lb.leaderboardId, lb]));
    const theirsMap = new Map((theirsTimes.results || []).map(lb => [lb.leaderboardId, lb]));
    const ids = new Set([...mineMap.keys(), ...theirsMap.keys()]);

    const merged = [];
    for (const id of ids) {
      const m = mineMap.get(id);
      const t = theirsMap.get(id);
      const mineLabel = m ? `#${m.rank} ${m.formattedScore ?? m.score ?? ""}`.trim() : "";
      const theirLabel = t ? `#${t.rank} ${t.formattedScore ?? t.score ?? ""}`.trim() : "";

      merged.push({
        leaderboardId: id,
        leaderboardTitle: m?.leaderboardTitle || t?.leaderboardTitle,
        format: m?.format || t?.format,
        me: mineLabel,
        them: theirLabel,
        both: Boolean(m && t)
      });
    }

    merged.sort((a, b) => {
      const w = Number(b.both) - Number(a.both);
      if (w !== 0) return w;
      return String(a.leaderboardTitle || "").localeCompare(String(b.leaderboardTitle || ""));
    });

    renderCompareTimes(merged);
  } catch (e) {
    compareTimesEl.innerHTML = `<div class="meta">Failed to load scores.</div>`;
  } finally {
    setLoading(compareLoadingEl, false);
  }
}

function renderRecentAchievements(items) {
  recentAchievementsEl.innerHTML = "";
  recentAchievementsItems = items;
  cacheSet("recentAchievements", { items });
  if (!items.length) {
    recentAchievementsVisible = RECENT_DEFAULT_ROWS;
    recentAchievementsEl.innerHTML = `<div class="meta">No recent achievements in this window.</div>`;
    if (recentAchievementsToggleBtn) recentAchievementsToggleBtn.hidden = true;
    if (recentAchievementsShowLessBtn) recentAchievementsShowLessBtn.hidden = true;
    return;
  }

  const visible = items.slice(0, Math.max(RECENT_DEFAULT_ROWS, recentAchievementsVisible));
  for (const a of visible) {
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
      <span class="pointsPill compact">+${a.points} pts</span>
      <span class="meta">${formatDate(a.date)}</span>
    `;

    const body = document.createElement("div");
    body.innerHTML = `
      <div><strong>${a.title}</strong></div>
      <div class="meta">${a.gameTitle}${a.consoleName ? " - " + a.consoleName : ""}</div>
      <div class="meta">${a.description || ""}</div>
    `;

    main.appendChild(title);
    main.appendChild(body);

    div.appendChild(badge);
    div.appendChild(main);
    recentAchievementsEl.appendChild(div);
  }

  if (recentAchievementsToggleBtn) {
    const canLoadMore =
      achievementsShowMoreCount < ACHIEVEMENTS_MAX_SHOW_MORE &&
      (achievementsLookbackHours < ACHIEVEMENTS_MAX_HOURS ||
      items.length >= achievementsMaxResults);
    recentAchievementsToggleBtn.hidden = !canLoadMore;
    recentAchievementsToggleBtn.textContent = "Show more";
  }
  if (recentAchievementsShowLessBtn) {
    recentAchievementsShowLessBtn.hidden = recentAchievementsVisible <= RECENT_DEFAULT_ROWS;
  }
}


function renderRecentTimes(items) {
  recentTimesEl.innerHTML = "";
  recentTimesItems = items;
  cacheSet("recentTimes", { items });
  if (!items.length) {
    recentTimesVisible = RECENT_DEFAULT_ROWS;
    recentTimesEl.innerHTML = `<div class="meta">No recent leaderboard scores found (based on recently played games).</div>`;
    if (recentTimesToggleBtn) recentTimesToggleBtn.hidden = true;
    return;
  }

  const visible = items.slice(0, Math.max(RECENT_DEFAULT_ROWS, recentTimesVisible));
  for (const t of visible) {
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
      <span class="pill mono scorePill">${t.formattedScore ?? t.score}</span>
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

  if (recentTimesToggleBtn) {
    recentTimesToggleBtn.hidden = visible.length >= items.length;
    recentTimesToggleBtn.textContent = "Show more";
  }
}

function getUsersIncludingMe() {
  const me = clampUsername(meInput.value);
  const users = Array.from(new Set([me, ...friends].map(clampUsername).filter(Boolean)));
  return { me, users };
}

async function refreshLeaderboard() {
  const ensured = ensureUsername();
  if (!ensured) return;
  const { me, users } = getUsersIncludingMe();
  if (!me) return;

  const cached = cacheGet("leaderboard");
  if (cached?.rows?.length) {
    renderLeaderboard(cached.rows, cached.me || me);
  }

  setLoading(leaderboardLoadingEl, true);

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
      nowPlayingHtml: "Loading...",
      dailyPoints: null
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

    const presencePromise = (async () => {
      const presencePairs = await Promise.all(users.map(async (u) => {
        try {
          const p = await fetchNowPlayingWithRetry(u, win, {
            retries: 4,
            delayMs: 1000,
            onRetry: (attempt) => {
              const row = rows.find(r => r.username === u);
              if (row) {
                row.nowPlayingHtml = `Loading... (retry ${attempt}/4)`;
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
          const icon = p.imageIcon ? `<img class="nowPlayingIcon" src="${iconUrl(p.imageIcon)}" alt="game" loading="lazy" />` : "";
          const details = p.richPresence ? `<div class="nowPlayingDetail">${safeText(p.richPresence)}</div>` : "";
          if (p.nowPlaying) {
            r.nowPlayingHtml = `
              <div class="nowPlayingWrap">
                <div class="nowPlaying">${icon}<span>&#9654; ${safeText(p.title)}</span></div>
                ${details}
              </div>
            `;
          } else {
            r.nowPlayingHtml = `
              <div class="nowPlayingWrap">
                <div class="nowPlaying">${icon}<span>${safeText(p.title)}${age ? ` (${age})` : ""}</span></div>
                ${details}
              </div>
            `;
          }
        } else {
          r.nowPlayingHtml = "";
        }
      }

      renderLeaderboard(rows, me);
    })().catch(() => {
      // Ignore background errors; leaderboard already rendered.
    });
    presencePromise.finally(() => setLoading(leaderboardLoadingEl, false));

    // 3) Fetch daily points in the background.
    (async () => {
      const dailyPairs = await Promise.all(users.map(async (u) => {
        try {
          const d = await fetchDaily(u);
          return [u, d?.points ?? 0];
        } catch {
          return [u, null];
        }
      }));
      const daily = Object.fromEntries(dailyPairs);
      for (const r of rows) {
        r.dailyPoints = daily[r.username];
      }
      updateDailyHistory(rows);
      renderLeaderboard(rows, me);
    })().catch(() => {});

  } catch (e) {
    setLoading(leaderboardLoadingEl, false);
    setStatus(e?.message || "Failed to load leaderboard.");
  }
}


async function refreshRecentAchievements({ reset = true } = {}) {
  const { me, users } = getUsersIncludingMe();
  if (!me) return;

  const cached = cacheGet("recentAchievements");
  if (cached?.items?.length) {
    renderRecentAchievements(cached.items);
  }

  if (reset) {
    achievementsLookbackHours = ACHIEVEMENTS_DEFAULT_HOURS;
    recentAchievementsVisible = RECENT_DEFAULT_ROWS;
    achievementsMaxResults = ACHIEVEMENTS_DEFAULT_MAX;
    achievementsShowMoreCount = 0;
  }

  setLoading(recentAchievementsLoadingEl, true);
  const minutes = Math.max(1, Math.floor(achievementsLookbackHours * 60));
  const perUserLimit = 10;

  try {
    const payloads = await Promise.all(
      users.map(u => fetchRecentAchievements(u, minutes, perUserLimit).catch(() => ({ results: [] })))
    );

    const combined = payloads.flatMap(p => p.results || []);
    combined.sort((a, b) => (Date.parse(b.date || "") || 0) - (Date.parse(a.date || "") || 0));
    renderRecentAchievements(combined.slice(0, achievementsMaxResults));
  } catch {
    // keep quiet; leaderboard is primary
  } finally {
    setLoading(recentAchievementsLoadingEl, false);
  }
}

async function refreshRecentTimes() {
  const { me, users } = getUsersIncludingMe();
  if (!me) return;

  const cached = cacheGet("recentTimes");
  if (cached?.items?.length) {
    renderRecentTimes(cached.items);
  }

  setLoading(recentTimesLoadingEl, true);
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
    recentTimesVisible = RECENT_DEFAULT_ROWS;
    renderRecentTimes(combined.slice(0, 30));

    if (!combined.length && errors.length) {
      // show the first error so it doesn't look like "nothing happened"
      setStatus(`Score fetch issue: ${errors[0]}`);
    }
  } catch (e) {
    setStatus(e?.message || "Failed to load recent scores.");
  } finally {
    setLoading(recentTimesLoadingEl, false);
  }
}

// UI wiring
tabButtons.forEach(btn => {
  btn.addEventListener("click", () => setActiveTab(btn.dataset.tab));
});

compareTabButtons.forEach(btn => {
  btn.addEventListener("click", () => setActiveCompareTab(btn.dataset.tab));
});

if (profileShowMoreBtn) {
  profileShowMoreBtn.addEventListener("click", () => {
    if (profileAllGamesLoaded) {
      collapseProfileGames();
    } else {
      loadAllProfileGames();
    }
  });
}

if (profileGameSearchEl) {
  profileGameSearchEl.addEventListener("input", applyProfileGameFilter);
}

if (recentAchievementsToggleBtn) {
  recentAchievementsToggleBtn.addEventListener("click", () => {
    if (achievementsShowMoreCount >= ACHIEVEMENTS_MAX_SHOW_MORE) {
      recentAchievementsToggleBtn.hidden = true;
      return;
    }
    const elapsed = Date.now() - achievementsLoadStart;
    if (elapsed < ACHIEVEMENTS_MIN_WAIT_MS) {
      setLoading(recentAchievementsLoadingEl, true);
      setTimeout(() => {
        recentAchievementsToggleBtn.click();
      }, ACHIEVEMENTS_MIN_WAIT_MS - elapsed);
      return;
    }
    achievementsShowMoreCount += 1;
    achievementsLookbackHours = Math.min(ACHIEVEMENTS_MAX_HOURS, achievementsLookbackHours + ACHIEVEMENTS_STEP_HOURS);
    achievementsMaxResults += ACHIEVEMENTS_STEP_MAX;
    recentAchievementsVisible = Math.min(
      recentAchievementsVisible + RECENT_STEP_ROWS,
      achievementsMaxResults
    );
    refreshRecentAchievements({ reset: false });
  });
}

if (recentAchievementsShowLessBtn) {
  recentAchievementsShowLessBtn.addEventListener("click", () => {
    recentAchievementsVisible = RECENT_DEFAULT_ROWS;
    achievementsMaxResults = Math.max(ACHIEVEMENTS_DEFAULT_MAX, achievementsMaxResults);
    achievementsShowMoreCount = 0;
    renderRecentAchievements(recentAchievementsItems);
  });
}

if (recentTimesToggleBtn) {
  recentTimesToggleBtn.addEventListener("click", () => {
    recentTimesVisible = Math.min(
      recentTimesVisible + RECENT_STEP_ROWS,
      recentTimesItems.length
    );
    renderRecentTimes(recentTimesItems);
  });
}

if (tabButtons.length) {
  setActiveTab(tabButtons[0].dataset.tab);
}

if (addFriendOpenBtn) {
  addFriendOpenBtn.addEventListener("click", openAddFriendModal);
}
if (addFriendCloseBtn) {
  addFriendCloseBtn.addEventListener("click", closeAddFriendModal);
}
if (addFriendCancelBtn) {
  addFriendCancelBtn.addEventListener("click", closeAddFriendModal);
}
if (addFriendConfirmBtn) {
  addFriendConfirmBtn.addEventListener("click", () => {
    addFriendFromModal();
  });
}

refreshBtn.addEventListener("click", () => {
  (async () => {
    refreshLeaderboard();
    await sleep(STAGGER_MS);
    refreshRecentAchievements();
    await sleep(STAGGER_MS);
    refreshRecentTimes();
    resetRefreshCountdown();
  })();
});


if (profileCloseBtn) {
  profileCloseBtn.addEventListener("click", () => {
    profilePanel.hidden = true;
    profileTitleNameEl.textContent = "";
    profileSummaryEl.innerHTML = "";
    if (profileInsightsEl) profileInsightsEl.innerHTML = "";
    profileSharedGamesEl.innerHTML = "";
    profileSharedGames = [];
    profileDisplayedGames = [];
    profileAllGamesLoaded = false;
    profileGamesEmptyMessage = "No shared games found in recent play history.";
    profileAutoLoadingAll = false;
    profileSkipAutoLoadOnce = false;
    profileGameAchievementCounts = new Map();
    profileGameAchievementPending = new Map();
    if (profileLegendMeEl) {
      profileLegendMeEl.textContent = "";
    }
    if (profileLegendThemEl) {
      profileLegendThemEl.textContent = "";
    }
    if (profileShowMoreBtn) {
      profileShowMoreBtn.disabled = false;
      profileShowMoreBtn.textContent = "Show more";
    }
    if (profileGamesNoteEl) {
      profileGamesNoteEl.textContent = "";
    }
    if (profileGameSearchEl) {
      profileGameSearchEl.value = "";
    }
    setLoading(profileLoadingEl, false);
    setLoading(compareLoadingEl, false);
    comparePanel.hidden = true;
    compareTitleGameEl.textContent = "";
    compareMetaEl.textContent = "";
    compareAchievementsEl.innerHTML = "";
    compareTimesEl.innerHTML = "";
    setActiveCompareTab("achievements");
    currentProfileUser = "";
  });
}

if (compareBackBtn) {
  compareBackBtn.addEventListener("click", () => {
    comparePanel.hidden = true;
    profilePanel.hidden = false;
    setLoading(compareLoadingEl, false);
    compareTitleGameEl.textContent = "";
    compareMetaEl.textContent = "";
    compareAchievementsEl.innerHTML = "";
    compareTimesEl.innerHTML = "";
  });
}

if (settingsBtn) {
  settingsBtn.addEventListener("click", openSettings);
}
if (settingsCloseBtn) {
  settingsCloseBtn.addEventListener("click", closeSettings);
}
if (settingsCancelBtn) {
  settingsCancelBtn.addEventListener("click", closeSettings);
}
if (settingsSaveBtn) {
  settingsSaveBtn.addEventListener("click", () => {
    meInput.value = clampUsername(meInput.value);
    saveState();
    closeSettings();
    startPresence();
    (async () => {
      refreshLeaderboard();
      await sleep(STAGGER_MS);
      refreshRecentAchievements();
      await sleep(STAGGER_MS);
      refreshRecentTimes();
      resetRefreshCountdown();
    })();
  });
}

meInput.addEventListener("keydown", (e) => {
  if (e.key === "Enter") {
    saveState();
    startPresence();
    (async () => {
      refreshLeaderboard();
      await sleep(STAGGER_MS);
      refreshRecentAchievements();
      await sleep(STAGGER_MS);
      refreshRecentTimes();
      resetRefreshCountdown();
    })();
  }
});

if (friendInput) {
  friendInput.addEventListener("keydown", (e) => {
    if (e.key === "Enter") addFriendFromModal();
  });
}

if (usernameModalConfirmBtn) {
  usernameModalConfirmBtn.addEventListener("click", () => {
    const entered = clampUsername(usernameModalInput?.value || "");
    if (!entered) {
      if (usernameModalErrorEl) usernameModalErrorEl.textContent = "Username is required.";
      return;
    }
    const apiKey = (usernameModalApiKeyInput?.value || "").trim();
    if (apiKeyInput) apiKeyInput.value = apiKey;
    (async () => {
      try {
        if (usernameModalLoadingEl) usernameModalLoadingEl.hidden = false;
        await fetchUserSummary(entered, { silent: true });
      } catch (e) {
        const msg = String(e?.message || "");
        const notFound = msg.includes("404") || msg.toLowerCase().includes("not found");
        const apiKeyMissing = msg.toLowerCase().includes("missing ra api key");
        const label = apiKeyMissing
          ? "API key required to validate this user."
          : notFound ? `User not found: ${entered}` : `Could not verify user: ${entered}`;
        if (usernameModalErrorEl) usernameModalErrorEl.textContent = label;
        if (usernameModalLoadingEl) usernameModalLoadingEl.hidden = true;
        return;
      }
      meInput.value = entered;
      saveState();
      if (usernameModal) usernameModal.hidden = true;
      if (usernameModalLoadingEl) usernameModalLoadingEl.hidden = true;
      startPresence();
      (async () => {
        refreshLeaderboard();
        await sleep(STAGGER_MS);
        refreshRecentAchievements();
        await sleep(STAGGER_MS);
        refreshRecentTimes();
        resetRefreshCountdown();
      })();
    })();
  });
}

if (usernameModalInput) {
  usernameModalInput.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && usernameModalConfirmBtn) {
      usernameModalConfirmBtn.click();
    }
  });
  usernameModalInput.addEventListener("input", () => {
    if (usernameModalErrorEl) usernameModalErrorEl.textContent = "";
  });
}

// initial load
const ensured = ensureUsername();
if (ensured) {
  (async () => {
    startPresence();
    refreshLeaderboard();
    await sleep(STAGGER_MS);
    refreshRecentAchievements();
    await sleep(STAGGER_MS);
    refreshRecentTimes();
    resetRefreshCountdown();
  })();
}
