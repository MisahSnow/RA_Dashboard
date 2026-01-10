function safeText(v){ return (v === null || v === undefined) ? "" : String(v); }

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

const LS_ME = "ra.me";
const LS_FRIENDS = "ra.friends";
const LS_API_KEY = "ra.apiKey";

const meInput = document.getElementById("meInput");
const apiKeyInput = document.getElementById("apiKeyInput");
const friendInput = document.getElementById("friendInput");
const addFriendOpenBtn = document.getElementById("addFriendOpenBtn");
const addFriendModal = document.getElementById("addFriendModal");
const addFriendConfirmBtn = document.getElementById("addFriendConfirmBtn");
const addFriendCloseBtn = document.getElementById("addFriendCloseBtn");
const addFriendCancelBtn = document.getElementById("addFriendCancelBtn");
const refreshBtn = document.getElementById("refreshBtn");
const tbody = document.querySelector("#leaderboard tbody");
const statusEl = document.getElementById("status");
const refreshCountdownEl = document.getElementById("refreshCountdown");

const settingsBtn = document.getElementById("settingsBtn");
const settingsModal = document.getElementById("settingsModal");
const settingsSaveBtn = document.getElementById("settingsSaveBtn");
const settingsCloseBtn = document.getElementById("settingsCloseBtn");
const settingsCancelBtn = document.getElementById("settingsCancelBtn");

const recentAchievementsEl = document.getElementById("recentAchievements");
const recentTimesEl = document.getElementById("recentTimes");
const recentGamesEl = document.getElementById("recentGames");
const recentAchievementsToggleBtn = document.getElementById("recentAchievementsToggleBtn");
const recentTimesToggleBtn = document.getElementById("recentTimesToggleBtn");
const leaderboardLoadingEl = document.getElementById("leaderboardLoading");
const profileLoadingEl = document.getElementById("profileLoading");
const compareLoadingEl = document.getElementById("compareLoading");
const recentAchievementsLoadingEl = document.getElementById("recentAchievementsLoading");
const recentTimesLoadingEl = document.getElementById("recentTimesLoading");

const profilePanel = document.getElementById("profilePanel");
const profileTitleNameEl = document.getElementById("profileTitleName");
const profileSummaryEl = document.getElementById("profileSummary");
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
let achievementsLookbackHours = ACHIEVEMENTS_DEFAULT_HOURS;
let achievementsMaxResults = ACHIEVEMENTS_DEFAULT_MAX;
const ACHIEVEMENTS_MIN_WAIT_MS = 5000;
const achievementsLoadStart = Date.now();

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

async function fetchJson(url) {
  const apiKey = (apiKeyInput?.value || "").trim();
  const headers = apiKey ? { "x-ra-api-key": apiKey } : {};
  const maxRetries = 2;
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      const res = await fetch(url, { headers });
      if (res.status === 429 && attempt < maxRetries) {
        const delayMs = 750 * Math.pow(2, attempt);
        setStatus(`Rate limited by RA API. Retrying in ${Math.round(delayMs / 1000)}s...`);
        await sleep(delayMs);
        continue;
      }
      if (!res.ok) {
        const text = await res.text().catch(() => "");
        throw new Error(text || `Request failed: ${res.status}`);
      }
      if (attempt > 0) setStatus("");
      return res.json();
    } catch (e) {
      if (attempt >= maxRetries) throw e;
      const delayMs = 750 * Math.pow(2, attempt);
      setStatus(`Connection issue. Retrying in ${Math.round(delayMs / 1000)}s...`);
      await sleep(delayMs);
    }
  }
  throw new Error("Request failed after retries.");
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

async function fetchUserSummary(username) {
  const u = encodeURIComponent(username);
  return fetchJson(`/api/user-summary/${u}`);
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
  friendInput.focus();
}

function closeAddFriendModal() {
  if (!addFriendModal) return;
  addFriendModal.hidden = true;
  if (friendInput) friendInput.value = "";
}

function addFriendFromModal() {
  const me = ensureUsername();
  const u = clampUsername(friendInput.value);
  if (!me) return setStatus("Set your username first.");
  if (!u) return;
  if (u.toLowerCase() === me.toLowerCase()) return setStatus("That's you. Add someone else.");

  if (!friends.includes(u)) friends.push(u);
  friendInput.value = "";
  saveState();
  closeAddFriendModal();

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

  const entered = clampUsername(window.prompt("Enter your RetroAchievements username:", "") || "");
  if (!entered) return "";

  meInput.value = entered;
  saveState();
  return entered;
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
  const total = rows.length;
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
      if (total > 1) {
        const idx = rows.indexOf(r);
        const ratio = Math.min(1, Math.max(0, idx / (total - 1)));
        const startHue = 45; // gold
        const endHue = 215; // blue
        const hue = Math.round(startHue + (endHue - startHue) * ratio);
        nameBtn.style.setProperty("--name-color", `hsl(${hue}, 70%, 60%)`);
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
  const promise = Promise.all([
    fetchGameAchievements(me, gameId).catch(() => null),
    fetchGameAchievements(target, gameId).catch(() => null)
  ]).then(([mine, theirs]) => {
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
          value.timestamp ?? value.time;
        if (candidate) return formatDate(candidate);
      }
      return formatDate(value);
    }
    if (value instanceof Date) return formatDate(value);
    if (value && typeof value === "object") return "";
    return String(value);
  }

  const items = [
    ["Total Points", summary.totalPoints],
    ["Retro Points", summary.retroPoints],
    ["Rank", summary.rank],
    ["Member Since", summary.memberSince],
    ["Last Activity", summary.lastActivity],
    ["Hardcore Points", summary.totalPointsHardcore],
    ["Softcore Points", summary.totalPointsSoftcore],
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

async function openProfile(username) {
  const target = clampUsername(username);
  if (!target) return;

  const { me } = getUsersIncludingMe();
  if (!me) return setStatus("Set your username first.");

  profilePanel.hidden = false;
  comparePanel.hidden = true;
  profileTitleNameEl.textContent = target;
  profileSummaryEl.innerHTML = `<div class="meta">Loading profile summary...</div>`;
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
    const [mine, theirs] = await Promise.all([
      fetchRecentGames(me, count),
      fetchRecentGames(target, count)
    ]);

    let summary = null;
    try {
      summary = await fetchUserSummary(target);
    } catch (e) {
      profileSummaryEl.innerHTML = `<div class="meta">Profile summary unavailable: ${safeText(e?.message || "error")}</div>`;
    }

    if (summary) renderProfileSummary(summary);

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
    profilePanel.scrollIntoView({ behavior: "smooth", block: "start" });
  } catch (e) {
    profileSummaryEl.innerHTML = `<div class="meta">Failed to load profile summary.</div>`;
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
  if (!items.length) {
    recentAchievementsVisible = RECENT_DEFAULT_ROWS;
    recentAchievementsEl.innerHTML = `<div class="meta">No recent achievements in this window.</div>`;
    if (recentAchievementsToggleBtn) recentAchievementsToggleBtn.hidden = true;
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

  if (recentAchievementsToggleBtn) {
    const canLoadMore =
      achievementsLookbackHours < ACHIEVEMENTS_MAX_HOURS ||
      items.length >= achievementsMaxResults;
    recentAchievementsToggleBtn.hidden = !canLoadMore;
    recentAchievementsToggleBtn.textContent = "Show more";
  }
}


function renderRecentTimes(items) {
  recentTimesEl.innerHTML = "";
  recentTimesItems = items;
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

    const presencePromise = (async () => {
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
          r.nowPlayingText = p.nowPlaying ? `&#9654; ${p.title}` : `${p.title}${age ? ` (${age})` : ""}`;
        } else {
          r.nowPlayingText = "";
        }
      }

      renderLeaderboard(rows, me);
    })().catch(() => {
      // Ignore background errors; leaderboard already rendered.
    });
    presencePromise.finally(() => setLoading(leaderboardLoadingEl, false));

  } catch (e) {
    setLoading(leaderboardLoadingEl, false);
    setStatus(e?.message || "Failed to load leaderboard.");
  }
}


async function refreshRecentAchievements({ reset = true } = {}) {
  const { me, users } = getUsersIncludingMe();
  if (!me) return;

  if (reset) {
    achievementsLookbackHours = ACHIEVEMENTS_DEFAULT_HOURS;
    recentAchievementsVisible = RECENT_DEFAULT_ROWS;
    achievementsMaxResults = ACHIEVEMENTS_DEFAULT_MAX;
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
    const elapsed = Date.now() - achievementsLoadStart;
    if (elapsed < ACHIEVEMENTS_MIN_WAIT_MS) {
      setLoading(recentAchievementsLoadingEl, true);
      setTimeout(() => {
        recentAchievementsToggleBtn.click();
      }, ACHIEVEMENTS_MIN_WAIT_MS - elapsed);
      return;
    }
    achievementsLookbackHours = Math.min(ACHIEVEMENTS_MAX_HOURS, achievementsLookbackHours + ACHIEVEMENTS_STEP_HOURS);
    achievementsMaxResults += ACHIEVEMENTS_STEP_MAX;
    recentAchievementsVisible = Math.min(
      recentAchievementsVisible + RECENT_STEP_ROWS,
      achievementsMaxResults
    );
    refreshRecentAchievements({ reset: false });
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
  addFriendConfirmBtn.addEventListener("click", addFriendFromModal);
}

refreshBtn.addEventListener("click", () => {
  refreshLeaderboard();
  refreshRecentAchievements();
  refreshRecentTimes();
  resetRefreshCountdown();
});


if (profileCloseBtn) {
  profileCloseBtn.addEventListener("click", () => {
    profilePanel.hidden = true;
    profileTitleNameEl.textContent = "";
    profileSummaryEl.innerHTML = "";
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
    refreshLeaderboard();
    refreshRecentAchievements();
    refreshRecentTimes();
    resetRefreshCountdown();
  });
}

meInput.addEventListener("keydown", (e) => {
  if (e.key === "Enter") {
    saveState();
    refreshLeaderboard();
    refreshRecentAchievements();
    refreshRecentTimes();
    resetRefreshCountdown();
  }
});

if (friendInput) {
  friendInput.addEventListener("keydown", (e) => {
    if (e.key === "Enter") addFriendFromModal();
  });
}

// initial load
const ensured = ensureUsername();
if (ensured) {
  refreshLeaderboard();
  refreshRecentAchievements();
  refreshRecentTimes();
  resetRefreshCountdown();
}
