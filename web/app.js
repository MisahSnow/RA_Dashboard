// RetroRivals client logic: UI state, API calls, and rendering helpers.
function safeText(v){ return (v === null || v === undefined) ? "" : String(v); }

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Client-side request queues to avoid flooding the API.
const CLIENT_REQUEST_INTERVAL_MS = 10;
const CLIENT_MAX_CONCURRENT = 4;
const clientRequestQueue = [];
let clientQueueActive = 0;
let clientQueueTimer = null;
let lastClientRequestAt = 0;
let activePageName = "dashboard";
const pausedPages = new Set();
const FAST_MAX_CONCURRENT = CLIENT_MAX_CONCURRENT;
const FAST_REQUEST_INTERVAL_MS = 100;
const fastRequestQueue = [];
let fastQueueActive = 0;
let lastFastRequestAt = 0;
let fastQueueTimer = null;

function updateQueueCounter() {
  if (apiQueueCounterEl) {
    const total = clientRequestQueue.length + fastRequestQueue.length;
    apiQueueCounterEl.textContent = `API Calls in Queue: ${total}`;
  }
}

function createAbortError() {
  try {
    return new DOMException("Aborted", "AbortError");
  } catch {
    const err = new Error("Aborted");
    err.name = "AbortError";
    return err;
  }
}

function enqueueClientFetch(url, options) {
  return new Promise((resolve, reject) => {
    if (options?.signal?.aborted) {
      reject(createAbortError());
      return;
    }
    clientRequestQueue.push({ url, options, resolve, reject, page: activePageName });
    updateQueueCounter();
    logApiQueueEvent(url, options);
    processClientQueue();
  });
}

function enqueueFastFetch(url, options) {
  return new Promise((resolve, reject) => {
    if (options?.signal?.aborted) {
      reject(createAbortError());
      return;
    }
    fastRequestQueue.push({ url, options, resolve, reject, page: activePageName });
    updateQueueCounter();
    logApiQueueEvent(url, options);
    processFastQueue();
  });
}

function dequeueNext(queue) {
  for (let i = 0; i < queue.length; i++) {
    const job = queue[i];
    if (!job?.page || !pausedPages.has(job.page)) {
      queue.splice(i, 1);
      return job;
    }
  }
  return null;
}

function processClientQueue() {
  if (clientQueueActive >= CLIENT_MAX_CONCURRENT || clientRequestQueue.length === 0) return;
  const now = Date.now();
  const waitMs = Math.max(0, CLIENT_REQUEST_INTERVAL_MS - (now - lastClientRequestAt));
  updateQueueCounter();
  if (waitMs > 0) {
    if (!clientQueueTimer) {
      clientQueueTimer = setTimeout(() => {
        clientQueueTimer = null;
        processClientQueue();
      }, waitMs);
    }
    return;
  }
  const job = dequeueNext(clientRequestQueue);
  if (!job) return;
  clientQueueActive += 1;
  lastClientRequestAt = Date.now();
  (async () => {
    if (job.options?.signal?.aborted) {
      job.reject(createAbortError());
      return;
    }
    const res = await fetch(job.url, job.options);
    logApiQueueEvent(job.url, job.options, res.status);
    if (res.status === 429 && (job.retries || 0) < 3) {
      clientRequestQueue.unshift({ ...job, retries: (job.retries || 0) + 1 });
      updateQueueCounter();
    } else {
      job.resolve(res);
    }
  })().catch((err) => {
    logApiQueueEvent(job.url, job.options, 0);
    job.reject(err);
  }).finally(() => {
    clientQueueActive -= 1;
    updateQueueCounter();
    processClientQueue();
  });
  processClientQueue();
}

function processFastQueue() {
  if (fastQueueActive >= FAST_MAX_CONCURRENT || fastRequestQueue.length === 0) return;
  const now = Date.now();
  const waitMs = Math.max(0, FAST_REQUEST_INTERVAL_MS - (now - lastFastRequestAt));
  if (waitMs > 0) {
    if (!fastQueueTimer) {
      fastQueueTimer = setTimeout(() => {
        fastQueueTimer = null;
        processFastQueue();
      }, waitMs);
    }
    return;
  }
  const job = dequeueNext(fastRequestQueue);
  if (!job) return;
  fastQueueActive += 1;
  lastFastRequestAt = Date.now();
  (async () => {
    if (job.options?.signal?.aborted) {
      job.reject(createAbortError());
      return;
    }
    const res = await fetch(job.url, job.options);
    logApiQueueEvent(job.url, job.options, res.status);
    const shouldRetry = (res.status === 429 || res.status === 423 || res.status === 503 || res.status === 504);
    if (shouldRetry && (job.retries || 0) < 10) {
      fastRequestQueue.push({ ...job, retries: (job.retries || 0) + 1 });
      updateQueueCounter();
    } else {
      job.resolve(res);
    }
  })().catch((err) => {
    logApiQueueEvent(job.url, job.options, 0);
    job.reject(err);
  }).finally(() => {
    fastQueueActive -= 1;
    updateQueueCounter();
    processFastQueue();
  });
  processFastQueue();
}

// LocalStorage keys for user settings and cached data.
const LS_API_KEY = "ra.apiKey";
const LS_USE_API_KEY = "ra.useApiKey";
const LS_CACHE_PREFIX = "ra.cache.";
const LS_CHALLENGE_LEAD_CACHE = "ra.challengeLeadCache";
const LS_DEBUG_UI = "ra.debugUi";
const LS_LEADERBOARD_RANGE = "ra.leaderboardRange";
const LS_PROFILE_COUNTS_PREFIX = "ra.profileCounts";
const LS_FIND_GAMES_CONSOLE = "ra.findGamesConsole";
const LS_FIND_GAMES_PLAYERS = "ra.findGamesPlayers";
const LS_FIND_GAMES_GENRES = "ra.findGamesGenres";
const LS_BACKLOG_PREFIX = "ra.backlog";
const friendSummaryCache = new Map();
const friendPresenceCache = new Map();
const PROFILE_COUNTS_CACHE_TTL_MS = 2 * 60 * 1000;
const RECENT_CACHE_TTL_MS = 2 * 60 * 1000;
const LS_RECENT_GAMES_PREFIX = "ra.recentGames";
const storedLeaderboardRange = localStorage.getItem(LS_LEADERBOARD_RANGE);
let leaderboardRange = ["daily", "weekly", "monthly"].includes(storedLeaderboardRange)
  ? storedLeaderboardRange
  : "monthly";
let activeChart = "daily";

// Core UI elements (settings, tabs, lists, modals).
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
const usernameModalUseApiKeyToggle = document.getElementById("usernameModalUseApiKeyToggle");
const usernameModalConfirmBtn = document.getElementById("usernameModalConfirmBtn");
const usernameModalErrorEl = document.getElementById("usernameModalError");
const usernameModalLoadingEl = document.getElementById("usernameModalLoading");
const refreshBtn = document.getElementById("refreshBtn");
const notificationsBtn = document.getElementById("notificationsBtn");
const notificationsBadge = document.getElementById("notificationsBadge");
const notificationsPanel = document.getElementById("notificationsPanel");
const notificationsCloseBtn = document.getElementById("notificationsCloseBtn");
const notificationsLoadingEl = document.getElementById("notificationsLoading");
const notificationsListEl = document.getElementById("notificationsList");
const tbody = document.querySelector("#leaderboard tbody");
const statusEl = document.getElementById("status");
const onlineUsersEl = document.getElementById("onlineUsers");
const onlineHintEl = document.getElementById("onlineHint");
const apiQueueCounterEl = document.getElementById("apiQueueCounter");
const apiQueueLogEntriesEl = document.getElementById("apiQueueLogEntries");
const systemLogEntriesEl = document.getElementById("systemLogEntries");
const apiQueueTabButtons = document.querySelectorAll(".apiQueueTab");
const LS_DEBUG_LOG_TAB = "ra.debugLogTab";
const leaderboardChartEl = document.getElementById("leaderboardChart");
const leaderboardHourlyChartEl = document.getElementById("leaderboardHourlyChart");
const leaderboardChartTitleEl = document.getElementById("leaderboardChartTitle");
const chartTabButtons = document.querySelectorAll(".chartTab");
const leaderboardPointsNoteEl = document.getElementById("leaderboardPointsNote");
const leaderboardTitleEl = document.getElementById("leaderboardTitle");
const leaderboardTabButtons = document.querySelectorAll(".leaderboardTab");
const leaderboardScopeFriendsBtn = document.getElementById("leaderboardScopeFriends");
const leaderboardScopeGroupBtn = document.getElementById("leaderboardScopeGroup");
const leaderboardGroupSelect = document.getElementById("leaderboardGroupSelect");
const pageButtons = document.querySelectorAll(".pageBtn");
const dashboardPage = document.getElementById("dashboardPage");
const challengesPage = document.getElementById("challengesPage");
const groupsPage = document.getElementById("groupsPage");
const profilePage = document.getElementById("profilePage");
const findGamesPage = document.getElementById("findGamesPage");
const backlogPage = document.getElementById("backlogPage");
const friendsPage = document.getElementById("friendsPage");
const gamePage = document.getElementById("gamePage");
const profileHostDashboard = document.getElementById("profileHostDashboard");
const profileHostProfile = document.getElementById("profileHostProfile");
const selfGameHostDashboard = document.getElementById("selfGameHostDashboard");
const selfGameHostProfile = document.getElementById("selfGameHostProfile");
const selfGameHostPage = document.getElementById("selfGameHostPage");
const compareHostPage = document.getElementById("compareHostPage");
const profileRecentGamesEl = document.getElementById("profileRecentGames");
const profileRecentLoadingEl = document.getElementById("profileRecentLoading");
const profileRecentTabButtons = document.querySelectorAll("[data-profile-recent-tab]");
const profileActivityListEl = document.getElementById("profileActivityList");
const profileActivityLoadingEl = document.getElementById("profileActivityLoading");
const profileSocialListEl = document.getElementById("profileSocialList");
const findGamesListEl = document.getElementById("findGamesList");
const findGamesAchievementsEl = document.getElementById("findGamesAchievements");
const findGamesLoadingEl = document.getElementById("findGamesLoading");
const gameLetterBarEl = document.getElementById("gameLetterBar");
const findGamesConsoleSelect = document.getElementById("findGamesConsole");
const findGamesSearchInput = document.getElementById("findGamesSearch");
const findGamesSortSelect = document.getElementById("findGamesSort");
const findGamesGenreSelect = document.getElementById("findGamesGenre");
const findGamesShowMoreBtn = document.getElementById("findGamesShowMoreBtn");
const imageModal = document.getElementById("imageModal");
const imageModalImg = document.getElementById("imageModalImg");
const imageModalCloseBtn = document.getElementById("imageModalCloseBtn");
const findGamesTabButtons = document.querySelectorAll("[data-find-tab]");
const findTabSearch = document.getElementById("findTabSearch");
const findTabSuggested = document.getElementById("findTabSuggested");
const findSearchListPanel = document.getElementById("findSearchListPanel");
const findSearchAchievementsPanel = document.getElementById("findSearchAchievementsPanel");
const findSearchBackBtn = document.getElementById("findSearchBackBtn");
const findSuggestedListPanel = document.getElementById("findSuggestedListPanel");
const findSuggestedAchievementsPanel = document.getElementById("findSuggestedAchievementsPanel");
const findSuggestedBackBtn = document.getElementById("findSuggestedBackBtn");
const findSuggestedListEl = document.getElementById("findSuggestedList");
const findSuggestedAchievementsEl = document.getElementById("findSuggestedAchievements");
const findSuggestedStatusEl = document.getElementById("findSuggestedStatus");
const findSuggestedLoadingEl = document.getElementById("findSuggestedLoading");
const backlogListEl = document.getElementById("backlogList");
const backlogStatusEl = document.getElementById("backlogStatus");
const backlogLoadingEl = document.getElementById("backlogLoading");
const backlogRemoveBtn = document.getElementById("backlogRemoveBtn");
const friendsListEl = document.getElementById("friendsList");
const friendsStatusEl = document.getElementById("friendsStatus");
const friendsLoadingEl = document.getElementById("friendsLoading");
const friendsAddBtn = document.getElementById("friendsAddBtn");
const groupsLoadingEl = document.getElementById("groupsLoading");
const groupNameInput = document.getElementById("groupNameInput");
const groupCreateBtn = document.getElementById("groupCreateBtn");
const groupCreateStatusEl = document.getElementById("groupCreateStatus");
const groupBrowseListEl = document.getElementById("groupBrowseList");
const groupMyListEl = document.getElementById("groupMyList");
const groupInvitesListEl = document.getElementById("groupInvitesList");
const socialPage = document.getElementById("socialPage");
const socialPostListEl = document.getElementById("socialPostList");
const socialTextInput = document.getElementById("socialTextBody");
const socialScreenshotSection = document.getElementById("socialScreenshotSection");
const socialAchievementSection = document.getElementById("socialAchievementSection");
const socialAchievementGameSelected = document.getElementById("socialAchievementGameSelected");
const socialAchievementGamesEl = document.getElementById("socialAchievementGames");
const socialAchievementStatusEl = document.getElementById("socialAchievementStatus");
const socialAchievementListEl = document.getElementById("socialAchievementList");
const socialUploadInput = document.getElementById("socialUpload");
const socialLinkGameBtn = document.getElementById("socialLinkGameBtn");
const socialGameRow = document.getElementById("socialGameRow");
const socialGameInput = document.getElementById("socialGame");
const socialGameResultsEl = document.getElementById("socialGameResults");
const socialPostBtn = document.getElementById("socialPostBtn");
const socialPreview = document.getElementById("socialPreview");
const socialPreviewImg = document.getElementById("socialPreviewImg");
const socialPreviewRemoveBtn = document.getElementById("socialPreviewRemoveBtn");
const socialStatusEl = document.getElementById("socialStatus");
const socialAddScreenshotBtn = document.getElementById("socialAddScreenshotBtn");
const socialAddAchievementBtn = document.getElementById("socialAddAchievementBtn");
const socialSidebarTrendingEl = document.getElementById("socialSidebarTrending");
const socialSidebarActivityEl = document.getElementById("socialSidebarActivity");
const socialSidebarSuggestionsEl = document.getElementById("socialSidebarSuggestions");
const socialTrendingTooltip = document.getElementById("socialTrendingTooltip");
const socialFiltersEl = document.querySelector(".socialFilters");
const socialFilterButtons = document.querySelectorAll(".socialFilters .filterPill");

// Debug UI: queue counters and logging.
if (apiQueueCounterEl) {
  apiQueueCounterEl.textContent = "API Calls in Queue: 0";
}

function setApiLogTab(tab) {
  if (!tab) return;
  localStorage.setItem(LS_DEBUG_LOG_TAB, tab);
  apiQueueTabButtons.forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.log === tab);
  });
  if (apiQueueLogEntriesEl) apiQueueLogEntriesEl.hidden = tab !== "queue";
  if (systemLogEntriesEl) systemLogEntriesEl.hidden = tab !== "system";
}

apiQueueTabButtons.forEach((btn) => {
  btn.addEventListener("click", () => setApiLogTab(btn.dataset.log));
});
setApiLogTab(localStorage.getItem(LS_DEBUG_LOG_TAB) || "queue");

leaderboardTabButtons.forEach((btn) => {
  btn.addEventListener("click", () => setLeaderboardRange(btn.dataset.range));
});
updateLeaderboardRangeNote();
updateLeaderboardTabState();

chartTabButtons.forEach((btn) => {
  btn.addEventListener("click", () => setActiveChart(btn.dataset.chart));
});
setActiveChart(activeChart);

function appendLogEntry(targetEl, message, extraClass = "") {
  if (!targetEl) return;
  if (localStorage.getItem(LS_DEBUG_UI) !== "true") return;
  const entry = document.createElement("div");
  entry.className = "apiQueueLogEntry";
  if (extraClass) entry.classList.add(extraClass);
  const ts = new Date().toLocaleTimeString();
  entry.textContent = `[${ts}] ${message}`;
  targetEl.appendChild(entry);
  const maxEntries = 200;
  while (targetEl.childElementCount > maxEntries) {
    targetEl.removeChild(targetEl.firstChild);
  }
  targetEl.scrollTop = targetEl.scrollHeight;
}

function logApiQueueEvent(url, options, status) {
  const method = options?.method || "GET";
  const statusLabel = Number.isFinite(status) ? ` ${status}` : "";
  appendLogEntry(apiQueueLogEntriesEl, `${method} ${url}${statusLabel}`);
  if (url?.startsWith("/api/game-achievements")) {
    const last = apiQueueLogEntriesEl?.lastElementChild;
    if (last) last.classList.add("fastQueue");
  }
  if (Number.isFinite(status)) {
    const last = apiQueueLogEntriesEl?.lastElementChild;
    if (last) {
      if (status === 423 || status === 429) {
        last.classList.add("status423");
      } else if (status >= 400) {
        last.classList.add("error");
      }
    }
  }
}

function logSystemMessage(message) {
  appendLogEntry(systemLogEntriesEl, message);
}

function applyDebugUiState() {
  const enabled = localStorage.getItem(LS_DEBUG_UI) === "true";
  if (apiQueueCounterEl) {
    apiQueueCounterEl.hidden = !enabled;
    apiQueueCounterEl.classList.toggle("debugHidden", !enabled);
  }
  const logEl = document.getElementById("apiQueueLog");
  if (logEl) {
    logEl.hidden = !enabled;
    logEl.classList.toggle("debugHidden", !enabled);
  }
}

// Settings modal and toggles.
const settingsBtn = document.getElementById("settingsBtn");
const settingsModal = document.getElementById("settingsModal");
const settingsSaveBtn = document.getElementById("settingsSaveBtn");
const settingsCloseBtn = document.getElementById("settingsCloseBtn");
const settingsCancelBtn = document.getElementById("settingsCancelBtn");
const useApiKeyToggle = document.getElementById("useApiKeyToggle");
const debugUiToggle = document.getElementById("debugUiToggle");

// Dashboard lists and loading states.
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

// Profile and comparison panels.
const profilePanel = document.getElementById("profilePanel");
const profileTitleNameEl = document.getElementById("profileTitleName");
const profileSummaryEl = document.getElementById("profileSummary");
const profileInsightsEl = document.getElementById("profileInsights");
const profileSharedGamesEl = document.getElementById("profileSharedGames");
const profileCloseBtn = document.getElementById("profileCloseBtn");
const profileShowMoreBtn = document.getElementById("profileShowMoreBtn");
const profileBacklogBtn = document.getElementById("profileBacklogBtn");
const profileGamesNoteEl = document.getElementById("profileGamesNote");
const profileGameSearchEl = document.getElementById("profileGameSearch");
const profileLegendMeEl = document.getElementById("profileLegendMe");
const profileLegendThemEl = document.getElementById("profileLegendThem");
let activeActivityTab = "achievements";

const comparePanel = document.getElementById("comparePanel");
const compareTitleGameEl = document.getElementById("compareTitleGame");
const compareMetaEl = document.getElementById("compareMeta");
const compareAchievementsEl = document.getElementById("compareAchievements");
const compareTimesEl = document.getElementById("compareTimes");
const compareBackBtn = document.getElementById("compareBackBtn");
const selfGamePanel = document.getElementById("selfGamePanel");
const selfGameTitleEl = document.getElementById("selfGameTitle");
const selfGameMetaEl = document.getElementById("selfGameMeta");
const selfGameAchievementsEl = document.getElementById("selfGameAchievements");
const selfGameBackBtn = document.getElementById("selfGameBackBtn");

// Challenge UI elements and modals.
const challengesLoadingEl = document.getElementById("challengesLoading");
const challengeOpponentInput = document.getElementById("challengeOpponent");
const challengeTypeSelect = document.getElementById("challengeType");
const challengeDurationInput = document.getElementById("challengeDuration");
const challengeScoreSelectBtn = document.getElementById("challengeScoreSelectBtn");
const challengeSendBtn = document.getElementById("challengeSendBtn");
const challengeErrorEl = document.getElementById("challengeError");
const challengeIncomingEl = document.getElementById("challengeIncoming");
const challengeOutgoingEl = document.getElementById("challengeOutgoing");
const challengeActiveEl = document.getElementById("challengeActive");
const challengeHistoryBtn = document.getElementById("challengeHistoryBtn");
const challengeHistoryModal = document.getElementById("challengeHistoryModal");
const challengeHistoryCloseBtn = document.getElementById("challengeHistoryCloseBtn");
const challengeHistoryList = document.getElementById("challengeHistoryList");
const challengePendingBtn = document.getElementById("challengePendingBtn");
const challengePendingModal = document.getElementById("challengePendingModal");
const challengePendingCloseBtn = document.getElementById("challengePendingCloseBtn");
const challengeScoreModal = document.getElementById("challengeScoreModal");
const challengeScoreCloseBtn = document.getElementById("challengeScoreCloseBtn");
const scoreAttackGamesList = document.getElementById("scoreAttackGamesList");
const scoreAttackBoardsList = document.getElementById("scoreAttackBoardsList");
const scoreAttackShowMoreBtn = document.getElementById("scoreAttackShowMoreBtn");
const scoreAttackSelectionEl = document.getElementById("scoreAttackSelection");
const challengeScoreSummaryEl = document.getElementById("challengeScoreSummary");
const scoreAttackGamesTitle = document.getElementById("scoreAttackGamesTitle");
const scoreAttackTabButtons = document.querySelectorAll(".scoreAttackTabBtn");
const scoreAttackGamesSearch = document.getElementById("scoreAttackGamesSearch");
const scoreAttackBoardsSearch = document.getElementById("scoreAttackBoardsSearch");

// Shared tab widgets.
const compareTabButtons = document.querySelectorAll(".compareTabBtn");
const compareTabPanels = document.querySelectorAll(".compareTabPanel");

const tabButtons = document.querySelectorAll(".tabBtn");
const tabPanels = document.querySelectorAll(".tabPanel");

// Profile state and pagination.
let currentProfileUser = "";
let profileLoadToken = 0;
let activeProfileLoadToken = 0;
let profileSharedGames = [];
let profileDisplayedGames = [];
let profileAllGamesLoaded = false;
let profileGamesFetchCount = 60;
let profileBaseGames = [];
let profileExpanded = false;
let profileGamesEmptyMessage = "No recent games found.";
let profileAutoLoadingAll = false;
let profileSkipAutoLoadOnce = false;
let profileGameAchievementCounts = new Map();
let profileGameAchievementPending = new Map();
let profileAllowCompare = true;
let profileIsSelf = false;
let selfGameReturnToProfile = false;
let compareReturnToProfile = false;
let profileCompletionByGameId = new Map();
let profileCompletionLoading = false;
let profileCompletionTarget = "";
let profileActivityUser = "";
let profileRecentGames = [];
let profileCommonGames = [];
let profileRecentTab = "recent";
let profileCompletionList = [];
let profileCompletionListLoading = false;
let profileBacklogItems = [];
let profileBacklogLoading = false;
const profileAchievementLimiter = createLimiter(2);
const summaryLimiter = createLimiter(2);
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
const PROFILE_GAMES_INITIAL = 60;
const PROFILE_GAMES_STEP = 10;
const PROFILE_GAMES_MAX = 200;
let recentAchievementsLoading = false;
let recentTimesLoading = false;
const STAGGER_MS = 400;
const PRESENCE_PING_MS = 15000;
let presenceTimer = null;
const PRESENCE_SESSION_KEY = "ra.presence.session";
let presenceSessionId = sessionStorage.getItem(PRESENCE_SESSION_KEY);
if (!presenceSessionId) {
  presenceSessionId = Math.random().toString(36).slice(2);
  sessionStorage.setItem(PRESENCE_SESSION_KEY, presenceSessionId);
}
let presenceRenderTimer = null;
let lastPresenceKey = "";
let lastPresenceMe = "";
let lastChartKey = "";
let lastHourlyChartKey = "";
let currentUser = "";
let friends = [];
let groups = [];
let groupBrowse = [];
let groupInvites = [];
let leaderboardScope = "friends";
let leaderboardGroupId = "";
const groupMembersCache = new Map();
let dailyHistoryCache = {};
let hourlyHistoryCache = {};
const challengeAvatarCache = new Map();
let challengesPollTimer = null;
let challengesTotalsTimer = null;
let challengesScoreTimer = null;
const challengeTotalsCache = new Map();
const activeChallengeCache = new Map();
let lastChallengesKey = "";
let lastLeaderboardKey = "";
let lastLeaderboardRows = [];
let leaderboardBaseRows = [];
let lastRecentAchievementsKey = "";
let lastRecentTimesKey = "";
let authResolved = false;
let challengeLeadCache = loadChallengeLeadCache();
let scoreAttackSharedGames = [];
let scoreAttackSelectedGame = null;
let scoreAttackSelectedBoard = null;
let scoreAttackLoadingAll = false;
let scoreAttackBoards = [];
let scoreAttackRecentMine = [];
let scoreAttackRecentTheirs = [];
let scoreAttackView = "shared";
let scoreAttackGameQuery = "";
let scoreAttackBoardQuery = "";
let findGamesLetter = "0-9";
let findGamesLoadedLetter = "";
let findGamesSelectedGameId = "";
const findGamesCache = new Map();
let findGamesInitialized = false;
let findGamesConsoleId = "";
let findGamesQuery = "";
let findGamesSort = "name";
let findGamesGenre = "all";
let findGamesTab = "search";
let findSuggestedLoadedFor = "";
let findSuggestedGames = [];
let findSuggestedSelectedGameId = "";
let findGamesConsolesCache = [];
const findGamesPlayersCache = new Map();
const findGamesPlayersCacheTs = new Map();
const findGamesPlayersRequested = new Set();
const findGamesPlayersControllers = new Map();
const findGamesPlayersLimiter = createLimiter(3);
let findGamesPlayersBatch = 0;
let findGamesPlayersBatchController = null;
let findGamesPlayersBatchKey = "";
let findGamesPlayersBatchResolvedKey = "";
const FIND_GAMES_INITIAL_VISIBLE = 200;
const FIND_GAMES_SHOW_MORE_STEP = 100;
let findGamesVisibleCount = FIND_GAMES_INITIAL_VISIBLE;
const FIND_GAMES_PLAYERS_FETCH_LIMIT = Infinity;
const FIND_GAMES_PLAYERS_BATCH_CONCURRENCY = 6;
const FIND_GAMES_PLAYERS_CACHE_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const FIND_GAMES_PLAYERS_CACHE_MAX = 6000;
let findGamesResortTimer = null;
let findGamesBatchSorting = false;
let findGamesPlayersPersistTimer = null;
const findGamesGenresCache = new Map();
const findGamesGenresCacheTs = new Map();
const findGamesGenresRequested = new Set();
const findGamesGenresControllers = new Map();
const findGamesGenresLimiter = createLimiter(3);
let findGamesGenresBatchController = null;
let findGamesGenresBatchKey = "";
const FIND_GAMES_GENRES_BATCH_CONCURRENCY = 6;
const FIND_GAMES_GENRES_CACHE_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const FIND_GAMES_GENRES_CACHE_MAX = 6000;
let findGamesGenresPersistTimer = null;
let backlogItems = [];
let backlogViewUser = "";
const backlogProgressCache = new Map();
const BACKLOG_PROGRESS_TTL_MS = 5 * 60 * 1000;
const backlogProgressLimiter = createLimiter(2);
const backlogProgressUpdateLimiter = createLimiter(2);
let backlogRenderToken = 0;
let backlogRemoveMode = false;
const SOCIAL_MAX_POSTS = 40;
const SOCIAL_MAX_IMAGE_BYTES = 2 * 1024 * 1024;
const SOCIAL_POLL_MS = 15000;
let socialPosts = [];
let socialDraftImageData = "";
let socialPollTimer = null;
let socialAttachScreenshot = false;
let socialAttachAchievement = false;
let socialAttachGame = false;
let socialComposerPanel = "";
let socialAchievementGame = null;
let socialAchievementItems = [];
let socialAchievementSelected = null;
let socialAchievementGames = [];
let notificationsPollTimer = null;
let notificationsUnreadCount = 0;
let notificationsEventSource = null;
let notificationsOpen = false;
let notificationsReadAfterClose = false;
let socialFilter = "all";
let socialGameSuggestions = [];
let socialGameSelected = null;
let socialGameSuggestionsLoaded = false;
let socialTrendingPlayers = new Map();

function clampUsername(s) {
  return (s || "").trim().replace(/\s+/g, "");
}

function normalizeUserKey(s) {
  return clampUsername(s).toLowerCase();
}

function setCurrentUser(username) {
  currentUser = clampUsername(username);
  if (meInput) meInput.value = currentUser;
  if (currentUser && usernameModal) {
    usernameModal.hidden = true;
  }
  updateSocialComposerState();
  if (socialPage && !socialPage.hidden) {
    loadSocialPostsFromServer({ silent: true });
  }
}

function loadState() {
  if (localStorage.getItem(LS_DEBUG_UI) === null) {
    localStorage.setItem(LS_DEBUG_UI, "false");
  }
  if (localStorage.getItem(LS_LEADERBOARD_RANGE) === null) {
    localStorage.setItem(LS_LEADERBOARD_RANGE, leaderboardRange);
  }
  if (apiKeyInput) apiKeyInput.value = localStorage.getItem(LS_API_KEY) || "";
  if (useApiKeyToggle) {
    useApiKeyToggle.checked = localStorage.getItem(LS_USE_API_KEY) === "true";
  }
  if (debugUiToggle) {
    debugUiToggle.checked = localStorage.getItem(LS_DEBUG_UI) === "true";
  }
  applyDebugUiState();
}

function saveState() {
  if (apiKeyInput) localStorage.setItem(LS_API_KEY, (apiKeyInput.value || "").trim());
  if (useApiKeyToggle) localStorage.setItem(LS_USE_API_KEY, useApiKeyToggle.checked ? "true" : "false");
  if (debugUiToggle) localStorage.setItem(LS_DEBUG_UI, debugUiToggle.checked ? "true" : "false");
  applyDebugUiState();
}

function setSocialStatus(message) {
  if (!socialStatusEl) return;
  socialStatusEl.textContent = message || "";
}

function refreshSocialComposerLayout() {
  const showGame = socialAttachGame && socialComposerPanel === "game";
  const showScreenshot = socialAttachScreenshot && socialComposerPanel === "screenshot";
  const showAchievement = socialAttachAchievement && socialComposerPanel === "achievement";
  if (socialGameRow) socialGameRow.hidden = !showGame;
  if (socialScreenshotSection) socialScreenshotSection.hidden = !showScreenshot;
  if (socialAchievementSection) socialAchievementSection.hidden = !showAchievement;
  if (socialAchievementGamesEl) {
    socialAchievementGamesEl.hidden = !showAchievement;
  }
  if (socialAddScreenshotBtn) {
    const hasScreenshot = !!socialDraftImageData;
    socialAddScreenshotBtn.classList.toggle("active", socialComposerPanel === "screenshot");
    socialAddScreenshotBtn.textContent = hasScreenshot ? "Screenshot Added" : "Add Screenshot";
    socialAddScreenshotBtn.classList.toggle("added", hasScreenshot);
  }
  if (socialAddAchievementBtn) {
    const hasAchievement = !!socialAchievementSelected;
    socialAddAchievementBtn.classList.toggle("active", socialComposerPanel === "achievement");
    socialAddAchievementBtn.textContent = hasAchievement ? "Achievement Added" : "Add Achievement";
    socialAddAchievementBtn.classList.toggle("added", hasAchievement);
  }
  if (socialLinkGameBtn) {
    const hasGame = !!socialGameSelected;
    socialLinkGameBtn.classList.toggle("active", socialComposerPanel === "game");
    socialLinkGameBtn.textContent = hasGame ? "Game Added" : "Add Game";
    socialLinkGameBtn.classList.toggle("added", hasGame);
    socialLinkGameBtn.disabled = socialAttachAchievement;
  }
  if (socialGameRow && socialAttachAchievement) {
    socialGameRow.hidden = true;
  }
  if (socialFiltersEl) {
    const composer = document.querySelector(".socialComposerBar");
    if (showScreenshot && socialScreenshotSection) {
      socialScreenshotSection.insertAdjacentElement("afterend", socialFiltersEl);
    } else if (showAchievement && socialAchievementSection) {
      socialAchievementSection.insertAdjacentElement("afterend", socialFiltersEl);
    } else if (showGame && socialGameRow) {
      socialGameRow.insertAdjacentElement("afterend", socialFiltersEl);
    } else if (composer) {
      composer.insertAdjacentElement("afterend", socialFiltersEl);
    }
  }
  updateSocialComposerState();
}

function setSocialComposerPanel(panel) {
  socialComposerPanel = panel || "";
  refreshSocialComposerLayout();
}

async function ensureSocialGameSuggestions() {
  if (socialGameSuggestionsLoaded) return;
  if (!currentUser) return;
  const cached = readRecentGamesCache(currentUser, 50);
  if (cached?.data?.results) {
    socialGameSuggestions = normalizeRecentGames(cached.data.results || []);
    socialGameSuggestionsLoaded = true;
    return;
  }
  try {
    const data = await fetchRecentGames(currentUser, 50);
    socialGameSuggestions = normalizeRecentGames(data?.results || []);
    socialGameSuggestionsLoaded = true;
  } catch {
    socialGameSuggestions = [];
  }
}

// --- Rendering helpers ---
function renderSocialGameResults(query) {
  if (!socialGameResultsEl) return;
  const q = (query || "").trim().toLowerCase();
  if (!q) {
    socialGameResultsEl.hidden = true;
    socialGameResultsEl.innerHTML = "";
    return;
  }
  const matches = socialGameSuggestions
    .filter((g) => (g.title || "").toLowerCase().includes(q))
    .slice(0, 8);
  if (!matches.length) {
    socialGameResultsEl.innerHTML = `<div class="meta">No matching games.</div>`;
    socialGameResultsEl.hidden = false;
    return;
  }
  const frag = document.createDocumentFragment();
  matches.forEach((g) => {
    const row = document.createElement("button");
    row.type = "button";
    row.className = "socialGameOption";
    row.dataset.gameId = String(g.gameId || "");
    row.dataset.gameTitle = g.title || "";
    row.innerHTML = `
      ${g.imageIcon ? `<img src="${iconUrl(g.imageIcon)}" alt="" loading="lazy" />` : ""}
      <div>
        <div>${safeText(g.title || "")}</div>
        <div class="socialGameOptionMeta">${safeText(g.consoleName || "")}</div>
      </div>
    `;
    frag.appendChild(row);
  });
  socialGameResultsEl.innerHTML = "";
  socialGameResultsEl.appendChild(frag);
  socialGameResultsEl.hidden = false;
}

function renderSocialAchievementGames(list) {
  if (!socialAchievementGamesEl) return;
  socialAchievementGamesEl.innerHTML = "";
  const items = Array.isArray(list) ? list : [];
  if (!items.length) {
    socialAchievementGamesEl.innerHTML = `<div class="meta">No recent games found.</div>`;
    return;
  }
  const frag = document.createDocumentFragment();
  items.forEach((game) => {
    const tile = document.createElement("div");
    tile.className = "tile clickable socialAchievementGameTile";
    tile.dataset.gameId = game.gameId;
    tile.dataset.gameTitle = game.title || "";
    tile.dataset.gameIcon = game.imageIcon || "";
    const img = document.createElement("img");
    img.src = iconUrl(game.imageIcon || "");
    img.alt = game.title || "Game";
    img.loading = "lazy";
    const title = document.createElement("div");
    title.className = "tileTitle";
    title.textContent = game.title || "";
    const meta = document.createElement("div");
    meta.className = "tileMeta";
    meta.textContent = game.consoleName || "";
    tile.append(img, title, meta);
    frag.append(tile);
  });
  socialAchievementGamesEl.append(frag);
}

function setSocialAchievementSelectedGame({ gameId, title = "", imageIcon = "" }) {
  socialAchievementGame = { gameId, title, imageIcon };
  if (socialAchievementGameSelected) {
    socialAchievementGameSelected.textContent = title ? `Selected: ${title}` : "Game selected.";
  }
}

function renderSocialAchievementList(list) {
  if (!socialAchievementListEl) return;
  socialAchievementListEl.innerHTML = "";
  const items = Array.isArray(list) ? list : [];
  if (!items.length) {
    if (socialAchievementStatusEl) {
      socialAchievementStatusEl.textContent = "No earned achievements found.";
    }
    return;
  }
  if (socialAchievementStatusEl) {
    socialAchievementStatusEl.textContent = `${items.length} earned achievements found.`;
  }
  const frag = document.createDocumentFragment();
  items.forEach((ach) => {
    const row = document.createElement("button");
    row.type = "button";
    row.className = "socialAchievementItem";
    if (socialAchievementSelected && String(socialAchievementSelected.id) === String(ach.id)) {
      row.classList.add("selected");
    }
    row.dataset.achievementId = String(ach.id || "");
    const img = document.createElement("img");
    img.src = iconUrl(ach.badgeUrl || "");
    img.alt = ach.title || "Achievement";
    img.loading = "lazy";
    const body = document.createElement("div");
    body.className = "socialAchievementBody";
    const title = document.createElement("div");
    title.className = "socialAchievementTitle";
    title.textContent = ach.title || "";
    const desc = document.createElement("div");
    desc.className = "socialAchievementDesc";
    desc.textContent = ach.description || "";
    body.append(title, desc);
    row.append(img, body);
    frag.append(row);
  });
  socialAchievementListEl.append(frag);
}

async function ensureSocialAchievementGames() {
  if (!currentUser) {
    if (socialAchievementStatusEl) socialAchievementStatusEl.textContent = "Set your username first.";
    return;
  }
  if (socialAchievementGames.length) return;
  const cached = readRecentGamesCache(currentUser, 30);
  if (cached?.data?.results) {
    socialAchievementGames = cached.data.results.slice();
    socialAchievementGames.sort((a, b) => {
      const da = Date.parse(a.lastPlayed || "") || 0;
      const db = Date.parse(b.lastPlayed || "") || 0;
      return db - da;
    });
    renderSocialAchievementGames(socialAchievementGames);
    if (!cached.stale) return;
  }
  try {
    if (socialAchievementStatusEl) socialAchievementStatusEl.textContent = "Loading recent games...";
    const data = await fetchRecentGames(currentUser, 30);
    socialAchievementGames = Array.isArray(data?.results) ? data.results.slice() : [];
    socialAchievementGames.sort((a, b) => {
      const da = Date.parse(a.lastPlayed || "") || 0;
      const db = Date.parse(b.lastPlayed || "") || 0;
      return db - da;
    });
    renderSocialAchievementGames(socialAchievementGames);
    if (socialAchievementStatusEl) socialAchievementStatusEl.textContent = "Select a game to see earned achievements.";
  } catch {
    if (socialAchievementStatusEl) socialAchievementStatusEl.textContent = "Failed to load recent games.";
  }
}

async function loadSocialAchievementList(gameId) {
  if (!currentUser || !gameId) return;
  if (socialAchievementStatusEl) socialAchievementStatusEl.textContent = "Loading achievements...";
  socialAchievementItems = [];
  socialAchievementSelected = null;
  renderSocialAchievementList([]);
  updateSocialComposerState();
  try {
    const data = await fetchGameAchievements(currentUser, gameId);
    const achievements = Array.isArray(data?.achievements) ? data.achievements : [];
    socialAchievementItems = achievements.filter(a => a.earned);
    renderSocialAchievementList(socialAchievementItems);
  } catch {
    if (socialAchievementStatusEl) socialAchievementStatusEl.textContent = "Failed to load achievements.";
  }
}

function setSocialPreview(dataUrl) {
  socialDraftImageData = dataUrl || "";
  if (socialPreviewImg) socialPreviewImg.src = socialDraftImageData || "";
  if (socialPreview) socialPreview.hidden = !socialDraftImageData;
  updateSocialComposerState();
}

function readImageAsDataUrl(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result);
    reader.onerror = () => reject(new Error("Failed to read image."));
    reader.readAsDataURL(file);
  });
}

function updateSocialComposerState() {
  const hasImage = !!socialDraftImageData;
  let canPost = false;
  let status = "";
  const gameLinkActive = socialAttachGame;
  const gameValue = (socialGameInput?.value || "").trim();
  const needsGameSelection = gameLinkActive && (!socialGameSelected || !gameValue);
  const hasText = !!(socialTextInput?.value || "").trim();
  if (!currentUser) {
    status = "Set your username in Settings to post.";
  } else {
    const needsAchievement = socialAttachAchievement && (!socialAchievementGame || !socialAchievementSelected);
    const needsScreenshot = socialAttachScreenshot && !hasImage;
    const hasAnyAttachment = socialAttachScreenshot || socialAttachAchievement || socialAttachGame;
    canPost = !needsGameSelection && !needsAchievement && !needsScreenshot && (hasAnyAttachment || hasText);
    if (needsGameSelection) status = "Select a game from the list.";
    else if (needsAchievement) status = "Select a game and achievement.";
    else if (needsScreenshot) status = "Add a screenshot to post.";
    else if (!hasAnyAttachment && !hasText) status = "Write something to post.";
  }
  if (socialPostBtn) {
    socialPostBtn.disabled = !canPost;
    socialPostBtn.textContent = "Post";
  }
  setSocialStatus(status);
}

function filterSocialPosts(posts, filter) {
  if (!filter || filter === "all") return posts;
  return posts.filter((post) => {
    const type = String(post?.postType || "").toLowerCase();
    const isAuto = !!post?.isAuto;
    if (filter === "achievement") {
      return type === "achievement";
    }
    if (filter === "completion") {
      return type === "completion" || isAuto;
    }
    if (filter === "posts") {
      return type === "text" || type === "screenshot";
    }
    return true;
  });
}

function renderSocialPosts(posts = socialPosts, targetEl = socialPostListEl, { showComments = true, showActions = true, limit = null, filter = "all" } = {}) {
  if (!targetEl) return;
  targetEl.innerHTML = "";
  const list = filterSocialPosts(Array.isArray(posts) ? posts : [], filter);
  if (!list.length) {
    targetEl.innerHTML = `<div class="meta">No screenshots yet. Be the first to post.</div>`;
    return;
  }
  const frag = document.createDocumentFragment();
  const slice = Number.isFinite(limit) ? list.slice(0, Math.max(0, limit)) : list;
  slice.forEach((post) => {
    const card = document.createElement("article");
    card.className = "socialPost";
    const isAuto = !!post?.isAuto;
    if (isAuto) card.classList.add("autoPost");
    if (post?.id) card.dataset.postId = post.id;

    let autoLabel = "";
    let autoGameTitle = "";
    if (isAuto) {
      const captionText = String(post?.caption || "").trim();
      const lower = captionText.toLowerCase();
      autoGameTitle = String(post?.game || "").trim();
      if (lower.startsWith("mastered ")) {
        autoLabel = "Mastered";
        if (!autoGameTitle) autoGameTitle = captionText.slice(9).trim();
      } else if (lower.startsWith("beaten ")) {
        autoLabel = "Completed";
        if (!autoGameTitle) autoGameTitle = captionText.slice(7).trim();
      } else if (lower.startsWith("completed ")) {
        autoLabel = "Completed";
        if (!autoGameTitle) autoGameTitle = captionText.slice(10).trim();
      } else {
        autoLabel = "Completed";
      }
      if (!autoGameTitle) autoGameTitle = "a game";
    }

    const header = document.createElement("div");
    header.className = "socialPostHeader";
    const author = document.createElement("div");
    author.className = "socialPostAuthor";
    author.textContent = post?.user || "Unknown";
    const time = document.createElement("div");
    time.className = "meta";
    time.textContent = formatDate(post?.createdAt);
    const isMine = currentUser && post?.user && normalizeUserKey(post.user) === normalizeUserKey(currentUser);
    const actions = document.createElement("div");
    actions.className = "socialPostActions";
    if (showActions && isMine && post?.id) {
      const removeBtn = document.createElement("button");
      removeBtn.className = "smallBtn dangerBtn";
      removeBtn.type = "button";
      removeBtn.textContent = "Remove";
      removeBtn.dataset.deletePostId = String(post.id);
      actions.appendChild(removeBtn);
    }
    if (post?.postType === "achievement" && !post?.isAuto) {
      header.classList.add("isAchievement");
      const title = document.createElement("div");
      title.className = "socialHeaderTitle";
      title.textContent = "Achievement";
      header.append(author, title, time);
      if (showActions) header.append(actions);
    } else if (isAuto) {
      header.classList.add("isAchievement");
      const title = document.createElement("div");
      title.className = "socialHeaderTitle";
      title.textContent = `Game ${autoLabel}`;
      header.append(author, title, time);
      if (showActions) header.append(actions);
    } else {
      header.append(author, time);
      if (showActions) header.append(actions);
    }
    card.append(header);

    const isAchievementPost = !isAuto && post?.postType === "achievement";

    if (isAchievementPost) {
      const achievementLayout = document.createElement("div");
      achievementLayout.className = "socialAutoLayout socialAchievementLayout";
      const badgeSrc = post?.imageUrl || "";
      const screenshotSrc = post?.imageData || "";
      let badgeImg = null;
      let shotImg = null;
      if (badgeSrc) {
        badgeImg = document.createElement("img");
        badgeImg.className = "socialPostImage socialAutoImage";
        badgeImg.loading = "lazy";
        badgeImg.src = badgeSrc;
        badgeImg.alt = post?.achievementTitle ? `Achievement ${post.achievementTitle}` : "Achievement";
        achievementLayout.append(badgeImg);
      }
      const info = document.createElement("div");
      info.className = "socialAutoInfo";
      const title = document.createElement("div");
      title.className = "socialAutoTitle";
      title.textContent = post?.achievementTitle || "Achievement unlocked";
      const meta = document.createElement("div");
      meta.className = "socialPostMeta";
      meta.textContent = post?.game ? `Game: ${post.game}` : "";
      info.append(title);
      if (post?.achievementDescription) {
        const desc = document.createElement("div");
        desc.className = "socialAchievementDesc";
        desc.textContent = post.achievementDescription;
        info.append(desc);
      }
      if (meta.textContent) info.append(meta);
      if (post?.caption) {
        const caption = document.createElement("div");
        caption.className = "socialAchievementCaption";
        caption.textContent = post.caption;
        info.append(caption);
      }
      achievementLayout.append(info);
      if (screenshotSrc) {
        shotImg = document.createElement("img");
        shotImg.className = "socialPostImage socialAchievementShot";
        shotImg.loading = "lazy";
        shotImg.src = screenshotSrc;
        shotImg.alt = post?.game ? `Screenshot from ${post.game}` : "Screenshot";
        achievementLayout.append(shotImg);
      }
      card.append(achievementLayout);
    } else if (!isAuto && post?.game) {
      const meta = document.createElement("div");
      meta.className = "socialPostMeta";
      meta.textContent = `Game: ${post.game}`;
      card.append(meta);
    }

    if (!isAuto && !isAchievementPost && post?.caption) {
      const caption = document.createElement("p");
      caption.className = "socialPostCaption";
      caption.textContent = post.caption;
      card.append(caption);
    }

    const reactions = document.createElement("div");
    reactions.className = "socialReactions";
    const likes = Number(post?.reactions?.likes || 0);
    const dislikes = Number(post?.reactions?.dislikes || 0);
    const userReaction = post?.reactions?.userReaction || "";
    const likeBtn = document.createElement("button");
    likeBtn.className = `smallBtn reactionBtn${userReaction === "like" ? " active" : ""}`;
    likeBtn.type = "button";
    likeBtn.dataset.reaction = "like";
    likeBtn.dataset.postId = String(post?.id || "");
    likeBtn.textContent = `Like ${likes}`;
    const dislikeBtn = document.createElement("button");
    dislikeBtn.className = `smallBtn reactionBtn${userReaction === "dislike" ? " active" : ""}`;
    dislikeBtn.type = "button";
    dislikeBtn.dataset.reaction = "dislike";
    dislikeBtn.dataset.postId = String(post?.id || "");
    dislikeBtn.textContent = `Dislike ${dislikes}`;
    reactions.append(likeBtn, dislikeBtn);

    const imgSrc = post?.imageData || post?.imageUrl || "";
    if (isAuto) {
      const autoLayout = document.createElement("div");
      autoLayout.className = "socialAutoLayout";

      if (imgSrc) {
        const img = document.createElement("img");
        img.className = "socialPostImage socialAutoImage";
        img.loading = "lazy";
        img.src = imgSrc;
        img.alt = `Game art for ${post?.game || post?.caption || "a game"}`;
        if (autoLabel === "Mastered") {
          img.classList.add("isMastered");
        } else if (autoLabel === "Completed") {
          img.classList.add("isCompleted");
        }
        autoLayout.append(img);
      }

      const autoInfo = document.createElement("div");
      autoInfo.className = "socialAutoInfo";

      const status = document.createElement("div");
      status.className = "socialAutoStatus";
      status.textContent = autoLabel;
      if (autoLabel === "Mastered") {
        status.classList.add("isMastered");
      }

      const title = document.createElement("div");
      title.className = "socialAutoTitle";
      title.textContent = autoGameTitle;

      autoInfo.append(status, title);
      autoLayout.append(autoInfo);
      card.append(autoLayout);
    } else if (!isAchievementPost && imgSrc) {
      const img = document.createElement("img");
      img.className = "socialPostImage";
      img.loading = "lazy";
      img.src = imgSrc;
      img.alt = `Screenshot posted by ${post?.user || "Unknown"}`;
      card.append(img);
    }

    if (showComments) {
      const commentsWrap = document.createElement("div");
      commentsWrap.className = "socialComments";
      const comments = Array.isArray(post?.comments) ? post.comments : [];
      if (!comments.length) {
        const empty = document.createElement("div");
        empty.className = "meta";
        empty.textContent = "No comments yet.";
        commentsWrap.append(empty);
      } else {
        comments.forEach((comment) => {
          const commentEl = document.createElement("div");
          commentEl.className = "socialComment";

          const commentHeader = document.createElement("div");
          commentHeader.className = "socialCommentHeader";
          const commentAuthor = document.createElement("div");
          commentAuthor.className = "socialCommentAuthor";
          commentAuthor.textContent = comment?.user || "Unknown";
          const commentTime = document.createElement("div");
          commentTime.className = "meta";
          commentTime.textContent = formatDate(comment?.createdAt);
          commentHeader.append(commentAuthor, commentTime);

          const commentBody = document.createElement("div");
          commentBody.className = "socialCommentBody";
          commentBody.textContent = comment?.text || "";

          commentEl.append(commentHeader, commentBody);
          commentsWrap.append(commentEl);
        });
      }

      const form = document.createElement("form");
      form.className = "socialCommentForm";
      if (post?.id) form.dataset.postId = post.id;
      const input = document.createElement("input");
      input.className = "socialCommentInput";
      input.type = "text";
      input.placeholder = currentUser ? "Add a comment" : "Set username to comment";
      input.autocomplete = "off";
      input.disabled = !currentUser;
      const btn = document.createElement("button");
      btn.className = "smallBtn";
      btn.type = "submit";
      btn.textContent = "Post";
      btn.disabled = !currentUser;
      form.append(input, btn, reactions);
      commentsWrap.append(form);

      card.append(commentsWrap);
    }
    frag.append(card);
  });
  targetEl.append(frag);
}

async function loadSocialPostsFromServer({ silent = false } = {}) {
  if (!socialPostListEl) return;
  try {
    if (!silent) setSocialStatus("Loading feed...");
    const data = await fetchSocialPosts(SOCIAL_MAX_POSTS);
    const results = Array.isArray(data?.results) ? data.results : [];
    socialPosts = results;
    renderSocialPosts(socialPosts, socialPostListEl, { filter: socialFilter });
    renderSocialSidebarActivity();
    renderSocialTrendingGames();
    loadFriendSuggestions().then(renderSocialSidebarSuggestions).catch(() => {
      renderSocialSidebarSuggestions([]);
    });
    const profileUser = clampUsername(currentProfileUser);
    const profilePosts = profileUser
      ? socialPosts.filter(p => normalizeUserKey(p?.user) === normalizeUserKey(profileUser))
      : [];
    renderSocialPosts(profilePosts, profileSocialListEl, { showComments: false, showActions: false, limit: 3 });
    if (!silent) setSocialStatus("");
  } catch (err) {
    const message = String(err?.message || "");
    if (message.toLowerCase().includes("not authenticated")) {
      if (!silent) setSocialStatus("Set your username in Settings to see the social feed.");
      if (socialPostListEl) {
        socialPostListEl.innerHTML = `<div class="meta">Set your username to see friends posts.</div>`;
      }
      if (profileSocialListEl) {
        profileSocialListEl.innerHTML = `<div class="meta">Set your username to see friends posts.</div>`;
      }
      return;
    }
    if (!silent) setSocialStatus("Failed to load social feed.");
    if (socialPostListEl) {
      socialPostListEl.innerHTML = `<div class="meta">Failed to load screenshots.</div>`;
    }
    if (profileSocialListEl) {
      profileSocialListEl.innerHTML = `<div class="meta">Failed to load screenshots.</div>`;
    }
  }
}

function startSocialPolling() {
  stopSocialPolling();
  socialPollTimer = setInterval(() => {
    loadSocialPostsFromServer({ silent: true });
  }, SOCIAL_POLL_MS);
}

function stopSocialPolling() {
  if (socialPollTimer) {
    clearInterval(socialPollTimer);
    socialPollTimer = null;
  }
}

function addSocialComment(postId, text) {
  if (!postId || !text) return;
  const payload = { text };
  return fetchServerJson(`/api/social/posts/${encodeURIComponent(postId)}/comments`, {
    method: "POST",
    body: payload
  });
}

loadState();
updateSocialComposerState();

function loadFindGamesPlayersCache() {
  try {
    const raw = localStorage.getItem(LS_FIND_GAMES_PLAYERS);
    if (!raw) return;
    const parsed = JSON.parse(raw);
    const items = parsed?.items || {};
    const now = Date.now();
    for (const [id, entry] of Object.entries(items)) {
      const players = Number(entry?.p);
      const ts = Number(entry?.t);
      if (!Number.isFinite(players) || !Number.isFinite(ts)) continue;
      if ((now - ts) > FIND_GAMES_PLAYERS_CACHE_TTL_MS) continue;
      const key = String(id);
      findGamesPlayersCache.set(key, players);
      findGamesPlayersCacheTs.set(key, ts);
    }
  } catch {
    // ignore cache failures
  }
}

function loadFindGamesGenresCache() {
  try {
    const raw = localStorage.getItem(LS_FIND_GAMES_GENRES);
    if (!raw) return;
    const parsed = JSON.parse(raw);
    const items = parsed?.items || {};
    const now = Date.now();
    for (const [id, entry] of Object.entries(items)) {
      const genre = String(entry?.g || "").trim();
      const ts = Number(entry?.t);
      if (!genre || !Number.isFinite(ts)) continue;
      if ((now - ts) > FIND_GAMES_GENRES_CACHE_TTL_MS) continue;
      const key = String(id);
      findGamesGenresCache.set(key, genre);
      findGamesGenresCacheTs.set(key, ts);
    }
  } catch {
    // ignore cache failures
  }
}

function persistFindGamesPlayersCache() {
  try {
    const entries = [];
    for (const [id, players] of findGamesPlayersCache.entries()) {
      const ts = Number(findGamesPlayersCacheTs.get(id)) || Date.now();
      entries.push({ id: String(id), p: players, t: ts });
    }
    entries.sort((a, b) => b.t - a.t);
    const limited = entries.slice(0, FIND_GAMES_PLAYERS_CACHE_MAX);
    const items = {};
    for (const entry of limited) {
      items[entry.id] = { p: entry.p, t: entry.t };
    }
    localStorage.setItem(LS_FIND_GAMES_PLAYERS, JSON.stringify({ v: 1, items }));
  } catch {
    // ignore cache failures
  }
}

function persistFindGamesGenresCache() {
  try {
    const entries = [];
    for (const [id, genre] of findGamesGenresCache.entries()) {
      const ts = Number(findGamesGenresCacheTs.get(id)) || Date.now();
      entries.push({ id: String(id), g: genre, t: ts });
    }
    entries.sort((a, b) => b.t - a.t);
    const limited = entries.slice(0, FIND_GAMES_GENRES_CACHE_MAX);
    const items = {};
    for (const entry of limited) {
      items[entry.id] = { g: entry.g, t: entry.t };
    }
    localStorage.setItem(LS_FIND_GAMES_GENRES, JSON.stringify({ v: 1, items }));
  } catch {
    // ignore cache failures
  }
}

function scheduleFindGamesPlayersPersist() {
  if (findGamesPlayersPersistTimer) return;
  findGamesPlayersPersistTimer = setTimeout(() => {
    findGamesPlayersPersistTimer = null;
    persistFindGamesPlayersCache();
  }, 500);
}

function scheduleFindGamesGenresPersist() {
  if (findGamesGenresPersistTimer) return;
  findGamesGenresPersistTimer = setTimeout(() => {
    findGamesGenresPersistTimer = null;
    persistFindGamesGenresCache();
  }, 500);
}

loadFindGamesPlayersCache();
loadFindGamesGenresCache();

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

function cacheGetWithMeta(key) {
  try {
    const raw = localStorage.getItem(`${LS_CACHE_PREFIX}${key}`);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") return null;
    return { data: parsed.data ?? null, ts: parsed.ts ?? null };
  } catch {
    return null;
  }
}

function recentGamesCacheKey(username) {
  return `${LS_RECENT_GAMES_PREFIX}:${clampUsername(username)}`;
}

function readRecentGamesCache(username, count) {
  try {
    const raw = localStorage.getItem(recentGamesCacheKey(username));
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") return null;
    const results = Array.isArray(parsed.results) ? parsed.results : null;
    if (!results || !results.length) return null;
    const storedCount = Number(parsed.count ?? results.length);
    if (!Number.isFinite(storedCount) || storedCount < count) return null;
    const ts = Number(parsed.ts ?? 0);
    const ageMs = Date.now() - ts;
    const data = { count, results: results.slice(0, count) };
    return { data, stale: !Number.isFinite(ageMs) || ageMs > RECENT_CACHE_TTL_MS };
  } catch {
    return null;
  }
}

function writeRecentGamesCache(username, data) {
  try {
    const results = Array.isArray(data?.results) ? data.results : [];
    const count = Number(data?.count ?? results.length);
    const payload = { ts: Date.now(), count: Number.isFinite(count) ? count : results.length, results };
    localStorage.setItem(recentGamesCacheKey(username), JSON.stringify(payload));
  } catch {
    // ignore cache failures
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

function getHourKey(d = new Date()) {
  const hour = new Date(d);
  hour.setMinutes(0, 0, 0);
  return hour.toISOString();
}

function setDailyHistory(history) {
  dailyHistoryCache = history || {};
}

function setHourlyHistory(history) {
  hourlyHistoryCache = history || {};
}

function loadChallengeLeadCache() {
  try {
    const raw = localStorage.getItem(LS_CHALLENGE_LEAD_CACHE);
    return raw ? JSON.parse(raw) : {};
  } catch {
    return {};
  }
}

function saveChallengeLeadCache() {
  try {
    localStorage.setItem(LS_CHALLENGE_LEAD_CACHE, JSON.stringify(challengeLeadCache));
  } catch {
    // Ignore cache failures.
  }
}

function pruneChallengeLeadCache(activeItems) {
  const keep = new Set(activeItems.map(item => String(item.id)));
  let changed = false;
  for (const key of Object.keys(challengeLeadCache)) {
    if (!keep.has(key)) {
      delete challengeLeadCache[key];
      changed = true;
    }
  }
  if (changed) saveChallengeLeadCache();
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

function leaderboardRankColor(idx, total) {
  const ratio = total > 1 ? Math.min(1, Math.max(0, idx / (total - 1))) : 0;
  const startHue = 45; // gold
  const endHue = 215; // blue
  const hue = Math.round(startHue + (endHue - startHue) * ratio);
  return `hsl(${hue}, 70%, 60%)`;
}

function getLeaderboardRangePoints(row) {
  if (leaderboardRange === "daily") {
    return row.dailyPoints ?? getDailyPointsFromHistory(row.username) ?? 0;
  }
  if (leaderboardRange === "weekly") {
    return row.weeklyPoints ?? getWeeklyPointsFromHistory(row.username) ?? 0;
  }
  return row.points ?? 0;
}

function getDailyPointsFromHistory(username) {
  const history = dailyHistoryCache?.[normalizeUserKey(username)];
  if (!history) return null;
  const todayKey = getLocalDateKey();
  return Number(history?.[todayKey] || 0);
}

function getWeeklyPointsFromHistory(username) {
  const history = dailyHistoryCache?.[normalizeUserKey(username)];
  if (!history) return null;
  const today = new Date();
  const day = today.getDay(); // 0=Sun, 1=Mon
  const offset = (day + 6) % 7; // Monday=0, Sunday=6
  const weekStart = new Date(today);
  weekStart.setDate(today.getDate() - offset);
  const days = [];
  for (let i = 0; i < 7; i++) {
    const d = new Date(weekStart);
    d.setDate(weekStart.getDate() + i);
    days.push(getLocalDateKey(d));
  }
  return days.reduce((sum, dayKey) => sum + Number(history?.[dayKey] || 0), 0);
}

function applyWeeklyPoints(rows) {
  rows.forEach((row) => {
    row.weeklyPoints = getWeeklyPointsFromHistory(row.username);
  });
}

function renderLeaderboardForRange(baseRows, me) {
  if (!Array.isArray(baseRows) || !baseRows.length) return;
  const myRow = baseRows.find(r => r.username === me);
  const myPoints = myRow ? getLeaderboardRangePoints(myRow) : 0;
  const rows = baseRows.map((row) => {
    const points = getLeaderboardRangePoints(row);
    return {
      ...row,
      points,
      deltaVsYou: points - myPoints,
      showDailyPoints: leaderboardRange === "monthly"
    };
  });
  rows.sort((a, b) => (b.points - a.points) || a.username.localeCompare(b.username));
  renderLeaderboard(rows, me);
  updateLeaderboardRangeNote();
  updateLeaderboardTabState();
}

function updateLeaderboardTabState() {
  leaderboardTabButtons.forEach((btn) => {
    const active = btn.dataset.range === leaderboardRange;
    btn.classList.toggle("active", active);
    btn.setAttribute("aria-selected", active ? "true" : "false");
  });
}

function updateLeaderboardRangeNote() {
  let title = "Monthly Leaderboard";
  let note = "Points column shows monthly total with today in parentheses.";
  if (leaderboardRange === "daily") {
    title = "Daily Leaderboard";
    note = "Points column shows today's total.";
  } else if (leaderboardRange === "weekly") {
    title = "Weekly Leaderboard";
    note = "Points column shows the last 7 days total.";
  }
  if (leaderboardTitleEl) leaderboardTitleEl.textContent = title;
  if (leaderboardPointsNoteEl) leaderboardPointsNoteEl.textContent = note;
}

function updateChallengeFormState() {
  const opponent = clampUsername(challengeOpponentInput?.value || "");
  const hours = Number(challengeDurationInput?.value || 0);
  const hasFriend = Boolean(opponent);
  const validHours = Number.isFinite(hours) && hours >= 1;
  const type = String(challengeTypeSelect?.value || "points");
  const needsScore = type === "score";
  const hasScore = Boolean(scoreAttackSelectedGame && scoreAttackSelectedBoard);
  const canSend = hasFriend && validHours && (!needsScore || hasScore);
  if (challengeSendBtn) challengeSendBtn.disabled = !canSend;
}

function setLeaderboardRange(range) {
  leaderboardRange = range;
  localStorage.setItem(LS_LEADERBOARD_RANGE, leaderboardRange);
  if (leaderboardBaseRows.length && currentUser) {
    renderLeaderboardForRange(leaderboardBaseRows, currentUser);
  } else {
    updateLeaderboardRangeNote();
    updateLeaderboardTabState();
  }
}

function setActiveChart(mode) {
  activeChart = mode === "hourly" ? "hourly" : "daily";
  chartTabButtons.forEach((btn) => {
    const active = btn.dataset.chart === activeChart;
    btn.classList.toggle("active", active);
    btn.setAttribute("aria-selected", active ? "true" : "false");
  });
  if (leaderboardChartEl) leaderboardChartEl.hidden = activeChart !== "daily";
  if (leaderboardHourlyChartEl) leaderboardHourlyChartEl.hidden = activeChart !== "hourly";
  if (leaderboardChartTitleEl) {
    leaderboardChartTitleEl.textContent = activeChart === "daily"
      ? "Daily Points (Last 7 Days)"
      : "Hourly Points (Last 24 Hours)";
  }
}

function renderLeaderboardChart(rows) {
  if (!leaderboardChartEl) return;
  const history = dailyHistoryCache || {};
  const total = rows.length;
  const days = [];
  const today = new Date();
  for (let i = 6; i >= 0; i--) {
    const d = new Date(today);
    d.setDate(today.getDate() - i);
    days.push(getLocalDateKey(d));
  }

  const series = rows.map((r, idx) => ({
    username: r.username,
    values: days.map(day => Number(history[normalizeUserKey(r.username)]?.[day] || 0)),
    color: r.nameColor || leaderboardRankColor(idx, total) || userColor(r.username)
  }));

  const chartKey = JSON.stringify(series.map(s => [s.username, s.values]));
  if (chartKey === lastChartKey) return;
  lastChartKey = chartKey;

  const max = Math.max(1, ...series.flatMap(s => s.values));
  const width = 800;
  const height = 160;
  const pad = 8;
  const labelPad = 30;
  const xStep = (width - (pad * 2) - labelPad) / (days.length - 1);
  const yScale = (val) => height - pad - ((val / max) * (height - pad * 2));
  const xFor = (idx) => pad + xStep * idx;
  const yFor = (val) => yScale(val);

  const axisY = `<line class="chartAxis" x1="${pad}" y1="${pad}" x2="${pad}" y2="${height - pad}" />`;
  const axisX = `<line class="chartAxis" x1="${pad}" y1="${height - pad}" x2="${width - pad - labelPad}" y2="${height - pad}" />`;

  const yTicks = 4;
  const yLabels = Array.from({ length: yTicks + 1 }, (_, i) => {
    const value = Math.round((max / yTicks) * i);
    const y = yFor(value);
    return `<text class="chartLabel" x="${width - pad - labelPad + 6}" y="${y}" text-anchor="start" dominant-baseline="middle">${value}</text>`;
  }).join("");

  const xLabels = days.map((day, idx) => {
    const label = day.slice(5);
    const x = xFor(idx);
    const y = height - pad + labelPad;
    return `<text class="chartLabel" x="${x}" y="${y}" text-anchor="middle">${label}</text>`;
  }).join("");

  const paths = series.map(s => {
    const d = s.values.map((v, idx) => {
      const x = xFor(idx);
      const y = yFor(v);
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
    <svg viewBox="0 0 ${width} ${height + labelPad + 6}" width="100%" height="${height + labelPad + 6}" preserveAspectRatio="none" role="img" aria-label="Daily points chart">
      ${axisY}
      ${axisX}
      ${yLabels}
      ${paths}
      ${xLabels}
    </svg>
    <div class="chartLegend">${legend}</div>
  `;
}

function renderHourlyChart(rows) {
  if (!leaderboardHourlyChartEl) return;
  const history = hourlyHistoryCache || {};
  const total = rows.length;
  const hours = [];
  const now = new Date();
  now.setMinutes(0, 0, 0);
  for (let i = 23; i >= 0; i--) {
    const d = new Date(now);
    d.setHours(now.getHours() - i);
    hours.push(d);
  }

  const series = rows.map((r, idx) => ({
    username: r.username,
    values: hours.map((d) => {
      const key = getHourKey(d);
      return Number(history[normalizeUserKey(r.username)]?.[key] || 0);
    }),
    color: r.nameColor || leaderboardRankColor(idx, total) || userColor(r.username)
  }));

  const chartKey = JSON.stringify(series.map(s => [s.username, s.values]));
  if (chartKey === lastHourlyChartKey) return;
  lastHourlyChartKey = chartKey;

  const max = Math.max(1, ...series.flatMap(s => s.values));
  const width = 800;
  const height = 160;
  const pad = 8;
  const labelPad = 30;
  const xStep = (width - (pad * 2) - labelPad) / (hours.length - 1);
  const yScale = (val) => height - pad - ((val / max) * (height - pad * 2));
  const xFor = (idx) => pad + xStep * idx;
  const yFor = (val) => yScale(val);

  const axisY = `<line class="chartAxis" x1="${pad}" y1="${pad}" x2="${pad}" y2="${height - pad}" />`;
  const axisX = `<line class="chartAxis" x1="${pad}" y1="${height - pad}" x2="${width - pad - labelPad}" y2="${height - pad}" />`;

  const yTicks = 4;
  const yLabels = Array.from({ length: yTicks + 1 }, (_, i) => {
    const value = Math.round((max / yTicks) * i);
    const y = yFor(value);
    return `<text class="chartLabel" x="${width - pad - labelPad + 6}" y="${y}" text-anchor="start" dominant-baseline="middle">${value}</text>`;
  }).join("");

  const xLabels = hours.map((d, idx) => {
    const label = d.toLocaleTimeString([], { hour: "numeric" });
    const x = xFor(idx);
    const y = height - pad + labelPad;
    return `<text class="chartLabel" x="${x}" y="${y}" text-anchor="middle">${label}</text>`;
  }).join("");

  const paths = series.map(s => {
    const d = s.values.map((v, idx) => {
      const x = xFor(idx);
      const y = yFor(v);
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

  leaderboardHourlyChartEl.innerHTML = `
    <svg viewBox="0 0 ${width} ${height + labelPad + 6}" width="100%" height="${height + labelPad + 6}" preserveAspectRatio="none" role="img" aria-label="Hourly points chart">
      ${axisY}
      ${axisX}
      ${yLabels}
      ${paths}
      ${xLabels}
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
  const me = currentUser;
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
  if (!users.length) {
    onlineUsersEl.innerHTML = `<div class="meta">No active viewers yet.</div>`;
    return;
  }
  onlineUsersEl.innerHTML = "";
  const frag = document.createDocumentFragment();
  for (const u of users) {
    const div = document.createElement("div");
    div.className = "onlineUser";
    div.textContent = u === me ? `${u} (you)` : u;
    frag.appendChild(div);
  }
  onlineUsersEl.appendChild(frag);
}

function schedulePresenceRender(users, me) {
  const key = JSON.stringify(users);
  if (key === lastPresenceKey && me === lastPresenceMe) return;
  if (presenceRenderTimer) return;
  presenceRenderTimer = setTimeout(() => {
    presenceRenderTimer = null;
    lastPresenceKey = key;
    lastPresenceMe = me;
    renderPresence(users, me);
  }, 200);
}

function startPresence() {
  const me = currentUser;
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
      schedulePresenceRender(data?.results || [], me);
    } catch {
      // ignore
    }
  };
  tick();
  presenceTimer = setInterval(tick, PRESENCE_PING_MS);
}

window.addEventListener("pagehide", sendPresenceRemove);

async function fetchJson(url, { silent = false, signal = null, immediate = false } = {}) {
  const apiKey = (apiKeyInput?.value || "").trim();
  const useApiKey = !!useApiKeyToggle?.checked;
  const headers = (useApiKey && apiKey) ? { "x-ra-api-key": apiKey } : {};
  const options = { headers };
  if (signal) options.signal = signal;
  const res = immediate ? await fetch(url, options) : await enqueueClientFetch(url, options);
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(text || `Request failed: ${res.status}`);
  }
  return res.json();
}

async function fetchServerJson(url, { method = "GET", body = null, silent = false } = {}) {
  const headers = {};
  const apiKey = (apiKeyInput?.value || "").trim();
  const useApiKey = !!useApiKeyToggle?.checked;
  if (useApiKey && apiKey) headers["x-ra-api-key"] = apiKey;
  if (body !== null) headers["Content-Type"] = "application/json";
  const res = await enqueueClientFetch(url, {
    method,
    headers,
    body: body !== null ? JSON.stringify(body) : undefined,
    credentials: "same-origin"
  });
  if (!res.ok) {
    let message = "";
    try {
      const data = await res.json();
      message = data?.error || data?.message || "";
    } catch {
      message = await res.text().catch(() => "");
    }
    if (!silent && message) setStatus(message);
    throw new Error(message || `Request failed: ${res.status}`);
  }
  const contentType = res.headers.get("content-type") || "";
  if (contentType.includes("application/json")) return res.json();
  return null;
}

async function fetchMonthly(username) {
  return fetchJson(`/api/monthly/${encodeURIComponent(username)}`);
}

async function loadFriendSuggestions(limit = 6) {
  const data = await fetchServerJson(`/api/friends/suggestions?limit=${encodeURIComponent(limit)}`, { silent: true });
  const list = Array.isArray(data?.results) ? data.results : [];
  return list.map(clampUsername).filter(Boolean);
}

function renderSocialSidebarActivity() {
  if (socialSidebarActivityEl) {
    const items = socialPosts.slice(0, 4);
    if (!items.length) {
      socialSidebarActivityEl.innerHTML = `<div class="meta">No recent activity.</div>`;
    } else {
      socialSidebarActivityEl.innerHTML = "";
      const frag = document.createDocumentFragment();
      items.forEach((post) => {
        const row = document.createElement("div");
        row.className = "socialMiniItem";
        const title = post?.user || "Friend";
        const time = post?.createdAt ? formatDate(post.createdAt) : "";
        row.innerHTML = `
          <div>
            <div class="socialMiniTitle">${safeText(title)}</div>
            <div class="socialMiniMeta">${safeText(post?.game || post?.caption || "New post")}</div>
          </div>
          <div class="socialMiniMeta">${safeText(time)}</div>
        `;
        frag.appendChild(row);
      });
      socialSidebarActivityEl.appendChild(frag);
    }
  }

}

function renderSocialSidebarSuggestions(list) {
  if (!socialSidebarSuggestionsEl) return;
  if (!list.length) {
    socialSidebarSuggestionsEl.innerHTML = `<div class="meta">No suggestions yet.</div>`;
    return;
  }
  socialSidebarSuggestionsEl.innerHTML = "";
  const frag = document.createDocumentFragment();
  list.forEach((name) => {
    const row = document.createElement("div");
    row.className = "socialMiniItem";
    row.innerHTML = `
      <div class="socialMiniTitle">${safeText(name)}</div>
      <button class="smallBtn" type="button" data-add-friend="${safeText(name)}">Add</button>
    `;
    frag.appendChild(row);
  });
  socialSidebarSuggestionsEl.appendChild(frag);
}

async function renderSocialTrendingGames() {
  if (!socialSidebarTrendingEl) return;
  const users = Array.from(new Set([currentUser, ...friends].map(clampUsername).filter(Boolean)));
  if (!users.length) {
    socialSidebarTrendingEl.innerHTML = `<div class="meta">No trending games yet.</div>`;
    return;
  }
  const counts = new Map();
  const playersByGame = new Map();
  for (const user of users.slice(0, 12)) {
    try {
      const cached = readRecentGamesCache(user, 12);
      const data = cached?.data || (await fetchRecentGames(user, 12));
      const recent = normalizeRecentGames(data?.results || []);
      const seen = new Set();
      for (const game of recent) {
        const id = String(game.gameId || "");
        if (!id || seen.has(id)) continue;
        seen.add(id);
        counts.set(id, {
          title: game.title || "",
          console: game.consoleName || "",
          count: (counts.get(id)?.count || 0) + 1
        });
        if (!playersByGame.has(id)) playersByGame.set(id, new Set());
        playersByGame.get(id).add(user);
      }
    } catch {
      // ignore user fetch errors
    }
  }
  const trending = Array.from(counts.entries())
    .map(([gameId, data]) => ({ gameId, ...data }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 5);
  if (!trending.length) {
    socialSidebarTrendingEl.innerHTML = `<div class="meta">No trending games yet.</div>`;
    return;
  }
  socialSidebarTrendingEl.innerHTML = "";
  const frag = document.createDocumentFragment();
  trending.forEach((item) => {
    const row = document.createElement("div");
    row.className = "socialMiniItem";
    row.dataset.gameId = String(item.gameId || "");
    row.innerHTML = `
      <div>
        <div class="socialMiniTitle">${safeText(item.title)}</div>
        <div class="socialMiniMeta">${safeText(item.console)}</div>
      </div>
      <div class="socialMiniMeta">${item.count} people</div>
    `;
    frag.appendChild(row);
  });
  socialSidebarTrendingEl.appendChild(frag);
  socialTrendingPlayers = playersByGame;
}

// --- API helpers (client <-> server) ---
async function fetchDaily(username) {
  return fetchJson(`/api/daily/${encodeURIComponent(username)}`);
}

async function fetchGroups() {
  return fetchServerJson("/api/groups");
}

async function fetchGroupBrowse() {
  return fetchServerJson("/api/groups/browse");
}

async function fetchGroupInvites() {
  return fetchServerJson("/api/groups/invites");
}

async function createGroup(name) {
  return fetchServerJson("/api/groups", { method: "POST", body: { name } });
}

async function joinGroup(groupId) {
  return fetchServerJson(`/api/groups/${encodeURIComponent(groupId)}/join`, { method: "POST" });
}

async function inviteToGroup(groupId, username) {
  return fetchServerJson(`/api/groups/${encodeURIComponent(groupId)}/invite`, {
    method: "POST",
    body: { username }
  });
}

async function acceptGroupInvite(inviteId) {
  return fetchServerJson(`/api/groups/invites/${encodeURIComponent(inviteId)}/accept`, { method: "POST" });
}

async function declineGroupInvite(inviteId) {
  return fetchServerJson(`/api/groups/invites/${encodeURIComponent(inviteId)}/decline`, { method: "POST" });
}

async function fetchGroupMembers(groupId) {
  return fetchServerJson(`/api/groups/${encodeURIComponent(groupId)}/members`);
}

async function fetchDailyHistory(users, days = 7) {
  const list = users.map(normalizeUserKey).filter(Boolean);
  if (!list.length) return {};
  const params = new URLSearchParams();
  params.set("users", list.join(","));
  params.set("days", String(days));
  const data = await fetchJson(`/api/daily-history?${params.toString()}`);
  return data?.results || {};
}

async function fetchHourlyHistory(users, hours = 24) {
  const list = users.map(normalizeUserKey).filter(Boolean);
  if (!list.length) return {};
  const params = new URLSearchParams();
  params.set("users", list.join(","));
  params.set("hours", String(hours));
  const data = await fetchJson(`/api/hourly-history?${params.toString()}`);
  return data?.results || {};
}

function setLeaderboardScope(scope) {
  leaderboardScope = scope === "group" ? "group" : "friends";
  if (leaderboardScopeFriendsBtn) {
    const active = leaderboardScope === "friends";
    leaderboardScopeFriendsBtn.classList.toggle("active", active);
    leaderboardScopeFriendsBtn.setAttribute("aria-selected", active ? "true" : "false");
  }
  if (leaderboardScopeGroupBtn) {
    const active = leaderboardScope === "group";
    leaderboardScopeGroupBtn.classList.toggle("active", active);
    leaderboardScopeGroupBtn.setAttribute("aria-selected", active ? "true" : "false");
  }
  if (leaderboardScope === "group" && !leaderboardGroupId && groups.length) {
    leaderboardGroupId = String(groups[0].id);
    if (leaderboardGroupSelect) leaderboardGroupSelect.value = leaderboardGroupId;
  }
  refreshLeaderboard();
}

function renderLeaderboardGroupSelect() {
  if (!leaderboardGroupSelect) return;
  leaderboardGroupSelect.innerHTML = "";
  const placeholder = document.createElement("option");
  placeholder.value = "";
  placeholder.textContent = groups.length ? "Select group" : "No groups yet";
  leaderboardGroupSelect.appendChild(placeholder);
  groups.forEach((g) => {
    const opt = document.createElement("option");
    opt.value = String(g.id);
    opt.textContent = g.name;
    leaderboardGroupSelect.appendChild(opt);
  });
  if (leaderboardGroupId) leaderboardGroupSelect.value = leaderboardGroupId;
  leaderboardGroupSelect.disabled = !groups.length;
  if (leaderboardScopeGroupBtn) leaderboardScopeGroupBtn.disabled = !groups.length;
}

function renderGroupList(targetEl, list, { mode = "browse" } = {}) {
  if (!targetEl) return;
  targetEl.innerHTML = "";
  const items = Array.isArray(list) ? list : [];
  if (!items.length) {
    const msg =
      mode === "browse" ? "No groups found yet." :
      mode === "mine" ? "You are not in any groups yet." :
      "No invites.";
    targetEl.innerHTML = `<div class="meta">${msg}</div>`;
    return;
  }

  const frag = document.createDocumentFragment();
  items.forEach((g) => {
    const row = document.createElement("div");
    row.className = "groupRow";

    const top = document.createElement("div");
    top.className = "groupRowTop";
    const title = document.createElement("div");
    title.className = "groupRowTitle";
    title.textContent = g.name || `Group ${g.id}`;
    const meta = document.createElement("div");
    meta.className = "groupRowMeta";
    const count = Number(g.member_count ?? g.memberCount ?? 0);
    meta.textContent = `${count} members`;
    top.append(title, meta);
    row.appendChild(top);

    const actions = document.createElement("div");
    actions.className = "groupRowActions";

    if (mode === "browse") {
      const isMember = g.is_member || g.isMember;
      const btn = document.createElement("button");
      btn.className = "smallBtn";
      btn.textContent = isMember ? "Member" : "Join";
      btn.disabled = !!isMember;
      if (!isMember) {
        btn.addEventListener("click", async () => {
          await joinGroup(g.id);
          await refreshGroupsPage();
          setStatus(`Joined ${g.name}.`);
        });
      }
      actions.appendChild(btn);
    } else if (mode === "mine") {
      const inviteWrap = document.createElement("div");
      inviteWrap.className = "groupInviteRow";
      const input = document.createElement("input");
      input.type = "text";
      input.placeholder = "Invite username";
      input.autocomplete = "off";
      const inviteBtn = document.createElement("button");
      inviteBtn.className = "smallBtn";
      inviteBtn.textContent = "Invite";
      inviteBtn.addEventListener("click", async () => {
        const target = clampUsername(input.value || "");
        if (!target) return;
        await inviteToGroup(g.id, target);
        input.value = "";
        setStatus(`Invite sent to ${target}.`);
      });
      inviteWrap.append(input, inviteBtn);

      const useBtn = document.createElement("button");
      useBtn.className = "smallBtn";
      useBtn.textContent = "Use for Leaderboard";
      useBtn.addEventListener("click", () => {
        leaderboardGroupId = String(g.id);
        renderLeaderboardGroupSelect();
        setLeaderboardScope("group");
      });

      actions.appendChild(inviteWrap);
      actions.appendChild(useBtn);
    }

    if (actions.childElementCount) row.appendChild(actions);
    frag.appendChild(row);
  });
  targetEl.appendChild(frag);
}

function renderInvitesList(list) {
  if (!groupInvitesListEl) return;
  groupInvitesListEl.innerHTML = "";
  const items = Array.isArray(list) ? list : [];
  if (!items.length) {
    groupInvitesListEl.innerHTML = `<div class="meta">No invites.</div>`;
    return;
  }
  const frag = document.createDocumentFragment();
  items.forEach((invite) => {
    const row = document.createElement("div");
    row.className = "groupRow";
    const top = document.createElement("div");
    top.className = "groupRowTop";
    const title = document.createElement("div");
    title.className = "groupRowTitle";
    title.textContent = invite.group_name || `Group ${invite.group_id}`;
    const meta = document.createElement("div");
    meta.className = "groupRowMeta";
    meta.textContent = invite.invited_by ? `Invited by ${invite.invited_by}` : "Invite";
    top.append(title, meta);
    row.appendChild(top);

    const actions = document.createElement("div");
    actions.className = "groupRowActions";
    const acceptBtn = document.createElement("button");
    acceptBtn.className = "smallBtn";
    acceptBtn.textContent = "Accept";
    acceptBtn.addEventListener("click", async () => {
      await acceptGroupInvite(invite.id);
      await refreshGroupsPage();
    });
    const declineBtn = document.createElement("button");
    declineBtn.className = "smallBtn";
    declineBtn.textContent = "Decline";
    declineBtn.addEventListener("click", async () => {
      await declineGroupInvite(invite.id);
      await refreshGroupsPage();
    });
    actions.append(acceptBtn, declineBtn);
    row.appendChild(actions);
    frag.appendChild(row);
  });
  groupInvitesListEl.appendChild(frag);
}

async function refreshGroupsPage() {
  if (!currentUser) return;
  setLoading(groupsLoadingEl, true);
  try {
    const [mineRes, browseRes, invitesRes] = await Promise.all([
      fetchGroups(),
      fetchGroupBrowse(),
      fetchGroupInvites()
    ]);
    groups = Array.isArray(mineRes?.results) ? mineRes.results : [];
    groupBrowse = Array.isArray(browseRes?.results) ? browseRes.results : [];
    groupInvites = Array.isArray(invitesRes?.results) ? invitesRes.results : [];
    groupMembersCache.clear();
    renderGroupList(groupBrowseListEl, groupBrowse, { mode: "browse" });
    renderGroupList(groupMyListEl, groups, { mode: "mine" });
    renderInvitesList(groupInvites);
    renderLeaderboardGroupSelect();
  } catch (e) {
    setStatus(e?.message || "Failed to load groups.");
  } finally {
    setLoading(groupsLoadingEl, false);
  }
}

async function fetchUserLevels(users) {
  const list = users.map(normalizeUserKey).filter(Boolean);
  if (!list.length) return {};
  const params = new URLSearchParams();
  params.set("users", list.join(","));
  const data = await fetchJson(`/api/user-levels?${params.toString()}`);
  const results = Array.isArray(data?.results) ? data.results : [];
  const map = {};
  results.forEach((row) => {
    const key = normalizeUserKey(row.username);
    if (key) map[key] = row.level;
  });
  return map;
}

async function fetchGameLeaderboards(gameId) {
  return fetchJson(`/api/game-leaderboards/${encodeURIComponent(gameId)}`);
}

async function fetchChallenges({ includeTotals = true } = {}) {
  const params = new URLSearchParams();
  params.set("totals", includeTotals ? "1" : "0");
  return fetchServerJson(`/api/challenges?${params.toString()}`, { silent: true });
}

async function createChallenge(opponent, hours, type, game, leaderboard) {
  return fetchServerJson("/api/challenges", {
    method: "POST",
    body: {
      opponent,
      hours,
      type,
      gameId: game?.gameId,
      gameTitle: game?.title,
      leaderboardId: leaderboard?.id,
      leaderboardTitle: leaderboard?.title
    }
  });
}

async function acceptChallenge(id) {
  return fetchServerJson(`/api/challenges/${encodeURIComponent(id)}/accept`, {
    method: "POST"
  });
}

async function declineChallenge(id) {
  return fetchServerJson(`/api/challenges/${encodeURIComponent(id)}/decline`, {
    method: "POST"
  });
}

async function cancelChallenge(id) {
  return fetchServerJson(`/api/challenges/${encodeURIComponent(id)}/cancel`, {
    method: "POST"
  });
}

async function fetchChallengeHistory() {
  return fetchServerJson("/api/challenges-history", { silent: true });
}

async function hydrateChallengeAvatars(items) {
  const usernames = new Set();
  for (const item of items) {
    if (item.creator_username) usernames.add(item.creator_username);
    if (item.opponent_username) usernames.add(item.opponent_username);
  }
  const targets = Array.from(usernames).filter(name => !challengeAvatarCache.has(normalizeUserKey(name)));
  if (!targets.length) return;

  await Promise.all(targets.map((name) => summaryLimiter(async () => {
    try {
      const data = await fetchUserSummary(name, { silent: true });
      challengeAvatarCache.set(normalizeUserKey(name), data?.userPic || "");
    } catch {
      challengeAvatarCache.set(normalizeUserKey(name), "");
    }
  })));

  applyChallengeAvatars();
}

function getChallengeAvatar(username) {
  return challengeAvatarCache.get(normalizeUserKey(username)) || "";
}

function applyChallengeAvatars() {
  const nodes = document.querySelectorAll("[data-avatar-user]");
  nodes.forEach((node) => {
    const username = node.getAttribute("data-avatar-user") || "";
    const url = getChallengeAvatar(username);
    if (!url) return;
    if (node.tagName === "IMG") {
      if (node.getAttribute("src")) return;
      node.setAttribute("src", iconUrl(url));
      node.classList.remove("placeholder");
      return;
    }
    const img = document.createElement("img");
    img.className = "challengeAvatar";
    img.loading = "lazy";
    img.alt = "";
    img.setAttribute("data-avatar-user", username);
    img.src = iconUrl(url);
    node.replaceWith(img);
  });
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
  const data = await fetchJson(`/api/recent-games/${u}?count=${encodeURIComponent(count)}`);
  if (data && Array.isArray(data.results)) {
    writeRecentGamesCache(username, data);
  }
  return data;
}

async function fetchUserSummary(username, opts) {
  const u = encodeURIComponent(username);
  return fetchJson(`/api/user-summary/${u}`, opts);
}

async function fetchUserCompletionProgress(username, count = 500, offset = 0) {
  const u = encodeURIComponent(username);
  return fetchJson(`/api/user-completion-progress/${u}?count=${encodeURIComponent(count)}&offset=${encodeURIComponent(offset)}`);
}

async function fetchGameAchievements(username, gameId) {
  const u = encodeURIComponent(username);
  return fetchJson(`/api/game-achievements/${u}/${encodeURIComponent(gameId)}`);
}

async function fetchGameTimes(username, gameId) {
  const u = encodeURIComponent(username);
  return fetchJson(`/api/game-times/${u}/${encodeURIComponent(gameId)}`);
}

async function fetchGameListByLetter(letter) {
  const consoleId = encodeURIComponent(findGamesConsoleId || "");
  return fetchJson(`/api/game-list?consoleId=${consoleId}&letter=${encodeURIComponent(letter)}`);
}

async function fetchGameAchievementsBasic(gameId) {
  return fetchJson(`/api/game-achievements-basic/${encodeURIComponent(gameId)}`);
}

async function fetchGamePlayers(gameId, signal) {
  return fetchJson(`/api/game-players?gameId=${encodeURIComponent(gameId)}`, { signal, immediate: true });
}

async function fetchGamePlayersRefresh(gameId, signal) {
  return fetchJson(`/api/game-players-refresh?gameId=${encodeURIComponent(gameId)}`, { signal });
}

async function fetchGamePlayersBatch(gameIds, signal) {
  const ids = Array.isArray(gameIds) ? gameIds.join(",") : "";
  return fetchJson(`/api/game-players-batch?ids=${encodeURIComponent(ids)}`, { signal, immediate: true });
}

async function fetchGameGenre(gameId, signal) {
  return fetchJson(`/api/game-genre?gameId=${encodeURIComponent(gameId)}`, { signal, immediate: true });
}

async function fetchGameGenreRefresh(gameId, signal) {
  return fetchJson(`/api/game-genre-refresh?gameId=${encodeURIComponent(gameId)}`, { signal });
}

async function fetchGameGenresBatch(gameIds, signal) {
  const ids = Array.isArray(gameIds) ? gameIds.join(",") : "";
  return fetchJson(`/api/game-genres-batch?ids=${encodeURIComponent(ids)}`, { signal, immediate: true });
}

async function fetchConsoles() {
  return fetchJson("/api/consoles");
}

async function fetchGameListForConsole(consoleId, letter) {
  return fetchJson(`/api/game-list?consoleId=${encodeURIComponent(consoleId)}&letter=${encodeURIComponent(letter)}`);
}

async function fetchAuthMe() {
  try {
    const data = await fetchServerJson("/api/auth/me", { silent: true });
    return clampUsername(data?.username || "");
  } catch {
    return "";
  }
}

async function fetchAuthMeWithRetry(retries = 2) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    const username = await fetchAuthMe();
    if (username) return username;
  }
  return "";
}

async function loginUser(username) {
  const data = await fetchServerJson("/api/auth/login", {
    method: "POST",
    body: { username }
  });
  return clampUsername(data?.username || username);
}

async function fetchSocialPosts(limit = SOCIAL_MAX_POSTS) {
  const count = Number.isFinite(limit) ? limit : SOCIAL_MAX_POSTS;
  return fetchServerJson(`/api/social/posts?limit=${encodeURIComponent(count)}`, { silent: true });
}

async function createSocialPost({ postType = "text", imageData = "", imageUrl = "", caption = "", game = "", achievementTitle = "", achievementDescription = "", achievementId = "" }) {
  return fetchServerJson("/api/social/posts", {
    method: "POST",
    body: {
      postType,
      imageData,
      imageUrl,
      caption,
      game,
      achievementTitle,
      achievementDescription,
      achievementId
    }
  });
}

async function setSocialReaction(postId, reaction) {
  return fetchServerJson(`/api/social/posts/${encodeURIComponent(postId)}/reaction`, {
    method: "POST",
    body: { reaction }
  });
}

async function loadFriendsFromServer() {
  try {
    const data = await fetchServerJson("/api/friends", { silent: true });
    const list = Array.isArray(data?.results) ? data.results : [];
    return list.map(clampUsername).filter(Boolean);
  } catch {
    return [];
  }
}

async function addFriendToServer(username) {
  await fetchServerJson("/api/friends", {
    method: "POST",
    body: { username }
  });
}

async function removeFriendFromServer(username) {
  await fetchServerJson(`/api/friends/${encodeURIComponent(username)}`, {
    method: "DELETE"
  });
}

// Lightweight UI helpers.
function setStatus(msg) {
  const text = msg || "";
  const lowered = text.toLowerCase();
  if (lowered.includes("ra api error 429") || lowered.includes("too many attempts") || lowered.includes("too_many_requests")) {
    return;
  }
  statusEl.textContent = text;
}

function setLoading(el, isLoading) {
  if (!el) return;
  el.hidden = !isLoading;
}

// Formatting helpers.
function formatCountdown(ms) {
  const total = Math.max(0, Math.ceil(ms / 1000));
  const m = Math.floor(total / 60);
  const s = String(total % 60).padStart(2, "0");
  return `${m}:${s}`;
}

// Auto-refresh countdown display.
function setNextRefresh(delayMs = AUTO_REFRESH_MS) {
  nextRefreshAt = Date.now() + delayMs;
}

function startRefreshCountdown() {
  if (refreshCountdownTimer) return;
  refreshCountdownTimer = setInterval(() => {
    if (!nextRefreshAt) return;
    const remaining = nextRefreshAt - Date.now();
      if (remaining <= 0) {
        refreshLeaderboard();
        if (activeActivityTab === "times") {
          refreshRecentTimes();
        } else {
          refreshRecentAchievements();
        }
        setNextRefresh();
        return;
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

function getUserValidationError(err, username) {
  const msg = String(err?.message || "");
  const notFound = msg.includes("404") || msg.toLowerCase().includes("not found");
  const apiKeyMissing = msg.toLowerCase().includes("missing ra api key");
  if (apiKeyMissing) return "API key required to validate this user.";
  if (notFound) return `User not found: ${username}`;
  return `Could not verify user: ${username}`;
}

function formatAgeSeconds(secsRaw) {
  const secs = Number(secsRaw);
  if (!Number.isFinite(secs) || secs < 0) return "";
  if (secs < 60) return `${Math.floor(secs)}s ago`;
  const mins = Math.floor(secs / 60);
  if (mins < 120) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  const days = Math.floor(hrs / 24);
  return `${days}d ago`;
}

function formatLastSeen(summary) {
  if (!summary) return "";
  let lastActiveValue = summary.lastActivity;
  if (lastActiveValue && typeof lastActiveValue === "object") {
    lastActiveValue =
      lastActiveValue.Date ?? lastActiveValue.date ??
      lastActiveValue.LastActivity ?? lastActiveValue.lastActivity ??
      lastActiveValue.LastUpdated ?? lastActiveValue.lastUpdated ??
      lastActiveValue.timestamp ?? lastActiveValue.time ??
      lastActiveValue.LastPlayed ?? lastActiveValue.lastPlayed ??
      "";
  }
  if (!lastActiveValue) return "";
  return formatDate(lastActiveValue);
}

async function hydrateFriendSummaries(list) {
  const targets = list
    .map(name => ({ name, key: normalizeUserKey(name) }))
    .filter(entry => entry.key && !friendSummaryCache.has(entry.key));
  if (!targets.length) return;
  await Promise.all(targets.map(({ name, key }) => summaryLimiter(async () => {
    try {
      const data = await fetchUserSummary(name, { silent: true });
      friendSummaryCache.set(key, data || null);
    } catch {
      friendSummaryCache.set(key, null);
    }
  })));
}

async function hydrateFriendPresence(list) {
  const targets = list
    .map(name => ({ name, key: normalizeUserKey(name) }))
    .filter(entry => entry.key && !friendPresenceCache.has(entry.key));
  if (!targets.length) return;
  await Promise.all(targets.map(({ name, key }) => summaryLimiter(async () => {
    try {
      const data = await fetchNowPlaying(name, 600);
      friendPresenceCache.set(key, data || null);
    } catch {
      friendPresenceCache.set(key, null);
    }
  })));
}

async function bootstrapAfterLogin() {
  startPresence();
  startNotificationsLive();
  refreshLeaderboard();
  await refreshGroupsPage();
  if (activeActivityTab === "times") {
    refreshRecentTimes();
  } else {
    refreshRecentAchievements();
  }
  refreshChallenges({ includeTotals: true });
  resetRefreshCountdown();
}

async function loginAndStart(username, { errorEl, loadingEl, closeModal } = {}) {
  try {
    if (loadingEl) loadingEl.hidden = false;
    const apiKey = (apiKeyInput?.value || "").trim();
    if (useApiKeyToggle?.checked && apiKey) {
      await fetchUserSummary(username, { silent: true });
    }
  } catch (e) {
    const message = getUserValidationError(e, username);
    if (errorEl) {
      errorEl.textContent = message;
    } else {
      setStatus(message);
    }
    if (loadingEl) loadingEl.hidden = true;
    return false;
  }

  try {
    const loggedIn = await loginUser(username);
    setCurrentUser(loggedIn);
    friends = await loadFriendsFromServer();
    renderChallengeFriendOptions(friends);
    saveState();
  } catch (e) {
    const message = String(e?.message || "Login failed.");
    if (errorEl) {
      errorEl.textContent = message;
    } else {
      setStatus(message);
    }
    if (loadingEl) loadingEl.hidden = true;
    return false;
  }

  if (closeModal) closeModal();
  if (loadingEl) loadingEl.hidden = true;
  await bootstrapAfterLogin();
  return true;
}

async function addFriendFromModal() {
  const me = ensureUsername();
  const u = clampUsername(friendInput.value);
  if (!me) {
    if (addFriendErrorEl) addFriendErrorEl.textContent = "Set your username first.";
    return;
  }
  if (!u) return;
  if (u.toLowerCase() === me.toLowerCase()) return setStatus("That's you. Add someone else.");

  try {
    if (addFriendLoadingEl) addFriendLoadingEl.hidden = false;
    const apiKey = (apiKeyInput?.value || "").trim();
    if (useApiKeyToggle?.checked && apiKey) {
      await fetchUserSummary(u, { silent: true });
    }
  } catch (e) {
    if (addFriendErrorEl) addFriendErrorEl.textContent = getUserValidationError(e, u);
    if (addFriendLoadingEl) addFriendLoadingEl.hidden = true;
    return;
  }

  try {
    await addFriendToServer(u);
    friends = Array.from(new Set([...friends, u]));
    renderChallengeFriendOptions(friends);
    if (activePageName === "friends") renderFriendsList(friends);
    friendInput.value = "";
    closeAddFriendModal();
  } catch (e) {
    if (addFriendErrorEl) addFriendErrorEl.textContent = String(e?.message || "Unable to add friend.");
  } finally {
    if (addFriendLoadingEl) addFriendLoadingEl.hidden = true;
  }

  refreshLeaderboard();
  if (activeActivityTab === "times") {
    refreshRecentTimes();
  } else {
    refreshRecentAchievements();
  }
}

function renderFriendsList(list = friends) {
  if (!friendsListEl) return;
  const sorted = Array.from(new Set(list.map(clampUsername).filter(Boolean))).sort((a, b) => a.localeCompare(b));
  if (!sorted.length) {
    friendsListEl.innerHTML = `<div class="meta">No friends added yet.</div>`;
    return;
  }
  const frag = document.createDocumentFragment();
  for (const name of sorted) {
    const summary = friendSummaryCache.get(normalizeUserKey(name)) || null;
    const avatarUrl = summary?.userPic ? iconUrl(summary.userPic) : "";
    const presence = friendPresenceCache.get(normalizeUserKey(name)) || null;
    const lastSeenAge = presence ? formatAgeSeconds(presence.ageSeconds) : "";
    const lastSeenDate = lastSeenAge || formatLastSeen(summary);
    const lastSeenText = presence?.nowPlaying
      ? "Playing now"
      : (lastSeenDate ? `Last seen ${lastSeenDate}` : "Last seen --");
    const row = document.createElement("div");
    row.className = "friendRow";
    row.innerHTML = `
      <div class="friendMain">
        <div class="friendAvatarWrap">
          ${avatarUrl ? `<img class="friendAvatar" src="${avatarUrl}" alt="" loading="lazy" />` : `<div class="friendAvatar placeholder"></div>`}
        </div>
        <div class="friendMeta">
          <button class="linkBtn friendName" type="button" data-profile="${safeText(name)}">${safeText(name)}</button>
          <div class="friendLastSeen">${safeText(lastSeenText)}</div>
        </div>
      </div>
      <button class="smallBtn" type="button" data-remove="${safeText(name)}">Remove</button>
    `;
    frag.appendChild(row);
  }
  friendsListEl.innerHTML = "";
  friendsListEl.appendChild(frag);
}

async function refreshFriendsPage() {
  if (!friendsListEl) return;
  const me = ensureUsername({ prompt: true });
  if (!me) {
    if (friendsStatusEl) friendsStatusEl.textContent = "Set your username first.";
    friendsListEl.innerHTML = "";
    return;
  }
  if (friendsStatusEl) friendsStatusEl.textContent = "";
  setLoading(friendsLoadingEl, true);
  try {
    friends = await loadFriendsFromServer();
    renderFriendsList(friends);
    await hydrateFriendSummaries(friends);
    await hydrateFriendPresence(friends);
    renderFriendsList(friends);
  } catch {
    if (friendsStatusEl) friendsStatusEl.textContent = "Failed to load friends.";
    friendsListEl.innerHTML = `<div class="meta">Unable to load friends.</div>`;
  } finally {
    setLoading(friendsLoadingEl, false);
  }
}

async function fetchNotifications({ unreadOnly = false, limit = 50, silent = false } = {}) {
  const params = new URLSearchParams();
  if (unreadOnly) params.set("unread", "1");
  if (limit) params.set("limit", String(limit));
  const url = params.toString()
    ? `/api/notifications?${params.toString()}`
    : "/api/notifications";
  return fetchServerJson(url, { silent });
}

async function markNotificationsRead(ids = []) {
  return fetchServerJson("/api/notifications/mark-read", {
    method: "POST",
    body: ids.length ? { ids } : {}
  });
}

async function deleteNotification(id) {
  return fetchServerJson(`/api/notifications/${encodeURIComponent(id)}`, {
    method: "DELETE"
  });
}

function updateNotificationsBadge(count) {
  notificationsUnreadCount = Number.isFinite(Number(count)) ? Number(count) : 0;
  if (!notificationsBadge) return;
  if (notificationsUnreadCount <= 0) {
    notificationsBadge.hidden = true;
    notificationsBadge.textContent = "0";
    return;
  }
  notificationsBadge.hidden = false;
  notificationsBadge.textContent = String(notificationsUnreadCount);
}

function renderNotifications(list = []) {
  if (!notificationsListEl) return;
  if (!list.length) {
    notificationsListEl.innerHTML = `<div class="meta">No notifications yet.</div>`;
    return;
  }
  const frag = document.createDocumentFragment();
  for (const item of list) {
    const row = document.createElement("div");
    row.className = `notifItem${item.read_at ? "" : " unread"}`;
    if (item.type) row.dataset.type = item.type;
    if (item.meta?.challengeId) row.dataset.challengeId = String(item.meta.challengeId);
    if (item.meta?.from) row.dataset.from = String(item.meta.from);
    row.dataset.id = String(item.id || "");
    const message = item.message || "Notification";
    const time = item.created_at ? formatDate(item.created_at) : "";
    const statusLabel = item.read_at ? "Read" : "Unread";
    row.innerHTML = `
      <div class="notifRow">
        <span class="notifTag ${item.read_at ? "read" : "unread"}">${statusLabel}</span>
        <button class="notifDeleteBtn" type="button" data-id="${safeText(item.id)}" aria-label="Delete notification">�</button>
      </div>
      <div class="notifMessage">${safeText(message)}</div>
      <div class="notifMeta">${time ? safeText(time) : ""}</div>
    `;
    frag.appendChild(row);
  }
  notificationsListEl.innerHTML = "";
  notificationsListEl.appendChild(frag);
}

async function refreshNotificationsBadge() {
  try {
    const data = await fetchNotifications({ unreadOnly: true, limit: 1, silent: true });
    updateNotificationsBadge(data?.unreadCount ?? 0);
  } catch {
    // ignore badge errors
  }
}

async function loadNotifications({ markRead = false } = {}) {
  if (!notificationsListEl) return;
  setLoading(notificationsLoadingEl, true);
  try {
    const data = await fetchNotifications({ unreadOnly: false, limit: 50, silent: true });
    const items = Array.isArray(data?.results) ? data.results : [];
    renderNotifications(items);
    updateNotificationsBadge(data?.unreadCount ?? 0);
    if (markRead && data?.unreadCount) {
      await markNotificationsRead();
      updateNotificationsBadge(0);
    }
  } catch {
    notificationsListEl.innerHTML = `<div class="meta">Failed to load notifications.</div>`;
  } finally {
    setLoading(notificationsLoadingEl, false);
  }
}

function openNotificationsPanel() {
  if (!notificationsPanel) return;
  notificationsPanel.hidden = false;
  notificationsOpen = true;
  notificationsReadAfterClose = true;
  loadNotifications({ markRead: false });
}

function closeNotificationsPanel() {
  if (!notificationsPanel) return;
  notificationsPanel.hidden = true;
  notificationsOpen = false;
  if (notificationsReadAfterClose) {
    notificationsReadAfterClose = false;
    markNotificationsRead().then(() => updateNotificationsBadge(0)).catch(() => {});
  }
}

function startNotificationsPolling() {
  refreshNotificationsBadge();
  if (notificationsPollTimer) clearInterval(notificationsPollTimer);
  notificationsPollTimer = setInterval(refreshNotificationsBadge, 30000);
}

function stopNotificationsPolling() {
  if (notificationsPollTimer) {
    clearInterval(notificationsPollTimer);
    notificationsPollTimer = null;
  }
}

function startNotificationsLive() {
  if (notificationsEventSource) return;
  if (!window.EventSource) {
    startNotificationsPolling();
    return;
  }
  try {
    const source = new EventSource("/api/notifications/stream");
    notificationsEventSource = source;
    source.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data || "{}");
        if (data && typeof data.unreadCount !== "undefined") {
          updateNotificationsBadge(data.unreadCount);
          if (notificationsOpen) {
            loadNotifications({ markRead: false });
          }
        }
      } catch {
        // ignore bad payloads
      }
    };
    source.onerror = () => {
      source.close();
      notificationsEventSource = null;
      startNotificationsPolling();
    };
  } catch {
    startNotificationsPolling();
  }
}

function stopNotificationsLive() {
  if (notificationsEventSource) {
    notificationsEventSource.close();
    notificationsEventSource = null;
  }
  stopNotificationsPolling();
}

function openSettings() {
  if (!settingsModal) return;
  if (meInput) meInput.value = currentUser;
  if (debugUiToggle) {
    debugUiToggle.checked = localStorage.getItem(LS_DEBUG_UI) === "true";
  }
  settingsModal.hidden = false;
  meInput.focus();
}

function closeSettings() {
  if (!settingsModal) return;
  settingsModal.hidden = true;
}

function ensureUsername({ prompt = true } = {}) {
  const existing = currentUser;
  if (existing) return existing;

  if (prompt && usernameModal) {
    usernameModal.hidden = false;
    if (usernameModalInput) {
      usernameModalInput.value = "";
      usernameModalInput.focus();
    }
    if (usernameModalApiKeyInput) {
      const useApiKey = !!useApiKeyToggle?.checked;
      usernameModalApiKeyInput.value = useApiKey ? (apiKeyInput?.value || "") : "";
    }
    if (usernameModalUseApiKeyToggle) {
      usernameModalUseApiKeyToggle.checked = !!useApiKeyToggle?.checked;
    }
    if (usernameModalErrorEl) usernameModalErrorEl.textContent = "";
    if (usernameModalLoadingEl) usernameModalLoadingEl.hidden = true;
  }
  return "";
}

function setActiveTab(name) {
  const wasActive = activeActivityTab;
  tabButtons.forEach(btn => {
    const isActive = btn.dataset.tab === name;
    btn.classList.toggle("active", isActive);
    btn.setAttribute("aria-selected", isActive ? "true" : "false");
  });

  tabPanels.forEach(panel => {
    const isActive = panel.id === `tab-${name}`;
    panel.classList.toggle("active", isActive);
  });
  activeActivityTab = name;
  if (name !== wasActive) {
    if (name === "achievements") {
      refreshRecentAchievements();
    } else if (name === "times") {
      refreshRecentTimes();
    }
  }
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

function moveProfilePanel(targetHost) {
  if (!profilePanel || !targetHost) return;
  if (profilePanel.parentElement !== targetHost) {
    targetHost.appendChild(profilePanel);
  }
}

function moveSelfGamePanel(targetHost) {
  if (!selfGamePanel || !targetHost) return;
  if (selfGamePanel.parentElement !== targetHost) {
    targetHost.appendChild(selfGamePanel);
  }
}

function moveComparePanel(targetHost) {
  if (!comparePanel || !targetHost) return;
  if (comparePanel.parentElement !== targetHost) {
    targetHost.appendChild(comparePanel);
  }
}

function setActivePage(name) {
  if (name && name !== activePageName) {
    pausedPages.add(activePageName);
    activePageName = name;
    pausedPages.delete(name);
    processClientQueue();
    processFastQueue();
  }
  const isProfileSelf = name === "profile" &&
    currentProfileUser && currentUser &&
    normalizeUserKey(currentProfileUser) === normalizeUserKey(currentUser);
  pageButtons.forEach(btn => {
    const isTarget = btn.dataset.page === name;
    const isProfileBtn = btn.dataset.page === "profile";
    const isActive = isTarget && (!isProfileBtn || isProfileSelf);
    btn.classList.toggle("active", isActive);
    btn.setAttribute("aria-selected", isActive ? "true" : "false");
  });
  if (dashboardPage) dashboardPage.hidden = name !== "dashboard";
  if (findGamesPage) findGamesPage.hidden = name !== "find-games";
  if (challengesPage) challengesPage.hidden = name !== "challenges";
  if (groupsPage) groupsPage.hidden = name !== "groups";
  if (socialPage) socialPage.hidden = name !== "social";
  if (profilePage) profilePage.hidden = name !== "profile";
  if (backlogPage) backlogPage.hidden = name !== "backlog";
  if (friendsPage) friendsPage.hidden = name !== "friends";
  if (gamePage) gamePage.hidden = name !== "game";
  if (profilePanel) profilePanel.hidden = name !== "profile";
  if (name !== "backlog") setBacklogRemoveMode(false);
  if (name === "find-games") {
    ensureFindGamesReady();
    if (findGamesTab === "search") {
      loadFindGamesLetter(findGamesLetter);
    } else {
      loadSuggestedGames();
    }
  }
  if (name === "backlog") {
    if (!backlogViewUser) setBacklogViewUser(currentUser);
    renderBacklog();
  }
  if (name === "social") {
    loadSocialPostsFromServer();
    updateSocialComposerState();
    startSocialPolling();
  } else {
    stopSocialPolling();
  }
  if (name === "friends") {
    refreshFriendsPage();
  }
  if (name === "groups") {
    refreshGroupsPage();
  }
  if (name === "profile") {
    moveProfilePanel(profileHostProfile);
    refreshCompletionBadges();
    loadSocialPostsFromServer({ silent: true });
  } else if (name === "game") {
    moveSelfGamePanel(selfGameHostPage);
    moveComparePanel(compareHostPage);
  } else {
    const isSelfOpen = currentProfileUser && currentUser &&
      currentProfileUser.toLowerCase() === currentUser.toLowerCase();
    if (!isSelfOpen) {
      moveProfilePanel(profileHostDashboard);
    }
    if (selfGamePanel) selfGamePanel.hidden = true;
    moveSelfGamePanel(selfGameHostPage);
    if (comparePanel) comparePanel.hidden = true;
    moveComparePanel(compareHostPage);
  }
}

function ensureFindGamesReady() {
  if (findGamesInitialized) return;
  findGamesInitialized = true;
  buildFindGamesLetters();
  if (findGamesTabButtons.length) {
    findGamesTabButtons.forEach((btn) => {
      btn.addEventListener("click", () => setFindGamesTab(btn.dataset.findTab));
    });
    setFindGamesTab(findGamesTab);
  }
  if (findGamesAchievementsEl) {
    findGamesAchievementsEl.innerHTML = `<div class="meta">Select a game to see achievements.</div>`;
    findGamesAchievementsEl.addEventListener("click", (e) => {
      const img = e.target.closest(".findGameMediaItem img");
      if (!img || !imageModal || !imageModalImg) return;
      const src = img.getAttribute("src");
      if (!src) return;
      imageModalImg.src = src;
      imageModal.hidden = false;
    });
  }
  if (findSuggestedAchievementsEl) {
    findSuggestedAchievementsEl.innerHTML = `<div class="meta">Select a game to see achievements.</div>`;
    findSuggestedAchievementsEl.addEventListener("click", (e) => {
      const img = e.target.closest(".findGameMediaItem img");
      if (!img || !imageModal || !imageModalImg) return;
      const src = img.getAttribute("src");
      if (!src) return;
      imageModalImg.src = src;
      imageModal.hidden = false;
    });
  }
  if (findSearchBackBtn) {
    findSearchBackBtn.addEventListener("click", () => {
      showFindGamesListView("search");
    });
  }
  if (findSuggestedBackBtn) {
    findSuggestedBackBtn.addEventListener("click", () => {
      showFindGamesListView("suggested");
    });
  }
  if (findGamesSearchInput) {
    findGamesSearchInput.addEventListener("input", () => {
      findGamesQuery = findGamesSearchInput.value || "";
      cancelFindGamesMetaRequests();
      resetFindGamesVisibleCount();
      refreshFindGamesSearch();
    });
  }
  if (findGamesSortSelect) {
    findGamesSortSelect.value = findGamesSort;
    findGamesSortSelect.addEventListener("change", () => {
      findGamesSort = String(findGamesSortSelect.value || "name");
      cancelFindGamesMetaRequests();
      resetFindGamesVisibleCount();
      refreshFindGamesSearch();
    });
  }
  if (findGamesGenreSelect) {
    findGamesGenreSelect.addEventListener("change", () => {
      findGamesGenre = String(findGamesGenreSelect.value || "all");
      resetFindGamesVisibleCount();
      refreshFindGamesSearch();
    });
  }
  if (findGamesShowMoreBtn) {
    findGamesShowMoreBtn.addEventListener("click", () => {
      findGamesVisibleCount += FIND_GAMES_SHOW_MORE_STEP;
      refreshFindGamesSearch();
    });
  }
  if (findGamesConsoleSelect) {
    findGamesConsoleSelect.addEventListener("change", () => {
      const selected = String(findGamesConsoleSelect.value || "");
      setFindGamesConsole(selected);
    });
  }
  if (findGamesListEl) {
    findGamesListEl.addEventListener("click", (e) => {
      const backlogBtn = e.target.closest("[data-backlog]");
      if (backlogBtn) {
        e.stopPropagation();
        const tile = backlogBtn.closest("[data-game-id]");
        if (!tile) return;
        const gameId = tile.getAttribute("data-game-id");
        if (isInBacklog(gameId)) {
          removeFromBacklog(gameId);
        } else {
          addToBacklog({
            gameId,
            title: tile.getAttribute("data-title") || "",
            consoleName: tile.getAttribute("data-console") || "",
            imageIcon: tile.getAttribute("data-image") || "",
            numAchievements: Number(tile.getAttribute("data-achievements") || 0),
            points: Number(tile.getAttribute("data-points") || 0)
          });
        }
        return;
      }
      const row = e.target.closest("[data-game-id]");
      if (!row) return;
      const game = {
        gameId: row.getAttribute("data-game-id"),
        title: row.getAttribute("data-title") || "",
        consoleName: row.getAttribute("data-console") || ""
      };
      showFindGamesAchievementsView("search");
      loadFindGameAchievements(game, { targetEl: findGamesAchievementsEl, listEl: findGamesListEl, selectedClass: "selected" });
    });
  }
  if (findSuggestedListEl) {
    findSuggestedListEl.addEventListener("click", (e) => {
      const backlogBtn = e.target.closest("[data-backlog]");
      if (backlogBtn) {
        e.stopPropagation();
        const tile = backlogBtn.closest("[data-game-id]");
        if (!tile) return;
        const gameId = tile.getAttribute("data-game-id");
        if (isInBacklog(gameId)) {
          removeFromBacklog(gameId);
        } else {
          addToBacklog({
            gameId,
            title: tile.getAttribute("data-title") || "",
            consoleName: tile.getAttribute("data-console") || "",
            imageIcon: tile.getAttribute("data-image") || "",
            numAchievements: Number(tile.getAttribute("data-achievements") || 0),
            points: Number(tile.getAttribute("data-points") || 0)
          });
        }
        return;
      }
      const tile = e.target.closest("[data-game-id]");
      if (!tile) return;
      const game = {
        gameId: tile.getAttribute("data-game-id"),
        title: tile.getAttribute("data-title") || "",
        consoleName: tile.getAttribute("data-console") || "",
        imageIcon: tile.getAttribute("data-image") || ""
      };
      showFindGamesAchievementsView("suggested");
      loadFindGameAchievements(game, { targetEl: findSuggestedAchievementsEl, listEl: findSuggestedListEl, selectedClass: "selected" });
    });
  }
  loadFindGamesConsoles();
}

function closeImageModal() {
  if (!imageModal || !imageModalImg) return;
  imageModal.hidden = true;
  imageModalImg.src = "";
}

function isInBacklog(gameId) {
  const gid = String(gameId ?? "");
  if (!gid) return false;
  const items = backlogItems.length ? backlogItems : [];
  return items.some(item => String(item.gameId) === gid);
}

async function fetchBacklog(username) {
  const user = clampUsername(username || "");
  if (!user) return [];
  const data = await fetchJson(`/api/backlog/${encodeURIComponent(user)}`);
  return Array.isArray(data?.results) ? data.results : [];
}

async function addToBacklog(game) {
  if (!currentUser) {
    setStatus("Set your username before adding to backlog.");
    return;
  }
  const gid = String(game.gameId ?? "");
  if (!gid) return;
  await fetchServerJson("/api/backlog", {
    method: "POST",
    body: {
      gameId: game.gameId,
      title: game.title || "",
      consoleName: game.consoleName || "",
      imageIcon: game.imageIcon || "",
      numAchievements: Number(game.numAchievements ?? 0),
      points: Number(game.points ?? 0)
    }
  });
  backlogItems = await fetchBacklog(currentUser);
  if (backlogStatusEl) backlogStatusEl.textContent = "Added to backlog.";
  refreshFindGameBacklogButtons();
  if (activePageName === "backlog") renderBacklog();
}

async function removeFromBacklog(gameId) {
  if (!currentUser) return;
  const gid = String(gameId ?? "");
  if (!gid) return;
  await fetchServerJson(`/api/backlog/${encodeURIComponent(gid)}`, { method: "DELETE" });
  backlogItems = await fetchBacklog(currentUser);
  if (backlogStatusEl) backlogStatusEl.textContent = "Removed from backlog.";
  refreshFindGameBacklogButtons();
  if (activePageName === "backlog") renderBacklog();
}

async function updateBacklogProgress(gameId, earned, total) {
  const gid = String(gameId ?? "");
  if (!gid) return;
  await fetchServerJson(`/api/backlog/${encodeURIComponent(gid)}/progress`, {
    method: "PUT",
    body: {
      awarded: Number(earned ?? 0),
      total: Number(total ?? 0)
    }
  });
}

function refreshFindGameBacklogButtons() {
  if (findGamesListEl) {
    if (findGamesQuery && findGamesQuery.trim()) {
      refreshFindGamesSearch();
    } else {
      renderFindGamesList(getFindGamesCachedList() || []);
    }
  }
  if (findSuggestedListEl) renderSuggestedGames(findSuggestedGames || []);
}

function setBacklogViewUser(username) {
  backlogViewUser = clampUsername(username || "");
  if (!isViewingOwnBacklog()) setBacklogRemoveMode(false);
}

const BACKLOG_TRASH_ICON = `
<svg class="iconTrash" viewBox="0 0 24 24" aria-hidden="true" focusable="false">
  <path d="M9 3h6l1 2h5v2H3V5h5l1-2zm1 6h2v9h-2V9zm4 0h2v9h-2V9zM7 9h2v9H7V9z"/>
</svg>`;

function isViewingOwnBacklog() {
  if (!currentUser || !backlogViewUser) return false;
  return normalizeUserKey(currentUser) === normalizeUserKey(backlogViewUser);
}

function updateBacklogRemoveUi() {
  const isSelf = isViewingOwnBacklog();
  if (backlogRemoveBtn) {
    backlogRemoveBtn.hidden = !isSelf;
    backlogRemoveBtn.innerHTML = backlogRemoveMode ? "X" : BACKLOG_TRASH_ICON;
    backlogRemoveBtn.setAttribute("aria-pressed", backlogRemoveMode ? "true" : "false");
    backlogRemoveBtn.setAttribute("aria-label", backlogRemoveMode ? "Exit remove mode" : "Enter remove mode");
  }
  if (backlogListEl) {
    backlogListEl.classList.toggle("backlogRemoveMode", backlogRemoveMode && isSelf);
  }
  if (!isSelf) backlogRemoveMode = false;
}

function setBacklogRemoveMode(enabled) {
  backlogRemoveMode = !!enabled && isViewingOwnBacklog();
  updateBacklogRemoveUi();
}

async function ensureBacklogLoaded() {
  if (!currentUser) return;
  if (backlogItems.length) return;
  try {
    backlogItems = await fetchBacklog(currentUser);
  } catch {
    backlogItems = [];
  }
}

async function fetchBacklogProgress(username, gameId) {
  const userKey = normalizeUserKey(username || "");
  const gid = String(gameId ?? "");
  if (!gid || !userKey) return null;
  const cacheKey = `${userKey}:${gid}`;
  const cached = backlogProgressCache.get(cacheKey);
  if (cached && (Date.now() - cached.ts) < BACKLOG_PROGRESS_TTL_MS) {
    return cached.data;
  }
  try {
    const data = await fetchGameAchievements(userKey, gid);
    const achievements = Array.isArray(data?.achievements) ? data.achievements : [];
    const earned = achievements.filter(a => a.earned || a.earnedHardcore).length;
    const result = { earned, total: achievements.length };
    backlogProgressCache.set(cacheKey, { ts: Date.now(), data: result });
    return result;
  } catch {
    return null;
  }
}

function splitBacklogByProgress(items, username) {
  const userKey = normalizeUserKey(username || "");
  const inProgress = [];
  const notStarted = [];
  items.forEach((item) => {
    const cacheKey = `${userKey}:${String(item.gameId ?? "")}`;
    const cached = backlogProgressCache.get(cacheKey);
    const earned = Number(cached?.data?.earned ?? item.startedAwarded ?? 0);
    if (earned > 0) {
      inProgress.push(item);
    } else {
      notStarted.push(item);
    }
  });
  return { inProgress, notStarted };
}

function renderBacklogTiles(items, username) {
  const userKey = normalizeUserKey(username || "");
  const progressMap = new Map();
  items.forEach((item) => {
    const cacheKey = `${userKey}:${String(item.gameId ?? "")}`;
    const cached = backlogProgressCache.get(cacheKey);
    if (cached?.data) {
      progressMap.set(String(item.gameId ?? ""), cached.data);
    } else if (Number(item.startedAwarded ?? 0) > 0 || Number(item.startedTotal ?? 0) > 0) {
      progressMap.set(String(item.gameId ?? ""), {
        earned: Number(item.startedAwarded ?? 0),
        total: Number(item.startedTotal ?? 0)
      });
    }
  });
  const grid = document.createElement("div");
  grid.className = "findGamesGrid";
  const frag = document.createDocumentFragment();
  for (const item of items) {
    const tile = document.createElement("div");
    tile.className = "tile";
    if (item.gameId) tile.setAttribute("data-game-id", String(item.gameId));

    const img = document.createElement("img");
    img.src = iconUrl(item.imageIcon);
    img.alt = safeText(item.title || "game");
    img.loading = "lazy";

    const title = document.createElement("div");
    title.className = "tileTitle";
    title.textContent = item.title || `Game ${safeText(item.gameId)}`;

    const consoleLine = document.createElement("div");
    consoleLine.className = "tileMeta";
    consoleLine.textContent = item.consoleName || "";

    const achievementsLine = document.createElement("div");
    achievementsLine.className = "tileMeta";
    achievementsLine.textContent = `${Number(item.numAchievements ?? 0)} achievements`;

    const pointsLine = document.createElement("div");
    pointsLine.className = "tileMeta";
    pointsLine.textContent = `${Number(item.points ?? 0)} points`;

    const progress = progressMap.get(String(item.gameId ?? ""));
    const progressLine = document.createElement("div");
    progressLine.className = "tileMeta progressLine";
    if (progress) {
      const total = Number(progress.total || 0);
      const earned = Number(progress.earned || 0);
      const pct = total > 0 ? Math.min(100, Math.max(0, Math.round((earned / total) * 100))) : 0;
      progressLine.style.setProperty("--progress", String(pct));
      const text = document.createElement("span");
      text.className = "progressLineText";
      text.textContent = `${earned} / ${total} achievements`;
      progressLine.appendChild(text);
    }

    tile.appendChild(img);
    tile.appendChild(title);
    if (consoleLine.textContent) tile.appendChild(consoleLine);
    tile.appendChild(achievementsLine);
    tile.appendChild(pointsLine);
    if (progressLine.textContent) tile.appendChild(progressLine);
    frag.appendChild(tile);
  }
  grid.appendChild(frag);
  return grid;
}

async function renderBacklog() {
  if (!backlogListEl || !backlogStatusEl) return;
  if (!currentUser) {
    backlogStatusEl.textContent = "Set your username to use the backlog.";
    backlogListEl.innerHTML = "";
    return;
  }
  const targetUser = backlogViewUser || currentUser;
  updateBacklogRemoveUi();
  const items = await fetchBacklog(targetUser);
  if (normalizeUserKey(targetUser) === normalizeUserKey(currentUser)) {
    backlogItems = items;
  }
  if (!items.length) {
    const label = backlogViewUser && currentUser && normalizeUserKey(backlogViewUser) !== normalizeUserKey(currentUser)
      ? `${backlogViewUser} has no backlog items.`
      : "No games in your backlog yet.";
    backlogStatusEl.textContent = label;
    backlogListEl.innerHTML = "";
    return;
  }
  const renderToken = ++backlogRenderToken;
  backlogStatusEl.textContent = `${items.length} games in your backlog.`;
  backlogListEl.innerHTML = "";
  const initialGroups = splitBacklogByProgress(items, targetUser);
  if (initialGroups.inProgress.length) {
    const header = document.createElement("h3");
    header.className = "sectionTitle";
    header.textContent = "Games in Progress";
    backlogListEl.appendChild(header);
    backlogListEl.appendChild(renderBacklogTiles(initialGroups.inProgress, targetUser));
  }
  if (initialGroups.notStarted.length) {
    const header = document.createElement("h3");
    header.className = "sectionTitle";
    header.textContent = "Not Started";
    backlogListEl.appendChild(header);
    backlogListEl.appendChild(renderBacklogTiles(initialGroups.notStarted, targetUser));
  }
  setLoading(backlogLoadingEl, false);
  Promise.all(items.map(item => backlogProgressLimiter(() => fetchBacklogProgress(targetUser, item.gameId))))
    .then(() => {
      if (renderToken !== backlogRenderToken) return;
      const isSelf = normalizeUserKey(targetUser) === normalizeUserKey(currentUser);
      let hasChanges = false;
      if (isSelf) {
        const updates = [];
        items.forEach((item) => {
          const cacheKey = `${normalizeUserKey(targetUser)}:${String(item.gameId ?? "")}`;
          const cached = backlogProgressCache.get(cacheKey)?.data;
          if (!cached) return;
          const earned = Number(cached.earned ?? 0);
          const total = Number(cached.total ?? 0);
          if (earned !== Number(item.startedAwarded ?? 0) || total !== Number(item.startedTotal ?? 0)) {
            item.startedAwarded = earned;
            item.startedTotal = total;
            hasChanges = true;
            updates.push({ gameId: item.gameId, earned, total });
          }
        });
        backlogItems = items;
        if (updates.length) {
          Promise.all(
            updates.map(update => backlogProgressUpdateLimiter(() => updateBacklogProgress(update.gameId, update.earned, update.total)))
          ).catch(() => {});
        }
      } else {
        items.forEach((item) => {
          const cacheKey = `${normalizeUserKey(targetUser)}:${String(item.gameId ?? "")}`;
          const cached = backlogProgressCache.get(cacheKey)?.data;
          if (!cached) return;
          const earned = Number(cached.earned ?? 0);
          const total = Number(cached.total ?? 0);
          if (earned !== Number(item.startedAwarded ?? 0) || total !== Number(item.startedTotal ?? 0)) {
            item.startedAwarded = earned;
            item.startedTotal = total;
            hasChanges = true;
          }
        });
      }
      if (!hasChanges) return;
      const { inProgress, notStarted } = splitBacklogByProgress(items, targetUser);
      backlogListEl.innerHTML = "";
      if (inProgress.length) {
        const header = document.createElement("h3");
        header.className = "sectionTitle";
        header.textContent = "Games in Progress";
        backlogListEl.appendChild(header);
        backlogListEl.appendChild(renderBacklogTiles(inProgress, targetUser));
      }
      if (notStarted.length) {
        const header = document.createElement("h3");
        header.className = "sectionTitle";
        header.textContent = "Not Started";
        backlogListEl.appendChild(header);
        backlogListEl.appendChild(renderBacklogTiles(notStarted, targetUser));
      }
    })
    .finally(() => {
      if (renderToken !== backlogRenderToken) return;
      setLoading(backlogLoadingEl, false);
    });
}

function setFindGamesTab(name) {
  if (!name) return;
  findGamesTab = name;
  if (findTabSearch) findTabSearch.hidden = name !== "search";
  if (findTabSuggested) findTabSuggested.hidden = name !== "suggested";
  findGamesTabButtons.forEach((btn) => {
    const active = btn.dataset.findTab === name;
    btn.classList.toggle("active", active);
    btn.setAttribute("aria-selected", active ? "true" : "false");
  });
  if (name === "search") {
    showFindGamesListView("search");
    ensureBacklogLoaded();
    loadFindGamesLetter(findGamesLetter);
  } else if (name === "suggested") {
    showFindGamesListView("suggested");
    ensureBacklogLoaded();
    loadSuggestedGames();
  }
}

function seriesInfoFromTitle(title) {
  const raw = String(title || "").trim();
  if (!raw) return null;
  const base = raw.split(/\s*[:\-��]\s*/)[0];
  const cleaned = base.replace(/\s*[\(\[].*?[\)\]]\s*/g, "").trim();
  if (!cleaned) return null;
  const stripped = cleaned.replace(/\s+(?:\d+|[ivxlcdm]+)$/i, "").trim();
  const label = stripped || cleaned;
  if (label.length < 3) return null;
  const key = label.toLowerCase().replace(/^the\s+/i, "").trim();
  if (!key) return null;
  return { key, label };
}

function setSelectedGameInList(listEl, gameId, selectedClass) {
  if (!listEl) return;
  const rows = listEl.querySelectorAll("[data-game-id]");
  rows.forEach((row) => {
    row.classList.toggle(selectedClass, row.dataset.gameId === String(gameId));
  });
}

function getConsoleMaker(name) {
  const value = String(name || "");
  if (!value) return "Other";
  const rules = [
    { label: "Sega", match: /sega|mega drive|genesis|saturn|dreamcast|game gear|master system|32x|sg-1000|nomad/i },
    { label: "Nintendo", match: /nintendo|famicom|nes|snes|super nintendo|game boy|gbc|gba|gamecube|wii|wii u|switch|virtual boy|pokemon mini|ds|3ds/i },
    { label: "Sony", match: /playstation|ps\s?one|ps1|ps2|ps3|ps4|ps5|psp|vita/i },
    { label: "Microsoft", match: /xbox/i },
    { label: "Atari", match: /atari|lynx|jaguar|2600|5200|7800/i },
    { label: "NEC", match: /pc-?engine|turbografx|tg-16|supergrafx|pc-?fx/i },
    { label: "SNK", match: /neo geo|ngp|ngpc/i },
    { label: "Commodore", match: /commodore|c64|amiga|cd32/i },
    { label: "Bandai", match: /bandai|wonderswan/i },
    { label: "Arcade", match: /arcade|mame|cps|neo geo mvs/i }
  ];
  for (const rule of rules) {
    if (rule.match.test(value)) return rule.label;
  }
  return "Other";
}

function sortConsolesByMaker(consoles) {
  const groups = new Map();
  for (const c of consoles) {
    const maker = getConsoleMaker(c.name || c.Name || "");
    if (!groups.has(maker)) groups.set(maker, []);
    groups.get(maker).push(c);
  }
  const makerOrder = [
    "Nintendo",
    "Sony",
    "Microsoft",
    "Sega",
    "Atari",
    "NEC",
    "SNK",
    "Commodore",
    "Bandai",
    "Arcade",
    "Other"
  ];
  const entries = Array.from(groups.entries()).sort((a, b) => {
    const ai = makerOrder.indexOf(a[0]);
    const bi = makerOrder.indexOf(b[0]);
    const aRank = ai === -1 ? makerOrder.length : ai;
    const bRank = bi === -1 ? makerOrder.length : bi;
    if (aRank !== bRank) return aRank - bRank;
    return a[0].localeCompare(b[0]);
  });
  for (const [, list] of entries) {
    list.sort((a, b) => String(a.name || a.Name || "").localeCompare(String(b.name || b.Name || "")));
  }
  return entries;
}

function showFindGamesListView(mode) {
  if (mode === "search") {
    if (findSearchListPanel) findSearchListPanel.hidden = false;
    if (findSearchAchievementsPanel) findSearchAchievementsPanel.hidden = true;
  }
  if (mode === "suggested") {
    if (findSuggestedListPanel) findSuggestedListPanel.hidden = false;
    if (findSuggestedAchievementsPanel) findSuggestedAchievementsPanel.hidden = true;
  }
}

function showFindGamesAchievementsView(mode) {
  if (mode === "search") {
    if (findSearchListPanel) findSearchListPanel.hidden = true;
    if (findSearchAchievementsPanel) findSearchAchievementsPanel.hidden = false;
  }
  if (mode === "suggested") {
    if (findSuggestedListPanel) findSuggestedListPanel.hidden = true;
    if (findSuggestedAchievementsPanel) findSuggestedAchievementsPanel.hidden = false;
  }
}

function getFindGamesCachedList() {
  if (!findGamesConsoleId) return null;
  const key = `${findGamesConsoleId}:${findGamesLetter}`;
  return findGamesCache.get(key) || null;
}

function getFindGamesAllCachedList() {
  if (!findGamesConsoleId) return null;
  const key = `${findGamesConsoleId}:all`;
  return findGamesCache.get(key) || null;
}

function refreshFindGamesSearch() {
  const query = findGamesQuery.trim();
  if (!query) {
    const list = getFindGamesCachedList();
    if (list) renderFindGamesList(list);
    return;
  }
  if (!findGamesConsoleId) return;
  const allList = getFindGamesAllCachedList();
  if (allList) {
    renderFindGamesList(allList);
    return;
  }
  loadFindGamesAllForSearch();
}

async function loadFindGamesAllForSearch() {
  if (!findGamesListEl) return;
  findGamesListEl.innerHTML = `<div class="meta">Loading all games for search...</div>`;
  setLoading(findGamesLoadingEl, true);
  try {
    const data = await fetchGameListByLetter("all");
    const results = Array.isArray(data?.results) ? data.results : [];
    const cacheKey = `${findGamesConsoleId}:all`;
    findGamesCache.set(cacheKey, results);
    renderFindGamesList(results);
  } catch (e) {
    findGamesListEl.innerHTML = `<div class="meta">Failed to load games for search.</div>`;
  } finally {
    setLoading(findGamesLoadingEl, false);
  }
}

async function loadSuggestedGames() {
  if (!findSuggestedListEl || !findSuggestedStatusEl) return;
  if (!currentUser) {
    findSuggestedStatusEl.textContent = "Set your username first to see suggestions.";
    findSuggestedListEl.innerHTML = "";
    return;
  }
  const userKey = normalizeUserKey(currentUser);
  if (findSuggestedLoadedFor === userKey && findSuggestedGames.length) {
    renderSuggestedGames(findSuggestedGames);
    return;
  }
  setLoading(findSuggestedLoadingEl, true);
  findSuggestedStatusEl.textContent = "Loading suggestions...";
  findSuggestedListEl.innerHTML = "";
  try {
    const recent = await fetchRecentGames(userKey, 40);
    const recentList = Array.isArray(recent?.results) ? recent.results : [];
    if (!recentList.length) {
      findSuggestedStatusEl.textContent = "No recent games found.";
      return;
    }

    const seriesMap = new Map();
    const recentIds = new Set();
    const recentTitles = new Set();
    if (!findGamesConsolesCache.length) {
      try {
        const consoleData = await fetchConsoles();
        findGamesConsolesCache = Array.isArray(consoleData?.results) ? consoleData.results : [];
      } catch {
        findGamesConsolesCache = [];
      }
    }
    const consoleNameMap = new Map(
      (findGamesConsolesCache || []).map(c => [String(c.name || "").toLowerCase(), String(c.id)])
    );
    const consoleIds = new Set();
    for (const g of recentList) {
      const gid = String(g.gameId ?? "");
      if (gid) recentIds.add(gid);
      const titleKey = String(g.title || "");
      if (titleKey) recentTitles.add(titleKey);
      const series = seriesInfoFromTitle(g.title);
      if (series) {
        if (!seriesMap.has(series.key) || series.label.length > seriesMap.get(series.key).length) {
          seriesMap.set(series.key, series.label);
        }
      }
      const cid = g.consoleId ? String(g.consoleId) : "";
      if (cid) {
        consoleIds.add(cid);
      } else if (g.consoleName) {
        const match = consoleNameMap.get(String(g.consoleName).toLowerCase());
        if (match) consoleIds.add(match);
      }
    }

    if (!seriesMap.size) {
      findSuggestedStatusEl.textContent = "No series found in your recent games.";
      return;
    }

    const consoleIdList = Array.from(consoleIds);
    if (!consoleIdList.length) {
      findSuggestedStatusEl.textContent = "No consoles found for recent games.";
      return;
    }
    const limitedConsoles = consoleIdList.slice(0, 6);

    const suggestions = new Map();
    for (const consoleId of limitedConsoles) {
      const cacheKey = `${consoleId}:all`;
      let list = findGamesCache.get(cacheKey);
      if (!list) {
        const payload = await fetchGameListForConsole(consoleId, "all");
        list = Array.isArray(payload?.results) ? payload.results : [];
        findGamesCache.set(cacheKey, list);
      }
      for (const game of list) {
        const gid = String(game.gameId ?? "");
        const titleKey = String(game.title || "");
        if (!gid || recentIds.has(gid) || suggestions.has(gid) || (titleKey && recentTitles.has(titleKey))) continue;
        const series = seriesInfoFromTitle(game.title);
        if (!series || !seriesMap.has(series.key)) continue;
        suggestions.set(gid, {
          ...game,
          series: seriesMap.get(series.key) || series.label
        });
      }
    }

    const suggestedList = Array.from(suggestions.values());
    for (let i = suggestedList.length - 1; i > 0; i -= 1) {
      const j = Math.floor(Math.random() * (i + 1));
      [suggestedList[i], suggestedList[j]] = [suggestedList[j], suggestedList[i]];
    }

    findSuggestedGames = suggestedList;
    findSuggestedLoadedFor = userKey;
    renderSuggestedGames(suggestedList);
    const seriesCount = new Set(suggestedList.map(g => g.series)).size;
    findSuggestedStatusEl.textContent = `Found ${suggestedList.length} games across ${seriesCount} series.`;
  } catch (err) {
    findSuggestedStatusEl.textContent = "Failed to load suggestions.";
  } finally {
    setLoading(findSuggestedLoadingEl, false);
  }
}

function renderSuggestedGames(games) {
  if (!findSuggestedListEl) return;
  findSuggestedListEl.innerHTML = "";
  if (!games.length) {
    findSuggestedListEl.innerHTML = `<div class="meta">No suggested games found.</div>`;
    return;
  }
  const frag = document.createDocumentFragment();
  const inBacklog = new Set(backlogItems.map(item => String(item.gameId)));
  for (const g of games) {
    const tile = document.createElement("div");
    tile.className = "tile clickable";
    if (String(g.gameId ?? "") === String(findSuggestedSelectedGameId)) {
      tile.classList.add("selected");
    }
    tile.setAttribute("role", "button");
    tile.tabIndex = 0;
    tile.dataset.gameId = String(g.gameId ?? "");
    tile.dataset.title = g.title || "";
    tile.dataset.console = g.consoleName || "";
    tile.dataset.image = g.imageIcon || "";
    tile.dataset.achievements = String(g.numAchievements ?? 0);
    tile.dataset.points = String(g.points ?? 0);

    const img = document.createElement("img");
    img.src = iconUrl(g.imageIcon);
    img.alt = safeText(g.title || "game");
    img.loading = "lazy";

    const title = document.createElement("div");
    title.className = "tileTitle";
    title.textContent = g.title || `Game ${safeText(g.gameId)}`;

    const consoleLine = document.createElement("div");
    consoleLine.className = "tileMeta";
    consoleLine.textContent = g.consoleName || "";

    const achievementsLine = document.createElement("div");
    achievementsLine.className = "tileMeta";
    const ach = Number(g.numAchievements ?? 0);
    achievementsLine.textContent = Number.isFinite(ach) ? `${ach} achievements` : "";

    const pointsLine = document.createElement("div");
    pointsLine.className = "tileMeta";
    const pts = Number(g.points ?? 0);
    pointsLine.textContent = Number.isFinite(pts) ? `${pts} points` : "";

    const playersLine = document.createElement("div");
    playersLine.className = "tileMeta";
    const players = Number(g.numDistinctPlayers ?? g.numPlayers ?? 0);
    playersLine.textContent = Number.isFinite(players) && players > 0
      ? `${players} player${players === 1 ? "" : "s"}`
      : "";

    tile.appendChild(img);
    tile.appendChild(title);
    if (consoleLine.textContent) tile.appendChild(consoleLine);
    if (achievementsLine.textContent) tile.appendChild(achievementsLine);
    if (pointsLine.textContent) tile.appendChild(pointsLine);
    if (playersLine.textContent) tile.appendChild(playersLine);
    const actions = document.createElement("div");
    actions.className = "tileActions";
    const addBtn = document.createElement("button");
    addBtn.className = "smallBtn";
    addBtn.type = "button";
    const isSaved = inBacklog.has(String(g.gameId ?? ""));
    addBtn.textContent = isSaved ? "Remove from Backlog" : "Add to Backlog";
    addBtn.setAttribute("data-backlog", "true");
    actions.appendChild(addBtn);
    tile.appendChild(actions);
    frag.appendChild(tile);
  }
  findSuggestedListEl.appendChild(frag);
}

function scoreFindGame(title, query) {
  if (!query) return 0;
  const t = String(title || "").toLowerCase();
  const q = String(query || "").toLowerCase().trim();
  if (!q) return 0;
  if (t === q) return 0;
  if (t.startsWith(q)) return 1;
  const idx = t.indexOf(q);
  if (idx >= 0) return 2 + idx;
  let ti = 0;
  let score = 0;
  for (let qi = 0; qi < q.length; qi += 1) {
    const ch = q[qi];
    const next = t.indexOf(ch, ti);
    if (next === -1) return null;
    score += next - ti;
    ti = next + 1;
  }
  return 50 + score;
}

async function loadFindGamesConsoles() {
  if (!findGamesConsoleSelect) return;
  findGamesConsoleSelect.innerHTML = `<option value="">Loading consoles...</option>`;
  setLoading(findGamesLoadingEl, true);
  try {
    const data = await fetchConsoles();
    const list = Array.isArray(data?.results) ? data.results : [];
    if (!list.length) {
      findGamesConsoleSelect.innerHTML = `<option value="">No consoles found</option>`;
      return;
    }
    findGamesConsolesCache = list;
    const stored = localStorage.getItem(LS_FIND_GAMES_CONSOLE) || "";
    findGamesConsoleSelect.innerHTML = "";
    const allOption = document.createElement("option");
    allOption.value = "all";
    allOption.textContent = "All";
    findGamesConsoleSelect.appendChild(allOption);
    const groups = sortConsolesByMaker(list);
    for (const [maker, items] of groups) {
      const optgroup = document.createElement("optgroup");
      optgroup.label = maker;
      for (const c of items) {
        const option = document.createElement("option");
        option.value = String(c.id ?? c.consoleId ?? "");
        option.textContent = c.name || `Console ${option.value}`;
        optgroup.appendChild(option);
      }
      findGamesConsoleSelect.appendChild(optgroup);
    }
    const initial = stored === "all"
      ? stored
      : (list.some(c => String(c.id ?? c.consoleId) === stored)
        ? stored
        : String(list[0].id ?? list[0].consoleId ?? ""));
    if (initial) {
      findGamesConsoleSelect.value = initial;
      setFindGamesConsole(initial);
    }
  } catch (e) {
    findGamesConsoleSelect.innerHTML = `<option value="">Failed to load consoles</option>`;
  } finally {
    setLoading(findGamesLoadingEl, false);
  }
}

function setFindGamesConsole(consoleId) {
  findGamesConsoleId = String(consoleId || "");
  if (findGamesConsoleId) {
    localStorage.setItem(LS_FIND_GAMES_CONSOLE, findGamesConsoleId);
  }
  findGamesLoadedLetter = "";
  findGamesSelectedGameId = "";
  findGamesCache.clear();
  findGamesQuery = "";
  cancelFindGamesMetaRequests();
  resetFindGamesVisibleCount();
  if (findGamesSearchInput) findGamesSearchInput.value = "";
  if (findGamesAchievementsEl) {
    findGamesAchievementsEl.innerHTML = `<div class="meta">Select a game to see achievements.</div>`;
  }
  if (activePageName === "find-games") {
    loadFindGamesLetter(findGamesLetter);
  }
}

function buildFindGamesLetters() {
  if (!gameLetterBarEl) return;
  const letters = ["all", "#"];
  for (let code = 65; code <= 90; code += 1) {
    letters.push(String.fromCharCode(code));
  }
  gameLetterBarEl.innerHTML = "";
  for (const letter of letters) {
    const btn = document.createElement("button");
    btn.className = "smallBtn letterBtn";
    btn.type = "button";
    btn.textContent = letter === "all" ? "All" : letter;
    btn.setAttribute("role", "tab");
    btn.setAttribute("aria-selected", letter === findGamesLetter ? "true" : "false");
    btn.dataset.letter = letter;
    btn.addEventListener("click", () => setFindGamesLetter(letter === "#" ? "0-9" : letter));
    gameLetterBarEl.appendChild(btn);
  }
  updateFindGamesLetterButtons();
}

function updateFindGamesLetterButtons() {
  if (!gameLetterBarEl) return;
  const buttons = gameLetterBarEl.querySelectorAll("button");
  buttons.forEach((btn) => {
    const active = btn.dataset.letter === findGamesLetter;
    btn.classList.toggle("active", active);
    btn.setAttribute("aria-selected", active ? "true" : "false");
  });
}

function setFindGamesLetter(letter) {
  if (!letter) return;
  findGamesLetter = letter;
  updateFindGamesLetterButtons();
  cancelFindGamesMetaRequests();
  resetFindGamesVisibleCount();
  loadFindGamesLetter(letter);
}

function updateFindGamesTilePlayers(gameId, players) {
  if (!findGamesListEl) return;
  const tile = findGamesListEl.querySelector(`[data-game-id="${gameId}"]`);
  if (!tile) return;
  const playersLine = tile.querySelector(".playersMeta");
  if (!playersLine) return;
  playersLine.textContent = `${players} player${players === 1 ? "" : "s"}`;
  playersLine.hidden = false;
  if (findGamesSort === "players" && !findGamesBatchSorting) {
    scheduleFindGamesResort();
  }
}

function setFindGamesPlayersCacheValue(gameId, players, { updateTile = true } = {}) {
  const key = String(gameId);
  findGamesPlayersCache.set(key, players);
  findGamesPlayersCacheTs.set(key, Date.now());
  if (updateTile) updateFindGamesTilePlayers(key, players);
  scheduleFindGamesPlayersPersist();
}

function setFindGamesGenresCacheValue(gameId, genre) {
  const key = String(gameId);
  findGamesGenresCache.set(key, genre);
  findGamesGenresCacheTs.set(key, Date.now());
  scheduleFindGamesGenresPersist();
}

function scheduleFindGamesResort() {
  if (findGamesResortTimer) return;
  findGamesResortTimer = setTimeout(() => {
    findGamesResortTimer = null;
    refreshFindGamesSearch();
  }, 120);
}

function resetFindGamesVisibleCount() {
  findGamesVisibleCount = FIND_GAMES_INITIAL_VISIBLE;
}

function cancelFindGamesMetaRequests() {
  cancelFindGamesPlayersRequests();
  cancelFindGamesGenresRequests();
}

function cancelFindGamesPlayersRequests() {
  findGamesPlayersBatch += 1;
  for (const controller of findGamesPlayersControllers.values()) {
    controller.abort();
  }
  findGamesPlayersControllers.clear();
  findGamesPlayersRequested.clear();
  if (findGamesPlayersBatchController) {
    findGamesPlayersBatchController.abort();
    findGamesPlayersBatchController = null;
  }
  findGamesPlayersBatchKey = "";
  findGamesPlayersBatchResolvedKey = "";
}

function cancelFindGamesGenresRequests() {
  for (const controller of findGamesGenresControllers.values()) {
    controller.abort();
  }
  findGamesGenresControllers.clear();
  findGamesGenresRequested.clear();
  if (findGamesGenresBatchController) {
    findGamesGenresBatchController.abort();
    findGamesGenresBatchController = null;
  }
  findGamesGenresBatchKey = "";
}

function queueFindGamesPlayersRefresh(gameId, { force = false } = {}) {
  const key = String(gameId || "");
  if (!key) return;
  if (!force && findGamesPlayersCache.has(key)) return;
  if (findGamesPlayersRequested.has(key)) return;
  findGamesPlayersRequested.add(key);
  const controller = new AbortController();
  const batch = findGamesPlayersBatch;
  findGamesPlayersControllers.set(key, controller);
  findGamesPlayersLimiter(async () => {
    try {
      if (controller.signal.aborted) return;
      const data = await fetchGamePlayersRefresh(key, controller.signal);
      if (batch !== findGamesPlayersBatch) return;
      const players = Number(data?.numDistinctPlayers ?? data?.players ?? 0);
        if (Number.isFinite(players) && players > 0) {
          setFindGamesPlayersCacheValue(key, players);
        }
    } catch (err) {
      if (err?.name === "AbortError") return;
      // ignore failures; will retry on next search
    } finally {
      findGamesPlayersRequested.delete(key);
      findGamesPlayersControllers.delete(key);
    }
  });
}

function requestFindGamesPlayersBatch(gameIds) {
  const ids = Array.isArray(gameIds) ? gameIds.filter(Boolean) : [];
  if (!ids.length) return;
  const key = buildPlayersBatchKey(ids);
  if (!findGamesPlayersBatchController && key === findGamesPlayersBatchKey) return;
  if (findGamesPlayersBatchController) {
    if (key === findGamesPlayersBatchKey) return;
    findGamesPlayersBatchController.abort();
  }
  const controller = new AbortController();
  findGamesPlayersBatchController = controller;
  findGamesPlayersBatchKey = key;
  findGamesBatchSorting = findGamesSort === "players";
  const batch = findGamesPlayersBatch;
  const chunkSize = 100;
  const chunks = [];
  for (let i = 0; i < ids.length; i += chunkSize) {
    chunks.push(ids.slice(i, i + chunkSize));
  }
  (async () => {
    try {
      const seen = new Set();
      let didUpdate = false;
      const batchLimiter = createLimiter(FIND_GAMES_PLAYERS_BATCH_CONCURRENCY);
      await Promise.all(chunks.map(chunk => batchLimiter(async () => {
        if (controller.signal.aborted) return;
        const data = await fetchGamePlayersBatch(chunk, controller.signal);
        if (batch !== findGamesPlayersBatch) return;
        const results = Array.isArray(data?.results) ? data.results : [];
        for (const row of results) {
          const gid = String(row?.gameId ?? "");
          if (!gid) continue;
          seen.add(gid);
          const players = Number(row?.numDistinctPlayers ?? row?.players ?? 0);
          if (Number.isFinite(players) && players > 0) {
            setFindGamesPlayersCacheValue(gid, players);
            didUpdate = true;
          }
          if (row?.stale || !Number.isFinite(players) || players <= 0) {
            queueFindGamesPlayersRefresh(gid, { force: true });
          }
        }
      })));
      if (controller.signal.aborted) return;
      for (const gid of ids) {
        if (controller.signal.aborted) return;
        if (!seen.has(gid)) {
          queueFindGamesPlayersRefresh(gid, { force: true });
        }
      }
      if (findGamesSort === "players" && didUpdate) {
        scheduleFindGamesResort();
      }
    } catch (err) {
      if (err?.name === "AbortError") return;
    } finally {
      findGamesBatchSorting = false;
      findGamesPlayersBatchResolvedKey = key;
      if (findGamesPlayersBatchController === controller) {
        findGamesPlayersBatchController = null;
      }
    }
  })();
}

function normalizeGenreValue(value) {
  const raw = String(value || "").trim();
  return raw || "Unknown";
}

function getFindGamesGenreValue(game) {
  const key = String(game?.gameId ?? "");
  if (!key) return "Unknown";
  const cached = findGamesGenresCache.get(key);
  return normalizeGenreValue(cached);
}

function updateFindGamesGenreOptions(genres) {
  if (!findGamesGenreSelect) return;
  const list = Array.from(new Set(genres.map(normalizeGenreValue)));
  list.sort((a, b) => a.localeCompare(b));
  const options = ["All genres", ...list];
  const values = ["all", ...list];
  const current = values.includes(findGamesGenre) ? findGamesGenre : "all";
  findGamesGenre = current;
  findGamesGenreSelect.innerHTML = "";
  for (let i = 0; i < options.length; i += 1) {
    const option = document.createElement("option");
    option.value = values[i];
    option.textContent = options[i];
    findGamesGenreSelect.appendChild(option);
  }
  findGamesGenreSelect.value = current;
}

function queueFindGamesGenreRefresh(gameId, { force = false } = {}) {
  const key = String(gameId || "");
  if (!key) return;
  if (!force && findGamesGenresCache.has(key)) return;
  if (findGamesGenresRequested.has(key)) return;
  findGamesGenresRequested.add(key);
  const controller = new AbortController();
  findGamesGenresControllers.set(key, controller);
  findGamesGenresLimiter(async () => {
    try {
      if (controller.signal.aborted) return;
      const data = await fetchGameGenreRefresh(key, controller.signal);
      const genre = normalizeGenreValue(data?.genre);
      if (genre && genre !== "Unknown") {
        setFindGamesGenresCacheValue(key, genre);
      }
    } catch (err) {
      if (err?.name === "AbortError") return;
    } finally {
      findGamesGenresRequested.delete(key);
      findGamesGenresControllers.delete(key);
    }
  });
}

function requestFindGamesGenresBatch(gameIds) {
  const ids = Array.isArray(gameIds) ? gameIds.filter(Boolean) : [];
  if (!ids.length) return;
  const key = buildPlayersBatchKey(ids);
  if (findGamesGenresBatchController) {
    if (key === findGamesGenresBatchKey) return;
    findGamesGenresBatchController.abort();
  }
  const controller = new AbortController();
  findGamesGenresBatchController = controller;
  findGamesGenresBatchKey = key;
  const chunkSize = 100;
  const chunks = [];
  for (let i = 0; i < ids.length; i += chunkSize) {
    chunks.push(ids.slice(i, i + chunkSize));
  }
  (async () => {
    try {
      const batchLimiter = createLimiter(FIND_GAMES_GENRES_BATCH_CONCURRENCY);
      let didUpdate = false;
      await Promise.all(chunks.map(chunk => batchLimiter(async () => {
        if (controller.signal.aborted) return;
        const data = await fetchGameGenresBatch(chunk, controller.signal);
        const results = Array.isArray(data?.results) ? data.results : [];
        for (const row of results) {
          const gid = String(row?.gameId ?? "");
          if (!gid) continue;
          const genre = normalizeGenreValue(row?.genre);
          if (genre && genre !== "Unknown") {
            setFindGamesGenresCacheValue(gid, genre);
            didUpdate = true;
          }
          if (row?.stale || !row?.genre) {
            queueFindGamesGenreRefresh(gid, { force: true });
          }
        }
      })));
      if (!controller.signal.aborted && didUpdate && findGamesSort === "genre") {
        scheduleFindGamesResort();
      }
    } catch (err) {
      if (err?.name === "AbortError") return;
    } finally {
      if (findGamesGenresBatchController === controller) {
        findGamesGenresBatchController = null;
      }
    }
  })();
}

function getFindGamesPlayersValue(game) {
  const cached = findGamesPlayersCache.get(String(game?.gameId ?? ""));
  const raw = cached ?? game?.numDistinctPlayers ?? game?.numPlayers;
  const value = Number(raw);
  return Number.isFinite(value) ? value : -1;
}

function buildPlayersBatchKey(ids) {
  const ordered = ids
    .map(id => String(id))
    .filter(Boolean)
    .sort((a, b) => Number(a) - Number(b));
  return ordered.join(",");
}

function hasFreshPlayersCacheForIds(ids) {
  const now = Date.now();
  for (const id of ids) {
    const key = String(id);
    if (!findGamesPlayersCache.has(key)) return false;
    const ts = Number(findGamesPlayersCacheTs.get(key));
    if (!Number.isFinite(ts) || (now - ts) > FIND_GAMES_PLAYERS_CACHE_TTL_MS) return false;
  }
  return true;
}

function sortFindGamesList(games) {
  const sorted = games.slice();
  if (findGamesSort === "players") {
    sorted.sort((a, b) => {
      const av = getFindGamesPlayersValue(a);
      const bv = getFindGamesPlayersValue(b);
      if (bv !== av) return bv - av;
      return String(a?.title || "").localeCompare(String(b?.title || ""));
    });
    return sorted;
  }
  if (findGamesSort === "points") {
    sorted.sort((a, b) => {
      const av = Number(a?.points ?? 0);
      const bv = Number(b?.points ?? 0);
      if (Number.isFinite(bv) && Number.isFinite(av) && bv !== av) return bv - av;
      return String(a?.title || "").localeCompare(String(b?.title || ""));
    });
    return sorted;
  }
  sorted.sort((a, b) => String(a?.title || "").localeCompare(String(b?.title || "")));
  return sorted;
}

function renderFindGamesList(games) {
  if (!findGamesListEl) return;
  findGamesListEl.innerHTML = "";
  const query = findGamesQuery.trim();
  let filtered = games;
  if (query) {
    filtered = games
      .map(g => ({ game: g, score: scoreFindGame(g.title, query) }))
      .filter(row => row.score !== null)
      .map(row => row.game);
  }
  if (findGamesGenreSelect) {
    const ids = filtered.map(g => String(g.gameId ?? ""));
    requestFindGamesGenresBatch(ids);
    const genres = filtered.map(g => getFindGamesGenreValue(g));
    updateFindGamesGenreOptions(genres);
    if (findGamesGenre !== "all") {
      filtered = filtered.filter(g => getFindGamesGenreValue(g) === findGamesGenre);
    }
  }
  if (!filtered.length) {
    const label = query ? "No games match your search." : "No games found for this letter.";
    findGamesListEl.innerHTML = `<div class="meta">${label}</div>`;
    if (findGamesShowMoreBtn) findGamesShowMoreBtn.hidden = true;
    return;
  }
  const shouldHydratePlayers = !!query || (!query && !!findGamesLetter);
  const isPlayersSort = findGamesSort === "players";
  if (isPlayersSort && shouldHydratePlayers) {
    const ids = filtered.map(g => String(g.gameId ?? ""));
    requestFindGamesPlayersBatch(ids);
  }
  filtered = sortFindGamesList(filtered);
  const visibleGames = filtered.slice(0, findGamesVisibleCount);
  const frag = document.createDocumentFragment();
  const inBacklog = new Set(backlogItems.map(item => String(item.gameId)));
  const hydrateSource = isPlayersSort ? filtered : visibleGames;
  const hydrateList = shouldHydratePlayers
    ? hydrateSource.slice(0, FIND_GAMES_PLAYERS_FETCH_LIMIT).map(g => String(g.gameId ?? ""))
    : [];
  const hydrateTargets = new Set(hydrateList);
  for (const game of visibleGames) {
    const gameId = String(game.gameId ?? "");
    const shouldHydrate = shouldHydratePlayers && hydrateTargets.has(gameId);
    const tile = document.createElement("div");
    tile.className = "tile clickable";
    tile.setAttribute("role", "button");
    tile.tabIndex = 0;
    tile.dataset.gameId = gameId;
    tile.dataset.title = game.title || "";
    tile.dataset.console = game.consoleName || "";
    tile.dataset.image = game.imageIcon || "";
    tile.dataset.achievements = String(game.numAchievements ?? 0);
    tile.dataset.points = String(game.points ?? 0);
    if (String(game.gameId ?? "") === String(findGamesSelectedGameId)) {
      tile.classList.add("selected");
    }

    const imgWrap = document.createElement("div");
    imgWrap.className = "tileImgWrap";
    const img = document.createElement("img");
    img.src = iconUrl(game.imageIcon);
    img.alt = safeText(game.title || "game");
    img.loading = "lazy";

    const title = document.createElement("div");
    title.className = "tileTitle";
    title.textContent = game.title || `Game ${safeText(game.gameId)}`;

    const consoleLine = document.createElement("div");
    consoleLine.className = "tileMeta consoleMeta";
    consoleLine.textContent = game.consoleName || "";

    const playersLine = document.createElement("div");
    playersLine.className = "tileMeta playersMeta";
    const cachedPlayers = findGamesPlayersCache.get(gameId);
    const rawPlayers = cachedPlayers ?? game.numDistinctPlayers ?? game.numPlayers;
    const players = Number(rawPlayers);
    playersLine.textContent = Number.isFinite(players) && players > 0
      ? `${players} player${players === 1 ? "" : "s"}`
      : "";

    const meta = document.createElement("div");
    meta.className = "tileMeta";
    const ach = Number(game.numAchievements ?? 0);
    const pts = Number(game.points ?? 0);
    const parts = [];
    if (Number.isFinite(ach)) parts.push(`${ach} achievements`);
    if (Number.isFinite(pts)) parts.push(`${pts} points`);
    meta.textContent = parts.join("\n");

    imgWrap.appendChild(img);
    if (playersLine.textContent || shouldHydrate) {
      playersLine.hidden = !playersLine.textContent;
      imgWrap.appendChild(playersLine);
    }
    tile.appendChild(imgWrap);
    tile.appendChild(title);
    if (findGamesConsoleId === "all" && consoleLine.textContent) {
      tile.appendChild(consoleLine);
    }
    tile.appendChild(meta);
    const actions = document.createElement("div");
    actions.className = "tileActions";
    const addBtn = document.createElement("button");
    addBtn.className = "smallBtn";
    addBtn.type = "button";
    const isSaved = inBacklog.has(String(game.gameId ?? ""));
    addBtn.textContent = isSaved ? "Remove from Backlog" : "Add to Backlog";
    addBtn.setAttribute("data-backlog", "true");
    actions.appendChild(addBtn);
    tile.appendChild(actions);
    frag.appendChild(tile);
  }
  findGamesListEl.appendChild(frag);
  if (findGamesShowMoreBtn) {
    const remaining = filtered.length - visibleGames.length;
    findGamesShowMoreBtn.hidden = remaining <= 0;
    if (remaining > 0) {
      const nextCount = Math.min(FIND_GAMES_SHOW_MORE_STEP, remaining);
      findGamesShowMoreBtn.textContent = `Show more (${nextCount})`;
    }
  }
  if (shouldHydratePlayers) {
    requestFindGamesPlayersBatch(hydrateList);
  }
}

function renderFindGameAchievements(payload, targetEl = findGamesAchievementsEl) {
  if (!targetEl) return;
  const achievements = Array.isArray(payload?.achievements) ? payload.achievements : [];
  const title = payload?.gameTitle || "Game";
  const consoleName = payload?.consoleName || "";
  const publisher = payload?.publisher || "";
  const developer = payload?.developer || "";
  const genre = payload?.genre || "";
  const released = payload?.released || "";
  const imageIcon = payload?.imageIcon || "";
  const imageTitle = payload?.imageTitle || "";
  const imageIngame = payload?.imageIngame || "";
  const imageBoxArt = payload?.imageBoxArt || "";
  const forumTopicId = payload?.forumTopicId || "";
  const parentGameId = payload?.parentGameId || "";
  const numAchievements = payload?.numAchievements ?? "";
  const numDistinctPlayers = payload?.numDistinctPlayers ?? "";
  targetEl.innerHTML = "";

  const header = document.createElement("div");
  header.className = "findAchievementsHeader";
  const headerTitle = document.createElement("h3");
  headerTitle.className = "findAchievementsTitle";
  headerTitle.textContent = safeText(title);
  const headerMeta = document.createElement("div");
  headerMeta.className = "meta";
  headerMeta.textContent = consoleName
    ? `${safeText(consoleName)} � ${achievements.length} achievements`
    : `${achievements.length} achievements`;
  header.appendChild(headerTitle);
  header.appendChild(headerMeta);
  targetEl.appendChild(header);

  const infoCard = document.createElement("div");
  infoCard.className = "findGameInfo";
  const infoImg = document.createElement("img");
  infoImg.className = "findGameInfoIcon";
  infoImg.src = iconUrl(imageIcon);
  infoImg.alt = safeText(title);
  infoImg.loading = "lazy";
  infoCard.appendChild(infoImg);

  const infoBody = document.createElement("div");
  infoBody.className = "findGameInfoBody";
  const infoTitle = document.createElement("div");
  infoTitle.className = "findGameInfoTitle";
  infoTitle.textContent = safeText(title);
  infoBody.appendChild(infoTitle);

  const infoMeta = document.createElement("div");
  infoMeta.className = "findGameInfoMeta";
  const metaItems = [
    { label: "Console", value: consoleName },
    { label: "Achievements", value: numAchievements },
    { label: "Players", value: numDistinctPlayers },
    { label: "Publisher", value: publisher },
    { label: "Developer", value: developer },
    { label: "Genre", value: genre },
    { label: "Released", value: released },
    { label: "Parent Game", value: parentGameId },
    { label: "Forum Topic", value: forumTopicId ? `#${forumTopicId}` : "" }
  ];
  for (const item of metaItems) {
    if (item.value === "" || item.value === null || item.value === undefined) continue;
    const row = document.createElement("div");
    row.className = "findGameInfoRow";
    const label = document.createElement("span");
    label.className = "findGameInfoLabel";
    label.textContent = item.label;
    const value = document.createElement("span");
    value.className = "findGameInfoValue";
    value.textContent = safeText(item.value);
    row.appendChild(label);
    row.appendChild(value);
    infoMeta.appendChild(row);
  }
  infoBody.appendChild(infoMeta);
  infoCard.appendChild(infoBody);
  targetEl.appendChild(infoCard);

  const mediaUrls = [
    { label: "Title", url: imageTitle },
    { label: "In-game", url: imageIngame },
    { label: "Box Art", url: imageBoxArt }
  ].filter(item => item.url);
  if (mediaUrls.length) {
    const media = document.createElement("div");
    media.className = "findGameMedia";
    for (const item of mediaUrls) {
      const frame = document.createElement("div");
      frame.className = "findGameMediaItem";
      const img = document.createElement("img");
      img.src = iconUrl(item.url);
      img.alt = `${safeText(title)} ${item.label}`;
      img.loading = "lazy";
      frame.appendChild(img);
      const caption = document.createElement("div");
      caption.className = "findGameMediaLabel";
      caption.textContent = item.label;
      frame.appendChild(caption);
      media.appendChild(frame);
    }
    targetEl.appendChild(media);
  }

  const list = document.createElement("div");
  list.className = "findAchievementsList";

  if (!achievements.length) {
    list.innerHTML = `<div class="meta">No achievements found for this game.</div>`;
  } else {
    for (const a of achievements) {
      const item = document.createElement("div");
      item.className = "findAchievementItem";

      const badge = document.createElement("img");
      badge.className = "findAchievementBadge";
      badge.src = iconUrl(a.badgeUrl);
      badge.alt = safeText(a.title || "achievement");
      badge.loading = "lazy";

      const body = document.createElement("div");
      const titleEl = document.createElement("div");
      titleEl.className = "findAchievementTitle";
      const points = Number(a.points || 0);
      titleEl.textContent = points ? `${safeText(a.title)} (${points} pts)` : safeText(a.title);

      const desc = document.createElement("div");
      desc.className = "findAchievementDesc";
      desc.textContent = safeText(a.description || "");

      body.appendChild(titleEl);
      body.appendChild(desc);

      item.appendChild(badge);
      item.appendChild(body);
      list.appendChild(item);
    }
  }
  targetEl.appendChild(list);
}

async function loadFindGamesLetter(letter) {
  if (!findGamesListEl) return;
  const normalized = letter || "0-9";
  if (findGamesQuery.trim()) {
    refreshFindGamesSearch();
    return;
  }
  if (!findGamesConsoleId) {
    findGamesListEl.innerHTML = `<div class="meta">Select a console to load games.</div>`;
    return;
  }
  const cacheKey = `${findGamesConsoleId}:${normalized}`;
  if (findGamesLoadedLetter === normalized && findGamesCache.has(cacheKey)) {
    renderFindGamesList(findGamesCache.get(cacheKey));
    return;
  }
  findGamesLoadedLetter = normalized;
  findGamesSelectedGameId = "";
  if (findGamesAchievementsEl) {
    findGamesAchievementsEl.innerHTML = `<div class="meta">Select a game to see achievements.</div>`;
  }
  if (findGamesCache.has(cacheKey)) {
    renderFindGamesList(findGamesCache.get(cacheKey));
    return;
  }
  findGamesListEl.innerHTML = `<div class="meta">Loading games...</div>`;
  setLoading(findGamesLoadingEl, true);
  try {
    const data = await fetchGameListByLetter(normalized);
    const results = Array.isArray(data?.results) ? data.results : [];
    findGamesCache.set(cacheKey, results);
    renderFindGamesList(results);
  } catch (e) {
    findGamesListEl.innerHTML = `<div class="meta">Failed to load games.</div>`;
  } finally {
    setLoading(findGamesLoadingEl, false);
  }
}

async function loadFindGameAchievements(game, { targetEl = findGamesAchievementsEl, listEl = null, selectedClass = "" } = {}) {
  if (!targetEl) return;
  const gameId = game?.gameId;
  if (!gameId) return;
  if (listEl === findGamesListEl) findGamesSelectedGameId = gameId;
  if (listEl === findSuggestedListEl) findSuggestedSelectedGameId = gameId;
  if (listEl && selectedClass) setSelectedGameInList(listEl, gameId, selectedClass);
  targetEl.innerHTML = `<div class="meta">Loading achievements...</div>`;
  try {
    const payload = await fetchGameAchievementsBasic(gameId);
    renderFindGameAchievements(payload, targetEl);
  } catch (e) {
    targetEl.innerHTML = `<div class="meta">Failed to load achievements.</div>`;
  }
}

function stopChallengePolling() {
  if (challengesPollTimer) {
    clearInterval(challengesPollTimer);
    challengesPollTimer = null;
  }
  if (challengesTotalsTimer) {
    clearInterval(challengesTotalsTimer);
    challengesTotalsTimer = null;
  }
  if (challengesScoreTimer) {
    clearInterval(challengesScoreTimer);
    challengesScoreTimer = null;
  }
}

function updateScoreAttackSelectionText() {
  if (!scoreAttackSelectionEl) return;
  if (!scoreAttackSelectedGame || !scoreAttackSelectedBoard) {
    scoreAttackSelectionEl.textContent = "Select a game and leaderboard for Score Attack.";
    if (challengeScoreSummaryEl) challengeScoreSummaryEl.textContent = "";
    updateChallengeFormState();
    return;
  }
  const text = `${scoreAttackSelectedGame.title} | ${scoreAttackSelectedBoard.title}`;
  scoreAttackSelectionEl.textContent = text;
  if (challengeScoreSummaryEl) challengeScoreSummaryEl.textContent = `Selected: ${text}`;
  updateChallengeFormState();
}

function renderScoreAttackGames(games) {
  if (!scoreAttackGamesList) return;
  scoreAttackGamesList.innerHTML = "";
  const query = scoreAttackGameQuery.trim().toLowerCase();
  const filtered = query
    ? games.filter(g => (g.title || "").toLowerCase().includes(query))
    : games;
  if (!filtered.length) {
    const label = scoreAttackView === "shared"
      ? "No shared games found."
      : scoreAttackView === "mine"
        ? "No recent games found for you."
        : "No recent games found for this friend.";
    scoreAttackGamesList.innerHTML = `<div class="meta">${label}</div>`;
    return;
  }
  const frag = document.createDocumentFragment();
  for (const g of filtered) {
    const tile = document.createElement("div");
    tile.className = "tile clickable";
    tile.setAttribute("role", "button");
    tile.tabIndex = 0;
    tile.dataset.gameId = String(g.gameId ?? "");
    tile.dataset.title = g.title || "";
    tile.dataset.imageIcon = g.imageIcon || "";

    const img = document.createElement("img");
    img.src = iconUrl(g.imageIcon);
    img.alt = safeText(g.title || "game");
    img.loading = "lazy";

    const title = document.createElement("div");
    title.className = "tileTitle";
    title.textContent = g.title || `Game ${safeText(g.gameId)}`;

    tile.appendChild(img);
    tile.appendChild(title);
    frag.appendChild(tile);

    const open = () => loadScoreAttackLeaderboards(g);
    tile.addEventListener("click", open);
    tile.addEventListener("keydown", (e) => {
      if (e.key === "Enter" || e.key === " ") { e.preventDefault(); open(); }
    });
  }
  scoreAttackGamesList.appendChild(frag);
}

function renderScoreAttackLeaderboards(gameTitle, boards) {
  if (!scoreAttackBoardsList) return;
  scoreAttackBoards = boards;
  scoreAttackBoardsList.innerHTML = "";
  const query = scoreAttackBoardQuery.trim().toLowerCase();
  const filtered = query
    ? boards.filter(b => (b.title || "").toLowerCase().includes(query))
    : boards;
  if (!filtered.length) {
    scoreAttackBoardsList.innerHTML = `<div class="meta">No leaderboards found for this game.</div>`;
    return;
  }
  const frag = document.createDocumentFragment();
  for (const lb of filtered) {
    const card = document.createElement("div");
    const selected = scoreAttackSelectedBoard?.id === lb.id;
    card.className = `challengeItem${selected ? " selected" : ""}`;
    card.setAttribute("data-board", String(lb.id || ""));
    card.innerHTML = `
      <div class="challengeRow">
        <div>${safeText(lb.title || "Leaderboard")}</div>
      </div>
      ${lb.description ? `<div class="challengeMeta">${safeText(lb.description)}</div>` : ""}
    `;
    frag.appendChild(card);
  }
  scoreAttackBoardsList.appendChild(frag);
}

function getScoreAttackViewList() {
  if (scoreAttackView === "mine") return scoreAttackRecentMine;
  if (scoreAttackView === "theirs") return scoreAttackRecentTheirs;
  return scoreAttackSharedGames;
}

function updateScoreAttackView(view) {
  scoreAttackView = view;
  if (scoreAttackGamesTitle) {
    scoreAttackGamesTitle.textContent =
      view === "shared" ? "Shared Games" : view === "mine" ? "Your Recent Games" : "Friend Recent Games";
  }
  scoreAttackTabButtons.forEach(btn => {
    btn.classList.toggle("active", btn.dataset.view === view);
  });
  renderScoreAttackGames(getScoreAttackViewList());
}

async function loadScoreAttackSharedGames({ count = 60 } = {}) {
  const me = currentUser;
  const friend = clampUsername(challengeOpponentInput?.value || "");
  if (!me || !friend) return;
  scoreAttackGamesList.innerHTML = `<div class="meta">Loading shared games...</div>`;
  try {
    const mineCache = readRecentGamesCache(me, count);
    const theirsCache = readRecentGamesCache(friend, count);
    if (mineCache?.data && theirsCache?.data) {
      const myMap = new Map((mineCache.data.results || []).map(g => [g.gameId, g]));
      const shared = [];
      for (const g of (theirsCache.data.results || [])) {
        if (!myMap.has(g.gameId)) continue;
        const mineGame = myMap.get(g.gameId);
        shared.push({
          gameId: g.gameId,
          title: g.title || mineGame?.title,
          imageIcon: g.imageIcon || mineGame?.imageIcon
        });
      }
      const seen = new Set();
      scoreAttackSharedGames = shared.filter(g => {
        const key = String(g.gameId ?? "");
        if (!key || seen.has(key)) return false;
        seen.add(key);
        return true;
      });
      scoreAttackRecentMine = (mineCache.data.results || []).map(g => ({
        gameId: g.gameId,
        title: g.title,
        imageIcon: g.imageIcon
      }));
      scoreAttackRecentTheirs = (theirsCache.data.results || []).map(g => ({
        gameId: g.gameId,
        title: g.title,
        imageIcon: g.imageIcon
      }));
      renderScoreAttackGames(getScoreAttackViewList());
    }

    const needMine = !mineCache || mineCache.stale;
    const needTheirs = !theirsCache || theirsCache.stale;
    const [mine, theirs] = await Promise.all([
      needMine ? fetchRecentGames(me, count) : Promise.resolve(mineCache.data),
      needTheirs ? fetchRecentGames(friend, count) : Promise.resolve(theirsCache.data)
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
    scoreAttackSharedGames = shared.filter(g => {
      const key = String(g.gameId ?? "");
      if (!key || seen.has(key)) return false;
      seen.add(key);
      return true;
    });
    scoreAttackRecentMine = (mine.results || []).map(g => ({
      gameId: g.gameId,
      title: g.title,
      imageIcon: g.imageIcon
    }));
    scoreAttackRecentTheirs = (theirs.results || []).map(g => ({
      gameId: g.gameId,
      title: g.title,
      imageIcon: g.imageIcon
    }));
    renderScoreAttackGames(getScoreAttackViewList());
    if (scoreAttackShowMoreBtn) {
      const allLoaded = count >= 200;
      scoreAttackShowMoreBtn.disabled = allLoaded;
      scoreAttackShowMoreBtn.textContent = allLoaded ? "All recent games loaded" : "Show more";
    }
  } catch (err) {
    scoreAttackGamesList.innerHTML = `<div class="meta">Failed to load shared games.</div>`;
  }
}

async function loadScoreAttackLeaderboards(game) {
  if (!game || !game.gameId) return;
  scoreAttackSelectedGame = game;
  scoreAttackSelectedBoard = null;
  updateScoreAttackSelectionText();
  scoreAttackBoardsList.innerHTML = `<div class="meta">Loading leaderboards...</div>`;
  try {
    const data = await fetchGameLeaderboards(game.gameId);
    const boards = Array.isArray(data?.results) ? data.results : [];
    renderScoreAttackLeaderboards(game.title, boards);
  } catch {
    scoreAttackBoardsList.innerHTML = `<div class="meta">Failed to load leaderboards.</div>`;
  }
}

function formatTimeLeft(endAt) {
  if (!endAt) return "";
  const end = (() => {
    if (endAt instanceof Date) return endAt.getTime();
    const raw = String(endAt);
    const hasTz = /Z|[+-]\d{2}:?\d{2}$/.test(raw);
    const normalized = hasTz ? raw : `${raw.replace(" ", "T")}Z`;
    const ts = Date.parse(normalized);
    return Number.isNaN(ts) ? NaN : ts;
  })();
  if (!end) return "";
  let remaining = Math.max(0, end - Date.now());
  const totalMinutes = Math.floor(remaining / 60000);
  const days = Math.floor(totalMinutes / 1440);
  const hours = Math.floor((totalMinutes % 1440) / 60);
  const minutes = totalMinutes % 60;
  if (days > 0) return `${days}d ${hours}h left`;
  if (hours > 0) return `${hours}h ${minutes}m left`;
  return `${minutes}m left`;
}

function formatLeaderboardDelta(value, format) {
  if (value === null || value === undefined || !Number.isFinite(value)) return "--";
  const fmt = String(format || "").toUpperCase();
  if (fmt.includes("MILLI")) {
    const centisTotal = Math.max(0, Math.round(value));
    const minutes = Math.floor(centisTotal / 6000);
    const seconds = Math.floor((centisTotal % 6000) / 100);
    const centis = centisTotal % 100;
    return `${minutes}:${String(seconds).padStart(2, "0")}.${String(centis).padStart(2, "0")}`;
  }
  if (fmt.includes("SEC") || fmt.includes("TIME")) {
    const totalSeconds = Math.max(0, Math.round(value));
    const hours = Math.floor(totalSeconds / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    const seconds = totalSeconds % 60;
    if (hours > 0) {
      return `${hours}:${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
    }
    return `${minutes}:${String(seconds).padStart(2, "0")}`;
  }
  if (fmt.includes("FRAME")) {
    return `${Math.round(value)}f`;
  }
  return String(Math.round(value));
}

function inferLeaderboardFormat(text) {
  if (!text) return "";
  const trimmed = String(text).trim();
  if (!trimmed) return "";
  if (trimmed.includes(":")) return "MILLISECS";
  if (trimmed.includes(".")) return "MILLISECS";
  return "";
}

function parseLeaderboardScore(text, format) {
  if (!text) return null;
  const trimmed = String(text).trim();
  if (!trimmed) return null;
  const fmt = String(format || "").toUpperCase() || inferLeaderboardFormat(trimmed);
  if (fmt.includes("MILLI")) {
    const [head, fracRaw] = trimmed.split(/[.,]/);
    const parts = head.split(":").map(p => Number(p));
    if (parts.some(n => Number.isNaN(n))) return null;
    let totalSeconds = 0;
    for (const part of parts) {
      totalSeconds = totalSeconds * 60 + part;
    }
    const centis = fracRaw ? Number(String(fracRaw).padEnd(2, "0").slice(0, 2)) : 0;
    if (Number.isNaN(centis)) return null;
    return totalSeconds * 100 + centis;
  }
  if (fmt.includes("SEC") || fmt.includes("TIME")) {
    const parts = trimmed.split(":").map(p => Number(p));
    if (parts.some(n => Number.isNaN(n))) return null;
    let totalSeconds = 0;
    for (const part of parts) {
      totalSeconds = totalSeconds * 60 + part;
    }
    return totalSeconds;
  }
  if (fmt.includes("FRAME")) {
    const frames = Number(trimmed);
    return Number.isFinite(frames) ? frames : null;
  }
  const numeric = Number(trimmed.replace(/,/g, ""));
  return Number.isFinite(numeric) ? numeric : null;
}

function renderChallengeList(items, container, type, me) {
  if (!container) return;
  container.innerHTML = "";
  if (!items.length) {
    container.innerHTML = `<div class="meta">No challenges yet.</div>`;
    return;
  }
  if (type === "active") {
    const hasScore = items.some(item => item.challenge_type === "score");
    if (hasScore && !challengesScoreTimer) {
      challengesScoreTimer = setInterval(() => refreshChallenges({ includeTotals: true }), 20000);
    } else if (!hasScore && challengesScoreTimer) {
      clearInterval(challengesScoreTimer);
      challengesScoreTimer = null;
    }
  }
  const frag = document.createDocumentFragment();
  for (const item of items) {
    const card = document.createElement("div");
    card.className = type === "active" ? "challengeItem challengeActiveCard" : "challengeItem";

    const top = document.createElement("div");
    top.className = "challengeRow";
    const title = document.createElement("div");

    if (type === "incoming") {
      title.textContent = `From ${safeText(item.creator_username)}`;
    } else if (type === "outgoing") {
      title.textContent = `To ${safeText(item.opponent_username)}`;
    } else {
      const opponent = normalizeUserKey(item.creator_username) === normalizeUserKey(me)
        ? item.opponent_username
        : item.creator_username;
      title.textContent = `Vs ${safeText(opponent)}`;
    }

    top.appendChild(title);

    if (type === "incoming") {
      const acceptBtn = document.createElement("button");
      acceptBtn.className = "smallBtn";
      acceptBtn.textContent = "Accept";
      acceptBtn.setAttribute("data-accept", String(item.id));
      top.appendChild(acceptBtn);

      const declineBtn = document.createElement("button");
      declineBtn.className = "smallBtn";
      declineBtn.textContent = "Decline";
      declineBtn.setAttribute("data-decline", String(item.id));
      top.appendChild(declineBtn);
    } else if (type === "outgoing") {
      const cancelBtn = document.createElement("button");
      cancelBtn.className = "smallBtn";
      cancelBtn.textContent = "Cancel";
      cancelBtn.setAttribute("data-cancel", String(item.id));
      top.appendChild(cancelBtn);
    }

    const meta = document.createElement("div");
    meta.className = "challengeMeta";
    const duration = Math.max(1, Number(item.duration_hours || 0));
    const parts = [`Duration: ${duration}h`];
    if (type === "active" && item.end_at) {
      parts.push(`Ends in ${formatTimeLeft(item.end_at)}`);
      if (item.challenge_type === "score" && item.game_title && item.leaderboard_title) {
        parts.push(`${safeText(item.game_title)} | ${safeText(item.leaderboard_title)}`);
      }
    } else if (item.created_at) {
      parts.push(`Created ${new Date(item.created_at).toLocaleString()}`);
    }
    meta.textContent = parts.join(" | ");

    if (type === "active") {
      const creatorPoints = item.creator_points;
      const opponentPoints = item.opponent_points;
      const creatorPointsText = creatorPoints === null || creatorPoints === undefined ? "--" : `+${creatorPoints}`;
      const opponentPointsText = opponentPoints === null || opponentPoints === undefined ? "--" : `+${opponentPoints}`;
      const scoreMode = item.challenge_type === "score";
      const inferredFormat = item.leaderboard_format || inferLeaderboardFormat(item.creator_current_score || item.opponent_current_score);
      const creatorScoreValue = parseLeaderboardScore(item.creator_current_score, inferredFormat)
        ?? (Number.isFinite(item.creator_current_score_value) ? item.creator_current_score_value : 0);
      const opponentScoreValue = parseLeaderboardScore(item.opponent_current_score, inferredFormat)
        ?? (Number.isFinite(item.opponent_current_score_value) ? item.opponent_current_score_value : 0);
      const hasScoreData = Boolean(
        item.creator_current_score ||
        item.opponent_current_score ||
        Number.isFinite(item.creator_current_score_value) ||
        Number.isFinite(item.opponent_current_score_value)
      );
      const lowerIsBetter = item.leaderboard_lower_is_better === true;
      let winner = null;
      let lead = 0;
      if (scoreMode && hasScoreData && creatorScoreValue !== null && opponentScoreValue !== null) {
        if (creatorScoreValue === opponentScoreValue) {
          winner = null;
          lead = 0;
        } else if (lowerIsBetter ? creatorScoreValue < opponentScoreValue : creatorScoreValue > opponentScoreValue) {
          winner = "creator";
          lead = Math.abs(creatorScoreValue - opponentScoreValue);
        } else {
          winner = "opponent";
          lead = Math.abs(creatorScoreValue - opponentScoreValue);
        }
        challengeLeadCache[String(item.id)] = {
          winner,
          lead,
          format: inferredFormat || null,
          ts: Date.now()
        };
        saveChallengeLeadCache();
      } else if (creatorPoints !== null && opponentPoints !== null) {
        if (creatorPoints > opponentPoints) winner = "creator";
        else if (opponentPoints > creatorPoints) winner = "opponent";
        lead = winner
          ? Math.abs((creatorPoints ?? 0) - (opponentPoints ?? 0))
          : 0;
      } else if (scoreMode && !hasScoreData) {
        const cached = challengeLeadCache[String(item.id)];
        if (cached) {
          winner = cached.winner || null;
          lead = Number.isFinite(cached.lead) ? cached.lead : 0;
        }
      }
      const cachedFormat = challengeLeadCache[String(item.id)]?.format;
      const scoreLeadText = scoreMode ? formatLeaderboardDelta(lead, inferredFormat || cachedFormat) : null;

      const left = document.createElement("div");
      left.className = "challengeSide" + (winner === "creator" ? " win" : winner === "opponent" ? " lose" : "");
      const right = document.createElement("div");
      right.className = "challengeSide" + (winner === "opponent" ? " win" : winner === "creator" ? " lose" : "");

    const leftAvatarUrl = getChallengeAvatar(item.creator_username);
    const rightAvatarUrl = getChallengeAvatar(item.opponent_username);
      const creatorBest = item.creator_current_score ?? item.creator_start_score ?? "--";
      const opponentBest = item.opponent_current_score ?? item.opponent_start_score ?? "--";
      const creatorTopText = scoreMode ? creatorBest : creatorPointsText;
      const opponentTopText = scoreMode ? opponentBest : opponentPointsText;
      left.innerHTML = `
      <div class="challengeSideTop">
        ${leftAvatarUrl ? `<img class="challengeAvatar" src="${iconUrl(leftAvatarUrl)}" alt="" loading="lazy" data-avatar-user="${safeText(item.creator_username)}" />` : `<span class="challengeAvatar placeholder" data-avatar-user="${safeText(item.creator_username)}"></span>`}
        <div class="challengeName">${safeText(item.creator_username)}</div>
      </div>
        <div class="challengePoints">${safeText(creatorTopText)}</div>
        ${winner === "creator" ? `<div class="challengeLead">${scoreMode ? safeText(scoreLeadText) : `+${lead}`} lead</div>` : ""}
      `;

      right.innerHTML = `
      <div class="challengeSideTop">
        ${rightAvatarUrl ? `<img class="challengeAvatar" src="${iconUrl(rightAvatarUrl)}" alt="" loading="lazy" data-avatar-user="${safeText(item.opponent_username)}" />` : `<span class="challengeAvatar placeholder" data-avatar-user="${safeText(item.opponent_username)}"></span>`}
        <div class="challengeName">${safeText(item.opponent_username)}</div>
      </div>
        <div class="challengePoints">${safeText(opponentTopText)}</div>
        ${winner === "opponent" ? `<div class="challengeLead">${scoreMode ? safeText(scoreLeadText) : `+${lead}`} lead</div>` : ""}
      `;

      const vs = document.createElement("div");
      vs.className = "challengeVs";
      vs.textContent = "VS";

      const cancelBtn = document.createElement("button");
      cancelBtn.className = "smallBtn";
      cancelBtn.textContent = "Cancel";
      cancelBtn.setAttribute("data-cancel", String(item.id));

      const body = document.createElement("div");
      body.className = "challengeActiveBody";
      body.appendChild(left);
      body.appendChild(vs);
      body.appendChild(right);

      const actions = document.createElement("div");
      actions.className = "challengeActions";
      actions.appendChild(cancelBtn);

      if (item.challenge_type === "score" && item.game_title && item.leaderboard_title) {
        const info = document.createElement("div");
        info.className = "challengeScoreInfo";
        info.innerHTML = `
          <span class="challengeScoreLabel">Score Attack</span>
          <span class="challengeScoreTitle">${safeText(item.game_title)} | ${safeText(item.leaderboard_title)}</span>
        `;
        card.appendChild(info);
      }

      card.appendChild(body);
      card.appendChild(actions);
      card.appendChild(meta);
    } else {
      card.appendChild(top);
      card.appendChild(meta);
    }
    frag.appendChild(card);
  }
  container.appendChild(frag);
}

function getChallengeRenderSignature(item) {
  return JSON.stringify([
    item.id,
    item.status,
    item.creator_username,
    item.opponent_username,
    item.duration_hours,
    item.end_at,
    item.challenge_type,
    item.game_title,
    item.leaderboard_title,
    item.creator_points,
    item.opponent_points,
    item.creator_current_score,
    item.opponent_current_score,
    item.leaderboard_format,
    item.leaderboard_lower_is_better
  ]);
}

function renderChallengeHistory(items) {
  if (!challengeHistoryList) return;
  challengeHistoryList.innerHTML = "";
  if (!items.length) {
    challengeHistoryList.innerHTML = `<div class="meta">No completed challenges yet.</div>`;
    return;
  }
  const frag = document.createDocumentFragment();
  for (const item of items) {
    const card = document.createElement("div");
    card.className = "challengeItem";
    const creator = safeText(item.creator_username);
    const opponent = safeText(item.opponent_username);
    const winner = item.winner === "tie" ? "Tie" : safeText(item.winner || "Pending");
  const leadValue = item.lead !== null && item.lead !== undefined ? item.lead : null;
  const lead = item.challenge_type === "score"
    ? (leadValue !== null ? `${formatLeaderboardDelta(leadValue, item.leaderboard_format)} lead` : "--")
    : (leadValue !== null ? `+${leadValue} lead` : "--");
  const meta = new Date(item.end_at || item.start_at || item.created_at).toLocaleString();
  const extra = item.challenge_type === "score" && item.game_title && item.leaderboard_title
    ? `${safeText(item.game_title)} | ${safeText(item.leaderboard_title)}`
    : "";
  const creatorPoints = item.creator_points ?? "--";
  const opponentPoints = item.opponent_points ?? "--";
  const creatorFinalScore = item.creator_final_score ?? "--";
  const opponentFinalScore = item.opponent_final_score ?? "--";

    card.innerHTML = `
      <div class="challengeRow">
        <div>${creator} vs ${opponent}</div>
        <div class="challengeMeta">${meta}</div>
      </div>
      <div class="challengeRow">
        <div class="challengeMeta">Winner: ${winner}</div>
        <div class="challengeMeta">Lead: ${lead}</div>
      </div>
    ${item.challenge_type === "score"
      ? `<div class="challengeMeta">${creator}: ${safeText(creatorFinalScore)} | ${opponent}: ${safeText(opponentFinalScore)}</div>`
      : `<div class="challengeMeta">${creator}: +${creatorPoints} | ${opponent}: +${opponentPoints}</div>`
    }
    ${extra ? `<div class="challengeMeta">${extra}</div>` : ""}
  `;
    frag.appendChild(card);
  }
  challengeHistoryList.appendChild(frag);
}

function renderChallengeFriendOptions(list) {
  if (!challengeOpponentInput) return;
  const current = challengeOpponentInput.value;
  const sorted = Array.from(new Set(list.map(clampUsername).filter(Boolean))).sort((a, b) => a.localeCompare(b));
  challengeOpponentInput.innerHTML = "";
  const placeholder = document.createElement("option");
  placeholder.value = "";
  placeholder.textContent = sorted.length ? "Select a friend" : "No friends added yet";
  challengeOpponentInput.appendChild(placeholder);

  for (const name of sorted) {
    const opt = document.createElement("option");
    opt.value = name;
    opt.textContent = name;
    challengeOpponentInput.appendChild(opt);
  }

  if (current && sorted.includes(current)) {
    challengeOpponentInput.value = current;
  }
  if (challengeScoreSelectBtn) {
    const hasFriend = Boolean(challengeOpponentInput.value);
    challengeScoreSelectBtn.disabled = !hasFriend;
  }
}

async function refreshChallenges({ includeTotals = true } = {}) {
  const ensured = ensureUsername({ prompt: true });
  if (!ensured) return;
  renderChallengeFriendOptions(friends);
  if (challengeErrorEl) challengeErrorEl.textContent = "";
  setLoading(challengesLoadingEl, true);
  try {
    const baseData = includeTotals
      ? await fetchChallenges({ includeTotals: false })
      : await fetchChallenges({ includeTotals });
    const incoming = Array.isArray(baseData?.incoming) ? baseData.incoming : [];
    const outgoing = Array.isArray(baseData?.outgoing) ? baseData.outgoing : [];
    const active = Array.isArray(baseData?.active) ? baseData.active : [];
    if (baseData?.warnings?.length) {
      if (challengeErrorEl) challengeErrorEl.textContent = baseData.warnings[0];
    }
    let activeForRender = active;
    const activeIds = new Set(active.map(item => String(item.id)));
    for (const id of activeChallengeCache.keys()) {
      if (!activeIds.has(id)) activeChallengeCache.delete(id);
    }
    for (const item of active) {
      const id = String(item.id);
      const cached = activeChallengeCache.get(id) || item;
      activeChallengeCache.set(id, { ...cached, ...item });
    }
    activeForRender = Array.from(activeChallengeCache.values()).map((item) => {
      const cachedTotals = challengeTotalsCache.get(String(item.id));
      if (!cachedTotals) return item;
      return {
        ...item,
        creator_points: cachedTotals.creator_points,
        opponent_points: cachedTotals.opponent_points
      };
    });

    activeForRender.sort((a, b) => {
      const aEnd = a.end_at ? Date.parse(a.end_at) : Number.POSITIVE_INFINITY;
      const bEnd = b.end_at ? Date.parse(b.end_at) : Number.POSITIVE_INFINITY;
      return aEnd - bEnd;
    });

    renderChallengeList(incoming, challengeIncomingEl, "incoming", ensured);
    renderChallengeList(outgoing, challengeOutgoingEl, "outgoing", ensured);
    renderChallengeList(activeForRender, challengeActiveEl, "active", ensured);
    pruneChallengeLeadCache(activeForRender);
    hydrateChallengeAvatars(active);

    if (includeTotals) {
      const totalsData = await fetchChallenges({ includeTotals: true });
      if (totalsData?.warnings?.length) {
        if (challengeErrorEl) challengeErrorEl.textContent = totalsData.warnings[0];
      }
      const totalsActive = Array.isArray(totalsData?.active) ? totalsData.active : [];
      for (const item of totalsActive) {
        if (item.creator_points !== null && item.opponent_points !== null) {
          challengeTotalsCache.set(String(item.id), {
            creator_points: item.creator_points,
            opponent_points: item.opponent_points
          });
        }
        activeChallengeCache.set(String(item.id), item);
      }
      const totalsActiveForRender = Array.from(activeChallengeCache.values());
      totalsActiveForRender.sort((a, b) => {
        const aEnd = a.end_at ? Date.parse(a.end_at) : Number.POSITIVE_INFINITY;
        const bEnd = b.end_at ? Date.parse(b.end_at) : Number.POSITIVE_INFINITY;
        return aEnd - bEnd;
      });
      renderChallengeList(totalsActiveForRender, challengeActiveEl, "active", ensured);
      hydrateChallengeAvatars(totalsActive);
    }
  } catch (e) {
    const msg = String(e?.message || "");
    if (msg.includes("429") || msg.includes("Too Many Attempts")) {
      if (challengeErrorEl) challengeErrorEl.textContent = "Rate limited by RetroAchievements. Retrying shortly...";
    } else {
      if (challengeErrorEl) challengeErrorEl.textContent = String(e?.message || "Failed to load challenges.");
    }
  } finally {
    setLoading(challengesLoadingEl, false);
  }
}

function renderLeaderboard(rows, me) {
  const key = JSON.stringify(rows.map(r => [
    r.username,
    r.points,
    r.deltaVsYou,
    r.unlocks,
    r.nowPlayingHtml,
    r.nowPlayingText,
    r.dailyPoints,
    r.weeklyPoints,
    r.avatarUrl,
    r.showDailyPoints
  ]));
  if (key === lastLeaderboardKey) return;
  lastLeaderboardKey = key;
  lastLeaderboardRows = rows.map(r => ({ ...r }));

  tbody.innerHTML = "";
  const cacheRows = leaderboardBaseRows.length ? leaderboardBaseRows : rows;
  cacheSet("leaderboard", { rows: cacheRows, me });
  const total = rows.length;
  const frag = document.createDocumentFragment();
  rows.forEach((r, idx) => {
    const tr = document.createElement("tr");
    tr.dataset.username = r.username || "";

    const delta = r.deltaVsYou;
    const cls = delta > 0 ? "delta-neg" : delta < 0 ? "delta-pos" : "delta-zero";
    const isMe = (r.username && me) ? r.username.toLowerCase() === me.toLowerCase() : false;
    const levelValue = Number.isFinite(Number(r.level)) ? Number(r.level) : 1;
    const avatar = r.avatarUrl
      ? `<img class="leaderboardAvatar" src="${iconUrl(r.avatarUrl)}" alt="" loading="lazy" />`
      : `<span class="leaderboardAvatar placeholder" aria-hidden="true"></span>`;
    tr.innerHTML = `
      <td><button class="linkBtn" type="button" data-profile="${safeText(r.username)}">${avatar}<span class="leaderboardIdentity"><span class="nameRank">${idx + 1}.</span><span class="leaderboardName"><span class="leaderboardLevel">Level ${levelValue}</span><strong>${safeText(r.username)}</strong></span>${isMe ? '<span class="note">(you)</span>' : ""}</span></button></td>
      <td>
        <strong>${Math.round(r.points)}</strong>
        ${r.showDailyPoints && r.dailyPoints !== null ? `<span class="dailyPoints">(+${Math.round(r.dailyPoints)})</span>` : ""}
      </td>
      <td class="${cls}"><strong>${delta > 0 ? "+" : ""}${Math.round(delta)}</strong></td>
      <td>${r.unlocks}</td>
      <td>${r.nowPlayingHtml || r.nowPlayingText || ""}</td>
    `;

    frag.appendChild(tr);

    const nameBtn = tr.querySelector("button[data-profile]");
    r.nameColor = leaderboardRankColor(idx, total);
    if (nameBtn) {
      nameBtn.style.setProperty("--name-color", r.nameColor);
    }
  });
  tbody.appendChild(frag);
  renderLeaderboardChart(rows);
  renderHourlyChart(rows);

  if (!tbody.dataset.bound) {
    tbody.dataset.bound = "true";
    tbody.addEventListener("click", (e) => {
      const nowPlayingBtn = e.target.closest("button[data-now-game-id]");
      if (nowPlayingBtn) {
        const gameId = nowPlayingBtn.getAttribute("data-now-game-id");
        const gameTitleRaw = nowPlayingBtn.getAttribute("data-now-game-title") || "";
        const gameTitle = decodeURIComponent(gameTitleRaw);
        const row = nowPlayingBtn.closest("tr");
        const target = row?.dataset?.username || "";
        const { me } = getUsersIncludingMe();
        if (!me) {
          setStatus("Set your username first.");
          return;
        }
        if (target && target.toLowerCase() !== me.toLowerCase()) {
          openGameCompare({ gameId, title: gameTitle }, target);
          return;
        }
        selfGameReturnToProfile = true;
        profileIsSelf = true;
        currentProfileUser = me;
        setActivePage("profile");
        openSelfGame({ gameId, title: gameTitle });
        return;
      }
      const profileBtn = e.target.closest("button[data-profile]");
      if (profileBtn) {
        const target = profileBtn.getAttribute("data-profile");
        const isSelf = currentUser && target && currentUser.toLowerCase() === target.toLowerCase();
        if (isSelf) {
          setActivePage("profile");
        } else {
          setActivePage("dashboard");
        }
        openProfile(target);
        return;
      }
    });
  }
}

function formatDate(d) {
  if (!d) return "";
  if (d instanceof Date) return d.toLocaleString();
  const raw = String(d);
  const hasTz = /Z|[+-]\d{2}:?\d{2}$/.test(raw);
  if (!hasTz) {
    const m = raw.match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})$/);
    if (m) {
      const y = Number(m[1]);
      const mo = Number(m[2]) - 1;
      const day = Number(m[3]);
      const h = Number(m[4]);
      const mi = Number(m[5]);
      const s = Number(m[6]);
      const utc = Date.UTC(y, mo, day, h, mi, s);
      return new Date(utc).toLocaleString();
    }
  }
  const isoLike = raw.replace(" ", "T");
  const t = Date.parse(hasTz ? raw : isoLike);
  if (!t) return d || "";
  return new Date(t).toLocaleString();
}

function iconUrl(rel) {
  if (!rel) return "";
  if (rel.startsWith("http")) return rel;
  if (rel.startsWith("//")) return `https:${rel}`;
  // RA returns paths like /Images/xxxx.png or /Badge/xxx.png
  return `https://retroachievements.org${rel}`;
}

function achievementUrl(id) {
  return `https://retroachievements.org/achievement/${encodeURIComponent(id)}`;
}


function renderProfileGamesList(games, emptyMessage) {
  profileSharedGamesEl.innerHTML = "";

  const orderedGames = Array.isArray(games) ? games : [];
  if (!orderedGames.length) {
    profileSharedGamesEl.innerHTML = `<div class="meta">${safeText(emptyMessage)}</div>`;
    return;
  }

  const frag = document.createDocumentFragment();
  for (const g of orderedGames) {
    const tile = document.createElement("div");
    const allowOpen = profileIsSelf || profileAllowCompare;
    tile.className = allowOpen ? "tile clickable" : "tile";
    if (g.gameId) tile.setAttribute("data-game-id", String(g.gameId));
    if (allowOpen) {
      tile.setAttribute("role", "button");
      tile.tabIndex = 0;
    }

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
    frag.appendChild(tile);
    if (g.gameId) {
      updateTileBadge(tile, g.gameId);
    }

    if (allowOpen) {
      const open = () => {
        if (profileIsSelf) {
          selfGameReturnToProfile = true;
          openSelfGame(g);
        } else {
          openGameCompare(g);
        }
      };
      tile.addEventListener("click", open);
      tile.addEventListener("keydown", (e) => {
        if (e.key === "Enter" || e.key === " ") { e.preventDefault(); open(); }
      });
    }
    const cached = profileGameAchievementCounts.get(String(g.gameId ?? ""));
    if (cached) {
      setTileAchievementMeta(meta, cached);
    } else if (g.gameId) {
      loadProfileGameAchievements(g.gameId, meta);
    }
  }
  profileSharedGamesEl.appendChild(frag);
  profileDisplayedGames = orderedGames;
}

function applyProfileGameFilter() {
  const query = (profileGameSearchEl?.value || "").trim().toLowerCase();
  if (activePageName === "profile" && profileRecentGamesEl) {
    renderProfileRecentTab(query);
    return;
  }

  const base = profileDisplayedGames || [];
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
  metaEl.innerHTML = profileIsSelf
    ? `Achievements: <span class="me">${meText}</span>`
    : `Achievements: <span class="me">${meText}</span> <span class="sep">|</span> <span class="them">${themText}</span>`;
  const tile = metaEl.closest(".tile");
  if (tile) {
    const gameId = tile.getAttribute("data-game-id");
    updateTileBadge(tile, gameId);
  }
}

function getProfileCountsCacheKey(me, target, gameId) {
  const m = clampUsername(me);
  const t = clampUsername(target);
  return `${LS_PROFILE_COUNTS_PREFIX}:${m}:${t}:${gameId}`;
}

function readProfileCountsCache(me, target, gameId) {
  try {
    const key = getProfileCountsCacheKey(me, target, gameId);
    const raw = localStorage.getItem(key);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") return null;
    const ts = Number(parsed.ts);
    if (!Number.isFinite(ts) || Date.now() - ts > PROFILE_COUNTS_CACHE_TTL_MS) return null;
    return parsed.counts || null;
  } catch {
    return null;
  }
}

function writeProfileCountsCache(me, target, gameId, counts) {
  try {
    const key = getProfileCountsCacheKey(me, target, gameId);
    const payload = { ts: Date.now(), counts };
    localStorage.setItem(key, JSON.stringify(payload));
  } catch {
    // ignore cache failures
  }
}

function getBeatenFlag(kindRaw) {
  const kind = String(kindRaw || "").toLowerCase();
  return Boolean(kind && kind.includes("beaten"));
}

function getCompletedFlag(kindRaw) {
  const kind = String(kindRaw || "").toLowerCase();
  return Boolean(kind && (kind.includes("completed") || kind.includes("beaten")));
}

function getMasteredFlag(kindRaw) {
  const kind = String(kindRaw || "").toLowerCase();
  return Boolean(kind && kind.includes("mastered"));
}

function getCompletionSortTs(game) {
  const raw = game?.highestAwardDate || game?.mostRecentAwardedDate || "";
  const ts = Date.parse(raw);
  return Number.isFinite(ts) ? ts : 0;
}

function updateTileBadge(tile, gameId) {
  if (!tile || !gameId) return;
  const counts = profileGameAchievementCounts.get(String(gameId));
  const target = profileIsSelf ? counts?.me : counts?.them;
  const hasAll = Boolean(target?.total && target?.earned !== null && target?.earned >= target?.total);
  const beatenKind = profileCompletionByGameId.get(String(gameId));
  const hasBeaten = getBeatenFlag(beatenKind);

  let badge = tile.querySelector(".completionBadge");
  if (!hasAll && !hasBeaten) {
    tile.classList.remove("completed");
    if (badge) badge.remove();
    return;
  }

  tile.classList.add("completed");
  if (!badge) {
    badge = document.createElement("span");
    badge.className = "completionBadge";
    tile.appendChild(badge);
  }
  badge.classList.toggle("gold", hasAll);
  badge.classList.toggle("silver", !hasAll && hasBeaten);
  badge.setAttribute("title", hasAll ? "All achievements" : "Beaten");
  badge.innerHTML = "&#9733;";
}

function isProfileGameCompleted(game) {
  const gameId = String(game?.gameId ?? "");
  if (!gameId) return false;
  const counts = profileGameAchievementCounts.get(gameId);
  const target = profileIsSelf ? counts?.me : counts?.them;
  const hasAll = Boolean(target?.total && target?.earned !== null && target?.earned >= target?.total);
  const beatenKind = profileCompletionByGameId.get(gameId);
  const hasBeaten = getBeatenFlag(beatenKind);
  return hasAll || hasBeaten;
}

function sortProfileGamesForDisplay(list) {
  return sortGamesByLastPlayed(list || []);
}

function refreshCompletionBadges() {
  if (!profileSharedGamesEl) return;
  const tiles = profileSharedGamesEl.querySelectorAll(".tile[data-game-id]");
  tiles.forEach((tile) => {
    const gameId = tile.getAttribute("data-game-id");
    updateTileBadge(tile, gameId);
  });
}

async function loadProfileGameAchievements(gameId, metaEl) {
  const target = clampUsername(currentProfileUser);
  const { me } = getUsersIncludingMe();
  if (!target || !me || !metaEl) return;
  const loadToken = activeProfileLoadToken;

  const key = String(gameId ?? "");
  if (!key) return;

  const cached = profileGameAchievementCounts.get(key);
  if (cached) {
    if (loadToken === activeProfileLoadToken) {
      setTileAchievementMeta(metaEl, cached);
    }
    return;
  }

  const localCached = readProfileCountsCache(me, target, key);
  if (localCached) {
    profileGameAchievementCounts.set(key, localCached);
    if (loadToken === activeProfileLoadToken) {
      setTileAchievementMeta(metaEl, localCached);
    }
    return;
  }

  const pending = profileGameAchievementPending.get(key);
  if (pending) {
    pending.then((counts) => {
      if (loadToken === activeProfileLoadToken && clampUsername(currentProfileUser) === target) {
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
    writeProfileCountsCache(me, target, key, counts);
    return counts;
  }).finally(() => {
    profileGameAchievementPending.delete(key);
  });

  profileGameAchievementPending.set(key, promise);

  try {
    const counts = await promise;
    if (loadToken === activeProfileLoadToken && clampUsername(currentProfileUser) === targetAtRequest) {
      setTileAchievementMeta(metaEl, counts);
    }
  } catch {
    if (loadToken === activeProfileLoadToken && clampUsername(currentProfileUser) === targetAtRequest) {
      metaEl.textContent = "Achievements: --/--";
    }
  }
}

function renderSharedGames(games, emptyMessage = "No recent games found.") {
  profileDisplayedGames = Array.isArray(games) ? games : [];
  profileGamesEmptyMessage = emptyMessage;
  applyProfileGameFilter();
}

function parseLastPlayed(value) {
  if (!value) return 0;
  const raw = String(value);
  const ts = Date.parse(raw);
  if (Number.isFinite(ts)) return ts;
  const m = raw.match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})$/);
  if (m) {
    const y = Number(m[1]);
    const mo = Number(m[2]) - 1;
    const day = Number(m[3]);
    const h = Number(m[4]);
    const mi = Number(m[5]);
    const s = Number(m[6]);
    return Date.UTC(y, mo, day, h, mi, s);
  }
  const alt = Date.parse(raw.replace(" ", "T") + "Z");
  return Number.isFinite(alt) ? alt : 0;
}

function mergeGameLists(primary, secondary) {
  const byId = new Map();
  for (const list of [primary, secondary]) {
    for (const g of list || []) {
      const key = String(g.gameId ?? "");
      if (!key) continue;
      const prev = byId.get(key);
      const next = {
        gameId: g.gameId,
        title: g.title || (prev?.title ?? `Game ${safeText(g.gameId)}`),
        imageIcon: g.imageIcon || prev?.imageIcon,
        lastPlayed: g.lastPlayed || prev?.lastPlayed || ""
      };
      if (!prev) {
        byId.set(key, next);
        continue;
      }
      const prevTs = parseLastPlayed(prev.lastPlayed);
      const nextTs = parseLastPlayed(g.lastPlayed);
      if (nextTs >= prevTs) {
        byId.set(key, next);
      }
    }
  }
  return Array.from(byId.values());
}

function sortGamesByLastPlayed(list) {
  return (list || []).slice().sort((a, b) => {
    const ta = parseLastPlayed(a?.lastPlayed);
    const tb = parseLastPlayed(b?.lastPlayed);
    return tb - ta;
  });
}

function normalizeGameList(list) {
  const seen = new Set();
  const out = [];
  for (const g of list || []) {
    const key = String(g.gameId ?? g.GameID ?? g.id ?? "");
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push({
      gameId: g.gameId ?? g.GameID ?? g.id,
      title: g.title ?? g.Title ?? "",
      imageIcon: g.imageIcon ?? g.ImageIcon ?? "",
      lastPlayed: g.lastPlayed ?? g.LastPlayed ?? ""
    });
  }
  return out;
}

function combineRecentGames(cachedResults, freshResults) {
  const cached = normalizeGameList(cachedResults || []);
  const fresh = normalizeGameList(freshResults || []);
  return mergeGameLists(cached, fresh);
}

function normalizeRecentGames(list) {
  return normalizeGameList(list || []);
}

function getCommonGames(mineList, theirsList) {
  const mineIds = new Set((mineList || []).map(g => String(g.gameId ?? "")).filter(Boolean));
  if (!mineIds.size) return [];
  return (theirsList || []).filter(g => mineIds.has(String(g.gameId ?? "")));
}

function normalizeCompletionList(list) {
  const items = Array.isArray(list) ? list : [];
  return items.map((row) => ({
    gameId: row.gameId ?? row.GameID,
    title: row.title ?? row.Title ?? "",
    imageIcon: row.imageIcon ?? row.ImageIcon ?? "",
    highestAwardKind: row.highestAwardKind ?? row.HighestAwardKind ?? "",
    highestAwardDate: row.highestAwardDate ?? row.HighestAwardDate ?? "",
    mostRecentAwardedDate: row.mostRecentAwardedDate ?? row.MostRecentAwardedDate ?? ""
  })).filter(g => g.gameId);
}

function normalizeBacklogGames(list) {
  const items = Array.isArray(list) ? list : [];
  return items.map((row) => ({
    gameId: row.gameId ?? row.GameID,
    title: row.title ?? row.Title ?? "",
    imageIcon: row.imageIcon ?? row.ImageIcon ?? "",
    startedAwarded: row.startedAwarded ?? row.StartedAwarded ?? 0,
    startedTotal: row.startedTotal ?? row.StartedTotal ?? 0
  })).filter(g => g.gameId);
}

function getBacklogProgressForGame(username, game) {
  const userKey = normalizeUserKey(username || "");
  const gameId = String(game?.gameId ?? "");
  if (!userKey || !gameId) return null;
  const cacheKey = `${userKey}:${gameId}`;
  const cached = backlogProgressCache.get(cacheKey);
  if (cached?.data) return cached.data;
  const startedAwarded = Number(game?.startedAwarded ?? 0);
  const startedTotal = Number(game?.startedTotal ?? 0);
  if (startedAwarded > 0 || startedTotal > 0) {
    return { earned: startedAwarded, total: startedTotal };
  }
  return null;
}

function sortBacklogItemsByProgress(items, username) {
  const list = normalizeBacklogGames(items);
  return list.sort((a, b) => {
    const aProgress = getBacklogProgressForGame(username, a);
    const bProgress = getBacklogProgressForGame(username, b);
    const aTotal = Number(aProgress?.total || 0);
    const bTotal = Number(bProgress?.total || 0);
    const aEarned = Number(aProgress?.earned || 0);
    const bEarned = Number(bProgress?.earned || 0);
    const aPct = aTotal > 0 ? aEarned / aTotal : 0;
    const bPct = bTotal > 0 ? bEarned / bTotal : 0;
    if (bPct !== aPct) return bPct - aPct;
    return String(a.title || "").localeCompare(String(b.title || ""));
  });
}

async function loadMoreProfileGames() {
  const target = clampUsername(currentProfileUser);
  if (!target || !profileShowMoreBtn) return;
  const { me } = getUsersIncludingMe();
  if (!me) {
    setStatus("Set your username first.");
    return;
  }
  const loadToken = activeProfileLoadToken;

  const nextCount = Math.min(PROFILE_GAMES_MAX, profileGamesFetchCount + PROFILE_GAMES_STEP);
  if (nextCount <= profileGamesFetchCount) {
    profileShowMoreBtn.hidden = true;
    return;
  }

  profileShowMoreBtn.disabled = true;
  profileShowMoreBtn.textContent = "Loading...";
  setLoading(profileLoadingEl, true);

  try {
    const mineCache = readRecentGamesCache(me, nextCount);
    const theirsCache = profileIsSelf ? null : readRecentGamesCache(target, nextCount);
    if (mineCache?.data && (profileIsSelf || theirsCache?.data)) {
      if (loadToken !== activeProfileLoadToken) return;
      const mineResultsCached = normalizeGameList(mineCache.data.results || []);
      const theirsResultsCached = profileIsSelf ? [] : normalizeGameList(theirsCache.data.results || []);
      const combinedCached = profileIsSelf
        ? mergeGameLists(profileSharedGames, mineResultsCached)
        : mergeGameLists(profileSharedGames, theirsResultsCached);
      const sortedCached = sortGamesByLastPlayed(combinedCached);
      profileSharedGames = sortedCached;
      profileDisplayedGames = sortedCached;
      profileGamesFetchCount = nextCount;
      renderSharedGames(sortedCached, "No recent games found.");
      refreshCompletionBadges();
    }

    const needMine = !mineCache || mineCache.stale;
    const needTheirs = profileIsSelf ? false : (!theirsCache || theirsCache.stale);
    const [mine, theirs] = await Promise.all([
      needMine ? fetchRecentGames(me, nextCount) : Promise.resolve(mineCache.data),
      profileIsSelf ? null : (needTheirs ? fetchRecentGames(target, nextCount) : Promise.resolve(theirsCache.data))
    ]);
    if (loadToken !== activeProfileLoadToken) return;

    const mineCombined = combineRecentGames(mineCache?.data?.results, mine?.results || []);
    const theirsCombined = profileIsSelf ? [] : combineRecentGames(theirsCache?.data?.results, theirs?.results || []);
    const combined = profileIsSelf
      ? mergeGameLists(profileSharedGames, mineCombined)
      : mergeGameLists(profileSharedGames, theirsCombined);
    const sortedCombined = sortGamesByLastPlayed(combined);
    const newAdded = combined.length - profileSharedGames.length;

    profileSharedGames = sortedCombined;
    profileDisplayedGames = sortedCombined;
    profileGamesFetchCount = nextCount;
    renderSharedGames(sortedCombined, "No recent games found.");
    if (!profileCompletionLoading) {
      if (!profileCompletionByGameId.size || profileCompletionTarget !== target) {
        loadProfileCompletionProgress(target, combined);
      } else {
        refreshCompletionBadges();
      }
    }

    const noMore = newAdded <= 0 || nextCount >= PROFILE_GAMES_MAX;
    profileShowMoreBtn.hidden = false;
    profileShowMoreBtn.disabled = false;
    profileShowMoreBtn.textContent = "Show less";
    profileAllGamesLoaded = noMore;
    profileExpanded = true;
  } catch (e) {
    profileShowMoreBtn.disabled = false;
    profileShowMoreBtn.textContent = "Show more";
    setStatus(e?.message || "Failed to load more games.");
  } finally {
    setLoading(profileLoadingEl, false);
  }
}

async function loadProfileCompletionProgress(username, games = []) {
  const target = clampUsername(username);
  if (!target) return;
  if (profileCompletionLoading && profileCompletionTarget === target) return;
  const loadToken = activeProfileLoadToken;

  profileCompletionLoading = true;
  profileCompletionTarget = target;
  const wantedIds = new Set((games || []).map(g => String(g.gameId ?? "")).filter(Boolean));
  if (!wantedIds.size) {
    profileCompletionLoading = false;
    return;
  }

  const map = new Map();
  const perPage = 500;
  let offset = 0;
  let total = null;
  let pages = 0;
  let foundWanted = 0;
  const maxPages = 10;

  try {
    while (pages < maxPages) {
      pages += 1;
      const data = await fetchUserCompletionProgress(target, perPage, offset);
      if (loadToken !== activeProfileLoadToken || clampUsername(currentProfileUser) !== target) return;

      const results = data?.results || [];
      for (const row of results) {
        const gameId = row.gameId ?? row.GameID;
        const kind = row.highestAwardKind ?? row.HighestAwardKind;
        if (!gameId || !kind) continue;
        const id = String(gameId);
        if (!wantedIds.has(id)) continue;
        if (!map.has(id)) {
          foundWanted += 1;
          const title = row.title ?? row.Title ?? "";
          const titleText = title ? ` "${title}"` : "";
          logSystemMessage(`FOUND completed game ${id}${titleText} (${kind})`);
        }
        map.set(id, String(kind));
      }

      if (total === null) {
        const t = Number(data?.total ?? data?.Total ?? 0);
        total = Number.isFinite(t) ? t : 0;
      }

      if (results.length < perPage) break;
      offset += perPage;

      if (total && offset >= total) break;
      if (foundWanted >= wantedIds.size) break;
    }
  } catch {
    // ignore completion failures
  }

  profileCompletionByGameId = map;
  if (loadToken === activeProfileLoadToken) {
    renderSharedGames(profileSharedGames, profileGamesEmptyMessage);
    refreshCompletionBadges();
    renderProfileRecentTab();
  }
  profileCompletionLoading = false;
}

function collapseProfileGames() {
  if (!profileShowMoreBtn) return;
  profileSharedGames = profileBaseGames || [];
  profileDisplayedGames = profileSharedGames;
  profileGamesFetchCount = PROFILE_GAMES_INITIAL;
  profileAllGamesLoaded = false;
  profileExpanded = false;
  renderSharedGames(profileSharedGames, "No recent games found.");
  profileShowMoreBtn.hidden = profileSharedGames.length >= PROFILE_GAMES_MAX && !profileExpanded;
  profileShowMoreBtn.textContent = "Show more";
}

function renderProfileSummary(summary) {
  profileSummaryEl.innerHTML = "";

  if (!summary) {
    profileSummaryEl.innerHTML = `<div class="meta">No profile summary available.</div>`;
    return;
  }

  const username = currentProfileUser || summary.username || "Player";
  const avatarUrl = summary.userPic ? iconUrl(summary.userPic) : "";
  const rank = Number.isFinite(Number(summary.rank)) ? `#${Number(summary.rank).toLocaleString()}` : "--";
  const totalPoints = Number.isFinite(Number(summary.totalPoints))
    ? Number(summary.totalPoints).toLocaleString()
    : "--";
  const retroPoints = Number.isFinite(Number(summary.retroPoints))
    ? Number(summary.retroPoints).toLocaleString()
    : "--";
  const completed = Number.isFinite(Number(summary.completedGames))
    ? Number(summary.completedGames).toLocaleString()
    : "--";
  const totalPointsRaw = Number(summary.totalPoints);
  const levelValue = Number.isFinite(Number(summary.level))
    ? Number(summary.level)
    : 1;
  const pointsUntilNext = Number.isFinite(totalPointsRaw)
    ? Math.max(0, (((levelValue + 1) / 3) ** 2) * 10 - totalPointsRaw)
    : null;

  const memberSince = summary.memberSince ? formatDate(summary.memberSince) : "";
  let lastActiveValue = summary.lastActivity;
  if (lastActiveValue && typeof lastActiveValue === "object") {
    lastActiveValue =
      lastActiveValue.Date ?? lastActiveValue.date ??
      lastActiveValue.LastActivity ?? lastActiveValue.lastActivity ??
      lastActiveValue.LastUpdated ?? lastActiveValue.lastUpdated ??
      lastActiveValue.timestamp ?? lastActiveValue.time ??
      lastActiveValue.LastPlayed ?? lastActiveValue.lastPlayed ??
      "";
  }
  const lastActive = lastActiveValue ? formatDate(lastActiveValue) : "";

  const card = document.createElement("div");
  card.className = "profileHeroCard";

  const top = document.createElement("div");
  top.className = "profileHeroTop";

  const avatarWrap = document.createElement("div");
  avatarWrap.className = "profileHeroAvatarWrap";
  if (avatarUrl) {
    const img = document.createElement("img");
    img.className = "profileHeroAvatar";
    img.alt = `${username} avatar`;
    img.loading = "lazy";
    img.src = avatarUrl;
    avatarWrap.appendChild(img);
  }

  const identity = document.createElement("div");
  identity.className = "profileHeroIdentity";
  const levelEl = document.createElement("div");
  levelEl.className = "profileHeroLevel";
  levelEl.textContent = `Level ${levelValue}`;
  const nextLevelEl = document.createElement("div");
  nextLevelEl.className = "profileHeroNext";
  nextLevelEl.textContent = pointsUntilNext !== null
    ? `Points Until Next Level: ${Math.max(0, Math.round(pointsUntilNext)).toLocaleString()}`
    : "Points Until Next Level: --";
  const nameEl = document.createElement("div");
  nameEl.className = "profileHeroName";
  nameEl.textContent = username;
  const rankEl = document.createElement("div");
  rankEl.className = "profileHeroRank";
  rankEl.innerHTML = `Rank <span>${rank}</span>`;
  identity.append(levelEl, nextLevelEl, nameEl, rankEl);
  top.append(avatarWrap, identity);

  const stats = document.createElement("div");
  stats.className = "profileHeroStats";
  const statItems = [
    { label: "Total Points", value: totalPoints },
    { label: "Retro Points", value: retroPoints },
    { label: "Completed", value: completed }
  ];
  statItems.forEach((item) => {
    const stat = document.createElement("div");
    stat.className = "profileHeroStat";
    const l = document.createElement("div");
    l.className = "profileHeroStatLabel";
    l.textContent = item.label;
    const v = document.createElement("div");
    v.className = "profileHeroStatValue";
    v.textContent = item.value;
    stat.append(l, v);
    stats.appendChild(stat);
  });

  const meta = document.createElement("div");
  meta.className = "profileHeroMeta";
  if (memberSince) {
    const item = document.createElement("div");
    item.textContent = `Member Since ${memberSince}`;
    meta.appendChild(item);
  }
  if (lastActive) {
    const item = document.createElement("div");
    item.textContent = `Last Active ${lastActive}`;
    meta.appendChild(item);
  }

  card.append(top, stats, meta);
  profileSummaryEl.appendChild(card);
}

function renderProfileInsights({ sharedCount, meSummary, themSummary, isSelf }) {
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
    if (isSelf) {
      return {
        label,
        valueHtml: `<strong>${fmt(me)}</strong>`
      };
    }
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
    {
      label: isSelf ? "Recent Games" : "Shared Games",
      valueHtml: `<strong>${Number(sharedCount ?? 0).toLocaleString()}</strong>`
    },
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

function buildProfileGameList(mineResults, theirsResults, isSelf) {
  const mineNormalized = normalizeGameList(mineResults || []);
  const theirsNormalized = normalizeGameList(theirsResults || []);
  if (isSelf) {
    return sortGamesByLastPlayed(mineNormalized);
  }
  return sortGamesByLastPlayed(theirsNormalized);
}

async function loadProfileActivity(username) {
  if (!profileActivityListEl) return;
  const target = clampUsername(username);
  if (!target) return;
  profileActivityUser = target;
  setLoading(profileActivityLoadingEl, true);
  try {
    const minutes = Math.max(1, Math.floor(ACHIEVEMENTS_DEFAULT_HOURS * 60));
    const data = await fetchRecentAchievements(target, minutes, 5);
    const items = Array.isArray(data?.results) ? data.results : [];
    renderProfileActivity(items);
  } catch (err) {
    profileActivityListEl.innerHTML = `<div class="meta">Failed to load activity.</div>`;
  } finally {
    setLoading(profileActivityLoadingEl, false);
  }
}

async function openProfile(username) {
  const target = clampUsername(username);
  if (!target) return;
  const loadToken = ++profileLoadToken;
  activeProfileLoadToken = loadToken;

  const { me } = getUsersIncludingMe();
  if (!me) return setStatus("Set your username first.");
  const isSelf = me.toLowerCase() === target.toLowerCase();

  moveProfilePanel(profileHostProfile);
  moveSelfGamePanel(selfGameHostProfile);
  profilePanel.hidden = false;
  comparePanel.hidden = true;
  if (selfGamePanel) selfGamePanel.hidden = true;
  profileSummaryEl.innerHTML = `<div class="meta">Loading profile summary...</div>`;
  if (profileInsightsEl) profileInsightsEl.innerHTML = `<div class="meta">Loading profile insights...</div>`;
  profileSharedGamesEl.innerHTML = `<div class="meta">Loading recent games...</div>`;
  currentProfileUser = target;
  setActivePage("profile");
  profileTitleNameEl.textContent = target;
  profileSharedGames = [];
  profileDisplayedGames = [];
  profileAllGamesLoaded = false;
  profileGamesEmptyMessage = "No recent games found.";
  profileAutoLoadingAll = false;
  profileSkipAutoLoadOnce = false;
  profileGamesFetchCount = PROFILE_GAMES_INITIAL;
  profileAllGamesLoaded = false;
  profileGameAchievementCounts = new Map();
  profileGameAchievementPending = new Map();
  profileCompletionByGameId = new Map();
  profileCompletionLoading = false;
  profileCompletionTarget = "";
  profileRecentGames = [];
  profileCommonGames = [];
  profileAllowCompare = !isSelf;
  profileIsSelf = isSelf;
  profileCompletionList = [];
  profileCompletionListLoading = false;
  profileBacklogItems = [];
  profileBacklogLoading = false;
  if (profileLegendMeEl) {
    profileLegendMeEl.textContent = me || "You";
  }
  if (profileLegendThemEl) {
    profileLegendThemEl.textContent = target || "Friend";
  }
  if (profileShowMoreBtn) {
    profileShowMoreBtn.disabled = true;
    profileShowMoreBtn.hidden = true;
    profileShowMoreBtn.textContent = "Show more";
  }
  if (profileCloseBtn) profileCloseBtn.hidden = isSelf;
  if (profileBacklogBtn) profileBacklogBtn.hidden = isSelf;
  if (profileGamesNoteEl) {
    profileGamesNoteEl.textContent = "Recent games sorted by last played.";
  }
  if (profileGameSearchEl) {
    profileGameSearchEl.value = "";
  }
  if (selfGameTitleEl) selfGameTitleEl.textContent = "";
  if (selfGameMetaEl) selfGameMetaEl.textContent = "";
  if (selfGameAchievementsEl) selfGameAchievementsEl.innerHTML = "";
  if (profileActivityListEl) {
    profileActivityListEl.innerHTML = `<div class="meta">Loading activity...</div>`;
  }
  if (profileRecentGamesEl) {
    profileRecentGamesEl.innerHTML = `<div class="meta">Loading recent games...</div>`;
  }
  setProfileRecentTab("recent");

  setLoading(profileLoadingEl, true);
  loadProfileActivity(target);
  loadProfileRecentGames(target);
  loadProfileCompletionList(target);
  loadProfileBacklog(target);

  let summaryRendered = false;
  try {
    const count = PROFILE_GAMES_INITIAL;
    const mineCache = readRecentGamesCache(me, count);
    const theirsCache = readRecentGamesCache(target, count);
    const cachedList = isSelf ? mineCache?.data?.results : theirsCache?.data?.results;
    if (cachedList) {
      if (loadToken !== activeProfileLoadToken) return;
      const normalizedCached = normalizeRecentGames(cachedList || []);
      const cachedMine = normalizeRecentGames(mineCache?.data?.results || []);
      const cachedTheirs = normalizeRecentGames(theirsCache?.data?.results || []);
      profileBaseGames = normalizedCached;
      profileSharedGames = normalizedCached;
      profileDisplayedGames = normalizedCached;
      profileCommonGames = isSelf ? normalizedCached : getCommonGames(cachedMine, cachedTheirs.length ? cachedTheirs : normalizedCached);
      profileAllGamesLoaded = normalizedCached.length >= PROFILE_GAMES_MAX;
      profileExpanded = false;
      renderSharedGames(normalizedCached, "No recent games found.");
      if (profileShowMoreBtn) {
        profileShowMoreBtn.hidden = true;
        profileShowMoreBtn.textContent = "Show more";
      }
      loadProfileCompletionProgress(target, normalizedCached);
    }

    const needMine = !mineCache || mineCache.stale;
    const needTheirs = !theirsCache || theirsCache.stale;

    let meSummary = null;
    let themSummaryRaw = null;
    try {
      meSummary = await fetchUserSummary(me).catch(() => null);
      themSummaryRaw = await fetchUserSummary(target).catch(() => null);
    } catch {
      meSummary = null;
      themSummaryRaw = null;
    }

    let mine = mineCache?.data || null;
    let theirs = theirsCache?.data || null;
    try {
      if (needMine) mine = await fetchRecentGames(me, count);
      if (needTheirs) theirs = await fetchRecentGames(target, count);
    } catch {
      // allow summary to render even if recent games fail
    }
    if (loadToken !== activeProfileLoadToken) return;

    const themSummary = isSelf ? meSummary : themSummaryRaw;
    if (themSummary) {
      renderProfileSummary(themSummary);
      summaryRendered = true;
    } else {
      profileSummaryEl.innerHTML = `<div class="meta">Profile summary unavailable.</div>`;
      summaryRendered = true;
    }

    const mineCombined = combineRecentGames(mineCache?.data?.results, mine?.results || []);
    const theirsCombined = combineRecentGames(theirsCache?.data?.results, theirs?.results || []);
    const baseList = isSelf ? (mine?.results || mineCombined) : (theirs?.results || theirsCombined);
    const normalized = normalizeRecentGames(baseList);
    const mineNormalized = normalizeRecentGames(isSelf ? baseList : (mine?.results || mineCombined));

    profileBaseGames = normalized;
    profileSharedGames = normalized;
    profileDisplayedGames = normalized;
    profileCommonGames = isSelf ? normalized : getCommonGames(mineNormalized, normalized);
    profileAllGamesLoaded = normalized.length >= PROFILE_GAMES_MAX;
    profileExpanded = false;
    renderSharedGames(normalized, "No recent games found.");
    loadProfileCompletionProgress(target, normalized);
    if (profileShowMoreBtn) {
      profileShowMoreBtn.hidden = true;
      profileShowMoreBtn.textContent = "Show more";
    }
    renderProfileInsights({
      sharedCount: normalized.length,
      meSummary,
      themSummary,
      isSelf
    });
    profilePanel.scrollIntoView({ behavior: "smooth", block: "start" });
  } catch (e) {
    if (!summaryRendered) {
      profileSummaryEl.innerHTML = `<div class="meta">Failed to load profile summary.</div>`;
    }
    if (profileInsightsEl) profileInsightsEl.innerHTML = `<div class="meta">Failed to load profile insights.</div>`;
    profileSharedGamesEl.innerHTML = `<div class="meta">Failed to load recent games.</div>`;
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

  const haveItems = items.filter(a => a.statusLabel !== "None");
  const noneItems = items.filter(a => a.statusLabel === "None");
  const sections = [
    { label: "Unlocked by you or them", items: haveItems },
    { label: "Neither earned", items: noneItems }
  ];

  const frag = document.createDocumentFragment();
  const renderDivider = (label) => {
    const div = document.createElement("div");
    div.className = "selfDivider";
    div.textContent = label;
    frag.appendChild(div);
  };

  let renderedAny = false;
  for (const section of sections) {
    if (!section.items.length) continue;
    if (renderedAny) renderDivider(section.label);
    renderedAny = true;

    for (const a of section.items) {
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
      frag.appendChild(row);
    }
  }

  compareAchievementsEl.appendChild(frag);
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

function renderSelfAchievements(items) {
  selfGameAchievementsEl.innerHTML = "";
  if (!items.length) {
    selfGameAchievementsEl.innerHTML = `<div class="meta">No achievements found for this game.</div>`;
    return;
  }

  const earnedItems = items.filter(a => a.earned);
  const getEarnedTs = (a) => {
    const raw = a.earnedHardcoreDate || a.earnedDate || "";
    const ts = Date.parse(raw);
    return Number.isFinite(ts) ? ts : 0;
  };
  earnedItems.sort((a, b) => getEarnedTs(b) - getEarnedTs(a));
  const lockedItems = items.filter(a => !a.earned);
  const sections = [
    { label: "Earned", items: earnedItems },
    { label: "Locked", items: lockedItems }
  ];

  const frag = document.createDocumentFragment();
  const renderDivider = (label) => {
    const div = document.createElement("div");
    div.className = "selfDivider";
    div.textContent = label;
    frag.appendChild(div);
  };

  let renderedAny = false;
  for (const section of sections) {
    if (!section.items.length) continue;
    if (renderedAny) renderDivider(section.label);
    renderedAny = true;

    for (const a of section.items) {
      const row = document.createElement("div");
      row.className = `compareItem${a.earned ? " earned" : ""}`;

      const img = document.createElement("img");
      img.className = "compareBadge";
      img.alt = safeText(a.title || "achievement");
      img.loading = "lazy";
      img.src = iconUrl(a.badgeUrl);

      const main = document.createElement("div");
      main.className = "compareMain";

      const titleRow = document.createElement("div");
      titleRow.className = "compareTitleRow";

      const title = document.createElement("div");
      title.className = "compareTitle";
      title.textContent = a.title || `Achievement ${safeText(a.id)}`;

      titleRow.appendChild(title);
      if (a.earned) {
        const earnedDate = a.earnedHardcoreDate || a.earnedDate;
        if (earnedDate) {
          const earnedMeta = document.createElement("div");
          earnedMeta.className = "compareTitleDate";
          earnedMeta.textContent = formatDate(earnedDate);
          titleRow.appendChild(earnedMeta);
        }
      }

      const desc = document.createElement("div");
      desc.className = "compareDesc";
      desc.textContent = a.description || "";

      main.appendChild(titleRow);
      main.appendChild(desc);

      const statusWrap = document.createElement("div");
      statusWrap.className = "statusWrap";

      const status = document.createElement("div");
      status.className = `statusPill ${a.earned ? "me" : "none"}`;
      status.textContent = a.earned ? "Earned" : "Locked";

      const pointsPill = document.createElement("div");
      pointsPill.className = "statusPill points";
      pointsPill.textContent = `+${a.points ?? 0} pts`;

      statusWrap.appendChild(status);
      statusWrap.appendChild(pointsPill);

      row.appendChild(img);
      row.appendChild(main);
      row.appendChild(statusWrap);
      frag.appendChild(row);
    }
  }

  selfGameAchievementsEl.appendChild(frag);
}

async function openSelfGame(game) {
  const { me } = getUsersIncludingMe();
  if (!me) return setStatus("Set your username first.");

  setActivePage("game");
  profilePanel.hidden = true;
  comparePanel.hidden = true;
  moveSelfGamePanel(selfGameHostPage);
  if (selfGamePanel) selfGamePanel.hidden = false;
  if (selfGameTitleEl) selfGameTitleEl.textContent = game.title || `Game ${safeText(game.gameId)}`;
  if (selfGameMetaEl) selfGameMetaEl.textContent = `User: ${me}`;
  if (selfGameAchievementsEl) selfGameAchievementsEl.innerHTML = `<div class="meta">Loading achievements...</div>`;

  try {
    const data = await fetchGameAchievements(me, game.gameId);
    const items = (data?.achievements || []).map(a => ({
      id: a.id,
      title: a.title,
      description: a.description,
      badgeUrl: a.badgeUrl,
      points: a.points,
      earned: a.earned,
      earnedDate: a.earnedDate,
      earnedHardcoreDate: a.earnedHardcoreDate
    }));
    renderSelfAchievements(items);
    if (selfGamePanel) {
      selfGamePanel.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  } catch {
    if (selfGameAchievementsEl) {
      selfGameAchievementsEl.innerHTML = `<div class="meta">Failed to load achievements.</div>`;
    }
  }
}

async function openGameCompare(game, targetOverride = "") {
  const target = clampUsername(targetOverride || currentProfileUser);
  if (!target) return;

  const { me } = getUsersIncludingMe();
  if (!me) return setStatus("Set your username first.");

  compareReturnToProfile = activePageName === "profile";
  setActivePage("game");
  profilePanel.hidden = true;
  if (selfGamePanel) selfGamePanel.hidden = true;
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
  const key = JSON.stringify({
    visible: recentAchievementsVisible,
    items: items.map(a => [a.achievementId, a.date, a.username])
  });
  if (key === lastRecentAchievementsKey) return;
  lastRecentAchievementsKey = key;

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
  const frag = document.createDocumentFragment();
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
    frag.appendChild(div);
  }
  recentAchievementsEl.appendChild(frag);

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
  const key = JSON.stringify({
    visible: recentTimesVisible,
    items: items.map(t => [t.leaderboardId, t.dateUpdated, t.username, t.score])
  });
  if (key === lastRecentTimesKey) return;
  lastRecentTimesKey = key;

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
  const frag = document.createDocumentFragment();
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

    frag.appendChild(div);
  }
  recentTimesEl.appendChild(frag);

  if (recentTimesToggleBtn) {
    recentTimesToggleBtn.hidden = visible.length >= items.length;
    recentTimesToggleBtn.textContent = "Show more";
  }
}

function getUsersIncludingMe() {
  const me = currentUser;
  const users = Array.from(new Set([me, ...friends].map(clampUsername).filter(Boolean)));
  return { me, users };
}

async function loadGroupMembersCached(groupId) {
  const key = String(groupId || "");
  if (!key) return [];
  const cached = groupMembersCache.get(key);
  if (cached?.users?.length) return cached.users;
  const data = await fetchGroupMembers(key);
  const results = Array.isArray(data?.results) ? data.results : [];
  const users = results.map(r => clampUsername(r.username)).filter(Boolean);
  groupMembersCache.set(key, { users, ts: Date.now() });
  return users;
}

async function getLeaderboardUsers() {
  const me = currentUser;
  if (!me) return { me: "", users: [] };
  if (leaderboardScope === "group" && leaderboardGroupId) {
    const users = await loadGroupMembersCached(leaderboardGroupId);
    const merged = Array.from(new Set([me, ...users].map(clampUsername).filter(Boolean)));
    return { me, users: merged };
  }
  return getUsersIncludingMe();
}

async function refreshLeaderboard() {
  const ensured = ensureUsername({ prompt: false });
  if (!ensured) return;
  const { me, users } = await getLeaderboardUsers();
  if (!me) return;
  if (!users.length) {
    if (tbody) tbody.innerHTML = `<tr><td colspan="6"><div class="meta">No users to show.</div></td></tr>`;
    return;
  }

  const cached = cacheGet("leaderboard");
  if (cached?.rows?.length) {
    leaderboardBaseRows = cached.rows.map(r => ({ ...r }));
    renderLeaderboardForRange(leaderboardBaseRows, cached.me || me);
  }

  setLoading(leaderboardLoadingEl, true);

  try {
    // 1) Load monthly points first and render immediately.
    const results = await Promise.all(users.map(u => fetchMonthly(u).then(m => [u, m])));
    const map = Object.fromEntries(results);
    const myPoints = map[me]?.points ?? 0;

    // Initial rows: show placeholder in Now Playing while we fetch presence.
    const previousByUser = Object.fromEntries(
      (lastLeaderboardRows || []).map(r => [r.username, r])
    );
    const baseRows = users.map(u => {
      const prev = previousByUser[u];
      const prevNow = prev?.nowPlayingHtml || "";
      const keepNow = prevNow && !prevNow.includes("Loading");
      return {
        username: u,
        points: map[u]?.points ?? 0,
        monthlyPoints: map[u]?.points ?? 0,
        level: map[u]?.level ?? prev?.level ?? null,
        deltaVsYou: (map[u]?.points ?? 0) - myPoints,
        unlocks: map[u]?.unlockCount ?? 0,
        nowPlayingHtml: keepNow ? prevNow : "Loading...",
        dailyPoints: prev?.dailyPoints ?? null,
        weeklyPoints: prev?.weeklyPoints ?? null,
        avatarUrl: prev?.avatarUrl ?? null
      };
    });

    baseRows.sort((a, b) => (b.points - a.points) || a.username.localeCompare(b.username));
    leaderboardBaseRows = baseRows.map(r => ({ ...r }));
    renderLeaderboardForRange(leaderboardBaseRows, me);

    // 1b) Load DB-backed levels and update rows without changing rank/points.
    (async () => {
      try {
        const levelMap = await fetchUserLevels(users);
        let didChange = false;
        leaderboardBaseRows.forEach((row) => {
          const key = normalizeUserKey(row.username);
          if (!key) return;
          const next = levelMap[key];
          if (Number.isFinite(next) && row.level !== next) {
            row.level = next;
            didChange = true;
          }
        });
        if (didChange) renderLeaderboardForRange(leaderboardBaseRows, me);
      } catch {
        // ignore level fetch errors
      }
    })().catch(() => {});

    // 2) Load daily history from the DB for a quick chart render.
    (async () => {
      try {
        const history = await fetchDailyHistory(users, 7);
        setDailyHistory(history);
        const hourly = await fetchHourlyHistory(users, 24);
        setHourlyHistory(hourly);
        applyWeeklyPoints(leaderboardBaseRows);
        renderLeaderboardForRange(leaderboardBaseRows, me);
        renderLeaderboardChart(leaderboardBaseRows);
        renderHourlyChart(leaderboardBaseRows);
      } catch {
        // ignore history errors
      }
    })().catch(() => {});

    // 3) Fetch presence in the background. Retry a few times (best-effort) but never block the leaderboard.
    const win = 120; // 2 minutes
    const sleep = (ms) => new Promise(r => setTimeout(r, ms));

    async function fetchNowPlayingWithRetry(username, windowSeconds, {
      retries = 4,
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
              const row = leaderboardBaseRows.find(r => r.username === u);
              if (row) {
                row.nowPlayingHtml = `Loading... (retry ${attempt}/4)`;
                renderLeaderboardForRange(leaderboardBaseRows, me);
              }
            }
          });
          return [u, p];
        } catch {
          return [u, null];
        }
      }));
      const presence = Object.fromEntries(presencePairs);

      for (const r of leaderboardBaseRows) {
        const p = presence[r.username];
        if (p && p.title) {
          const age = (typeof p.ageSeconds === "number")
            ? (() => {
                const secs = p.ageSeconds;
                if (secs < 60) return `${secs}s ago`;
                const mins = Math.floor(secs / 60);
                if (mins < 120) return `${mins}m ago`;
                const hrs = Math.floor(mins / 60);
                if (hrs < 24) return `${hrs}h ago`;
                const days = Math.floor(hrs / 24);
                return `${days}d ago`;
              })()
            : "";
          const icon = p.imageIcon ? `<img class="nowPlayingIcon" src="${iconUrl(p.imageIcon)}" alt="game" loading="lazy" />` : "";
          const details = p.richPresence ? `<div class="nowPlayingDetail">${safeText(p.richPresence)}</div>` : "";
          const gameId = p.gameId ?? p.GameID;
          const gameTitle = p.title ?? "";
          const titleLabel = p.nowPlaying
            ? `&#9654; ${safeText(p.title)}`
            : `${safeText(p.title)}${age ? ` (${age})` : ""}`;
          const titleContent = gameId
            ? `<button class="linkBtn nowPlayingBtn" type="button" data-now-game-id="${safeText(gameId)}" data-now-game-title="${encodeURIComponent(gameTitle)}">${icon}<span>${titleLabel}</span></button>`
            : `${icon}<span>${titleLabel}</span>`;
          if (p.nowPlaying) {
            r.nowPlayingHtml = `
              <div class="nowPlayingWrap">
                <div class="nowPlaying">${titleContent}</div>
                ${details}
              </div>
            `;
          } else {
            r.nowPlayingHtml = `
              <div class="nowPlayingWrap">
                <div class="nowPlaying">${titleContent}</div>
                ${details}
              </div>
            `;
          }
        } else {
          r.nowPlayingHtml = "";
        }
      }

      renderLeaderboardForRange(leaderboardBaseRows, me);
    })().catch(() => {
      // Ignore background errors; leaderboard already rendered.
    });
    presencePromise.finally(() => setLoading(leaderboardLoadingEl, false));

    // 4) Fetch daily points and history in the background.
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
      for (const r of leaderboardBaseRows) {
        r.dailyPoints = daily[r.username];
      }
      renderLeaderboardForRange(leaderboardBaseRows, me);

      try {
        const history = await fetchDailyHistory(users, 7);
        setDailyHistory(history);
        const hourly = await fetchHourlyHistory(users, 24);
        setHourlyHistory(hourly);
        applyWeeklyPoints(leaderboardBaseRows);
        renderLeaderboardForRange(leaderboardBaseRows, me);
        renderLeaderboardChart(leaderboardBaseRows);
        renderHourlyChart(leaderboardBaseRows);
      } catch {
        // ignore history errors
      }
    })().catch(() => {});

    // 5) Fetch avatars in the background.
    (async () => {
      const avatarPairs = await Promise.all(users.map((u) => summaryLimiter(async () => {
        try {
          const data = await fetchUserSummary(u, { silent: true });
          return [u, data?.userPic || null];
        } catch {
          return [u, null];
        }
      })));
      const avatarMap = Object.fromEntries(avatarPairs);
      for (const r of leaderboardBaseRows) {
        r.avatarUrl = avatarMap[r.username];
      }
      renderLeaderboardForRange(leaderboardBaseRows, me);
    })().catch(() => {});

  } catch (e) {
    setLoading(leaderboardLoadingEl, false);
    setStatus(e?.message || "Failed to load leaderboard.");
  }
}


async function refreshRecentAchievements({ reset = true } = {}) {
  if (activeActivityTab !== "achievements") return;
  const { me, users } = getUsersIncludingMe();
  if (!me) return;
  if (recentAchievementsLoading) return;

  const cached = cacheGetWithMeta("recentAchievements");
  if (cached?.data?.items?.length) {
    renderRecentAchievements(cached.data.items);
    const ageMs = Date.now() - Number(cached.ts || 0);
    if (Number.isFinite(ageMs) && ageMs < RECENT_CACHE_TTL_MS) {
      return;
    }
  }

  if (reset) {
    achievementsLookbackHours = ACHIEVEMENTS_DEFAULT_HOURS;
    recentAchievementsVisible = RECENT_DEFAULT_ROWS;
    achievementsMaxResults = ACHIEVEMENTS_DEFAULT_MAX;
    achievementsShowMoreCount = 0;
  }

  recentAchievementsLoading = true;
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
    recentAchievementsLoading = false;
    setLoading(recentAchievementsLoadingEl, false);
  }
}

function renderProfileActivity(items) {
  if (!profileActivityListEl) return;
  profileActivityListEl.innerHTML = "";
  const list = Array.isArray(items) ? items : [];
  if (!list.length) {
    profileActivityListEl.innerHTML = `<div class="meta">No recent achievements yet.</div>`;
    return;
  }

  const frag = document.createDocumentFragment();
  list.slice(0, 12).forEach((a) => {
    const div = document.createElement("div");
    div.className = "item profileActivityItem";
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
      <strong>${a.title}</strong>
      <span class="pointsPill compact">+${a.points} pts</span>
      <span class="meta">${formatDate(a.date)}</span>
    `;

    const body = document.createElement("div");
    body.innerHTML = `
      <div class="meta">${a.gameTitle}${a.consoleName ? " - " + a.consoleName : ""}</div>
      <div class="meta">${a.description || ""}</div>
    `;

    main.appendChild(title);
    main.appendChild(body);
    div.appendChild(badge);
    div.appendChild(main);
    frag.appendChild(div);
  });
  profileActivityListEl.appendChild(frag);
}

function setProfileRecentTab(name) {
  profileRecentTab = name || "recent";
  profileRecentTabButtons.forEach((btn) => {
    const isActive = btn.dataset.profileRecentTab === profileRecentTab;
    btn.classList.toggle("active", isActive);
    btn.setAttribute("aria-selected", isActive ? "true" : "false");
  });
  if ((profileRecentTab === "completed" || profileRecentTab === "mastered") && currentProfileUser) {
    loadProfileCompletionList(currentProfileUser);
  }
  if (profileRecentTab === "backlog" && currentProfileUser) {
    loadProfileBacklog(currentProfileUser);
  }
  renderProfileRecentTab();
}

function getProfileRecentCompletionKind(game) {
  const id = String(game?.gameId ?? "");
  if (!id) return "";
  return profileCompletionByGameId.get(id) || "";
}

function filterProfileRecentByCompletion(list, mode) {
  const items = Array.isArray(list) ? list : [];
  if (!items.length) return [];
  return items.filter((game) => {
    const kind = getProfileRecentCompletionKind(game);
    const isMastered = getMasteredFlag(kind);
    if (mode === "mastered") return isMastered;
    if (mode === "completed") return getCompletedFlag(kind) && !isMastered;
    return false;
  });
}

function filterProfileCompletionList(mode) {
  const items = Array.isArray(profileCompletionList) ? profileCompletionList : [];
  if (!items.length) return [];
  return items.filter((game) => {
    const kind = game.highestAwardKind || "";
    const isMastered = getMasteredFlag(kind);
    if (mode === "mastered") return isMastered;
    if (mode === "completed") return getCompletedFlag(kind) && !isMastered;
    return false;
  });
}

function renderProfileRecentTab(queryOverride = null) {
  if (!profileRecentGamesEl) return;
  const query = queryOverride !== null
    ? String(queryOverride || "").trim().toLowerCase()
    : (profileGameSearchEl?.value || "").trim().toLowerCase();

  let baseList = [];
  let emptyMessage = "No recent games found.";

  if (profileRecentTab === "common") {
    baseList = profileCommonGames || [];
    emptyMessage = "No games in common found.";
  } else if (profileRecentTab === "completed") {
    if (profileCompletionListLoading && !profileCompletionList.length) {
      profileRecentGamesEl.innerHTML = `<div class="meta">Loading completed games...</div>`;
      return;
    }
    baseList = filterProfileCompletionList("completed");
    emptyMessage = "No completed games found.";
  } else if (profileRecentTab === "mastered") {
    if (profileCompletionListLoading && !profileCompletionList.length) {
      profileRecentGamesEl.innerHTML = `<div class="meta">Loading mastered games...</div>`;
      return;
    }
    baseList = filterProfileCompletionList("mastered");
    emptyMessage = "No mastered games found.";
  } else if (profileRecentTab === "backlog") {
    if (profileBacklogLoading && !profileBacklogItems.length) {
      profileRecentGamesEl.innerHTML = `<div class="meta">Loading backlog...</div>`;
      return;
    }
    baseList = sortBacklogItemsByProgress(profileBacklogItems, currentProfileUser);
    emptyMessage = "No backlog games found.";
  } else {
    baseList = profileRecentGames || [];
  }

  let filtered = baseList;
  if (query) {
    filtered = baseList.filter(g => (g.title || "").toLowerCase().includes(query));
    if (!filtered.length) emptyMessage = "No games match your search.";
  }

  renderProfileRecentGames(filtered, emptyMessage);
}

function renderProfileRecentGames(list, emptyMessage = "No recent games found.") {
  if (!profileRecentGamesEl) return;
  profileRecentGamesEl.innerHTML = "";
  const items = Array.isArray(list) ? list : [];
  if (!items.length) {
    profileRecentGamesEl.innerHTML = `<div class="meta">${safeText(emptyMessage)}</div>`;
    return;
  }
  const frag = document.createDocumentFragment();
  items.forEach((g) => {
    const tile = document.createElement("div");
    const allowOpen = profileIsSelf || profileAllowCompare;
    tile.className = allowOpen ? "tile clickable" : "tile";
    if (g.gameId) tile.setAttribute("data-game-id", String(g.gameId));
    if (allowOpen) {
      tile.setAttribute("role", "button");
      tile.tabIndex = 0;
    }

    const img = document.createElement("img");
    img.src = iconUrl(g.imageIcon);
    img.alt = safeText(g.title || "game");
    img.loading = "lazy";

    const title = document.createElement("div");
    title.className = "tileTitle";
    title.textContent = g.title || `Game ${safeText(g.gameId)}`;

    tile.appendChild(img);
    tile.appendChild(title);
    if (profileRecentTab === "backlog") {
      const progress = getBacklogProgressForGame(currentProfileUser, g);
      if (progress) {
        const total = Number(progress.total || 0);
        const earned = Number(progress.earned || 0);
        const pct = total > 0 ? Math.min(100, Math.max(0, Math.round((earned / total) * 100))) : 0;
        const progressLine = document.createElement("div");
        progressLine.className = "tileMeta progressLine";
        progressLine.style.setProperty("--progress", String(pct));
        const text = document.createElement("span");
        text.className = "progressLineText";
        text.textContent = `${earned} / ${total} achievements`;
        progressLine.appendChild(text);
        tile.appendChild(progressLine);
      }
    }
    frag.appendChild(tile);

    if (allowOpen) {
      const open = () => {
        if (profileIsSelf) {
          selfGameReturnToProfile = true;
          openSelfGame(g);
        } else {
          openGameCompare(g);
        }
      };
      tile.addEventListener("click", open);
      tile.addEventListener("keydown", (e) => {
        if (e.key === "Enter" || e.key === " ") { e.preventDefault(); open(); }
      });
    }
  });
  profileRecentGamesEl.appendChild(frag);
}

async function loadProfileCompletionList(username) {
  const target = clampUsername(username);
  if (!target) return;
  if (profileCompletionList.length && clampUsername(currentProfileUser) === target) return;
  if (profileCompletionListLoading) return;
  profileCompletionListLoading = true;
  const loadToken = activeProfileLoadToken;

  try {
    const perPage = 500;
    let offset = 0;
    let total = null;
    let pages = 0;
    const maxPages = 20;
    const combined = [];

    while (pages < maxPages) {
      pages += 1;
      const data = await fetchUserCompletionProgress(target, perPage, offset);
      if (loadToken !== activeProfileLoadToken || clampUsername(currentProfileUser) !== target) return;
      const results = data?.results || [];
      if (results.length) combined.push(...results);

      if (total === null) {
        const t = Number(data?.total ?? data?.Total ?? 0);
        total = Number.isFinite(t) ? t : 0;
      }

      if (results.length < perPage) break;
      offset += perPage;
      if (total && offset >= total) break;
    }

    const normalized = normalizeCompletionList(combined);
    normalized.sort((a, b) => getCompletionSortTs(b) - getCompletionSortTs(a));
    profileCompletionList = normalized;

    if (normalized.length) {
      const map = new Map();
      normalized.forEach((row) => {
        if (!row.gameId || !row.highestAwardKind) return;
        map.set(String(row.gameId), String(row.highestAwardKind));
      });
      map.forEach((kind, id) => {
        profileCompletionByGameId.set(id, kind);
      });
      refreshCompletionBadges();
    }

    renderProfileRecentTab();
  } catch {
    if (profileRecentTab === "completed" || profileRecentTab === "mastered") {
      if (profileRecentGamesEl) {
        profileRecentGamesEl.innerHTML = `<div class="meta">Failed to load completed games.</div>`;
      }
    }
  } finally {
    profileCompletionListLoading = false;
  }
}

async function loadProfileBacklog(username) {
  const target = clampUsername(username);
  if (!target) return;
  if (profileBacklogLoading) return;
  if (profileBacklogItems.length && clampUsername(currentProfileUser) === target) {
    renderProfileRecentTab();
    return;
  }

  profileBacklogLoading = true;
  try {
    const items = await fetchBacklog(target);
    profileBacklogItems = items;
    renderProfileRecentTab();

    const games = normalizeBacklogGames(items);
    await Promise.all(
      games.map((g) => backlogProgressLimiter(() => fetchBacklogProgress(target, g.gameId).catch(() => null)))
    );
    if (clampUsername(currentProfileUser) === target) {
      renderProfileRecentTab();
    }
  } catch {
    if (profileRecentTab === "backlog" && profileRecentGamesEl) {
      profileRecentGamesEl.innerHTML = `<div class="meta">Failed to load backlog.</div>`;
    }
  } finally {
    profileBacklogLoading = false;
  }
}

async function loadProfileRecentGames(username) {
  if (!profileRecentGamesEl) return;
  const target = clampUsername(username);
  if (!target) return;
  setLoading(profileRecentLoadingEl, true);
  try {
    const data = await fetchRecentGames(target, 24);
    const items = normalizeRecentGames(data?.results || []);
    profileRecentGames = items;
    renderProfileRecentTab();
    loadProfileCompletionProgress(target, items);
  } catch {
    profileRecentGamesEl.innerHTML = `<div class="meta">Failed to load recent games.</div>`;
  } finally {
    setLoading(profileRecentLoadingEl, false);
  }
}

async function refreshRecentTimes() {
  if (activeActivityTab !== "times") return;
  const { me, users } = getUsersIncludingMe();
  if (!me) return;
  if (recentTimesLoading) return;

  const cached = cacheGetWithMeta("recentTimes");
  if (cached?.data?.items?.length) {
    renderRecentTimes(cached.data.items);
    const ageMs = Date.now() - Number(cached.ts || 0);
    if (Number.isFinite(ageMs) && ageMs < RECENT_CACHE_TTL_MS) {
      return;
    }
  }

  recentTimesLoading = true;
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
    recentTimesLoading = false;
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

if (leaderboardScopeFriendsBtn) {
  leaderboardScopeFriendsBtn.addEventListener("click", () => setLeaderboardScope("friends"));
}
if (leaderboardScopeGroupBtn) {
  leaderboardScopeGroupBtn.addEventListener("click", () => setLeaderboardScope("group"));
}
if (leaderboardGroupSelect) {
  leaderboardGroupSelect.addEventListener("change", () => {
    leaderboardGroupId = leaderboardGroupSelect.value || "";
    if (leaderboardGroupId) setLeaderboardScope("group");
    else setLeaderboardScope("friends");
  });
}

if (profileShowMoreBtn) {
  profileShowMoreBtn.addEventListener("click", () => {
    if (profileExpanded) {
      collapseProfileGames();
    } else {
      loadMoreProfileGames();
    }
  });
}

if (profileBacklogBtn) {
  profileBacklogBtn.addEventListener("click", () => {
    setBacklogViewUser(currentProfileUser || currentUser);
    setActivePage("backlog");
  });
}

if (backlogRemoveBtn) {
  backlogRemoveBtn.addEventListener("click", () => {
    setBacklogRemoveMode(!backlogRemoveMode);
  });
}

if (backlogListEl) {
  backlogListEl.addEventListener("click", (e) => {
    if (!backlogRemoveMode || !isViewingOwnBacklog()) return;
    const tile = e.target.closest(".tile[data-game-id]");
    if (!tile) return;
    const gameId = tile.getAttribute("data-game-id");
    if (!gameId) return;
    removeFromBacklog(gameId);
  });
}

if (socialAddScreenshotBtn) {
  socialAddScreenshotBtn.addEventListener("click", () => {
    if (socialComposerPanel === "screenshot") {
      setSocialComposerPanel("");
      return;
    }
    socialAttachScreenshot = true;
    setSocialComposerPanel("screenshot");
  });
}
if (socialAddAchievementBtn) {
  socialAddAchievementBtn.addEventListener("click", () => {
    if (socialComposerPanel === "achievement") {
      setSocialComposerPanel("");
      return;
    }
    socialAttachAchievement = true;
    setSocialComposerPanel("achievement");
    if (socialAchievementGamesEl) socialAchievementGamesEl.hidden = false;
    ensureSocialAchievementGames();
  });
}
refreshSocialComposerLayout();

if (socialTextInput) {
  socialTextInput.addEventListener("input", updateSocialComposerState);
}

if (socialGameInput) {
  socialGameInput.addEventListener("input", async () => {
    socialGameSelected = null;
    await ensureSocialGameSuggestions();
    renderSocialGameResults(socialGameInput.value);
    updateSocialComposerState();
  });
}

if (socialGameResultsEl) {
  socialGameResultsEl.addEventListener("click", (e) => {
    const option = e.target.closest(".socialGameOption");
    if (!option) return;
    const gameId = Number(option.dataset.gameId);
    const title = option.dataset.gameTitle || "";
    if (!Number.isFinite(gameId)) return;
    socialGameSelected = { gameId, title };
    if (socialGameInput) socialGameInput.value = title;
    socialGameResultsEl.hidden = true;
    socialGameResultsEl.innerHTML = "";
    if (socialLinkGameBtn) {
      socialLinkGameBtn.textContent = "Game Added";
      socialLinkGameBtn.classList.add("added");
    }
    setSocialComposerPanel("");
    updateSocialComposerState();
  });
}

if (socialLinkGameBtn) {
  socialLinkGameBtn.addEventListener("click", () => {
    if (socialComposerPanel === "game") {
      setSocialComposerPanel("");
      return;
    }
    socialAttachGame = true;
    setSocialComposerPanel("game");
    if (socialGameInput) {
      socialGameInput.focus();
      ensureSocialGameSuggestions().then(() => {
        renderSocialGameResults(socialGameInput.value);
      });
    }
  });
}

if (socialAchievementGamesEl) {
  socialAchievementGamesEl.addEventListener("click", async (e) => {
    const tile = e.target.closest(".socialAchievementGameTile");
    if (!tile) return;
    const gameId = Number(tile.dataset.gameId);
    const title = tile.dataset.gameTitle || "";
    const icon = tile.dataset.gameIcon || "";
    if (!Number.isFinite(gameId)) return;
    setSocialAchievementSelectedGame({ gameId, title, imageIcon: icon });
    if (socialAchievementGamesEl) socialAchievementGamesEl.hidden = true;
    await loadSocialAchievementList(gameId);
    updateSocialComposerState();
  });
}

if (socialAchievementListEl) {
  socialAchievementListEl.addEventListener("click", (e) => {
    const item = e.target.closest(".socialAchievementItem");
    if (!item) return;
    const achievementId = item.dataset.achievementId;
    socialAchievementSelected = socialAchievementItems.find(a => String(a.id) === String(achievementId)) || null;
    socialAchievementListEl.querySelectorAll(".socialAchievementItem").forEach((el) => {
      el.classList.toggle("selected", el === item);
    });
    if (socialAttachAchievement && socialAchievementGame) {
      socialAttachGame = false;
      socialGameSelected = null;
      if (socialGameInput) socialGameInput.value = "";
      if (socialGameResultsEl) {
        socialGameResultsEl.hidden = true;
        socialGameResultsEl.innerHTML = "";
      }
    }
    if (socialAchievementSelected) {
      if (socialAddAchievementBtn) socialAddAchievementBtn.textContent = "Achievement Added";
      if (socialAddAchievementBtn) socialAddAchievementBtn.classList.add("added");
      if (socialAchievementGamesEl) socialAchievementGamesEl.hidden = true;
      setSocialComposerPanel("");
    }
    updateSocialComposerState();
  });
}

if (socialUploadInput) {
  socialUploadInput.addEventListener("change", async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    if (!file.type.startsWith("image/")) {
      setSocialStatus("Please choose an image file.");
      e.target.value = "";
      return;
    }
    if (file.size > SOCIAL_MAX_IMAGE_BYTES) {
      setSocialStatus("Screenshot too large. Keep it under 2MB.");
      e.target.value = "";
      return;
    }
    try {
      setSocialStatus("Loading preview...");
      const dataUrl = await readImageAsDataUrl(file);
      setSocialPreview(String(dataUrl || ""));
      if (socialAddScreenshotBtn) {
        socialAddScreenshotBtn.textContent = "Screenshot Added";
        socialAddScreenshotBtn.classList.add("added");
      }
      setSocialComposerPanel("");
    } catch (err) {
      setSocialStatus("Failed to load screenshot.");
    }
  });
}

if (socialPreviewRemoveBtn) {
  socialPreviewRemoveBtn.addEventListener("click", () => {
    if (socialUploadInput) socialUploadInput.value = "";
    setSocialPreview("");
  });
}

if (socialPostBtn) {
  socialPostBtn.addEventListener("click", async () => {
    if (!currentUser) {
      setSocialStatus("Set your username in Settings to post.");
      return;
    }
    try {
      socialPostBtn.disabled = true;
      const caption = (socialTextInput?.value || "").trim();
      const hasAnyAttachment = socialAttachScreenshot || socialAttachAchievement || socialAttachGame;
      if (!hasAnyAttachment && !caption) {
        setSocialStatus("Write something to post.");
        return;
      }
      if (socialAttachGame && !socialGameSelected) {
        setSocialStatus("Select a game from the list.");
        return;
      }
      if (socialAttachScreenshot && !socialDraftImageData) {
        setSocialStatus("Add a screenshot to post.");
        return;
      }
      if (socialAttachAchievement && (!socialAchievementGame || !socialAchievementSelected)) {
        setSocialStatus("Select a game and achievement.");
        return;
      }

      const postType = socialAttachAchievement
        ? "achievement"
        : (socialAttachScreenshot ? "screenshot" : "text");
      const game =
        (socialAttachAchievement ? socialAchievementGame?.title : socialGameSelected?.title) ||
        (socialGameInput?.value || "").trim();
      const payload = {
        postType,
        caption,
        game,
        imageData: socialAttachScreenshot ? socialDraftImageData : "",
        imageUrl: socialAttachAchievement ? iconUrl(socialAchievementSelected?.badgeUrl || "") : "",
        achievementTitle: socialAttachAchievement ? socialAchievementSelected?.title || "" : "",
        achievementDescription: socialAttachAchievement ? socialAchievementSelected?.description || "" : "",
        achievementId: socialAttachAchievement ? socialAchievementSelected?.id || "" : ""
      };

      setSocialStatus("Posting...");
      await createSocialPost(payload);
      if (socialGameInput) socialGameInput.value = "";
      if (socialTextInput) socialTextInput.value = "";
      if (socialUploadInput) socialUploadInput.value = "";
      setSocialPreview("");
      socialAchievementSelected = null;
      socialAchievementGame = null;
      socialAttachScreenshot = false;
      socialAttachAchievement = false;
      socialAttachGame = false;
    if (socialAddScreenshotBtn) {
      socialAddScreenshotBtn.classList.remove("active");
      socialAddScreenshotBtn.textContent = "Add Screenshot";
      socialAddScreenshotBtn.classList.remove("added");
    }
    if (socialAddAchievementBtn) {
      socialAddAchievementBtn.classList.remove("active");
      socialAddAchievementBtn.textContent = "Add Achievement";
      socialAddAchievementBtn.classList.remove("added");
    }
    if (socialLinkGameBtn) {
      socialLinkGameBtn.classList.remove("active");
      socialLinkGameBtn.classList.remove("added");
      socialLinkGameBtn.textContent = "Add Game";
    }
      socialGameSelected = null;
      if (socialGameResultsEl) {
        socialGameResultsEl.hidden = true;
        socialGameResultsEl.innerHTML = "";
      }
      refreshSocialComposerLayout();
      renderSocialAchievementList(socialAchievementItems);
      await loadSocialPostsFromServer({ silent: true });
      setSocialStatus("");
    } catch (err) {
      setSocialStatus("Failed to post.");
    } finally {
      updateSocialComposerState();
    }
  });
}

function bindSocialList(listEl) {
  if (!listEl) return;
  listEl.addEventListener("submit", async (e) => {
    const form = e.target.closest(".socialCommentForm");
    if (!form) return;
    e.preventDefault();
    const input = form.querySelector(".socialCommentInput");
    const text = (input?.value || "").trim();
    if (!text) return;
    if (!currentUser) {
      setSocialStatus("Set your username in Settings to comment.");
      return;
    }
    try {
      setSocialStatus("Posting comment...");
      await addSocialComment(form.dataset.postId, text);
      await loadSocialPostsFromServer({ silent: true });
      setSocialStatus("");
    } catch (err) {
      setSocialStatus("Failed to post comment.");
    }
  });
  listEl.addEventListener("click", (e) => {
    const deleteBtn = e.target.closest("button[data-delete-post-id]");
    if (deleteBtn) {
      const id = deleteBtn.getAttribute("data-delete-post-id");
      if (!id) return;
      (async () => {
        try {
          await fetchServerJson(`/api/social/posts/${encodeURIComponent(id)}`, { method: "DELETE" });
          await loadSocialPostsFromServer({ silent: true });
        } catch {
          setSocialStatus("Failed to remove post.");
        }
      })();
      return;
    }
    const reactionBtn = e.target.closest("button[data-reaction]");
    if (reactionBtn) {
      const postId = reactionBtn.getAttribute("data-post-id");
      const reaction = reactionBtn.getAttribute("data-reaction");
      if (!postId || !reaction) return;
      const card = reactionBtn.closest(".socialPost");
      const current = card?.querySelector(".reactionBtn.active")?.getAttribute("data-reaction") || "";
      const nextReaction = current === reaction ? "none" : reaction;
      (async () => {
        try {
          const data = await setSocialReaction(postId, nextReaction);
          if (!data) return;
          const likeBtn = card?.querySelector(".reactionBtn[data-reaction='like']");
          const dislikeBtn = card?.querySelector(".reactionBtn[data-reaction='dislike']");
          if (likeBtn) {
            likeBtn.classList.toggle("active", data.userReaction === "like");
            likeBtn.textContent = `Like ${data.likes ?? 0}`;
          }
          if (dislikeBtn) {
            dislikeBtn.classList.toggle("active", data.userReaction === "dislike");
            dislikeBtn.textContent = `Dislike ${data.dislikes ?? 0}`;
          }
        } catch {
          setSocialStatus("Failed to update reaction.");
        }
      })();
      return;
    }
    const img = e.target.closest(".socialPostImage");
    if (!img || !imageModal || !imageModalImg) return;
    const src = img.getAttribute("src");
    if (!src) return;
    imageModalImg.src = src;
    imageModal.hidden = false;
  });
}

bindSocialList(socialPostListEl);
bindSocialList(profileSocialListEl);

if (profileGameSearchEl) {
  profileGameSearchEl.addEventListener("input", applyProfileGameFilter);
}

if (profileRecentTabButtons.length) {
  profileRecentTabButtons.forEach((btn) => {
    btn.addEventListener("click", () => setProfileRecentTab(btn.dataset.profileRecentTab));
  });
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
if (friendsAddBtn) {
  friendsAddBtn.addEventListener("click", openAddFriendModal);
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

if (friendsListEl) {
  friendsListEl.addEventListener("click", (e) => {
    const removeBtn = e.target.closest("button[data-remove]");
    if (removeBtn) {
      const u = removeBtn.getAttribute("data-remove");
      (async () => {
        try {
          await removeFriendFromServer(u);
          friends = friends.filter(x => x !== u);
          renderChallengeFriendOptions(friends);
          renderFriendsList(friends);
          refreshLeaderboard();
          if (activeActivityTab === "times") {
            refreshRecentTimes();
          } else {
            refreshRecentAchievements();
          }
        } catch (err) {
          setStatus(String(err?.message || "Unable to remove friend."));
        }
      })();
      return;
    }
    const profileBtn = e.target.closest("button[data-profile]");
    if (profileBtn) {
      const target = profileBtn.getAttribute("data-profile");
      setActivePage("dashboard");
      openProfile(target);
    }
  });
}

if (groupCreateBtn) {
  groupCreateBtn.addEventListener("click", async () => {
    const name = (groupNameInput?.value || "").trim();
    if (!name) return;
    try {
      if (groupCreateStatusEl) groupCreateStatusEl.textContent = "Creating group...";
      await createGroup(name);
      if (groupNameInput) groupNameInput.value = "";
      if (groupCreateStatusEl) groupCreateStatusEl.textContent = "Group created.";
      await refreshGroupsPage();
    } catch (e) {
      if (groupCreateStatusEl) {
        groupCreateStatusEl.textContent = String(e?.message || "Failed to create group.");
      }
    }
  });
}

if (groupNameInput) {
  groupNameInput.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && groupCreateBtn) {
      groupCreateBtn.click();
    }
  });
}

// --- Event wiring ---
refreshBtn.addEventListener("click", () => {
  (async () => {
    refreshLeaderboard();
    if (activeActivityTab === "times") {
      refreshRecentTimes();
    } else {
      refreshRecentAchievements();
    }
    resetRefreshCountdown();
  })();
});


if (profileCloseBtn) {
  profileCloseBtn.addEventListener("click", () => {
    const shouldReturnToDashboard = profilePage && !profilePage.hidden;
    profilePanel.hidden = true;
    profileTitleNameEl.textContent = "";
    profileSummaryEl.innerHTML = "";
    profileAllowCompare = true;
    profileIsSelf = false;
    if (profileInsightsEl) profileInsightsEl.innerHTML = "";
    profileSharedGamesEl.innerHTML = "";
    profileSharedGames = [];
    profileDisplayedGames = [];
    profileAllGamesLoaded = false;
    profileGamesEmptyMessage = "No recent games found.";
    profileAutoLoadingAll = false;
    profileSkipAutoLoadOnce = false;
    profileGameAchievementCounts = new Map();
    profileGameAchievementPending = new Map();
    profileCompletionByGameId = new Map();
    profileCompletionLoading = false;
    profileCompletionTarget = "";
    if (profileLegendMeEl) {
      profileLegendMeEl.textContent = "";
    }
    if (profileLegendThemEl) {
      profileLegendThemEl.textContent = "";
    }
    if (profileShowMoreBtn) {
      profileShowMoreBtn.disabled = true;
      profileShowMoreBtn.hidden = true;
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
    if (selfGamePanel) selfGamePanel.hidden = true;
    compareTitleGameEl.textContent = "";
    compareMetaEl.textContent = "";
    compareAchievementsEl.innerHTML = "";
    compareTimesEl.innerHTML = "";
    if (selfGameTitleEl) selfGameTitleEl.textContent = "";
    if (selfGameMetaEl) selfGameMetaEl.textContent = "";
    if (selfGameAchievementsEl) selfGameAchievementsEl.innerHTML = "";
    if (profileActivityListEl) profileActivityListEl.innerHTML = "";
    if (profileRecentGamesEl) profileRecentGamesEl.innerHTML = "";
    setActiveCompareTab("achievements");
    currentProfileUser = "";
    activeProfileLoadToken = 0;
    if (shouldReturnToDashboard) {
      setActivePage("dashboard");
    }
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
    if (compareReturnToProfile) {
      compareReturnToProfile = false;
      setActivePage("profile");
      return;
    }
    setActivePage("dashboard");
  });
}

if (selfGameBackBtn) {
  selfGameBackBtn.addEventListener("click", () => {
    if (selfGamePanel) selfGamePanel.hidden = true;
    if (selfGameTitleEl) selfGameTitleEl.textContent = "";
    if (selfGameMetaEl) selfGameMetaEl.textContent = "";
    if (selfGameAchievementsEl) selfGameAchievementsEl.innerHTML = "";
    if (selfGameReturnToProfile) {
      selfGameReturnToProfile = false;
      const { me } = getUsersIncludingMe();
      if (me) {
        openProfile(me);
        return;
      }
    }
    setActivePage("dashboard");
  });
}

if (settingsBtn) {
  settingsBtn.addEventListener("click", openSettings);
}
if (notificationsBtn) {
  notificationsBtn.addEventListener("click", (e) => {
    e.stopPropagation();
    if (notificationsPanel?.hidden) {
      openNotificationsPanel();
    } else {
      closeNotificationsPanel();
    }
  });
}
if (notificationsCloseBtn) {
  notificationsCloseBtn.addEventListener("click", () => {
    closeNotificationsPanel();
  });
}
if (notificationsListEl) {
  notificationsListEl.addEventListener("click", (e) => {
    const deleteBtn = e.target.closest(".notifDeleteBtn");
    if (deleteBtn) {
      const id = deleteBtn.getAttribute("data-id");
      if (!id) return;
      (async () => {
        try {
          await deleteNotification(id);
          await loadNotifications({ markRead: false });
        } catch {
          setStatus("Failed to delete notification.");
        }
      })();
      return;
    }
    const item = e.target.closest(".notifItem");
    if (!item) return;
    const type = item.dataset.type || "";
    if (type === "challenge_pending") {
      setActivePage("challenges");
      stopChallengePolling();
      refreshChallenges({ includeTotals: true });
      challengesPollTimer = setInterval(() => refreshChallenges({ includeTotals: false }), 10000);
      challengesTotalsTimer = setInterval(() => refreshChallenges({ includeTotals: true }), 60000);
      openChallengePending();
      closeNotificationsPanel();
      return;
    }
    if (type === "friend_added") {
      const from = item.dataset.from || "";
      if (from) {
        setActivePage("dashboard");
        openProfile(from);
        closeNotificationsPanel();
      }
    }
  });
}

if (socialSidebarSuggestionsEl) {
  socialSidebarSuggestionsEl.addEventListener("click", (e) => {
    const btn = e.target.closest("button[data-add-friend]");
    if (!btn) return;
    const target = btn.getAttribute("data-add-friend");
    if (!target) return;
    (async () => {
      try {
        await addFriendToServer(target);
        friends = Array.from(new Set([...friends, target]));
        renderChallengeFriendOptions(friends);
        await loadSocialPostsFromServer({ silent: true });
      } catch {
        setStatus("Failed to add friend.");
      }
    })();
  });
}

if (socialSidebarTrendingEl && socialTrendingTooltip) {
  socialSidebarTrendingEl.addEventListener("mousemove", (e) => {
    const row = e.target.closest(".socialMiniItem");
    if (!row) return;
    const gameId = row.dataset.gameId || "";
    if (!gameId) return;
    const players = Array.from(socialTrendingPlayers.get(gameId) || []);
    if (!players.length) return;
    const list = players.slice(0, 6).map(p => `<div>${safeText(p)}</div>`).join("");
    socialTrendingTooltip.innerHTML = `
      <div class="socialTooltipTitle">Recent players</div>
      <div class="socialTooltipList">${list}</div>
    `;
    const wrap = document.querySelector(".wrap");
    if (wrap) {
      const rect = wrap.getBoundingClientRect();
      const scale = rect.width && wrap.offsetWidth ? rect.width / wrap.offsetWidth : 1;
      const x = (e.clientX - rect.left) / scale + 16;
      const y = (e.clientY - rect.top) / scale + 10;
      socialTrendingTooltip.style.left = `${x}px`;
      socialTrendingTooltip.style.top = `${y}px`;
    } else {
      socialTrendingTooltip.style.left = `${e.clientX + 6}px`;
      socialTrendingTooltip.style.top = `${e.clientY + 10}px`;
    }
    socialTrendingTooltip.hidden = false;
  });
  socialSidebarTrendingEl.addEventListener("mouseleave", () => {
    socialTrendingTooltip.hidden = true;
  });
}

if (socialFilterButtons.length) {
  socialFilterButtons.forEach((btn) => {
    btn.addEventListener("click", () => {
      socialFilter = btn.dataset.filter || "all";
      socialFilterButtons.forEach((b) => b.classList.toggle("active", b === btn));
      renderSocialPosts(socialPosts, socialPostListEl, { filter: socialFilter });
    });
  });
}
if (settingsCloseBtn) {
  settingsCloseBtn.addEventListener("click", closeSettings);
}
if (settingsCancelBtn) {
  settingsCancelBtn.addEventListener("click", closeSettings);
}
document.addEventListener("click", (e) => {
  if (!notificationsPanel || notificationsPanel.hidden) return;
  if (notificationsPanel.contains(e.target)) return;
  if (notificationsBtn && notificationsBtn.contains(e.target)) return;
  closeNotificationsPanel();
});
pageButtons.forEach(btn => {
  btn.addEventListener("click", () => {
    const page = btn.dataset.page;
    if (page === "backlog") {
      setBacklogViewUser(currentUser);
      setActivePage("backlog");
      return;
    }
    setActivePage(page);
    if (page === "challenges") {
      refreshChallenges({ includeTotals: true });
      stopChallengePolling();
      challengesPollTimer = setInterval(() => refreshChallenges({ includeTotals: false }), 10000);
      challengesTotalsTimer = setInterval(() => refreshChallenges({ includeTotals: true }), 60000);
    } else if (page === "profile") {
      stopChallengePolling();
      if (!currentUser) {
        setStatus("Set your username first.");
        return;
      }
      openProfile(currentUser, { silentScroll: true });
    } else {
      stopChallengePolling();
    }
  });
});
if (settingsSaveBtn) {
  settingsSaveBtn.addEventListener("click", () => {
    const entered = clampUsername(meInput.value);
    if (!entered) {
      setStatus("Username is required.");
      return;
    }
    if (useApiKeyToggle?.checked && !(apiKeyInput?.value || "").trim()) {
      setStatus("API key required when enabled.");
      return;
    }
    (async () => {
      await loginAndStart(entered, { closeModal: closeSettings });
    })();
  });
}

meInput.addEventListener("keydown", (e) => {
  if (e.key === "Enter") {
    if (settingsSaveBtn) settingsSaveBtn.click();
  }
});

if (friendInput) {
  friendInput.addEventListener("keydown", (e) => {
    if (e.key === "Enter") addFriendFromModal();
  });
}

if (challengeSendBtn) {
  challengeSendBtn.addEventListener("click", async () => {
    const opponent = clampUsername(challengeOpponentInput?.value || "");
    const hours = Number(challengeDurationInput?.value || 24);
    const type = String(challengeTypeSelect?.value || "points");
    if (challengeErrorEl) challengeErrorEl.textContent = "";
    if (!friends.length) {
      if (challengeErrorEl) challengeErrorEl.textContent = "Add a friend before creating a challenge.";
      return;
    }
    if (!opponent) {
      if (challengeErrorEl) challengeErrorEl.textContent = "Select a friend.";
      return;
    }
    if (type === "score") {
      if (!scoreAttackSelectedGame || !scoreAttackSelectedBoard) {
        if (challengeErrorEl) challengeErrorEl.textContent = "Select a game and leaderboard for Score Attack.";
        return;
      }
    }
    try {
      setLoading(challengesLoadingEl, true);
      await createChallenge(opponent, hours, type, scoreAttackSelectedGame, scoreAttackSelectedBoard);
      if (challengeOpponentInput) challengeOpponentInput.value = "";
      await refreshChallenges();
    } catch (e) {
      if (challengeErrorEl) challengeErrorEl.textContent = String(e?.message || "Failed to send challenge.");
    } finally {
      setLoading(challengesLoadingEl, false);
    }
  });
}

if (challengeDurationInput) {
  challengeDurationInput.addEventListener("input", () => {
    updateChallengeFormState();
  });
}

function openChallengeHistory() {
  if (!challengeHistoryModal) return;
  challengeHistoryModal.hidden = false;
  if (challengeHistoryList) challengeHistoryList.innerHTML = "";
  (async () => {
    try {
      const data = await fetchChallengeHistory();
      const items = Array.isArray(data?.results) ? data.results : [];
      renderChallengeHistory(items);
    } catch (err) {
      if (challengeHistoryList) {
        challengeHistoryList.innerHTML = `<div class="meta">Failed to load history.</div>`;
      }
    }
  })();
}

function closeChallengeHistory() {
  if (!challengeHistoryModal) return;
  challengeHistoryModal.hidden = true;
}

if (challengeHistoryBtn) {
  challengeHistoryBtn.addEventListener("click", openChallengeHistory);
}

if (challengeHistoryCloseBtn) {
  challengeHistoryCloseBtn.addEventListener("click", closeChallengeHistory);
}

if (challengeScoreSelectBtn) {
  challengeScoreSelectBtn.addEventListener("click", () => {
    const friend = clampUsername(challengeOpponentInput?.value || "");
    if (!friend) {
      if (challengeErrorEl) challengeErrorEl.textContent = "Select a friend first.";
      return;
    }
    if (!challengeScoreModal) return;
    challengeScoreModal.hidden = false;
    updateScoreAttackSelectionText();
    if (scoreAttackShowMoreBtn) {
      scoreAttackShowMoreBtn.disabled = scoreAttackLoadingAll;
      scoreAttackShowMoreBtn.textContent = scoreAttackLoadingAll ? "All recent games loaded" : "Show more";
    }
    loadScoreAttackSharedGames({ count: scoreAttackLoadingAll ? 200 : 60 });
    updateScoreAttackView(scoreAttackView);
    if (scoreAttackGamesSearch) scoreAttackGamesSearch.value = scoreAttackGameQuery;
    if (scoreAttackBoardsSearch) scoreAttackBoardsSearch.value = scoreAttackBoardQuery;
  });
}

updateChallengeFormState();

if (challengeScoreCloseBtn) {
  challengeScoreCloseBtn.addEventListener("click", () => {
    if (challengeScoreModal) challengeScoreModal.hidden = true;
  });
}


if (scoreAttackShowMoreBtn) {
  scoreAttackShowMoreBtn.addEventListener("click", async () => {
    if (scoreAttackLoadingAll) return;
    scoreAttackLoadingAll = true;
    await loadScoreAttackSharedGames({ count: 200 });
    scoreAttackShowMoreBtn.textContent = "All recent games loaded";
    scoreAttackShowMoreBtn.disabled = true;
  });
}

scoreAttackTabButtons.forEach(btn => {
  btn.addEventListener("click", () => {
    const view = btn.dataset.view || "shared";
    updateScoreAttackView(view);
  });
});

if (scoreAttackGamesSearch) {
  scoreAttackGamesSearch.addEventListener("input", () => {
    scoreAttackGameQuery = scoreAttackGamesSearch.value || "";
    renderScoreAttackGames(getScoreAttackViewList());
  });
}

if (scoreAttackBoardsSearch) {
  scoreAttackBoardsSearch.addEventListener("input", () => {
    scoreAttackBoardQuery = scoreAttackBoardsSearch.value || "";
    renderScoreAttackLeaderboards(scoreAttackSelectedGame?.title || "", scoreAttackBoards);
  });
}

if (scoreAttackBoardsList) {
  scoreAttackBoardsList.addEventListener("click", (e) => {
    const card = e.target.closest("[data-board]");
    if (!card) return;
    const id = Number(card.getAttribute("data-board"));
    if (!Number.isFinite(id)) return;
    const board = scoreAttackBoards.find(b => Number(b.id) === id);
    scoreAttackSelectedBoard = board || { id, title: "" };
    scoreAttackBoardsList.querySelectorAll(".challengeItem").forEach(el => {
      el.classList.toggle("selected", Number(el.getAttribute("data-board")) === id);
    });
    updateScoreAttackSelectionText();
    if (challengeScoreModal) challengeScoreModal.hidden = true;
  });
}

if (challengeTypeSelect) {
  challengeTypeSelect.addEventListener("change", () => {
    const isScore = challengeTypeSelect.value === "score";
    if (challengeScoreSelectBtn) challengeScoreSelectBtn.hidden = !isScore;
    if (!isScore) {
      scoreAttackSelectedGame = null;
      scoreAttackSelectedBoard = null;
      updateScoreAttackSelectionText();
      if (challengeScoreSummaryEl) challengeScoreSummaryEl.textContent = "";
    }
    updateChallengeFormState();
  });
  challengeScoreSelectBtn.hidden = challengeTypeSelect.value !== "score";
}

if (challengeOpponentInput) {
  challengeOpponentInput.addEventListener("change", () => {
    scoreAttackSelectedGame = null;
    scoreAttackSelectedBoard = null;
    scoreAttackSharedGames = [];
    scoreAttackRecentMine = [];
    scoreAttackRecentTheirs = [];
    scoreAttackBoards = [];
    scoreAttackLoadingAll = false;
    scoreAttackView = "shared";
    if (scoreAttackGamesList) scoreAttackGamesList.innerHTML = "";
    if (scoreAttackBoardsList) scoreAttackBoardsList.innerHTML = "";
    if (scoreAttackShowMoreBtn) {
      scoreAttackShowMoreBtn.disabled = false;
      scoreAttackShowMoreBtn.textContent = "Show more";
    }
    if (challengeScoreSelectBtn) {
      challengeScoreSelectBtn.disabled = !challengeOpponentInput.value;
    }
    updateScoreAttackSelectionText();
    updateScoreAttackView(scoreAttackView);
    updateChallengeFormState();
  });
}

function openChallengePending() {
  if (!challengePendingModal) return;
  challengePendingModal.hidden = false;
  refreshChallenges({ includeTotals: false });
}

function closeChallengePending() {
  if (!challengePendingModal) return;
  challengePendingModal.hidden = true;
}

if (challengePendingBtn) {
  challengePendingBtn.addEventListener("click", openChallengePending);
}

if (challengePendingCloseBtn) {
  challengePendingCloseBtn.addEventListener("click", closeChallengePending);
}

if (imageModalCloseBtn) {
  imageModalCloseBtn.addEventListener("click", closeImageModal);
}
if (imageModal) {
  imageModal.addEventListener("click", (e) => {
    if (e.target === imageModal) closeImageModal();
  });
}

if (challengeIncomingEl) {
  challengeIncomingEl.addEventListener("click", async (e) => {
    const btn = e.target.closest("button[data-accept]");
    if (!btn) return;
    const id = btn.getAttribute("data-accept");
    if (!id) return;
    try {
      setLoading(challengesLoadingEl, true);
      await acceptChallenge(id);
      await refreshChallenges();
    } catch (err) {
      if (challengeErrorEl) challengeErrorEl.textContent = String(err?.message || "Failed to accept challenge.");
    } finally {
      setLoading(challengesLoadingEl, false);
    }
  });
}

if (challengeIncomingEl) {
  challengeIncomingEl.addEventListener("click", async (e) => {
    const btn = e.target.closest("button[data-decline]");
    if (!btn) return;
    const id = btn.getAttribute("data-decline");
    if (!id) return;
    try {
      setLoading(challengesLoadingEl, true);
      await declineChallenge(id);
      await refreshChallenges();
    } catch (err) {
      if (challengeErrorEl) challengeErrorEl.textContent = String(err?.message || "Failed to decline challenge.");
    } finally {
      setLoading(challengesLoadingEl, false);
    }
  });
}

if (challengeOutgoingEl) {
  challengeOutgoingEl.addEventListener("click", async (e) => {
    const btn = e.target.closest("button[data-cancel]");
    if (!btn) return;
    const id = btn.getAttribute("data-cancel");
    if (!id) return;
    try {
      setLoading(challengesLoadingEl, true);
      await cancelChallenge(id);
      await refreshChallenges();
    } catch (err) {
      if (challengeErrorEl) challengeErrorEl.textContent = String(err?.message || "Failed to cancel challenge.");
    } finally {
      setLoading(challengesLoadingEl, false);
    }
  });
}

if (challengeActiveEl) {
  challengeActiveEl.addEventListener("click", async (e) => {
    const btn = e.target.closest("button[data-cancel]");
    if (!btn) return;
    const id = btn.getAttribute("data-cancel");
    if (!id) return;
    try {
      setLoading(challengesLoadingEl, true);
      await cancelChallenge(id);
      await refreshChallenges();
    } catch (err) {
      if (challengeErrorEl) challengeErrorEl.textContent = String(err?.message || "Failed to cancel challenge.");
    } finally {
      setLoading(challengesLoadingEl, false);
    }
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
    const wantsCustomApiKey = !!usernameModalUseApiKeyToggle?.checked;
    if (wantsCustomApiKey && !apiKey) {
      if (usernameModalErrorEl) usernameModalErrorEl.textContent = "API key required when enabled.";
      return;
    }
    if (apiKeyInput) apiKeyInput.value = apiKey;
    if (useApiKeyToggle) useApiKeyToggle.checked = wantsCustomApiKey && !!apiKey;
    saveState();
    (async () => {
      await loginAndStart(entered, {
        errorEl: usernameModalErrorEl,
        loadingEl: usernameModalLoadingEl,
        closeModal: () => {
          if (usernameModal) usernameModal.hidden = true;
        }
      });
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
(async () => {
  const me = await fetchAuthMeWithRetry(3);
  authResolved = true;
  if (me) {
    setCurrentUser(me);
    fetchUserSummary(me).catch(() => {});
    friends = await loadFriendsFromServer();
    await bootstrapAfterLogin();
  } else {
    ensureUsername();
  }
})();




