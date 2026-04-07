export async function fetchJson(url, options = {}) {
  const res = await fetch(url, {
    credentials: 'include',
    headers: {
      'Content-Type': 'application/json',
      ...(options.headers || {}),
    },
    ...options,
  });

  if (!res.ok) {
    let message = `Request failed: ${res.status}`;
    try {
      const data = await res.json();
      if (data?.error) message = data.error;
    } catch {
      // ignore
    }
    throw new Error(message);
  }

  return res.json();
}

export function getMe() {
  return fetchJson('/api/auth/me');
}

export function getFriends() {
  return fetchJson('/api/friends');
}

export function getMonthly(username) {
  return fetchJson(`/api/monthly/${encodeURIComponent(username)}`);
}

export function getDailyHistory(usernames, days = 7) {
  const users = usernames.map(encodeURIComponent).join(',');
  return fetchJson(`/api/daily-history?users=${users}&days=${days}`);
}