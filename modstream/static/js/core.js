// Shared UI helpers. All user content is inserted via textContent (never
// innerHTML), so posted text can't inject markup or scripts.

export const VERDICT = {
  safe: { label: "Safe", title: "Looks safe", sub: "No harmful signals detected", icon: "check" },
  review: { label: "Needs review", title: "Needs human review", sub: "Ambiguous signals — context matters", icon: "alert" },
  flagged: { label: "Flagged", title: "Harmful content detected", sub: "This would be hidden or blocked", icon: "alert" },
};

const LABELS = {
  toxicity: "Toxicity", severe_toxicity: "Severe toxicity", obscene: "Obscene", insult: "Insult",
  identity_attack: "Identity attack", identity_hate: "Identity hate", threat: "Threat",
  sexual_explicit: "Sexually explicit",
};

// --- DOM ---------------------------------------------------------------------

export function h(tag, props = {}, ...children) {
  const el = document.createElement(tag);
  for (const [key, value] of Object.entries(props ?? {})) {
    if (value == null || value === false) continue;
    if (key === "class") el.className = value;
    else if (key === "style" && typeof value === "object") Object.assign(el.style, value);
    else if (key.startsWith("on")) el.addEventListener(key.slice(2), value);
    else el.setAttribute(key, value === true ? "" : value);
  }
  for (const child of children.flat()) {
    if (child == null || child === false) continue;
    el.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
  return el;
}

export function icon(name, cls = "") {
  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("class", `icon ${cls}`.trim());
  svg.setAttribute("aria-hidden", "true");
  const use = document.createElementNS("http://www.w3.org/2000/svg", "use");
  use.setAttribute("href", `#i-${name}`);
  svg.append(use);
  return svg;
}

export const hue = (name) => [...name].reduce((sum, ch) => sum + ch.codePointAt(0), 0) % 360;

export function avatar(name, cls = "") {
  return h("span", { class: `avatar ${cls}`, style: `--hue:${hue(name)}`, "aria-hidden": "true" }, name.slice(0, 1).toUpperCase());
}

export function badge(verdict) {
  const v = VERDICT[verdict];
  return h("span", { class: `badge badge-${verdict}` }, icon(v.icon), v.label);
}

export const pct = (x) => `${Math.round(x * 100)}%`;

// --- API ---------------------------------------------------------------------

export class ApiError extends Error {}

export async function api(path, { method = "GET", body } = {}) {
  const res = await fetch(`/api/v1${path}`, {
    method,
    headers: body ? { "Content-Type": "application/json" } : {},
    body: body ? JSON.stringify(body) : undefined,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new ApiError(data?.error?.message ?? `Request failed (${res.status})`);
  return data;
}

export const analyze = (text, source = "api", record = true) =>
  api("/analyze", { method: "POST", body: { text, source, record } });

// --- Result rendering ----------------------------------------------------------

export function highlight(text, matches = []) {
  const frag = document.createDocumentFragment();
  let cursor = 0;
  for (const m of matches) {
    if (m.start < cursor) continue;
    frag.append(text.slice(cursor, m.start));
    const why = m.directed ? "aimed at a person" : m.tier === "mild" ? "not aimed at anyone" : m.tier;
    frag.append(h("mark", { class: m.tier, title: `${m.category} · ${why}` }, text.slice(m.start, m.end)));
    cursor = m.end;
  }
  frag.append(text.slice(cursor));
  return frag;
}

export function renderResult(result, text) {
  const v = VERDICT[result.verdict];
  const scores = Object.entries(result.model_scores ?? {}).sort((a, b) => b[1] - a[1]);
  const tone = (x) => (x >= 0.7 ? "flagged" : x >= 0.4 ? "review" : "safe");

  return h("div", { class: "result" },
    h("div", { class: `verdict ${result.verdict}` },
      icon(v.icon, "icon-lg"),
      h("div", {}, h("div", { class: "verdict-title" }, v.title), h("div", { class: "verdict-sub" }, v.sub)),
      h("div", { class: "verdict-score" }, pct(result.risk), h("small", {}, "risk score")),
    ),
    result.categories.length > 0 && h("div", { class: "row", style: "--gap:6px" },
      result.categories.map((c) => h("span", { class: "badge badge-outline" }, c)),
    ),
    h("div", {}, h("h4", {}, "Highlighted text"), h("div", { class: "quote" }, highlight(text, result.matches))),
    result.reasons.length > 0 && h("div", {}, h("h4", {}, "Why"), h("ul", { class: "reasons" }, result.reasons.map((r) => h("li", {}, r)))),
    scores.length > 0 && h("div", {},
      h("h4", {}, "Model scores"),
      h("div", { class: "score-list" }, scores.map(([label, value]) =>
        h("div", { class: "score-row" },
          h("span", {}, LABELS[label] ?? label),
          meter(value, tone(value)),
          h("span", { class: "num" }, pct(value)),
        ))),
    ),
    h("div", { class: "result-meta" },
      h("span", {}, icon("cpu", "icon-sm"), result.engine),
      h("span", {}, icon("zap", "icon-sm"), `${result.latency_ms} ms`),
      h("span", {}, icon("lock", "icon-sm"), "Text not stored"),
    ),
  );
}

export function meter(value, tone = "") {
  const fill = h("span");
  requestAnimationFrame(() => requestAnimationFrame(() => { fill.style.width = pct(value); }));
  return h("div", { class: `meter ${tone}` }, fill);
}

// --- Pre-send nudge -------------------------------------------------------------

/** Show the "are you sure?" dialog. Resolves to "edit" or "send". */
export function nudge(result, text) {
  const dialog = document.getElementById("nudge");
  const flagged = result.verdict === "flagged";
  dialog.querySelector("[data-nudge-icon]").classList.toggle("flagged", flagged);
  dialog.querySelector("[data-nudge-title]").textContent = flagged
    ? "This may be hurtful to others"
    : "Take a second look?";
  dialog.querySelector("[data-nudge-text]").textContent = flagged
    ? "Our safety check flagged this message. If you send it, it will be hidden behind a content warning."
    : "Some words here are often used to hurt people. Make sure your meaning is clear.";
  dialog.querySelector("[data-nudge-quote]").replaceChildren(highlight(text, result.matches));
  dialog.returnValue = "";
  dialog.showModal();
  return new Promise((resolve) =>
    dialog.addEventListener("close", () => resolve(dialog.returnValue || "edit"), { once: true }));
}

/**
 * Intercept a form submit, run the safety check first, and only submit once
 * the text is safe or the user explicitly chooses to send anyway.
 */
export function guardForm(form, field, source = "composer") {
  form.addEventListener("submit", async (event) => {
    if (form.dataset.checked === "1") return;
    event.preventDefault();
    const text = field.value.trim();
    if (!text) return field.focus();
    const button = form.querySelector("[type=submit]");
    button?.classList.add("is-loading");
    try {
      const result = await analyze(text, source, false);
      if (result.verdict === "safe" || (await nudge(result, text)) === "send") {
        form.dataset.checked = "1";
        form.requestSubmit();
        return;
      }
      field.focus();
    } catch {
      form.dataset.checked = "1"; // the server still checks everything on submit
      form.requestSubmit();
    } finally {
      button?.classList.remove("is-loading");
    }
  });
}

// --- Relative time ----------------------------------------------------------------

const rtf = new Intl.RelativeTimeFormat(undefined, { numeric: "auto", style: "short" });

export function timeAgo(iso) {
  const secs = (new Date(iso) - Date.now()) / 1000;
  const units = [["year", 31536000], ["month", 2592000], ["day", 86400], ["hour", 3600], ["minute", 60]];
  for (const [unit, size] of units) if (Math.abs(secs) >= size) return rtf.format(Math.round(secs / size), unit);
  return "just now";
}

export function refreshTimes(root = document) {
  root.querySelectorAll("time[data-ago]").forEach((el) => {
    el.textContent = timeAgo(el.dateTime);
    el.title = new Date(el.dateTime).toLocaleString();
  });
}

// --- Global chrome -------------------------------------------------------------

function setupTheme() {
  const button = document.querySelector("[data-theme-toggle]");
  const root = document.documentElement;
  const isDark = () => root.dataset.theme
    ? root.dataset.theme === "dark"
    : matchMedia("(prefers-color-scheme: dark)").matches;
  const sync = () => button.querySelector("use").setAttribute("href", isDark() ? "#i-sun" : "#i-moon");
  button.addEventListener("click", () => {
    root.dataset.theme = isDark() ? "light" : "dark";
    try { localStorage.setItem("theme", root.dataset.theme); } catch {}
    sync();
  });
  sync();
}

function setupNav() {
  const toggle = document.querySelector("[data-nav-toggle]");
  const setOpen = (open) => {
    document.body.classList.toggle("nav-open", open);
    toggle.setAttribute("aria-expanded", String(open));
    toggle.querySelector("use").setAttribute("href", open ? "#i-x" : "#i-menu");
  };
  toggle.addEventListener("click", () => setOpen(!document.body.classList.contains("nav-open")));
  document.addEventListener("keydown", (e) => e.key === "Escape" && setOpen(false));
  document.querySelectorAll("#site-nav a").forEach((a) => a.addEventListener("click", () => setOpen(false)));
}

setupTheme();
setupNav();
refreshTimes();
setInterval(refreshTimes, 30_000);
