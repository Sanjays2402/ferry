/* Ferry dashboard client: websocket stats + polled tables + throughput chart. */
(function () {
  "use strict";

  const $ = (id) => document.getElementById(id);
  const dot = $("conn-dot"), connText = $("conn-text");

  function setConn(on, text) {
    dot.className = "dot " + (on ? "on" : "off");
    connText.textContent = text;
  }

  /* ---------- toasts ---------- */
  function toast(msg, kind) {
    const el = document.createElement("div");
    el.className = "toast" + (kind ? " " + kind : "");
    el.textContent = msg;
    $("toasts").appendChild(el);
    setTimeout(() => el.classList.add("show"), 10);
    setTimeout(() => { el.classList.remove("show"); setTimeout(() => el.remove(), 300); }, 3200);
  }

  /* ---------- live stats via websocket ---------- */
  let ws = null;
  function connect() {
    const proto = location.protocol === "https:" ? "wss:" : "ws:";
    ws = new WebSocket(proto + "//" + location.host + "/ws");
    ws.onopen = () => setConn(true, "live");
    ws.onclose = () => { setConn(false, "reconnecting…"); setTimeout(connect, 2000); };
    ws.onerror = () => ws.close();
    ws.onmessage = (ev) => {
      const msg = JSON.parse(ev.data);
      if (msg.type === "stats") renderStats(msg.data);
    };
  }

  function renderStats(s) {
    const t = s.tasks;
    $("c-queued").textContent = t.queued;
    $("c-active").textContent = t.claimed + t.running;
    $("c-done").textContent = t.done;
    $("c-dead").textContent = t.dead + t.failed;
    $("c-workers").textContent = s.workers.length;
    renderQueues(s.queues, s.paused || []);
    renderWorkers(s.workers);
  }

  function renderQueues(queues, paused) {
    const tb = document.querySelector("#queues-table tbody");
    tb.innerHTML = queues.length ? "" : '<tr><td colspan="6" class="empty">no queues yet — enqueue a task to begin</td></tr>';
    for (const q of queues) {
      const isPaused = paused.includes(q.queue);
      const tr = document.createElement("tr");
      tr.innerHTML =
        `<td class="mono">${esc(q.queue)}</td>` +
        `<td>${isPaused ? '<span class="pill paused">paused</span>' : '<span class="pill active">active</span>'}</td>` +
        `<td>${q.queued}</td><td>${q.active}</td><td>${q.dead}</td>` +
        `<td class="actions">` +
        (isPaused
          ? `<button class="btn small" data-resume="${esc(q.queue)}">resume</button>`
          : `<button class="btn small" data-pause="${esc(q.queue)}">pause</button>`) +
        `<button class="btn small danger" data-purge="${esc(q.queue)}">purge queued</button></td>`;
      tb.appendChild(tr);
    }
    tb.querySelectorAll("[data-purge]").forEach((b) =>
      b.addEventListener("click", async () => {
        if (!confirm(`Purge all queued tasks in "${b.dataset.purge}"?`)) return;
        const r = await (await fetch("/api/tasks/purge?queue=" + encodeURIComponent(b.dataset.purge), { method: "POST" })).json();
        toast(`purged ${r.purged} task(s)`);
        refreshTables();
      })
    );
    tb.querySelectorAll("[data-pause]").forEach((b) =>
      b.addEventListener("click", async () => {
        await fetch("/api/queues/" + encodeURIComponent(b.dataset.pause) + "/pause", { method: "POST" });
        toast(`queue "${b.dataset.pause}" paused — workers will skip it`);
      })
    );
    tb.querySelectorAll("[data-resume]").forEach((b) =>
      b.addEventListener("click", async () => {
        await fetch("/api/queues/" + encodeURIComponent(b.dataset.resume) + "/resume", { method: "POST" });
        toast(`queue "${b.dataset.resume}" resumed`);
      })
    );
  }

  function renderWorkers(workers) {
    const tb = document.querySelector("#workers-table tbody");
    tb.innerHTML = workers.length ? "" : '<tr><td colspan="4" class="empty">no workers online</td></tr>';
    for (const w of workers) {
      const tr = document.createElement("tr");
      tr.innerHTML =
        `<td class="mono">${esc(w.worker_id)}</td><td class="mono">${esc(w.queues)}</td>` +
        `<td>${w.concurrency}</td><td>${relTime(w.last_beat)}</td>`;
      tb.appendChild(tr);
    }
  }

  /* ---------- task table (polled) ---------- */
  const selected = new Set();

  async function refreshTables() {
    const status = $("f-status").value, queue = $("f-queue").value.trim();
    const qs = new URLSearchParams({ limit: "100" });
    if (status) qs.set("status", status);
    if (queue) qs.set("queue", queue);
    const tasks = await (await fetch("/api/tasks?" + qs)).json();
    const tb = document.querySelector("#tasks-table tbody");
    tb.innerHTML = tasks.length ? "" : '<tr><td colspan="8" class="empty">no tasks match</td></tr>';
    for (const t of tasks) {
      const tr = document.createElement("tr");
      tr.dataset.taskId = t.id;
      tr.classList.add("clickable");
      const retryBtn = (t.status === "failed" || t.status === "dead")
        ? `<button class="btn small" data-retry="${t.id}">retry</button>` : "";
      tr.innerHTML =
        `<td><input type="checkbox" class="sel" ${selected.has(t.id) ? "checked" : ""}></td>` +
        `<td class="mono" title="${esc(t.id)}">${esc(t.task_name)}<br><span class="muted">${esc(t.id.slice(0, 8))}</span></td>` +
        `<td class="mono">${esc(t.queue)}</td>` +
        `<td><span class="status ${t.status}">${t.status}</span></td>` +
        `<td>${t.attempts}/${t.max_retries}</td>` +
        `<td class="mono">${esc((t.worker_id || "—").slice(0, 18))}</td>` +
        `<td>${t.finished_at ? relTime(t.finished_at) : "—"}</td>` +
        `<td>${retryBtn}</td>`;
      tb.appendChild(tr);
    }
    tb.querySelectorAll("tr[data-task-id]").forEach((tr) => {
      tr.addEventListener("click", (ev) => {
        if (ev.target.closest("button, input, a")) return;
        openDrawer(tr.dataset.taskId);
      });
    });
    tb.querySelectorAll("input.sel").forEach((cb) =>
      cb.addEventListener("change", () => {
        const id = cb.closest("tr").dataset.taskId;
        if (cb.checked) selected.add(id); else selected.delete(id);
        updateBulkBar();
      })
    );
    tb.querySelectorAll("[data-retry]").forEach((b) =>
      b.addEventListener("click", async (ev) => {
        ev.stopPropagation();
        const r = await (await fetch(`/api/tasks/${b.dataset.retry}/retry`, { method: "POST" })).json();
        toast(r.retried ? "task requeued" : "task no longer retryable", r.retried ? "" : "warn");
        refreshTables();
      })
    );
    $("sel-all").checked = tasks.length > 0 && tasks.every((t) => selected.has(t.id));
    updateBulkBar();
    drawChart();
  }

  function updateBulkBar() {
    $("sel-count").textContent = selected.size;
    $("f-retry-selected").classList.toggle("hidden", selected.size === 0);
  }

  $("sel-all").addEventListener("change", (ev) => {
    document.querySelectorAll("#tasks-table tbody tr[data-task-id]").forEach((tr) => {
      const id = tr.dataset.taskId;
      if (ev.target.checked) selected.add(id); else selected.delete(id);
      tr.querySelector("input.sel").checked = ev.target.checked;
    });
    updateBulkBar();
  });

  $("f-retry-selected").addEventListener("click", async () => {
    let n = 0;
    for (const id of [...selected]) {
      const r = await (await fetch(`/api/tasks/${id}/retry`, { method: "POST" })).json();
      if (r.retried) { n++; selected.delete(id); }
    }
    toast(n ? `requeued ${n} task(s)` : "nothing was retryable", n ? "" : "warn");
    refreshTables();
  });

  $("f-retry-dead").addEventListener("click", async () => {
    const r = await (await fetch("/api/tasks/retry-dead", { method: "POST" })).json();
    toast(r.retried ? `requeued ${r.retried} dead task(s)` : "no dead tasks to retry", r.retried ? "" : "warn");
    refreshTables();
  });

  /* ---------- task detail drawer ---------- */
  async function openDrawer(taskId) {
    const t = await (await fetch(`/api/tasks/${taskId}`)).json();
    $("d-title").textContent = t.task_name;
    const timeline = [
      ["enqueued", t.created_at],
      ["claimed", t.claimed_at],
      ["finished", t.finished_at],
    ].filter(([, v]) => v).map(([k, v]) => `<div class="tl-row"><span class="tl-dot"></span><span>${k}</span><span class="muted">${relTime(v)}</span></div>`).join("");
    const kv = (k, v) => `<div class="kv"><span class="muted">${k}</span><span class="mono">${esc(v)}</span></div>`;
    let html =
      `<div class="d-meta">` +
      kv("id", t.id) +
      `<div class="kv"><span class="muted">status</span><span><span class="status ${t.status}">${t.status}</span></span></div>` +
      kv("queue", t.queue) +
      kv("priority", t.priority) +
      kv("attempts", `${t.attempts} / ${t.max_retries}`) +
      kv("worker", t.worker_id || "—") +
      `</div>` +
      (timeline ? `<h3>Timeline</h3><div class="tl">${timeline}</div>` : "") +
      `<h3>Arguments</h3><pre class="code">${esc(JSON.stringify({ args: t.args_decoded, kwargs: t.kwargs_decoded }, null, 2))}</pre>`;
    if (t.result_decoded !== null && t.result_decoded !== undefined)
      html += `<h3>Result</h3><pre class="code">${esc(JSON.stringify(t.result_decoded, null, 2))}</pre>`;
    if (t.result_expired)
      html += `<p class="muted">result payload expired (result TTL)</p>`;
    if (t.error)
      html += `<h3>Error</h3><pre class="code error">${esc(t.error)}</pre>`;
    $("d-body").innerHTML = html;
    $("drawer").classList.add("open");
    $("drawer").setAttribute("aria-hidden", "false");
    $("drawer-scrim").classList.remove("hidden");
  }

  function closeDrawer() {
    $("drawer").classList.remove("open");
    $("drawer").setAttribute("aria-hidden", "true");
    $("drawer-scrim").classList.add("hidden");
  }
  $("d-close").addEventListener("click", closeDrawer);
  $("drawer-scrim").addEventListener("click", closeDrawer);
  document.addEventListener("keydown", (ev) => { if (ev.key === "Escape") closeDrawer(); });

  /* ---------- throughput chart ---------- */
  async function drawChart() {
    const data = await (await fetch("/api/throughput?minutes=60")).json();
    const canvas = $("chart"), ctx = canvas.getContext("2d");
    const w = (canvas.width = canvas.offsetWidth * 2);
    const h = (canvas.height = 240);
    ctx.clearRect(0, 0, w, h);
    if (!data.length) {
      ctx.fillStyle = "#8b96ad"; ctx.font = "24px sans-serif";
      ctx.fillText("no finished tasks in the last hour", 20, h / 2);
      return;
    }
    const byMinute = {};
    for (const d of data) byMinute[d.minute] = (byMinute[d.minute] || 0) + d.n;
    const minutes = Object.keys(byMinute).sort();
    const max = Math.max(...Object.values(byMinute), 1);
    const bw = w / 60;
    minutes.forEach((m, i) => {
      const v = byMinute[m], bh = (v / max) * (h - 30);
      ctx.fillStyle = "#e8b04b";
      const x = w - (minutes.length - i) * bw + 1;
      ctx.fillRect(x, h - 10 - bh, bw - 2, bh);
    });
    ctx.fillStyle = "#8b96ad"; ctx.font = "20px sans-serif";
    ctx.fillText(`peak ${max}/min`, 12, 24);
  }

  /* ---------- helpers ---------- */
  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  }
  function relTime(iso) {
    const s = Math.max(0, Math.round((Date.now() - new Date(iso).getTime()) / 1000));
    if (s < 5) return "just now";
    if (s < 60) return s + "s ago";
    if (s < 3600) return Math.floor(s / 60) + "m ago";
    return Math.floor(s / 3600) + "h ago";
  }

  $("f-refresh").addEventListener("click", refreshTables);
  $("f-status").addEventListener("change", refreshTables);
  $("f-queue").addEventListener("input", refreshTables);

  connect();
  refreshTables();
  setInterval(refreshTables, 5000);
})();
