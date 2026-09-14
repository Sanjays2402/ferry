/* Ferry dashboard client: websocket stats + polled tables + throughput chart. */
(function () {
  "use strict";

  const $ = (id) => document.getElementById(id);
  const dot = $("conn-dot"), connText = $("conn-text");

  function setConn(on, text) {
    dot.className = "dot " + (on ? "on" : "off");
    connText.textContent = text;
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
    renderQueues(s.queues);
    renderWorkers(s.workers);
  }

  function renderQueues(queues) {
    const tb = document.querySelector("#queues-table tbody");
    tb.innerHTML = queues.length ? "" : '<tr><td colspan="5" class="empty">no queues yet — enqueue a task to begin</td></tr>';
    for (const q of queues) {
      const tr = document.createElement("tr");
      tr.innerHTML =
        `<td class="mono">${esc(q.queue)}</td><td>${q.queued}</td><td>${q.active}</td>` +
        `<td>${q.dead}</td>` +
        `<td><button class="btn small danger" data-purge="${esc(q.queue)}">purge queued</button></td>`;
      tb.appendChild(tr);
    }
    tb.querySelectorAll("[data-purge]").forEach((b) =>
      b.addEventListener("click", async () => {
        if (!confirm(`Purge all queued tasks in "${b.dataset.purge}"?`)) return;
        await fetch("/api/tasks/purge?queue=" + encodeURIComponent(b.dataset.purge), { method: "POST" });
        refreshTables();
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
  async function refreshTables() {
    const status = $("f-status").value, queue = $("f-queue").value.trim();
    const qs = new URLSearchParams({ limit: "100" });
    if (status) qs.set("status", status);
    if (queue) qs.set("queue", queue);
    const tasks = await (await fetch("/api/tasks?" + qs)).json();
    const tb = document.querySelector("#tasks-table tbody");
    tb.innerHTML = tasks.length ? "" : '<tr><td colspan="7" class="empty">no tasks match</td></tr>';
    for (const t of tasks) {
      const tr = document.createElement("tr");
      const retryBtn = (t.status === "failed" || t.status === "dead")
        ? `<button class="btn small" data-retry="${t.id}">retry</button>` : "";
      tr.innerHTML =
        `<td class="mono" title="${esc(t.id)}">${esc(t.task_name)}<br><span class="muted">${esc(t.id.slice(0, 8))}</span></td>` +
        `<td class="mono">${esc(t.queue)}</td>` +
        `<td><span class="status ${t.status}">${t.status}</span></td>` +
        `<td>${t.attempts}/${t.max_retries}</td>` +
        `<td class="mono">${esc((t.worker_id || "—").slice(0, 18))}</td>` +
        `<td>${t.finished_at ? relTime(t.finished_at) : "—"}</td>` +
        `<td>${retryBtn}</td>`;
      tb.appendChild(tr);
    }
    tb.querySelectorAll("[data-retry]").forEach((b) =>
      b.addEventListener("click", async () => {
        await fetch(`/api/tasks/${b.dataset.retry}/retry`, { method: "POST" });
        refreshTables();
      })
    );
    drawChart();
  }

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
