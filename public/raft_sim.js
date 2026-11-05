// A small, faithful-enough Raft simulator rendered with SVG.
// Leader election + log replication + commit, with crashes, partitions and packet loss.

const ROLE_COLOR = {
  follower: "#60a5fa",
  candidate: "#f59e0b",
  leader: "#10b981",
  dead: "#475569",
};
const TERM_COLORS = ["#38bdf8", "#a78bfa", "#f472b6", "#fb923c", "#facc15", "#4ade80", "#2dd4bf", "#818cf8"];
const termColor = (t) => (t <= 0 ? "#334155" : TERM_COLORS[(t - 1) % TERM_COLORS.length]);

const ELECTION_MIN = 600;
const ELECTION_MAX = 1200;
const HEARTBEAT = 220;
const DELAY_MIN = 90;
const DELAY_MAX = 170;
const MAX_ENTRIES_PER_AE = 4;

const SVGNS = "http://www.w3.org/2000/svg";
const rand = (a, b) => a + Math.random() * (b - a);
const h = (tag, attrs = {}, parent) => {
  const e = document.createElementNS(SVGNS, tag);
  for (const k in attrs) e.setAttribute(k, attrs[k]);
  if (parent) parent.appendChild(e);
  return e;
};

function render({ model, el }) {
  // ---------- DOM skeleton ----------
  const root = document.createElement("div");
  root.className = "rs-root";
  root.innerHTML = `
    <div class="rs-toolbar">
      <button class="rs-btn rs-play">⏸ Pause</button>
      <button class="rs-btn rs-step" disabled>⏭ Step</button>
      <label class="rs-ctl">speed <input class="rs-speed" type="range" min="0.05" max="1.5" step="0.05" value="0.5"><span class="rs-speed-v">0.5×</span></label>
      <label class="rs-ctl">loss <input class="rs-loss" type="range" min="0" max="0.6" step="0.05" value="0"><span class="rs-loss-v">0%</span></label>
      <span class="rs-spacer"></span>
      <button class="rs-btn rs-accent rs-req">✉ Client request</button>
      <button class="rs-btn rs-part">✂ Partition leader</button>
      <button class="rs-btn rs-reset">↺ Reset</button>
    </div>
    <div class="rs-main">
      <div class="rs-stage"></div>
      <div class="rs-side">
        <div class="rs-panel-title">Replicated logs <span class="rs-dim">(solid = committed)</span></div>
        <div class="rs-logs"></div>
        <div class="rs-panel-title">Events</div>
        <div class="rs-feed"></div>
      </div>
    </div>
    <div class="rs-legend">
      <span><i style="background:${ROLE_COLOR.follower}"></i>follower</span>
      <span><i style="background:${ROLE_COLOR.candidate}"></i>candidate</span>
      <span><i style="background:${ROLE_COLOR.leader}"></i>leader</span>
      <span><i style="background:${ROLE_COLOR.dead}"></i>crashed</span>
      <span class="rs-dim">ring = election timer · click a server to crash / restart it</span>
    </div>`;
  el.appendChild(root);

  const $ = (s) => root.querySelector(s);
  const stage = $(".rs-stage");
  const W = 460, H = 420, CX = W / 2, CY = H / 2 + 6, R = 150, NR = 34;
  const svg = h("svg", { viewBox: `0 0 ${W} ${H}`, class: "rs-svg" }, stage);
  const defs = h("defs", {}, svg);
  defs.innerHTML = `
    <radialGradient id="rsglow" cx="50%" cy="50%" r="50%">
      <stop offset="0%" stop-color="#1e293b" stop-opacity="0.9"/>
      <stop offset="100%" stop-color="#0b1020" stop-opacity="0"/>
    </radialGradient>
    <filter id="rsblur" x="-50%" y="-50%" width="200%" height="200%"><feGaussianBlur stdDeviation="6"/></filter>`;
  h("circle", { cx: CX, cy: CY, r: R + 70, fill: "url(#rsglow)" }, svg);
  const gLinks = h("g", {}, svg);
  const gPart = h("g", {}, svg);
  const gMsgs = h("g", {}, svg);
  const gNodes = h("g", {}, svg);
  const gBursts = h("g", {}, svg);

  // ---------- simulation state ----------
  let N, nodes, msgs, bursts, now, events, partitioned, cmdCounter, logsDirty;
  let playing = true;
  let speed = 0.5;
  let loss = 0;

  const pos = (i) => {
    const a = -Math.PI / 2 + (2 * Math.PI * i) / N;
    return [CX + R * Math.cos(a), CY + R * Math.sin(a)];
  };
  const majority = () => Math.floor(N / 2) + 1;
  const lastIndex = (nd) => nd.log.length;
  const lastTerm = (nd) => (nd.log.length ? nd.log[nd.log.length - 1].term : 0);
  const termAt = (nd, idx) => (idx <= 0 ? 0 : nd.log[idx - 1] ? nd.log[idx - 1].term : -1);

  function log(text, kind = "") {
    events.unshift({ t: now, text, kind });
    events.length = Math.min(events.length, 9);
    renderFeed();
  }

  function resetTimer(nd) {
    nd.timeoutLen = rand(ELECTION_MIN, ELECTION_MAX);
    nd.deadline = now + nd.timeoutLen;
  }

  function reset() {
    N = Math.max(3, Math.min(9, model.get("n") | 0 || 5));
    now = 0;
    msgs = [];
    bursts = [];
    events = [];
    partitioned = new Set();
    cmdCounter = 0;
    nodes = [];
    for (let i = 0; i < N; i++) {
      const nd = {
        id: i, alive: true, state: "follower", term: 0, votedFor: null, log: [],
        commitIndex: 0, votes: new Set(), nextIndex: [], matchIndex: [], hbDue: 0, leaderId: null,
      };
      resetTimer(nd);
      nodes.push(nd);
    }
    buildStatic();
    logsDirty = true;
    log(`Cluster of ${N} servers booted. Everyone is a follower in term 0.`);
    syncPython();
  }

  // ---------- messaging ----------
  function send(from, to, type, body) {
    if (!nodes[from].alive) return;
    const cut = partitioned.has(from) !== partitioned.has(to);
    const lost = !cut && Math.random() < loss;
    msgs.push({ from, to, type, body, t0: now, t1: now + rand(DELAY_MIN, DELAY_MAX), drop: cut || lost });
  }

  function stepDownIfStale(nd, term) {
    if (term > nd.term) {
      if (nd.state !== "follower") log(`S${nd.id + 1} saw term ${term} > ${nd.term} → steps down to follower`, "warn");
      nd.term = term;
      nd.votedFor = null;
      nd.state = "follower";
      return true;
    }
    return false;
  }

  function startElection(nd) {
    nd.term += 1;
    nd.state = "candidate";
    nd.votedFor = nd.id;
    nd.votes = new Set([nd.id]);
    resetTimer(nd);
    log(`S${nd.id + 1} timed out → candidate for term ${nd.term}`, "cand");
    for (let j = 0; j < N; j++)
      if (j !== nd.id)
        send(nd.id, j, "RV", { term: nd.term, cand: nd.id, lastIndex: lastIndex(nd), lastTerm: lastTerm(nd) });
  }

  function becomeLeader(nd) {
    nd.state = "leader";
    nd.leaderId = nd.id;
    nd.nextIndex = Array(N).fill(lastIndex(nd) + 1);
    nd.matchIndex = Array(N).fill(0);
    nd.matchIndex[nd.id] = lastIndex(nd);
    nd.hbDue = now;
    log(`★ S${nd.id + 1} won ${nd.votes.size}/${N} votes → LEADER of term ${nd.term}`, "lead");
    syncPython();
  }

  function sendAE(nd, j) {
    const ni = nd.nextIndex[j];
    const prevIndex = ni - 1;
    const entries = nd.log.slice(prevIndex, prevIndex + MAX_ENTRIES_PER_AE).map((e) => ({ ...e }));
    send(nd.id, j, "AE", {
      term: nd.term, leader: nd.id, prevIndex, prevTerm: termAt(nd, prevIndex), entries, leaderCommit: nd.commitIndex,
    });
  }

  function broadcastAE(nd) {
    for (let j = 0; j < N; j++) if (j !== nd.id) sendAE(nd, j);
    nd.hbDue = now + HEARTBEAT;
  }

  function advanceCommit(nd) {
    for (let n = lastIndex(nd); n > nd.commitIndex; n--) {
      if (nd.log[n - 1].term !== nd.term) break; // only count replicas for current-term entries (§5.4.2)
      let count = 0;
      for (let j = 0; j < N; j++) if ((j === nd.id ? lastIndex(nd) : nd.matchIndex[j]) >= n) count++;
      if (count >= majority()) {
        const old = nd.commitIndex;
        nd.commitIndex = n;
        log(`✔ Leader S${nd.id + 1} commits index ${old + 1 === n ? n : `${old + 1}–${n}`} (on ${count}/${N} servers)`, "commit");
        logsDirty = true;
        syncPython();
        return;
      }
    }
  }

  function handle(m) {
    const nd = nodes[m.to];
    const b = m.body;
    if (stepDownIfStale(nd, b.term)) logsDirty = true;
    switch (m.type) {
      case "RV": {
        const upToDate = b.lastTerm > lastTerm(nd) || (b.lastTerm === lastTerm(nd) && b.lastIndex >= lastIndex(nd));
        const grant = b.term === nd.term && (nd.votedFor === null || nd.votedFor === b.cand) && upToDate;
        if (grant) {
          nd.votedFor = b.cand;
          resetTimer(nd);
        } else if (b.term === nd.term && !upToDate) {
          log(`S${nd.id + 1} refuses S${b.cand + 1}: candidate log not up-to-date`, "warn");
        }
        send(nd.id, m.from, "RVR", { term: nd.term, granted: grant });
        break;
      }
      case "RVR": {
        if (nd.state === "candidate" && b.term === nd.term && b.granted) {
          nd.votes.add(m.from);
          if (nd.votes.size >= majority()) becomeLeader(nd);
        }
        break;
      }
      case "AE": {
        if (b.term < nd.term) {
          send(nd.id, m.from, "AER", { term: nd.term, success: false });
          break;
        }
        if (nd.state !== "follower") {
          log(`S${nd.id + 1} hears from leader S${b.leader + 1} → follower`, "warn");
          nd.state = "follower";
        }
        nd.leaderId = b.leader;
        resetTimer(nd);
        if (b.prevIndex > lastIndex(nd) || termAt(nd, b.prevIndex) !== b.prevTerm) {
          send(nd.id, m.from, "AER", { term: nd.term, success: false });
          break;
        }
        let changed = false;
        b.entries.forEach((e, k) => {
          const idx = b.prevIndex + 1 + k;
          if (nd.log.length >= idx && nd.log[idx - 1].term !== e.term) {
            log(`S${nd.id + 1} truncates conflicting entries from index ${idx}`, "warn");
            nd.log.length = idx - 1;
          }
          if (nd.log.length < idx) {
            nd.log.push(e);
            changed = true;
          }
        });
        const lastNew = b.prevIndex + b.entries.length;
        if (b.leaderCommit > nd.commitIndex) {
          nd.commitIndex = Math.min(b.leaderCommit, lastNew);
          changed = true;
        }
        if (changed) logsDirty = true;
        send(nd.id, m.from, "AER", { term: nd.term, success: true, match: lastNew });
        break;
      }
      case "AER": {
        if (nd.state !== "leader" || b.term !== nd.term) break;
        if (b.success) {
          nd.matchIndex[m.from] = Math.max(nd.matchIndex[m.from], b.match);
          nd.nextIndex[m.from] = nd.matchIndex[m.from] + 1;
          advanceCommit(nd);
          if (nd.nextIndex[m.from] <= lastIndex(nd)) sendAE(nd, m.from);
        } else {
          nd.nextIndex[m.from] = Math.max(1, nd.nextIndex[m.from] - 1);
          sendAE(nd, m.from); // back up and retry right away
        }
        break;
      }
    }
  }

  function tick(dt) {
    now += dt;
    for (const nd of nodes) {
      if (!nd.alive) continue;
      if (nd.state === "leader") {
        if (now >= nd.hbDue) broadcastAE(nd);
      } else if (now >= nd.deadline) {
        startElection(nd);
      }
    }
    const due = [];
    msgs = msgs.filter((m) => {
      if (m.t1 <= now) { due.push(m); return false; }
      if (m.drop && (now - m.t0) / (m.t1 - m.t0) >= 0.5) { addBurst(m, 0.5); return false; }
      return true;
    });
    due.sort((a, b) => a.t1 - b.t1);
    for (const m of due) {
      if (!nodes[m.to].alive) { addBurst(m, 1); continue; }
      handle(m);
    }
    bursts = bursts.filter((b) => now - b.t < 350);
  }

  function addBurst(m, frac) {
    const [x0, y0] = pos(m.from), [x1, y1] = pos(m.to);
    const dx = x1 - x0, dy = y1 - y0, d = Math.hypot(dx, dy);
    const back = frac === 1 ? NR / d : 0;
    bursts.push({ x: x0 + dx * (frac - back), y: y0 + dy * (frac - back), t: now });
  }

  // ---------- controls ----------
  function clientRequest() {
    const leader = nodes.find((n) => n.alive && n.state === "leader" && !partitioned.has(n.id))
      || nodes.find((n) => n.alive && n.state === "leader");
    if (!leader) { log("✉ Client request rejected: no leader right now — retry soon", "warn"); return; }
    cmdCounter++;
    const key = "xyz"[cmdCounter % 3];
    const cmd = `${key}←${cmdCounter}`;
    leader.log.push({ term: leader.term, cmd });
    leader.matchIndex[leader.id] = lastIndex(leader);
    logsDirty = true;
    log(`✉ Client → S${leader.id + 1}: "${cmd}" appended at index ${lastIndex(leader)}`, "req");
    broadcastAE(leader);
  }

  function togglePartition() {
    if (partitioned.size) {
      partitioned.clear();
      log("🩹 Partition healed", "warn");
    } else {
      const leader = nodes.find((n) => n.alive && n.state === "leader");
      if (!leader) { log("No leader to partition yet", "warn"); return; }
      partitioned.add(leader.id);
      log(`✂ S${leader.id + 1} (leader) cut off from the rest of the cluster`, "warn");
    }
    $(".rs-part").textContent = partitioned.size ? "🩹 Heal partition" : "✂ Partition leader";
    $(".rs-part").classList.toggle("rs-on", partitioned.size > 0);
    msgs.forEach((m) => { if (partitioned.has(m.from) !== partitioned.has(m.to)) m.drop = true; });
  }

  function toggleNode(i) {
    const nd = nodes[i];
    if (nd.alive) {
      nd.alive = false;
      log(`💥 S${i + 1} crashed (${nd.state} in term ${nd.term})`, "warn");
      nd.state = "follower";
    } else {
      nd.alive = true;
      nd.state = "follower";
      nd.commitIndex = 0; // volatile; persistent: term, votedFor, log
      nd.votes = new Set();
      resetTimer(nd);
      log(`♻ S${i + 1} restarted with persisted term ${nd.term} and ${nd.log.length} log entries`);
    }
    logsDirty = true;
    syncPython();
  }

  $(".rs-play").onclick = () => {
    playing = !playing;
    $(".rs-play").textContent = playing ? "⏸ Pause" : "▶ Play";
    $(".rs-step").disabled = playing;
  };
  $(".rs-step").onclick = () => { for (let k = 0; k < 10; k++) tick(4); draw(); };
  $(".rs-speed").oninput = (e) => { speed = +e.target.value; $(".rs-speed-v").textContent = `${speed}×`; };
  $(".rs-loss").oninput = (e) => { loss = +e.target.value; $(".rs-loss-v").textContent = `${Math.round(loss * 100)}%`; };
  $(".rs-req").onclick = clientRequest;
  $(".rs-part").onclick = togglePartition;
  $(".rs-reset").onclick = () => {
    if (partitioned.size) togglePartition();
    reset();
  };

  // ---------- drawing ----------
  let nodeEls = [];
  function buildStatic() {
    gLinks.innerHTML = "";
    gNodes.innerHTML = "";
    nodeEls = [];
    for (let i = 0; i < N; i++)
      for (let j = i + 1; j < N; j++) {
        const [x0, y0] = pos(i), [x1, y1] = pos(j);
        h("line", { x1: x0, y1: y0, x2: x1, y2: y1, class: "rs-link", "data-i": i, "data-j": j }, gLinks);
      }
    const circ = 2 * Math.PI * (NR + 9);
    for (let i = 0; i < N; i++) {
      const [x, y] = pos(i);
      const g = h("g", { class: "rs-node", transform: `translate(${x},${y})` }, gNodes);
      const halo = h("circle", { r: NR + 16, class: "rs-halo", filter: "url(#rsblur)" }, g);
      h("circle", { r: NR + 9, class: "rs-ring-bg" }, g);
      const ring = h("circle", { r: NR + 9, class: "rs-ring", "stroke-dasharray": `${circ} ${circ}`, transform: "rotate(-90)" }, g);
      const body = h("circle", { r: NR, class: "rs-body" }, g);
      const name = h("text", { y: -4, class: "rs-name" }, g);
      name.textContent = `S${i + 1}`;
      const term = h("text", { y: 15, class: "rs-term" }, g);
      const badge = h("text", { y: -NR - 18, class: "rs-badge" }, g);
      g.addEventListener("click", () => toggleNode(i));
      nodeEls.push({ g, halo, ring, body, term, badge, circ });
    }
  }

  function draw() {
    // links + partitions
    gLinks.querySelectorAll("line").forEach((l) => {
      const i = +l.dataset.i, j = +l.dataset.j;
      const cut = partitioned.has(i) !== partitioned.has(j);
      const dead = !nodes[i].alive || !nodes[j].alive;
      l.setAttribute("class", "rs-link" + (cut ? " rs-cut" : dead ? " rs-deadlink" : ""));
    });
    gPart.innerHTML = "";
    for (const i of partitioned) {
      const [x, y] = pos(i);
      h("circle", { cx: x, cy: y, r: NR + 26, class: "rs-partition" }, gPart);
    }

    // nodes
    nodes.forEach((nd, i) => {
      const e = nodeEls[i];
      const color = nd.alive ? ROLE_COLOR[nd.state] : ROLE_COLOR.dead;
      e.body.setAttribute("fill", color);
      e.halo.setAttribute("fill", color);
      e.halo.setAttribute("opacity", nd.alive && nd.state !== "follower" ? 0.55 : 0);
      e.term.textContent = nd.alive ? `term ${nd.term}` : "✕ down";
      let frac = 0;
      if (nd.alive && nd.state !== "leader") frac = Math.max(0, (nd.deadline - now) / nd.timeoutLen);
      if (nd.alive && nd.state === "leader") frac = Math.max(0, (nd.hbDue - now) / HEARTBEAT);
      e.ring.setAttribute("stroke-dashoffset", e.circ * (1 - frac));
      e.ring.setAttribute("stroke", nd.state === "leader" ? "#6ee7b7" : nd.state === "candidate" ? "#fcd34d" : "#93c5fd");
      e.ring.setAttribute("opacity", nd.alive ? 0.9 : 0);
      e.badge.textContent = !nd.alive ? "" : nd.state === "leader" ? "★ leader" : nd.state === "candidate" ? `votes ${nd.votes.size}/${majority()}` : "";
      e.badge.setAttribute("fill", nd.state === "leader" ? "#6ee7b7" : "#fcd34d");
      e.g.classList.toggle("rs-dead", !nd.alive);
    });

    // messages
    let html = "";
    for (const m of msgs) {
      const [x0, y0] = pos(m.from), [x1, y1] = pos(m.to);
      let p = (now - m.t0) / (m.t1 - m.t0);
      if (m.drop) p = Math.min(p, 0.5);
      const d = Math.hypot(x1 - x0, y1 - y0);
      const s = NR / d; // start/end at circle edge
      const q = s + (1 - 2 * s) * p;
      const x = x0 + (x1 - x0) * q, y = y0 + (y1 - y0) * q;
      let fill, r = 5, stroke = "none";
      if (m.type === "RV") { fill = "#fbbf24"; r = 6.5; }
      else if (m.type === "RVR") { fill = m.body.granted ? "#34d399" : "#f43f5e"; r = 5; }
      else if (m.type === "AE") { fill = m.body.entries.length ? "#c084fc" : "#7dd3fc"; r = m.body.entries.length ? 7 : 4.5; }
      else { fill = m.body.success ? "#cbd5e1" : "#f43f5e"; r = 3.5; }
      if (m.drop) stroke = "#f43f5e";
      html += `<circle cx="${x}" cy="${y}" r="${r}" fill="${fill}" stroke="${stroke}" stroke-width="1.5" class="rs-msg"/>`;
    }
    gMsgs.innerHTML = html;

    let bh = "";
    for (const b of bursts) {
      const k = (now - b.t) / 350;
      const s = 6 + 8 * k;
      bh += `<g transform="translate(${b.x},${b.y})" opacity="${1 - k}"><path d="M${-s} ${-s}L${s} ${s}M${s} ${-s}L${-s} ${s}" stroke="#f43f5e" stroke-width="3" stroke-linecap="round"/></g>`;
    }
    gBursts.innerHTML = bh;

    if (logsDirty) { renderLogs(); logsDirty = false; }
  }

  function applied(nd) {
    const st = {};
    for (let k = 0; k < nd.commitIndex; k++) {
      const [key, v] = nd.log[k].cmd.split("←");
      st[key] = v;
    }
    return Object.keys(st).sort().map((k) => `${k}=${st[k]}`).join(" ") || "∅";
  }

  function renderLogs() {
    const maxLen = Math.max(8, ...nodes.map((n) => n.log.length));
    const start = Math.max(0, maxLen - 14);
    let out = `<div class="rs-logrow rs-idx"><span class="rs-lname"></span>`;
    for (let k = start; k < maxLen; k++) out += `<span class="rs-cell rs-idxcell">${k + 1}</span>`;
    out += `<span class="rs-sm">state machine</span></div>`;
    for (const nd of nodes) {
      const color = nd.alive ? ROLE_COLOR[nd.state] : ROLE_COLOR.dead;
      out += `<div class="rs-logrow ${nd.alive ? "" : "rs-logdead"}"><span class="rs-lname" style="color:${color}">S${nd.id + 1}${nd.state === "leader" && nd.alive ? "★" : ""}</span>`;
      for (let k = start; k < maxLen; k++) {
        const e = nd.log[k];
        if (!e) { out += `<span class="rs-cell rs-empty"></span>`; continue; }
        const committed = k < nd.commitIndex;
        const c = termColor(e.term);
        out += `<span class="rs-cell ${committed ? "rs-committed" : "rs-pending"}" style="--c:${c}" title="index ${k + 1} · term ${e.term} · ${e.cmd}">${e.term}</span>`;
      }
      out += `<span class="rs-sm">${applied(nd)}</span></div>`;
    }
    $(".rs-logs").innerHTML = out;
  }

  function renderFeed() {
    $(".rs-feed").innerHTML = events
      .map((e, i) => `<div class="rs-ev rs-k-${e.kind}" style="opacity:${1 - i * 0.08}"><span class="rs-t">${(e.t / 1000).toFixed(2)}s</span>${e.text}</div>`)
      .join("");
  }

  let lastSync = "";
  function syncPython() {
    const leader = nodes.find((n) => n.alive && n.state === "leader" && !partitioned.has(n.id));
    const maxTerm = Math.max(...nodes.map((n) => n.term));
    const commit = Math.max(...nodes.map((n) => n.commitIndex));
    const key = `${leader ? leader.id : -1}|${maxTerm}|${commit}`;
    if (key === lastSync) return;
    lastSync = key;
    model.set("leader", leader ? leader.id + 1 : 0);
    model.set("term", maxTerm);
    model.set("commit_index", commit);
    model.save_changes();
  }

  // ---------- main loop ----------
  let last = performance.now();
  let raf;
  let frame = 0;
  function loop(t) {
    const dtReal = Math.min(50, t - last);
    last = t;
    if (playing) {
      let simDt = dtReal * speed;
      while (simDt > 0) { const d = Math.min(4, simDt); tick(d); simDt -= d; }
      draw();
      if (++frame % 15 === 0) syncPython();
    }
    raf = requestAnimationFrame(loop);
  }

  model.on("change:n", () => reset());
  reset();
  draw();
  raf = requestAnimationFrame(loop);
  return () => cancelAnimationFrame(raf);
}

export default { render };
