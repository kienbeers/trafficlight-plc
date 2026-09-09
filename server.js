const path = require('path');
const fs = require('fs');
const express = require('express');
const mqtt = require('mqtt');
const { MongoClient } = require('mongodb');

// ===== CẤU HÌNH =====
// Gateway ảo có DB RIÊNG (độc lập cloud): Mongo riêng đóng trong Docker (service "db").
// Cấu hình ban đầu seed từ nodes.seed.json; thay đổi do lệnh cloud lưu vào DB riêng này.
const CFG = {
    broker: process.env.SIM_BROKER || 'mqtt://167.254.68.86:1883',
    username: process.env.SIM_USER || 'admin',
    password: process.env.SIM_PASS || 'admin123',
    clientId: process.env.SIM_CLIENT_ID || 'gateway-sim',
    httpPort: Number(process.env.SIM_PORT || 4100),
    statusMs: Number(process.env.SIM_STATUS_MS || 1000),
    trafficMs: Number(process.env.SIM_TRAFFIC_MS || 5000),
    mongoUrl: process.env.MONGO_URL || 'mongodb://127.0.0.1:27017',
    dbName: process.env.MONGO_DB || 'gateway_sim',
    seedFile: path.join(__dirname, 'nodes.seed.json'),
};

const SYS_MODE_CODE = { AUTO: 0, MANUAL: 1, ADAPTIVE: 2, SAFE: 3 };
const SYS_MODE_BY_CODE = { 0: 'AUTO', 1: 'MANUAL', 2: 'ADAPTIVE', 3: 'SAFE' };
const LAMP_CODE = { RED: 0, YELLOW: 1, GREEN: 2 };

let db = null;

async function loadNodes() {
    const client = new MongoClient(CFG.mongoUrl, { serverSelectionTimeoutMS: 8000 });
    await client.connect();
    db = client.db(CFG.dbName);
    const col = db.collection('nodes');
    const seed = JSON.parse(fs.readFileSync(CFG.seedFile, 'utf8'));
    const existing = new Set((await col.find({}, { projection: { id: 1 } }).toArray()).map((d) => d.id));
    const toAdd = seed.filter((s) => !existing.has(s.id));
    if (toAdd.length) {
        await col.insertMany(toAdd.map((s) => ({ ...s, systemMode: 'AUTO', activeProfile: null })));
        console.log(`[sim] Thêm ${toAdd.length} nút mới vào DB riêng (${CFG.dbName}.nodes): ${toAdd.map((s) => s.id).join(', ')}`);
    }
    const docs = await col.find({}).sort({ id: 1 }).toArray();
    return docs.map(buildNode);
}

function buildNode(it) {
    const nd = (it.directions || []).slice().sort((a, b) => a.directionPlc - b.directionPlc);
    const codeToPlc = new Map(nd.map((d) => [d.code, d.directionPlc]));
    const dirsPlc = nd.map((d) => d.directionPlc);
    const phases = (it.phases || []).map((p, i) => {
        const greenDirs = new Set();
        const greens = p.greens || (p.greenCodes || []).map((c) => ({ directionCode: c }));
        greens.forEach((g) => { const plc = codeToPlc.get(g.directionCode); if (plc) greenDirs.add(plc); });
        // greenSec khác nhau mỗi nút/pha (16..37s) → chu kỳ lệch nhau, không đồng bộ.
        return { index: i + 1, code: p.code || `P${i + 1}`, greenSec: p.greenSec || (16 + Math.floor(Math.random() * 22)), yellowSec: p.yellowSec || 3, allRedSec: p.allRedSec || 2, greenDirs };
    });
    const activeProfile = it.activeProfile || { minGreen: 12, maxGreen: 60, yellow: 3, allRed: 2, lowQueue: 3, baseGreen: phases.map((p) => p.greenSec) };
    // Bắt đầu ở pha + giây ngẫu nhiên → mỗi nút lệch pha, hiển thị khác nhau.
    const startIdx = phases.length ? Math.floor(Math.random() * phases.length) : 0;
    const sp = phases[startIdx];
    const startElapsed = sp ? Math.floor(Math.random() * (sp.greenSec + sp.yellowSec + sp.allRedSec)) : 0;
    return {
        id: it.id, name: it.name, gatewayId: it.gatewayId || `GW-${it.id}`,
        dirsMeta: nd, dirs: dirsPlc, phases,
        sim: {
            enabled: true, phaseIdx: startIdx, elapsed: startElapsed, systemMode: it.systemMode || 'AUTO', hb: 0, pingCounter: 0, lastApproved: null,
            activeProfile, traffic: nd.map(() => ({ q: 3 + Math.random() * 12 })),
        },
        lastStatus: null, lastStatusAt: 0, lastTraffic: null, lastTrafficAt: 0, cmdLog: [],
    };
}

// Lưu thay đổi cấu hình (do lệnh cloud) vào DB RIÊNG của PLC — cloud và PLC lưu tách biệt.
function persistNode(node) {
    if (!db) return;
    const phases = node.phases.map((p) => ({
        code: p.code, greenSec: p.greenSec, yellowSec: p.yellowSec, allRedSec: p.allRedSec,
        greenCodes: [...p.greenDirs].map((plc) => { const d = node.dirsMeta.find((x) => x.directionPlc === plc); return d ? d.code : String(plc); }),
    }));
    db.collection('nodes').updateOne({ id: node.id }, { $set: { systemMode: node.sim.systemMode, activeProfile: node.sim.activeProfile, phases } }).catch(() => {});
}

const durOf = (p) => p.greenSec + p.yellowSec + p.allRedSec;
const nowIso = () => new Date().toISOString();

function todFactor() {
    const h = new Date().getHours();
    if ((h >= 6 && h < 9) || (h >= 16 && h < 19)) return 1.0;
    if (h >= 22 || h < 6) return 0.25;
    return 0.6;
}

function genTraffic(node) {
    const tf = todFactor();
    const directions = node.dirsMeta.map((d, i) => {
        const st = node.sim.traffic[i];
        st.q = Math.max(0, Math.min(35, st.q + (Math.random() * 6 - 3)));
        const queue = Math.round(st.q);
        const flow = Math.round((300 + tf * 1500) * (0.6 + Math.random() * 0.8));
        const speed = Math.max(5, Math.round(48 - queue * 1.1 - tf * 8 + (Math.random() * 6 - 3)));
        const density = Math.round(flow / Math.max(speed, 5));
        const occupancy = Math.min(0.95, +(density / 150).toFixed(2));
        const win = flow * (CFG.trafficMs / 1000) / 3600;
        const class_counts = {
            motorbike: Math.round(win * 0.70), car: Math.round(win * 0.22),
            bus: Math.round(win * 0.03), truck: Math.round(win * 0.05),
        };
        return {
            direction_code: d.code, direction_plc: d.directionPlc,
            flow, density, occupancy, speed, queue, class_counts,
            confidence: +(0.85 + Math.random() * 0.14).toFixed(2),
        };
    });
    return { intersection_id: node.id, source: 'camera', ts: Math.floor(Date.now() / 1000), sample_window_sec: CFG.trafficMs / 1000, camera_online: true, directions };
}

function buildStatus(node) {
    const sim = node.sim;
    const p = node.phases[sim.phaseIdx];
    const stage = sim.elapsed < p.greenSec ? 'green' : (sim.elapsed < p.greenSec + p.yellowSec ? 'yellow' : 'allred');
    const safe = sim.systemMode === 'SAFE';
    const colorFor = (d) => {
        if (safe) return 'YELLOW';
        if (p.greenDirs.has(d)) return stage === 'green' ? 'GREEN' : (stage === 'yellow' ? 'YELLOW' : 'RED');
        return 'RED';
    };
    const h = new Date().getHours();
    const windowIndex = ((h >= 6 && h < 9) || (h >= 16 && h < 19)) ? 0 : ((h >= 22 || h < 6) ? 2 : 1);
    const status = {
        intersection_id: node.id, Timestamp: nowIso(),
        SystemMode: SYS_MODE_CODE[sim.systemMode], ConnectivityStatus: 0, EffectiveOffline: 0, ServerLinkOK: 1,
        Fault_Latched: 0, MaintenanceLock: 0, RTC_Valid: 1, Current_Phase: sim.phaseIdx,
        AnomalyAlert: 0, AnomalyAlert_Reason: 0, LastHeartbeat_Age: 0, CurrentWindowIndex: windowIndex,
        PLC_Heartbeat_Counter: ++sim.hb, NumDirections: node.dirs.length, NumPhases: node.phases.length,
        CurrentActiveProfile_MinGreen: sim.activeProfile.minGreen, CurrentActiveProfile_MaxGreen: sim.activeProfile.maxGreen,
        CurrentActiveProfile_YellowTime: sim.activeProfile.yellow, CurrentActiveProfile_AllRedTime: sim.activeProfile.allRed,
        CurrentActiveProfile_LowQueueThreshold: sim.activeProfile.lowQueue, CurrentActiveProfile_Age: 45, CurrentActiveProfile_ForWindow: windowIndex,
    };
    const greenLeft = Math.max(0, p.greenSec - sim.elapsed);
    node.dirs.forEach((d, i) => {
        const color = colorFor(d);
        status[`Output_Lamp_Color_${i}`] = LAMP_CODE[color];
        status[`Output_Lamp_Blink_${i}`] = safe ? 1 : 0;
        status[`RemainingTime_${i}`] = color === 'GREEN' ? greenLeft : 0;
        status[`SensorFault_${i}`] = 0;
        status[`SmoothedQueue_${i}`] = Math.round((node.sim.traffic[i] && node.sim.traffic[i].q) || 0);
    });
    node.phases.forEach((ph, i) => {
        status[`CurrentActiveProfile_BaseGreen_${i}`] = ph.greenSec;
        let bits = 0; ph.greenDirs.forEach((d) => { bits |= (1 << (d - 1)); });
        status[`PhaseDirectionMap_${i}`] = bits;
    });
    if (sim.lastApproved) {
        status.ApprovedCmd_Source = 1; status.ApprovedCmd_Type = sim.lastApproved.typeCode;
        status.ApprovedCmd_RejectReason = 0; status.ApprovedCmd_Timestamp = Math.floor(Date.now() / 1000);
    }
    return status;
}

function advance(node) {
    const sim = node.sim;
    if (sim.systemMode !== 'AUTO' && sim.systemMode !== 'ADAPTIVE') return;
    const dur = durOf(node.phases[sim.phaseIdx]);
    sim.elapsed += Math.round(CFG.statusMs / 1000) || 1;
    if (sim.elapsed >= dur) { sim.elapsed = 0; sim.phaseIdx = (sim.phaseIdx + 1) % node.phases.length; }
}

const events = [];
function logEvent(dir, id, summary, payload) {
    events.unshift({ t: Date.now(), dir, id, summary, payload });
    while (events.length > 80) events.pop();
}

async function main() {
    console.log(`[sim] Đang đọc cấu hình nút từ DB riêng: ${CFG.mongoUrl}/${CFG.dbName}…`);
    const nodes = await loadNodes();
    const nodeById = new Map(nodes.map((n) => [n.id, n]));
    console.log(`[sim] Nạp ${nodes.length} nút: ${nodes.map((n) => n.id).join(', ')}`);

    let url = CFG.broker;
    try { const u = new URL(CFG.broker); if (u.port === '8883' && u.protocol === 'mqtt:') u.protocol = 'mqtts:'; url = u.toString(); } catch (e) { /* noop */ }
    const client = mqtt.connect(url, { clientId: CFG.clientId, username: CFG.username, password: CFG.password, reconnectPeriod: 5000, rejectUnauthorized: false });
    const state = { connected: false };

    client.on('connect', () => {
        state.connected = true;
        console.log(`[sim] MQTT connected → ${url}`);
        client.subscribe(['plc/+/command', 'plc/+/pong'], { qos: 1 });
    });
    client.on('reconnect', () => { state.connected = false; });
    client.on('close', () => { state.connected = false; });
    client.on('error', (e) => console.error('[sim] MQTT lỗi:', e.message));

    client.on('message', (topic, buf) => {
        const parts = String(topic).split('/');
        if (parts[0] !== 'plc' || parts.length !== 3) return;
        const id = parts[1]; const kind = parts[2];
        const node = nodeById.get(id);
        if (!node) return;
        let msg; try { msg = JSON.parse(buf.toString()); } catch (e) { return; }
        if (kind === 'pong') { logEvent('recv', id, `pong #${msg.Server_Pong_Counter}`, msg); return; }
        if (kind !== 'command') return;
        handleCommand(node, msg);
    });

    function handleCommand(node, cmd) {
        const sim = node.sim;
        const type = Number(cmd.Cmd_Type);
        const TYPE_NAME = { 0: 'SET_MODE', 1: 'SET_PHASE', 6: 'SET_ACTIVE_PROFILE', 7: 'SET_FALLBACK_PROFILE', 8: 'SET_RTC_TIME', 10: 'SET_TOPOLOGY' };
        if (type === 0) { const m = SYS_MODE_BY_CODE[Number(cmd.Cmd_TargetMode)]; if (m) { sim.systemMode = m; sim.elapsed = 0; } }
        else if (type === 1) { const wp = Number(cmd.Cmd_TargetPhase); sim.phaseIdx = ((wp % node.phases.length) + node.phases.length) % node.phases.length; sim.elapsed = 0; sim.systemMode = 'MANUAL'; }
        else if (type === 6) { applyActiveProfile(node, cmd); }
        else if (type === 10) { applyTopology(node, cmd); }
        persistNode(node);
        sim.lastApproved = { typeCode: type };
        const reqId = cmd.Request_Id != null ? cmd.Request_Id : cmd.request_id;
        const label = `${TYPE_NAME[type] || 'CMD_' + type}`;
        node.cmdLog.unshift({ t: Date.now(), type, label, reqId, payload: cmd, stage: 'received' });
        while (node.cmdLog.length > 30) node.cmdLog.pop();
        logEvent('recv', node.id, `lệnh ${label}${reqId ? ' #' + reqId : ''}`, cmd);
        if (reqId != null) {
            client.publish(`plc/${node.id}/ack`, JSON.stringify({ intersection_id: node.id, Request_Id: reqId, Cmd_Type: type, Stage: 0, Reject_Reason: 0, Timestamp: nowIso() }), { qos: 1 });
            const p = node.phases[sim.phaseIdx];
            const remainMs = Math.max(2000, (durOf(p) - sim.elapsed) * 1000);
            setTimeout(() => {
                client.publish(`plc/${node.id}/ack`, JSON.stringify({ intersection_id: node.id, Request_Id: reqId, Cmd_Type: type, Stage: 1, Reject_Reason: 0, Timestamp: nowIso() }), { qos: 1 });
                const e = node.cmdLog.find((x) => x.reqId === reqId); if (e) e.stage = 'applied';
                logEvent('sent', node.id, `ACK applied ${label} #${reqId}`, null);
            }, remainMs);
            logEvent('sent', node.id, `ACK received ${label} #${reqId}`, null);
        }
    }

    function applyActiveProfile(node, cmd) {
        const ap = node.sim.activeProfile;
        if (cmd.Cmd_MinGreen != null) ap.minGreen = Number(cmd.Cmd_MinGreen);
        if (cmd.Cmd_MaxGreen != null) ap.maxGreen = Number(cmd.Cmd_MaxGreen);
        if (cmd.Cmd_YellowTime != null) ap.yellow = Number(cmd.Cmd_YellowTime);
        if (cmd.Cmd_AllRedTime != null) ap.allRed = Number(cmd.Cmd_AllRedTime);
        if (cmd.Cmd_LowQueueThreshold != null) ap.lowQueue = Number(cmd.Cmd_LowQueueThreshold);
        node.phases.forEach((p, i) => { const g = cmd[`Cmd_BaseGreen_${i}`]; if (g != null) { ap.baseGreen[i] = Number(g); p.greenSec = Number(g) || p.greenSec; } p.yellowSec = ap.yellow; p.allRedSec = ap.allRed; });
    }
    function applyTopology(node, cmd) {
        const np = Number(cmd.Cmd_NumPhases) || node.phases.length;
        if (np < 1) return;
        const rebuilt = [];
        for (let i = 0; i < np; i++) {
            const bits = Number(cmd[`Cmd_PhaseDirectionMap_${i}`]) || 0;
            const greenDirs = new Set();
            node.dirs.forEach((d) => { if (bits & (1 << (d - 1))) greenDirs.add(d); });
            const old = node.phases[i] || {};
            rebuilt.push({ index: i + 1, code: old.code || `P${i + 1}`, greenSec: old.greenSec || 20, yellowSec: old.yellowSec || 3, allRedSec: old.allRedSec || 2, greenDirs });
        }
        node.phases.length = 0; node.phases.push(...rebuilt);
        if (node.sim.phaseIdx >= node.phases.length) { node.sim.phaseIdx = 0; node.sim.elapsed = 0; }
    }

    // Vòng status + ping (mỗi statusMs); ping mỗi 20s.
    setInterval(() => {
        if (!state.connected) return;
        for (const node of nodes) {
            if (!node.sim.enabled) continue;
            const st = buildStatus(node);
            client.publish(`plc/${node.id}/status`, JSON.stringify(st), { qos: 1 });
            node.lastStatus = st; node.lastStatusAt = Date.now();
            advance(node);
            if (node.sim.hb % 20 === 0) client.publish(`plc/${node.id}/ping`, JSON.stringify({ intersection_id: node.id, PLC_Ping_Counter: ++node.sim.pingCounter }), { qos: 1 });
        }
    }, CFG.statusMs);

    // Vòng traffic (data AI) mỗi trafficMs.
    setInterval(() => {
        if (!state.connected) return;
        for (const node of nodes) {
            if (!node.sim.enabled) continue;
            const tr = genTraffic(node);
            client.publish(`plc/${node.id}/traffic`, JSON.stringify(tr), { qos: 1 });
            node.lastTraffic = tr; node.lastTrafficAt = Date.now();
        }
    }, CFG.trafficMs);

    // ── Web UI + API ──
    const app = express();
    app.use(express.json());
    app.use(express.static(path.join(__dirname, 'public')));

    app.get('/api/state', (req, res) => {
        res.json({
            broker: url, connected: state.connected,
            statusMs: CFG.statusMs, trafficMs: CFG.trafficMs,
            count: nodes.length, enabledCount: nodes.filter((n) => n.sim.enabled).length,
            nodes: nodes.map((n) => {
                const st = n.lastStatus; const tr = n.lastTraffic;
                const lamps = n.dirs.map((d, i) => ({ code: n.dirsMeta[i].code, color: st ? ['red', 'yellow', 'green'][st[`Output_Lamp_Color_${i}`]] : 'off' }));
                let avgSpeed = null, maxQueue = null, totFlow = null;
                if (tr) { avgSpeed = Math.round(tr.directions.reduce((a, d) => a + d.speed, 0) / tr.directions.length); maxQueue = Math.max(...tr.directions.map((d) => d.queue)); totFlow = tr.directions.reduce((a, d) => a + d.flow, 0); }
                return { id: n.id, name: n.name, enabled: n.sim.enabled, mode: n.sim.systemMode, phaseIdx: n.sim.phaseIdx, phaseCount: n.phases.length, dirs: n.dirs.length, lamps, avgSpeed, maxQueue, totFlow, lastStatusAt: n.lastStatusAt, lastTrafficAt: n.lastTrafficAt };
            }),
            events: events.slice(0, 50),
        });
    });

    app.get('/api/node/:id', (req, res) => {
        const n = nodeById.get(req.params.id);
        if (!n) return res.status(404).json({ error: 'không có nút' });
        res.json({
            id: n.id, name: n.name, gatewayId: n.gatewayId, enabled: n.sim.enabled, mode: n.sim.systemMode,
            config: { directions: n.dirsMeta.map((d) => ({ code: d.code, plc: d.directionPlc, road: d.roadName, movements: (d.movements || []).map((m) => `${m.turn}→${m.toDirectionCode || '·'}`) })), phases: n.phases.map((p) => ({ code: p.code, greenDirs: [...p.greenDirs], greenSec: p.greenSec })) },
            lastStatus: n.lastStatus, lastTraffic: n.lastTraffic, cmdLog: n.cmdLog,
        });
    });

    app.post('/api/toggle', (req, res) => {
        const { id, all, enabled } = req.body || {};
        if (all) { nodes.forEach((n) => { n.sim.enabled = !!enabled; }); logEvent('sent', '*', `${enabled ? 'BẬT' : 'TẮT'} tất cả nút`, null); }
        else { const n = nodeById.get(id); if (!n) return res.status(404).json({ error: 'không có nút' }); n.sim.enabled = !!enabled; logEvent('sent', id, `${enabled ? 'BẬT' : 'TẮT'} nút`, null); }
        res.json({ ok: true });
    });

    app.listen(CFG.httpPort, () => console.log(`[sim] Web quản lý: http://localhost:${CFG.httpPort}`));
}

main().catch((e) => { console.error('[sim] lỗi khởi động:', e.message); process.exit(1); });
