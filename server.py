import eventlet
eventlet.monkey_patch()

import json
import logging
import os
import re
import secrets
import threading
import time
import urllib.request
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from functools import wraps
from urllib.parse import urlparse, urlunparse

from flask import (Flask, Response, abort, flash, jsonify, redirect,
                   render_template, request, stream_with_context, url_for)
from flask_login import (LoginManager, UserMixin, current_user, login_required,
                         login_user, logout_user)
from flask_migrate import Migrate
from flask_socketio import SocketIO, join_room
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, text
from werkzeug.exceptions import BadRequest
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.security import check_password_hash, generate_password_hash

try:
    from zoneinfo import ZoneInfo
    LOCAL_TZ = ZoneInfo(os.environ.get('APP_TIMEZONE', 'Europe/Copenhagen'))
except Exception:  # tzdata missing -> show UTC
    LOCAL_TZ = timezone.utc

logging.basicConfig(level=logging.INFO)

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL', 'sqlite:///healthcare.db').replace('postgres://', 'postgresql://', 1)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {'pool_recycle': 300, 'pool_pre_ping': True}

_secret = os.environ.get('SECRET_KEY')
if not _secret:
    _secret = secrets.token_hex(32)
    logging.warning("SECRET_KEY is not set - using a random key (users are logged out on every restart). Set SECRET_KEY in the environment.")
app.config['SECRET_KEY'] = _secret
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'   # blocks cross-site POSTs (CSRF) with the session cookie
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SECURE'] = os.environ.get('COOKIE_SECURE', '1' if os.environ.get('RAILWAY_ENVIRONMENT') else '0') == '1'
app.config['REMEMBER_COOKIE_SAMESITE'] = 'Lax'

# Shared secret that robots / ESP32 devices send in the X-Device-Key header.
# If it is not set the device API stays open (backwards compatible) and a warning is logged.
DEVICE_KEY = os.environ.get('DEVICE_KEY', '').strip()
CAM_TOKEN = os.environ.get('CAM_TOKEN', '').strip()
if not DEVICE_KEY:
    logging.warning("DEVICE_KEY is not set - device API endpoints are NOT protected. Set DEVICE_KEY in the environment.")

db = SQLAlchemy(app)
migrate = Migrate(app, db)
login_manager = LoginManager(app)
login_manager.login_view = 'login'
socketio = SocketIO(app, async_mode='eventlet')

URGENCIES = ('normal', 'urgent', 'very_urgent', 'critical')
ONLINE_TIMEOUT = 60      # seconds without heartbeat -> offline
COMMAND_TTL = 60         # device commands older than this are dropped (never open a door hours later)


# ---------------------------------------------------------------- models
class Nurse(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    cpr = db.Column(db.String(20), unique=True)
    name = db.Column(db.String(100))
    phone_number = db.Column(db.String(20))
    municipality = db.Column(db.String(100))
    password = db.Column(db.String(255))
    is_admin = db.Column(db.Boolean, default=False)


class Client(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    cpr = db.Column(db.String(20), unique=True)
    name = db.Column(db.String(100))
    address = db.Column(db.String(255))
    doctor_name = db.Column(db.String(100))
    clinic_id = db.Column(db.String(50))
    municipality = db.Column(db.String(100))
    emergency_info = db.Column(db.Text)
    psychological_info = db.Column(db.Text)
    last_updated = db.Column(db.DateTime)
    ip_address = db.Column(db.String(20))
    camera_url = db.Column(db.String(255))
    # Device name of the ESP32 door lock (it listens on /stream?client_id=<device_name>)
    device_name = db.Column(db.String(64))


class Medicine(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    client_id = db.Column(db.Integer, db.ForeignKey('client.id'))
    name = db.Column(db.String(100))
    side_effect = db.Column(db.Text)
    active = db.Column(db.Boolean, default=True)
    schedule_type = db.Column(db.String(20), default='daily')
    days = db.Column(db.String(200), default='')   # comma-separated
    times = db.Column(db.String(200), default='')  # comma-separated

    def get_days_list(self):
        return [d.strip() for d in self.days.split(',') if d.strip()] if self.days else []

    def get_times_list(self):
        return [t.strip() for t in self.times.split(',') if t.strip()] if self.times else []

    def set_days_list(self, days):
        self.days = ', '.join(str(d) for d in days) if isinstance(days, list) and days else ''

    def set_times_list(self, times):
        self.times = ', '.join(str(t) for t in times) if isinstance(times, list) and times else ''

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'side_effect': self.side_effect or '',
            'active': self.active,
            'schedule_type': self.schedule_type or 'daily',
            'days': self.get_days_list(),
            'times': self.get_times_list(),
        }


class MedTime(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    client_id = db.Column(db.Integer, db.ForeignKey('client.id'))
    med_id = db.Column(db.String(50))
    time = db.Column(db.String(20))
    date = db.Column(db.String(20))
    active = db.Column(db.Boolean, default=True)
    taken = db.Column(db.Boolean, default=False)


class Reminder(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    client_id = db.Column(db.Integer, db.ForeignKey('client.id'))
    rem_id = db.Column(db.String(50))
    date = db.Column(db.String(20))
    note = db.Column(db.Text)


class Notification(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    client_id = db.Column(db.Integer, db.ForeignKey('client.id'))
    user_name = db.Column(db.String(100))
    date_time = db.Column(db.DateTime, default=datetime.utcnow)
    note = db.Column(db.Text)
    urgency = db.Column(db.String(20))
    hidden = db.Column(db.Boolean, default=False)
    client = db.relationship('Client', backref=db.backref('notifications', lazy=True))


class Visit(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    client_id = db.Column(db.Integer, db.ForeignKey('client.id'))
    user_name = db.Column(db.String(100))
    nurse_id = db.Column(db.Integer, db.ForeignKey('nurse.id'))
    nurse_name = db.Column(db.String(100))
    date = db.Column(db.String(20))
    time = db.Column(db.String(20))
    next_visit_date = db.Column(db.String(20))
    notes = db.Column(db.Text)

    def to_dict(self):
        return {
            'id': self.id,
            'nurse_name': self.nurse_name,
            'date': self.date,
            'time': self.time,
            'next_visit_date': self.next_visit_date,
            'notes': self.notes,
        }


class Vital(db.Model):
    """Vital-sign readings uploaded by the robot (new table, created automatically)."""
    id = db.Column(db.Integer, primary_key=True)
    client_id = db.Column(db.Integer, db.ForeignKey('client.id'), index=True)
    ts = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    spo2 = db.Column(db.Float)
    pulse = db.Column(db.Float)
    temp_object = db.Column(db.Float)
    temp_ambient = db.Column(db.Float)

    def to_dict(self):
        return {
            'ts': self.ts.replace(tzinfo=timezone.utc).isoformat() if self.ts else None,
            'spo2': self.spo2,
            'pulse': self.pulse,
            'temp_object': self.temp_object,
            'temp_ambient': self.temp_ambient,
        }


# ---------------------------------------------------------------- helpers
@app.template_filter('localtime')
def localtime_filter(dt, fmt='%Y-%m-%d %H:%M'):
    """Render a naive-UTC datetime in the configured local timezone."""
    if not dt:
        return ''
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(LOCAL_TZ).strftime(fmt)


def normalize_room(s):
    return re.sub(r'[^A-Za-z0-9]+', '', s or '').lower()


def _is_superuser(user=None):
    user = user or current_user
    return bool(getattr(user, 'is_admin', False) or getattr(user, 'municipality', '') == 'all')


def _can_access_client(client):
    return _is_superuser() or (client.municipality or '') == (getattr(current_user, 'municipality', '') or '')


def get_client_or_403(client_id):
    client = Client.query.get_or_404(client_id)
    if not _can_access_client(client):
        abort(403)
    return client


def _wants_json():
    return request.is_json or request.headers.get('X-Requested-With') or request.path.startswith('/api/')


def _device_key_ok():
    if not DEVICE_KEY and not CAM_TOKEN:
        return True
    supplied = (request.headers.get('X-Device-Key') or request.headers.get('X-Cam-Token')
                or request.args.get('key') or '')
    return bool(supplied) and supplied in {k for k in (DEVICE_KEY, CAM_TOKEN) if k}


def device_required(view):
    """Endpoints used by robots / ESP32 devices: device key OR a logged-in nurse."""
    @wraps(view)
    def wrapper(*args, **kwargs):
        if _device_key_ok() or current_user.is_authenticated:
            return view(*args, **kwargs)
        return jsonify({'status': 'error', 'message': 'invalid device key'}), 401
    return wrapper


def camera_stream_url(raw):
    if not raw:
        return None
    u = raw.strip()
    if u.endswith('/stream'):
        return u
    if not u.startswith(('http://', 'https://')):
        u = 'http://' + u
    p = urlparse(u)
    netloc = f"{p.hostname}:{p.port or 81}"
    return urlunparse((p.scheme, netloc, '/stream', '', '', ''))


def emit_for_client(event, payload, municipality):
    """Send a Socket.IO event only to nurses allowed to see this client."""
    socketio.emit(event, payload, to='admins')
    if municipality:
        socketio.emit(event, payload, to=f"muni:{municipality}")


def create_notification(client, note, urgency='normal', user_name='', hidden=False):
    urgency = urgency if urgency in URGENCIES else 'normal'
    n = Notification(client_id=client.id, user_name=user_name or client.name or '',
                     note=(note or '')[:2000], urgency=urgency, date_time=datetime.utcnow(),
                     hidden=hidden)
    db.session.add(n)
    db.session.commit()
    if not hidden:
        emit_for_client('new_notification', {
            'client_id': client.id,
            'client_name': client.name,
            'notification': {
                'id': n.id, 'urgency': n.urgency, 'note': n.note,
                'date_time': n.date_time.replace(tzinfo=timezone.utc).isoformat(),
                'user_name': n.user_name,
            }
        }, client.municipality)
    return n


# ---------------------------------------------------------------- device command bus (SSE)
# Every SSE subscriber gets its own queue, so a robot and a door lock listening on the
# same id both receive every message. Commands for offline devices wait at most COMMAND_TTL.
_bus_lock = threading.Lock()
_subscribers = defaultdict(list)                     # room -> [deque, ...]
_pending = defaultdict(lambda: deque(maxlen=50))      # room -> deque of messages
_device_seen = {}                                     # room -> last time an SSE client was connected


def push_command(target, msg, ttl=COMMAND_TTL):
    room = normalize_room(target)
    if not room:
        return 0
    msg = dict(msg)
    msg.setdefault('target', target)
    msg.setdefault('ts', datetime.utcnow().isoformat())
    msg['_exp'] = time.time() + ttl
    with _bus_lock:
        subs = list(_subscribers.get(room, []))
        if subs:
            for q in subs:
                q.append(msg)
        else:
            _pending[room].append(msg)
    app.logger.info(f"[BUS] {msg.get('type')} -> '{room}' (listeners={len(subs)})")
    return len(subs)


def device_online(target):
    room = normalize_room(target)
    with _bus_lock:
        if _subscribers.get(room):
            return True
    return time.time() - _device_seen.get(room, 0) < 20


@app.route('/stream')
@device_required
def stream():
    client_id = (request.args.get('client_id') or '').strip()
    room = normalize_room(client_id)
    if not room:
        return "missing client_id", 400

    q = deque(maxlen=200)
    with _bus_lock:
        _subscribers[room].append(q)
        while _pending[room]:
            q.append(_pending[room].popleft())
        _device_seen[room] = time.time()

    @stream_with_context
    def event_stream():
        try:
            yield f"data: {json.dumps({'type': 'connected', 'client_id': client_id})}\n\n"
            last_keep = time.time()
            while True:
                if q:
                    msg = q.popleft()
                    if msg.get('_exp', 0) < time.time():
                        continue
                    yield f"data: {json.dumps({k: v for k, v in msg.items() if not k.startswith('_')})}\n\n"
                    last_keep = time.time()
                else:
                    if time.time() - last_keep > 15:
                        yield ": keepalive\n\n"
                        last_keep = time.time()
                        _device_seen[room] = time.time()
                    time.sleep(0.2)
        finally:
            with _bus_lock:
                try:
                    _subscribers[room].remove(q)
                except ValueError:
                    pass
                _device_seen[room] = time.time()
            app.logger.info(f"[SSE] closed '{room}'")

    return Response(event_stream(), headers={
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    })


# ---------------------------------------------------------------- online tracking
online_clients = {}   # cpr -> (last_heartbeat_ts, municipality)


def cleanup_old_heartbeats():
    now = time.time()
    for cpr, (ts, muni) in list(online_clients.items()):
        if now - ts > ONLINE_TIMEOUT:
            online_clients.pop(cpr, None)
            emit_for_client('client_status_update', {'client_cpr': cpr, 'status': 'offline'}, muni)


def _start_heartbeat_cleanup_thread():
    def _loop():
        while True:
            try:
                cleanup_old_heartbeats()
            except Exception as e:
                app.logger.error(f"Heartbeat cleanup error: {e}")
            time.sleep(15)
    threading.Thread(target=_loop, daemon=True).start()


# ---------------------------------------------------------------- Socket.IO (dashboard only)
@socketio.on('connect')
def handle_connect(auth=None):
    if not current_user.is_authenticated:
        return False
    if _is_superuser():
        join_room('admins')
    else:
        join_room(f"muni:{current_user.municipality or ''}")


# ---------------------------------------------------------------- device API
def _upsert_client(cpr, user=None):
    user = user or {}
    muni = (user.get('municipality') or '').strip()
    if muni.lower() == 'all':
        muni = ''
    client = Client.query.filter_by(cpr=cpr).first()
    if not client:
        client = Client(cpr=cpr, name=(user.get('name') or '').strip() or cpr, municipality=muni,
                        last_updated=datetime.now(), ip_address=request.remote_addr)
        for f in ('address', 'doctor_name', 'clinic_id', 'emergency_info', 'psychological_info'):
            setattr(client, f, user.get(f, '') or '')
        db.session.add(client)
        db.session.commit()
        return client
    changed = False
    # The robot may only fill empty fields; the nurse dashboard is the source of truth.
    if muni and not client.municipality:
        client.municipality, changed = muni, True
    name = (user.get('name') or '').strip()
    if name and (not client.name or client.name == client.cpr):
        client.name, changed = name, True
    if client.ip_address != request.remote_addr:
        client.ip_address, changed = request.remote_addr, True
    if changed:
        client.last_updated = datetime.now()
        db.session.commit()
    return client


@app.route('/api/client_heartbeat', methods=['POST'])
@device_required
def client_heartbeat():
    data = request.get_json(silent=True) or {}
    cpr = (data.get('client_id') or '').strip()
    if not cpr:
        return jsonify({'status': 'error', 'message': 'missing client_id'}), 400
    try:
        client = _upsert_client(cpr, data)
        muni = client.municipality
    except Exception as e:
        db.session.rollback()
        app.logger.error(f"Heartbeat upsert error for {cpr}: {e}")
        muni = ''
    was_online = cpr in online_clients
    online_clients[cpr] = (time.time(), muni)
    if not was_online:
        emit_for_client('client_status_update', {'client_cpr': cpr, 'status': 'online'}, muni)
    return jsonify({'status': 'success', 'server_time': datetime.utcnow().isoformat()})


@app.route('/api/client/<string:client_cpr>/medicines')
@device_required
def get_client_medicines_public(client_cpr):
    client = Client.query.filter_by(cpr=client_cpr).first()
    if not client:
        return jsonify({'status': 'error', 'message': 'Client not found'}), 404
    medicines = Medicine.query.filter_by(client_id=client.id).all()
    return jsonify({
        'status': 'success',
        'client_id': client.id,
        'client_name': client.name,
        'medicines': [m.to_dict() for m in medicines],
    })


@app.route('/api/client/<string:client_cpr>/visits', methods=['GET', 'POST'])
@device_required
def device_visits(client_cpr):
    client = Client.query.filter_by(cpr=client_cpr).first()
    if not client:
        return jsonify({'status': 'error', 'message': 'Client not found'}), 404
    if request.method == 'GET':
        visits = Visit.query.filter_by(client_id=client.id).order_by(Visit.date.desc(), Visit.time.desc()).limit(50).all()
        return jsonify({'status': 'success', 'visits': [v.to_dict() for v in visits]})
    data = request.get_json(silent=True) or {}
    if not data.get('date') or not data.get('time'):
        return jsonify({'status': 'error', 'message': 'Date and time are required'}), 400
    visit = Visit(client_id=client.id, user_name=client.name, nurse_name=data.get('nurse_name', ''),
                  date=data['date'], time=data['time'], next_visit_date=data.get('next_visit_date'),
                  notes=data.get('notes', ''))
    db.session.add(visit)
    db.session.commit()
    return jsonify({'status': 'success', 'visit': visit.to_dict()})


@app.route('/api/client/<string:client_cpr>/vitals', methods=['POST'])
@device_required
def device_vitals(client_cpr):
    client = Client.query.filter_by(cpr=client_cpr).first()
    if not client:
        return jsonify({'status': 'error', 'message': 'Client not found'}), 404
    data = request.get_json(silent=True) or {}

    def num(key, lo, hi):
        try:
            v = float(data.get(key))
            return v if lo <= v <= hi else None
        except (TypeError, ValueError):
            return None

    v = Vital(client_id=client.id, spo2=num('spo2', 50, 100), pulse=num('pulse', 20, 250),
              temp_object=num('temp_object', 15, 45), temp_ambient=num('temp_ambient', -20, 60))
    if v.spo2 is None and v.pulse is None and v.temp_object is None:
        return jsonify({'status': 'error', 'message': 'no valid values'}), 400
    db.session.add(v)
    db.session.commit()
    emit_for_client('vitals_update', {'client_id': client.id, 'vitals': v.to_dict()}, client.municipality)

    # Simple clinical thresholds -> alert (at most one per 10 minutes per client)
    problems = []
    if v.spo2 is not None and v.spo2 < 90:
        problems.append(f"SpO₂ {v.spo2:.0f}%")
    if v.pulse is not None and (v.pulse < 40 or v.pulse > 130):
        problems.append(f"pulse {v.pulse:.0f} bpm")
    if problems:
        recent = Notification.query.filter(
            Notification.client_id == client.id,
            Notification.note.like('Abnormal vital signs%'),
            Notification.date_time > datetime.utcnow() - timedelta(minutes=10)).first()
        if not recent:
            urgency = 'critical' if (v.spo2 is not None and v.spo2 < 85) else 'very_urgent'
            create_notification(client, "Abnormal vital signs: " + ", ".join(problems), urgency, 'Robot')
    return jsonify({'status': 'success'})


@app.route('/api/full_sync', methods=['POST'])
@device_required
def full_sync():
    """Kept for older robots. Medicines are managed ONLY on the server, so they are never
    deleted or overwritten here (this used to wipe nurse changes)."""
    data = request.get_json(silent=True) or {}
    try:
        cpr = ((data.get('user') or {}).get('cpr') or '').strip()
        if not cpr:
            return jsonify({'status': 'error', 'message': 'missing cpr'}), 400
        client = _upsert_client(cpr, data.get('user'))
        full = data.get('full_data') or {}
        if full.get('med_times'):
            MedTime.query.filter_by(client_id=client.id).delete()
            for mt in full['med_times']:
                db.session.add(MedTime(client_id=client.id, med_id=str(mt.get('id', '')), time=mt.get('time', ''),
                                       date=mt.get('date', ''), active=mt.get('active', True), taken=mt.get('taken', False)))
        if full.get('reminders'):
            Reminder.query.filter_by(client_id=client.id).delete()
            for r in full['reminders']:
                db.session.add(Reminder(client_id=client.id, rem_id=str(r.get('id', '')), date=r.get('date', ''), note=r.get('note', '')))
        db.session.commit()
        return jsonify({'status': 'success'})
    except Exception as e:
        db.session.rollback()
        app.logger.error(f"full_sync error: {e}")
        return jsonify({'status': 'error', 'message': 'sync failed'}), 500


@app.route('/api/find_client')
@device_required
def find_client():
    cpr = request.args.get('cpr')
    if not cpr:
        return jsonify({'status': 'error', 'message': 'CPR required'}), 400
    client = Client.query.filter_by(cpr=cpr).first()
    if not client:
        return jsonify({'status': 'error', 'message': 'Client not found'}), 404
    return jsonify({'status': 'success', 'id': client.id, 'name': client.name, 'cpr': client.cpr})


@app.route('/api/notification', methods=['POST'])
@device_required
def receive_notification():
    data = request.get_json(silent=True) or {}
    if 'client_id' not in data:
        return jsonify({'status': 'error', 'message': 'Invalid data'}), 400
    client = Client.query.filter_by(cpr=data['client_id']).first()
    if not client:
        return jsonify({'status': 'error', 'message': 'Client not found'}), 404
    create_notification(client, data.get('note', ''), data.get('urgency', 'normal'), data.get('user_name', ''))
    return jsonify({'status': 'success'})


# ---------------------------------------------------------------- camera
CAM_FRAMES = defaultdict(lambda: {"bytes": None, "ts": 0.0})


def _cam_key(client_id):
    return normalize_room(client_id)


@app.post("/api/clients/<client_id>/camera/frame")
@device_required
def api_camera_frame(client_id):
    if request.mimetype and "multipart/form-data" in request.mimetype:
        file = request.files.get("frame")
        if not file:
            abort(400, description="missing file 'frame'")
        data = file.read()
    else:
        data = request.get_data() or b""
    if not data:
        abort(400, description="empty frame")
    CAM_FRAMES[_cam_key(client_id)] = {"bytes": data, "ts": time.time()}
    return {"ok": True}


@app.post("/api/clients/<client_id>/camera/mjpeg_push")
@device_required
def api_camera_mjpeg_push(client_id):
    ctype = request.headers.get("Content-Type", "")
    m = re.search(r'boundary="?([^";]+)"?', ctype)
    if "multipart/x-mixed-replace" not in ctype or not m:
        raise BadRequest("Expect Content-Type: multipart/x-mixed-replace; boundary=...")
    b_boundary = ("--" + m.group(1)).encode("ascii")
    stream_in = request.stream
    key = _cam_key(client_id)

    def read_exact(n):
        buf = bytearray()
        while len(buf) < n:
            chunk = stream_in.read(n - len(buf))
            if not chunk:
                break
            buf.extend(chunk)
        return bytes(buf)

    while True:
        line = stream_in.readline()
        if not line:
            return {"ok": False, "msg": "closed before first boundary"}
        if line.strip().startswith(b_boundary):
            break
    while True:
        headers = {}
        while True:
            line = stream_in.readline()
            if not line:
                return {"ok": True}
            if line in (b"\r\n", b"\n"):
                break
            try:
                k, v = line.decode("latin-1").split(":", 1)
                headers[k.strip().lower()] = v.strip()
            except ValueError:
                continue
        try:
            n = int(headers.get("content-length", ""))
        except ValueError:
            raise BadRequest("Part missing Content-Length")
        jpeg = read_exact(n)
        if len(jpeg) != n:
            return {"ok": False, "msg": "short read"}
        CAM_FRAMES[key] = {"bytes": jpeg, "ts": time.time()}
        trail = stream_in.readline()
        while trail and not trail.strip().startswith(b_boundary):
            trail = stream_in.readline()
        if not trail:
            return {"ok": True}


def _find_client_by_cpr_or_id(client_ref):
    client = Client.query.filter_by(cpr=client_ref).first()
    if not client and str(client_ref).isdigit():
        client = Client.query.get(int(client_ref))
    return client


@app.get("/client/<client_id>/mjpeg")
@login_required
def client_mjpeg(client_id):
    client = _find_client_by_cpr_or_id(client_id)
    if not client or not _can_access_client(client):
        abort(403)
    key = _cam_key(client.cpr or str(client.id))

    @stream_with_context
    def generate():
        last_sent = 0.0
        started = time.time()
        while time.time() - started < 15 * 60:   # cap a single viewing session at 15 min
            frame = CAM_FRAMES[key]
            buf, ts = frame["bytes"], frame["ts"]
            if buf is None or ts <= last_sent:
                time.sleep(0.05)
                continue
            last_sent = ts
            yield (b"--frame\r\nContent-Type: image/jpeg\r\nContent-Length: " + str(len(buf)).encode()
                   + b"\r\n\r\n" + buf + b"\r\n")

    resp = Response(generate(), mimetype="multipart/x-mixed-replace; boundary=frame")
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    resp.headers["X-Accel-Buffering"] = "no"
    return resp


@app.route('/camera_proxy/<int:client_id>')
@login_required
def camera_proxy(client_id):
    client = get_client_or_403(client_id)
    stream_url = camera_stream_url(client.camera_url)
    if not stream_url:
        return "No camera URL", 400
    try:
        upstream = urllib.request.urlopen(urllib.request.Request(stream_url, headers={'User-Agent': 'Mozilla/5.0'}), timeout=10)
    except Exception as e:
        app.logger.error(f"Camera proxy error for {stream_url}: {e}")
        return "Camera not reachable", 502

    def generate():
        try:
            while True:
                chunk = upstream.read(4096)
                if not chunk:
                    break
                yield chunk
        finally:
            upstream.close()

    return Response(generate(), content_type=upstream.headers.get('Content-Type', 'multipart/x-mixed-replace'),
                    headers={'Cache-Control': 'no-cache'})


# ---------------------------------------------------------------- nurse actions on devices
def _lock_target(client):
    return (client.device_name or '').strip() or (client.cpr or '').strip()


@app.post('/client/<int:client_id>/camera/open')
@login_required
def camera_open(client_id):
    client = get_client_or_403(client_id)
    push_command(client.cpr, {'type': 'servo_set', 'angle': 180})
    create_notification(client, f"📷 Camera opened by {current_user.name}", 'normal', current_user.name, hidden=True)
    return jsonify({'ok': True, 'stream_url': url_for('client_mjpeg', client_id=(client.cpr or client.id))})


@app.post('/client/<int:client_id>/camera/close')
@login_required
def camera_close(client_id):
    client = get_client_or_403(client_id)
    push_command(client.cpr, {'type': 'servo_set', 'angle': 0})
    return jsonify({'ok': True})


def _lock_action(client_id, action):
    client = get_client_or_403(client_id)
    device = _lock_target(client)
    if not device:
        return jsonify({'status': 'error', 'message': 'No device name configured for this client'}), 400
    listeners = push_command(device, {'type': 'lock', 'action': action, 'target': device}, ttl=30)
    icon = '🔓' if action == 'open' else '🔒'
    create_notification(client, f"{icon} Door {action} sent by {current_user.name}", 'normal', current_user.name, hidden=True)
    return jsonify({'ok': True, 'delivered': listeners > 0,
                    'message': 'sent' if listeners else 'lock is offline - command will expire in 30s'})


@app.post('/client/<int:client_id>/lock/open')
@login_required
def lock_open(client_id):
    return _lock_action(client_id, 'open')


@app.post('/client/<int:client_id>/lock/close')
@login_required
def lock_close(client_id):
    return _lock_action(client_id, 'close')


@app.post('/client/<int:client_id>/servo/toggle')
@login_required
def toggle_client_servo(client_id):
    client = get_client_or_403(client_id)
    push_command(client.cpr, {'type': 'servo_toggle'})
    return jsonify({'status': 'ok'})


@app.post('/client/<int:client_id>/robot/say')
@login_required
def robot_say(client_id):
    client = get_client_or_403(client_id)
    data = request.get_json(silent=True) or request.form
    msg = (data.get('text') or '').strip()[:500]
    if not msg:
        return jsonify({'ok': False, 'message': 'empty message'}), 400
    listeners = push_command(client.cpr, {'type': 'say', 'text': msg, 'from': current_user.name}, ttl=120)
    return jsonify({'ok': True, 'delivered': listeners > 0})


@app.post('/client/<int:client_id>/robot/sync')
@login_required
def robot_sync(client_id):
    client = get_client_or_403(client_id)
    listeners = push_command(client.cpr, {'type': 'medicine_updated'})
    return jsonify({'ok': True, 'delivered': listeners > 0})


def notify_client_medicine_update(client):
    if client and client.cpr:
        push_command(client.cpr, {'type': 'medicine_updated'}, ttl=600)


# ---------------------------------------------------------------- notifications
@app.route('/notification/<int:notification_id>')
@login_required
def get_notification(notification_id):
    n = Notification.query.get_or_404(notification_id)
    client = Client.query.get(n.client_id)
    if client and not _can_access_client(client):
        abort(403)
    return jsonify({
        'id': n.id, 'user_name': n.user_name, 'client_name': client.name if client else '',
        'date_time': n.date_time.replace(tzinfo=timezone.utc).isoformat() if n.date_time else None,
        'urgency': n.urgency, 'note': n.note, 'client_id': n.client_id,
    })


@app.route('/notification/<int:notification_id>/hide', methods=['POST'])
@login_required
def hide_notification(notification_id):
    n = Notification.query.get_or_404(notification_id)
    client = Client.query.get(n.client_id)
    if client and not _can_access_client(client):
        abort(403)
    n.hidden = True
    db.session.commit()
    return jsonify({'status': 'success'})


@app.route('/client/<int:client_id>/notifications/hide_all', methods=['POST'])
@login_required
def hide_all_notifications(client_id):
    client = get_client_or_403(client_id)
    Notification.query.filter_by(client_id=client.id, hidden=False).update({'hidden': True})
    db.session.commit()
    return jsonify({'status': 'success'})


@app.route('/client/<int:client_id>/notifications')
@login_required
def notification_history(client_id):
    client = get_client_or_403(client_id)
    notifications = Notification.query.filter_by(client_id=client.id).order_by(Notification.date_time.desc()).limit(500).all()
    return render_template('notification_history.html', client=client, notifications=notifications)


# ---------------------------------------------------------------- pages
@app.route('/')
@login_required
def dashboard():
    municipality_filter = request.args.get('municipality', '')
    q = Client.query
    if _is_superuser():
        if municipality_filter:
            q = q.filter_by(municipality=municipality_filter)
    else:
        q = q.filter_by(municipality=current_user.municipality)
    clients = q.order_by(Client.name.asc()).all()
    ids = [c.id for c in clients]

    municipalities = []
    if _is_superuser():
        municipalities = sorted(m[0] for m in db.session.query(Client.municipality).distinct().all() if m[0])

    unread = defaultdict(list)
    latest_vitals = {}
    if ids:
        for n in (Notification.query.filter(Notification.client_id.in_(ids), Notification.hidden == False)  # noqa: E712
                  .order_by(Notification.date_time.desc()).limit(500).all()):
            unread[n.client_id].append(n)
        sub = (db.session.query(Vital.client_id, func.max(Vital.id).label('mid'))
               .filter(Vital.client_id.in_(ids)).group_by(Vital.client_id).subquery())
        for v in Vital.query.join(sub, Vital.id == sub.c.mid).all():
            latest_vitals[v.client_id] = v

    for c in clients:
        c.unread_notifications = unread.get(c.id, [])
        c.is_online = c.cpr in online_clients
        c.latest_vital = latest_vitals.get(c.id)

    stats = {
        'total': len(clients),
        'online': sum(1 for c in clients if c.is_online),
        'alerts': sum(len(c.unread_notifications) for c in clients),
        'critical': sum(1 for c in clients for n in c.unread_notifications if n.urgency in ('critical', 'very_urgent')),
    }
    return render_template('dashboard.html', clients=clients, municipalities=municipalities,
                           current_filter=municipality_filter, stats=stats)


@app.route('/client/<int:client_id>')
@login_required
def client_detail(client_id):
    client = get_client_or_403(client_id)
    medicines = Medicine.query.filter_by(client_id=client.id).all()
    visits = Visit.query.filter_by(client_id=client.id).order_by(Visit.date.desc(), Visit.time.desc()).limit(5).all()
    notifications = Notification.query.filter_by(client_id=client.id).order_by(Notification.date_time.desc()).limit(8).all()
    vitals = Vital.query.filter_by(client_id=client.id).order_by(Vital.id.desc()).limit(12).all()
    return render_template('client_detail.html',
                           client=client, medicines=medicines, visits=visits,
                           notifications=notifications, vitals=vitals,
                           notification_count=Notification.query.filter_by(client_id=client.id).count(),
                           visit_count=Visit.query.filter_by(client_id=client.id).count(),
                           robot_online=client.cpr in online_clients,
                           lock_online=device_online(_lock_target(client)) if (client.device_name and client.device_name != client.cpr) else None,
                           camera_live=(time.time() - CAM_FRAMES[_cam_key(client.cpr or str(client.id))]['ts']) < 10)


@app.route('/api/client/<int:client_id>/live')
@login_required
def client_live(client_id):
    client = get_client_or_403(client_id)
    v = Vital.query.filter_by(client_id=client.id).order_by(Vital.id.desc()).first()
    return jsonify({
        'robot_online': client.cpr in online_clients,
        'lock_online': device_online(_lock_target(client)) if (client.device_name and client.device_name != client.cpr) else None,
        'camera_live': (time.time() - CAM_FRAMES[_cam_key(client.cpr or str(client.id))]['ts']) < 10,
        'vitals': v.to_dict() if v else None,
    })


@app.route('/client/<int:client_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_client(client_id):
    client = get_client_or_403(client_id)
    if request.method == 'POST':
        for f in ('name', 'address', 'doctor_name', 'clinic_id', 'emergency_info', 'psychological_info', 'camera_url', 'device_name'):
            setattr(client, f, (request.form.get(f, getattr(client, f)) or '').strip())
        if _is_superuser():
            client.municipality = (request.form.get('municipality', client.municipality) or '').strip()
        client.last_updated = datetime.now()
        db.session.commit()
        flash("Client updated successfully", "success")
        return redirect(url_for('client_detail', client_id=client.id))
    return render_template('edit_client.html', client=client)


@app.post('/client/<int:client_id>/delete')
@login_required
def delete_client(client_id):
    client = get_client_or_403(client_id)
    try:
        for model in (Medicine, MedTime, Reminder, Notification, Visit, Vital):
            model.query.filter_by(client_id=client.id).delete()
        db.session.delete(client)
        db.session.commit()
        flash("Client deleted successfully.", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Error deleting client: {e}", "error")
    return redirect(url_for('dashboard'))


# ----- medicines (server UI is the single source of truth)
def _medicine_from_form(medicine):
    medicine.name = (request.form.get('name') or '').strip()
    medicine.side_effect = request.form.get('side_effect', '')
    medicine.active = 'active' in request.form
    medicine.schedule_type = request.form.get('schedule_type', 'daily')
    medicine.set_days_list(request.form.getlist('days') if medicine.schedule_type == 'weekly' else [])
    times = []
    for t in (request.form.get('times') or '').split(','):
        t = t.strip()
        if not t:
            continue
        try:
            times.append(datetime.strptime(t, '%H:%M').strftime('%H:%M'))
        except ValueError:
            raise ValueError(f"Invalid time '{t}' (use HH:MM, e.g. 08:00)")
    medicine.set_times_list(times)
    if not medicine.name:
        raise ValueError("Medicine name is required")


@app.route('/client/<int:client_id>/manage_medicines')
@login_required
def manage_client_medicines(client_id):
    client = get_client_or_403(client_id)
    medicines = Medicine.query.filter_by(client_id=client.id).all()
    return render_template('manage_medicines.html', client=client, medicines=medicines)


@app.route('/client/<int:client_id>/medicines/add', methods=['GET', 'POST'])
@login_required
def add_medicine_server(client_id):
    client = get_client_or_403(client_id)
    if request.method == 'POST':
        medicine = Medicine(client_id=client.id)
        try:
            _medicine_from_form(medicine)
            db.session.add(medicine)
            db.session.commit()
        except ValueError as e:
            db.session.rollback()
            flash(str(e), "error")
            return render_template('add_medicine.html', client=client)
        notify_client_medicine_update(client)
        flash("Medicine added - the robot updates within seconds", "success")
        return redirect(url_for('manage_client_medicines', client_id=client.id))
    return render_template('add_medicine.html', client=client)


@app.route('/medicine/<int:medicine_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_medicine_server(medicine_id):
    medicine = Medicine.query.get_or_404(medicine_id)
    client = get_client_or_403(medicine.client_id)
    if request.method == 'POST':
        try:
            _medicine_from_form(medicine)
            db.session.commit()
        except ValueError as e:
            db.session.rollback()
            flash(str(e), "error")
            return render_template('edit_medicine.html', medicine=medicine, client=client)
        notify_client_medicine_update(client)
        flash("Medicine updated - the robot updates within seconds", "success")
        return redirect(url_for('manage_client_medicines', client_id=client.id))
    return render_template('edit_medicine.html', medicine=medicine, client=client)


@app.route('/medicine/<int:medicine_id>/delete', methods=['POST'])
@login_required
def delete_medicine_server(medicine_id):
    medicine = Medicine.query.get_or_404(medicine_id)
    client = get_client_or_403(medicine.client_id)
    db.session.delete(medicine)
    db.session.commit()
    notify_client_medicine_update(client)
    flash("Medicine deleted", "success")
    return redirect(url_for('manage_client_medicines', client_id=client.id))


# JSON medicine API for the dashboard / integrations
@app.route('/client/<int:client_id>/medicines', methods=['GET', 'POST'])
@login_required
def client_medicines_api(client_id):
    client = get_client_or_403(client_id)
    if request.method == 'GET':
        return jsonify({'medicines': [m.to_dict() for m in Medicine.query.filter_by(client_id=client.id).all()]})
    data = request.get_json(silent=True) or {}
    if not data.get('name'):
        return jsonify({'status': 'error', 'message': 'name required'}), 400
    m = Medicine(client_id=client.id, name=data['name'], side_effect=data.get('side_effect', ''),
                 active=data.get('active', True), schedule_type=data.get('schedule_type', 'daily'))
    m.set_days_list(data.get('days', []))
    m.set_times_list(data.get('times', []))
    db.session.add(m)
    db.session.commit()
    notify_client_medicine_update(client)
    return jsonify({'status': 'success', 'medicine': m.to_dict()})


@app.route('/medicine/<int:medicine_id>', methods=['PUT', 'DELETE'])
@login_required
def medicine_api(medicine_id):
    m = Medicine.query.get_or_404(medicine_id)
    client = get_client_or_403(m.client_id)
    if request.method == 'DELETE':
        db.session.delete(m)
    else:
        data = request.get_json(silent=True) or {}
        m.name = data.get('name', m.name)
        m.side_effect = data.get('side_effect', m.side_effect)
        m.active = data.get('active', m.active)
        m.schedule_type = data.get('schedule_type', m.schedule_type)
        if 'days' in data:
            m.set_days_list(data['days'])
        if 'times' in data:
            m.set_times_list(data['times'])
    db.session.commit()
    notify_client_medicine_update(client)
    return jsonify({'status': 'success'})


# ----- visits
@app.route('/client/<int:client_id>/add_visit', methods=['GET', 'POST'])
@login_required
def add_visit(client_id):
    client = get_client_or_403(client_id)
    if request.method == 'POST':
        visit = Visit(client_id=client.id, user_name=client.name, nurse_id=current_user.id,
                      nurse_name=current_user.name, date=request.form['date'], time=request.form['time'],
                      next_visit_date=request.form.get('next_visit_date'), notes=request.form.get('notes', ''))
        db.session.add(visit)
        db.session.commit()
        flash("Visit recorded successfully", "success")
        return redirect(url_for('client_detail', client_id=client.id))
    return render_template('add_visit.html', client=client, now=datetime.now(LOCAL_TZ))


@app.route('/client/<int:client_id>/visits')
@login_required
def visit_history(client_id):
    client = get_client_or_403(client_id)
    visits = Visit.query.filter_by(client_id=client.id).order_by(Visit.date.desc(), Visit.time.desc()).all()
    if request.args.get('format') == 'json' or (request.accept_mimetypes.best == 'application/json'):
        return jsonify({'status': 'success', 'visits': [v.to_dict() for v in visits]})
    return render_template('visit_history.html', client=client, visits=visits)


@app.route('/visits')
@login_required
def view_visits():
    return redirect(url_for('dashboard'))


# ---------------------------------------------------------------- auth & users
_login_failures = defaultdict(list)   # ip -> [timestamps]


def _safe_next(target):
    if target and target.startswith('/') and not target.startswith('//'):
        return target
    return None


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        ip = request.remote_addr or '?'
        recent = [t for t in _login_failures[ip] if time.time() - t < 300]
        _login_failures[ip] = recent
        if len(recent) >= 8:
            flash('Too many failed attempts. Try again in a few minutes.', 'error')
            return render_template('login.html'), 429
        cpr = (request.form.get('cpr') or '').strip()
        password = request.form.get('password') or ''
        nurse = Nurse.query.filter_by(cpr=cpr).first()
        if nurse and check_password_hash(nurse.password, password):
            _login_failures.pop(ip, None)
            login_user(nurse, remember=bool(request.form.get('remember')))
            if password in ('admin', '1234', 'password') or len(password) < 8:
                flash('Your password is weak/default. Please choose a new one now.', 'error')
                return redirect(url_for('change_password'))
            return redirect(_safe_next(request.args.get('next')) or url_for('dashboard'))
        _login_failures[ip].append(time.time())
        flash('Invalid CPR or password', 'error')
    return render_template('login.html')


@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))


def _admin_only():
    if not current_user.is_admin:
        flash('Admin access required', 'error')
        return redirect(url_for('dashboard'))
    return None


@app.route('/admin/create_user', methods=['GET', 'POST'])
@login_required
def create_user():
    denied = _admin_only()
    if denied:
        return denied
    if request.method == 'POST':
        cpr = (request.form.get('cpr') or '').strip()
        pw = request.form.get('password') or ''
        if len(pw) < 8:
            flash('Password must be at least 8 characters', 'error')
        elif Nurse.query.filter_by(cpr=cpr).first():
            flash('A user with this CPR already exists', 'error')
        else:
            db.session.add(Nurse(cpr=cpr, name=request.form.get('name'), phone_number=request.form.get('phone'),
                                 municipality=(request.form.get('municipality') or '').strip(),
                                 password=generate_password_hash(pw), is_admin='is_admin' in request.form))
            db.session.commit()
            flash('User created successfully', 'success')
            return redirect(url_for('manage_users'))
    return render_template('create_user.html')


@app.route('/admin/manage_users')
@login_required
def manage_users():
    denied = _admin_only()
    if denied:
        return denied
    return render_template('manage_users.html', nurses=Nurse.query.order_by(Nurse.name).all())


@app.route('/admin/edit_user/<int:user_id>', methods=['GET', 'POST'])
@login_required
def edit_user(user_id):
    denied = _admin_only()
    if denied:
        return denied
    nurse = Nurse.query.get_or_404(user_id)
    if request.method == 'POST':
        nurse.cpr = request.form.get('cpr', nurse.cpr)
        nurse.name = request.form.get('name', nurse.name)
        nurse.phone_number = request.form.get('phone', nurse.phone_number)
        nurse.municipality = (request.form.get('municipality', nurse.municipality) or '').strip()
        nurse.is_admin = 'is_admin' in request.form
        pw = request.form.get('password')
        if pw:
            if len(pw) < 8:
                flash('Password must be at least 8 characters', 'error')
                return render_template('edit_user.html', nurse=nurse)
            nurse.password = generate_password_hash(pw)
        db.session.commit()
        flash('User updated successfully', 'success')
        return redirect(url_for('manage_users'))
    return render_template('edit_user.html', nurse=nurse)


@app.route('/admin/delete_user/<int:user_id>', methods=['POST'])
@login_required
def delete_user(user_id):
    denied = _admin_only()
    if denied:
        return denied
    if current_user.id == user_id:
        flash('You cannot delete your own account', 'error')
        return redirect(url_for('manage_users'))
    db.session.delete(Nurse.query.get_or_404(user_id))
    db.session.commit()
    flash('User deleted successfully', 'success')
    return redirect(url_for('manage_users'))


@app.route('/change_password', methods=['GET', 'POST'])
@login_required
def change_password():
    if request.method == 'POST':
        current_password = request.form.get('current_password') or ''
        new_password = request.form.get('new_password') or ''
        if not check_password_hash(current_user.password, current_password):
            flash('Current password is incorrect', 'error')
        elif new_password != request.form.get('confirm_password'):
            flash('New passwords do not match', 'error')
        elif len(new_password) < 8 or new_password in ('admin', 'password'):
            flash('Password must be at least 8 characters and not a default password', 'error')
        else:
            current_user.password = generate_password_hash(new_password)
            db.session.commit()
            flash('Password changed successfully', 'success')
            return redirect(url_for('dashboard'))
    return render_template('change_password.html')


# ---------------------------------------------------------------- misc
@login_manager.user_loader
def load_user(user_id):
    return Nurse.query.get(int(user_id))


@app.errorhandler(403)
def forbidden(_e):
    if _wants_json():
        return jsonify({'status': 'error', 'message': 'Access denied'}), 403
    flash("You don't have access to this client", "error")
    return redirect(url_for('dashboard'))


@app.route('/health')
def health_check():
    try:
        db.session.execute(text('SELECT 1'))
        return jsonify({'status': 'healthy', 'database': 'connected'})
    except Exception as e:
        app.logger.error(f"Health check failed: {e}")
        return jsonify({'status': 'unhealthy'}), 500


@app.route('/api/connection_status')
@login_required
def connection_status():
    if not current_user.is_admin:
        abort(403)
    with _bus_lock:
        listeners = {k: len(v) for k, v in _subscribers.items() if v}
        pending = {k: len(v) for k, v in _pending.items() if v}
    return jsonify({'listeners': listeners, 'pending': pending,
                    'online_clients': {k: v[0] for k, v in online_clients.items()}})


def init_db():
    with app.app_context():
        db.create_all()
        if not Nurse.query.filter_by(is_admin=True).first():
            pw = os.environ.get('ADMIN_PASSWORD') or 'admin'
            db.session.add(Nurse(cpr='admin', name='Admin', phone_number='', municipality='all',
                                 password=generate_password_hash(pw), is_admin=True))
            db.session.commit()
            logging.warning("Created default admin user 'admin' - change the password after the first login.")


def create_app():
    return app


# Create missing tables (e.g. Vital) also when started by gunicorn.
try:
    init_db()
except Exception as _e:  # never block startup because of this
    logging.error(f"init_db failed: {_e}")

_start_heartbeat_cleanup_thread()


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    socketio.run(app, host='0.0.0.0', port=port, debug=False)
