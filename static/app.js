// Shared helpers for the nurse dashboard
(function () {
  const LC = (window.LC = window.LC || {});

  LC.escape = function (s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
    }[c]));
  };

  LC.toast = function (text, kind, onClick) {
    let stack = document.querySelector('.toast-stack');
    if (!stack) {
      stack = document.createElement('div');
      stack.className = 'toast-stack';
      document.body.appendChild(stack);
    }
    const t = document.createElement('div');
    t.className = 'toast ' + (kind || '');
    t.textContent = text;
    t.onclick = () => { if (onClick) onClick(); t.remove(); };
    stack.appendChild(t);
    setTimeout(() => t.remove(), kind === 'critical' ? 15000 : 6000);
  };

  LC.post = async function (url, body) {
    const res = await fetch(url, {
      method: 'POST',
      credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json', 'X-Requested-With': 'fetch' },
      body: JSON.stringify(body || {})
    });
    let data = {};
    try { data = await res.json(); } catch (_) {}
    if (!res.ok) throw new Error(data.message || ('HTTP ' + res.status));
    return data;
  };

  LC.beep = function () {
    try {
      const ctx = new (window.AudioContext || window.webkitAudioContext)();
      const o = ctx.createOscillator(); const g = ctx.createGain();
      o.connect(g); g.connect(ctx.destination); o.frequency.value = 880; g.gain.value = 0.08;
      o.start(); setTimeout(() => { o.stop(); ctx.close(); }, 350);
    } catch (_) {}
  };

  LC.fmtTime = function (iso) {
    if (!iso) return '';
    const d = new Date(iso);
    return isNaN(d) ? iso : d.toLocaleString();
  };

  // Socket.IO with a small event bus
  const handlers = {};
  LC.on = function (evt, fn) { (handlers[evt] = handlers[evt] || []).push(fn); };
  function dispatch(evt, data) { (handlers[evt] || []).forEach(fn => { try { fn(data); } catch (e) { console.error(e); } }); }

  document.addEventListener('DOMContentLoaded', function () {
    if (typeof io === 'undefined' || !document.body.dataset.auth) return;
    const dot = document.getElementById('liveDot');
    const socket = io({ transports: ['websocket', 'polling'] });
    socket.on('connect', () => dot && dot.classList.add('on'));
    socket.on('disconnect', () => dot && dot.classList.remove('on'));
    ['client_status_update', 'new_notification', 'vitals_update'].forEach(evt =>
      socket.on(evt, data => dispatch(evt, data)));

    // global alert toast on every page
    LC.on('new_notification', data => {
      const n = data.notification || {};
      if (n.urgency === 'critical' || n.urgency === 'very_urgent') LC.beep();
      LC.toast(`${data.client_name || 'Client'}: ${n.note}`, n.urgency,
        () => { window.location.href = '/client/' + data.client_id; });
    });
  });
})();
