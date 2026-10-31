const express = require('express');
const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');
const TelegramBot = require('node-telegram-bot-api');
const Database = require('better-sqlite3');
const bcrypt = ('bcryptjs');
const jwt = require('jsonwebtoken');
const path = require('path');

const PORT = process.env.PORT || 3000;
const BOT_TOKEN = process.env.BOT_TOKEN;
const ADMIN_CHAT_ID = parseInt(process.env.ADMIN_CHAT_ID, 10);
const JWT_SECRET = process.env.JWT_SECRET || 'zedicius-secret-change-me';
const DATA_DIR = process.env.DATA_DIR || '.';

if (!BOT_TOKEN || !ADMIN_CHAT_ID || isNaN(ADMIN_CHAT_ID)) {
  console.error('❌ Missing BOT_TOKEN or ADMIN_CHAT_ID.');
  process.exit(1);
}

const dbPath = path.join(DATA_DIR, 'zedicius.db');
const db = new Database(dbPath);
db.pragma('journal_mode = WAL');

db.exec(`
  CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    phone TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
  );
  CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_ref TEXT UNIQUE NOT NULL,
    user_id INTEGER NOT NULL,
    items TEXT NOT NULL,
    total INTEGER NOT NULL,
    delivery_method TEXT,
    delivery_number TEXT,
    payment_message TEXT,
    status TEXT DEFAULT 'pending',
    created_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (user_id) REFERENCES users(id)
  );
`);

console.log(`📦 DB: ${dbPath}`);

const app = express();
app.use(express.json({ limit: '20kb' }));
app.use(express.static(__dirname));

function authRequired(req, res, next) {
  const token = req.headers.authorization?.replace('Bearer ', '');
  if (!token) return res.status(401).json({ error: 'Not logged in. Please login first.' });
  try {
    const payload = jwt.verify(token, JWT_SECRET);
    req.userId = payload.userId;
    req.phone = payload.phone;
    next();
  } catch {
    return res.status(401).json({ error: 'Session expired. Please login again.' });
  }
}

app.post('/api/register', async (req, res) => {
  const { phone, password } = req.body;
  if (!phone || !password) return res.status(400).json({ error: 'Phone and password required' });
  const cleanPhone = phone.replace(/\s/g, '');
  if (!/^(\+?254|0)?7\d{8}$/.test(cleanPhone) && !/^(\+?254|0)?1\d{8}$/.test(cleanPhone)) {
    return res.status(400).json({ error: 'Invalid phone. Use: 07XXXXXXXX' });
  }
  if (password.length < 6) return res.status(400).json({ error: 'Password must be at least 6 characters' });

  const existing = db.prepare('SELECT id FROM users WHERE phone = ?').get(cleanPhone);
  if (existing) return res.status(409).json({ error: 'Phone already registered. Try logging in.' });

  const hash = await bcrypt.hash(password, 10);
  const info = db.prepare('INSERT INTO users (phone, password_hash) VALUES (?, ?)').run(cleanPhone, hash);
  const token = jwt.sign({ userId: info.lastInsertRowid, phone: cleanPhone }, JWT_SECRET, { expiresIn: '7d' });
  res.json({ token, phone: cleanPhone });
});

app.post('/api/login', async (req, res) => {
  const { phone, password } = req.body;
  if (!phone || !password) return res.status(400).json({ error: 'Phone and password required' });
  const cleanPhone = phone.replace(/\s/g, '');

  const user = db.prepare('SELECT * FROM users WHERE phone = ?').get(cleanPhone);
  if (!user) return res.status(401).json({ error: 'Invalid phone or password' });

  const match = await bcrypt.compare(password, user.password_hash);
  if (!match) return res.status(401).json({ error: 'Invalid phone or password' });

  const token = jwt.sign({ userId: user.id, phone: user.phone }, JWT_SECRET, { expiresIn: '7d' });
  res.json({ token, phone: user.phone });
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });
const activeOrders = new Map();
const ORDER_TTL_MS = 30 * 60 * 1000;

setInterval(() => {
  const now = Date.now();
  for (const [id, order] of activeOrders) {
    if (now - order.createdAt > ORDER_TTL_MS) {
      if (order.ws && order.ws.readyState === WebSocket.OPEN) {
        order.ws.send(JSON.stringify({ type: 'expired' }));
        order.ws.close(1000);
      }
      activeOrders.delete(id);
    }
  }
}, 60 * 1000);

wss.on('connection', (ws) => {
  let boundOrderId = null;
  ws.on('message', (data) => {
    try {
      const { orderId } = JSON.parse(data.toString());
      if (activeOrders.has(orderId)) {
        const order = activeOrders.get(orderId);
        if (order.ws && order.ws !== ws && order.ws.readyState === WebSocket.OPEN) order.ws.close(1000);
        order.ws = ws;
        boundOrderId = orderId;
      }
    } catch {}
  });
  ws.on('close', () => {
    if (boundOrderId && activeOrders.has(boundOrderId)) {
      const order = activeOrders.get(boundOrderId);
      if (order.ws === ws) order.ws = null;
    }
  });
  ws.on('error', () => {
    if (boundOrderId && activeOrders.has(boundOrderId)) {
      const order = activeOrders.get(boundOrderId);
      if (order.ws === ws) order.ws = null;
    }
  });
});

app.post('/api/create-order', authRequired, (req, res) => {
  const { items, paymentMessage, deliveryMethod, deliveryNumber } = req.body;
  if (!items || !Array.isArray(items) || items.length === 0) return res.status(400).json({ error: 'Cart is empty' });
  if (!paymentMessage) return res.status(400).json({ error: 'Payment message required' });
  if (!deliveryMethod) return res.status(400).json({ error: 'Select delivery method' });
  if (!deliveryNumber) return res.status(400).json({ error: 'Delivery number required' });

  const total = items.reduce((s, i) => s + i.price, 0);
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let orderId = '';
  for (let i = 0; i < 6; i++) orderId += chars[Math.floor(Math.random() * chars.length)];

  db.prepare('INSERT INTO orders (order_ref, user_id, items, total, delivery_method, delivery_number, payment_message, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?)')
    .run(orderId, req.userId, JSON.stringify(items), total, deliveryMethod, deliveryNumber, paymentMessage.substring(0, 4000), 'pending');

  activeOrders.set(orderId, { orderId, userId: req.userId, phone: req.phone, items, total, deliveryMethod, deliveryNumber, ws: null, createdAt: Date.now() });

  const itemsText = items.map(i => `  • ${i.label} (${i.category}) — KSh ${i.price}`).join('\n');
  const adminMsg =
    `🔔 NEW ORDER: ${orderId}\n` +
    `━━━━━━━━━━━━━━━━━\n` +
    `👤 Customer: ${req.phone}\n` +
    `📦 Items:\n${itemsText}\n` +
    `━━━━━━━━━━━━━━━━━\n` +
    `💰 Total: KSh ${total.toLocaleString()}\n` +
    `📲 Deliver: ${deliveryMethod} → ${deliveryNumber}\n` +
    `━━━━━━━━━━━━━━━━━\n` +
    `💬 Payment Message:\n${paymentMessage.substring(0, 3000)}\n` +
    `━━━━━━━━━━━━━━━━━\n` +
    `⚠️ Verify the payment above. Is it correct?`;

  const keyboard = {
    inline_keyboard: [
      [{ text: '✅ Correct Payment', callback_data: `correct:${orderId}` }],
      [{ text: '❌ Wrong Payment', callback_data: `wrong:${orderId}` }]
    ]
  };

  bot.sendMessage(ADMIN_CHAT_ID, adminMsg, { reply_markup: keyboard }).catch(err =>
    console.error('Failed to notify admin:', err.message)
  );

  res.json({ orderId, total });
});

const bot = new TelegramBot(BOT_TOKEN, { polling: true });

bot.on('callback_query', (query) => {
  if (query.from.id !== ADMIN_CHAT_ID) return;
  const [action, orderId] = query.data.split(':');
  const order = activeOrders.get(orderId);

  if (!order) {
    bot.answerCallbackQuery(query.id, { text: '❌ Order already processed', show_alert: true });
    return;
  }

  if (action === 'correct') {
    if (order.ws && order.ws.readyState === WebSocket.OPEN) {
      order.ws.send(JSON.stringify({ type: 'confirmed', orderId, deliveryMethod: order.deliveryMethod, deliveryNumber: order.deliveryNumber, total: order.total }));
    }
    db.prepare("UPDATE orders SET status = 'confirmed' WHERE order_ref = ?").run(orderId);
    bot.editMessageText(
      `✅ CONFIRMED: ${orderId}\nPayment verified.\nOdds will be sent via ${order.deliveryMethod} to ${order.deliveryNumber}.`,
      { chat_id: ADMIN_CHAT_ID, message_id: query.message.message_id }
    ).catch(() => {});
    bot.answerCallbackQuery(query.id, { text: '✅ Confirmed' });
  } else if (action === 'wrong') {
    if (order.ws && order.ws.readyState === WebSocket.OPEN) {
      order.ws.send(JSON.stringify({ type: 'rejected', orderId, expectedAmount: order.total }));
    }
    db.prepare("UPDATE orders SET status = 'rejected' WHERE order_ref = ?").run(orderId);
    bot.editMessageText(
      `❌ REJECTED: ${orderId}\nPayment incorrect. User notified to try again.`,
      { chat_id: ADMIN_CHAT_ID, message_id: query.message.message_id }
    ).catch(() => {});
    bot.answerCallbackQuery(query.id, { text: '❌ Rejected' });
  }

  activeOrders.delete(orderId);
});

bot.on('message', (msg) => {
  if (msg.from.id !== ADMIN_CHAT_ID) return;
  if ((msg.text || '').trim() === '/start') {
    bot.sendMessage(msg.chat.id,
      '👋 Zedicius QuruFixed Tips Bot\n\n' +
      'You will receive orders here.\n' +
      'Click ✅ Correct or ❌ Wrong to respond to the user.'
    );
  }
});

app.get('/healthz', (_req, res) => res.sendStatus(200));
app.get('*', (_req, res) => res.sendFile(path.join(__dirname, 'index.html')));

server.listen(PORT, () => {
  console.log(`✅ Zedicius QuruFixed Tips running on port ${PORT}`);
  console.log(`🤖 Bot polling (admin: ${ADMIN_CHAT_ID})`);
});

process.on('SIGTERM', () => {
  console.log('SIGTERM. Shutting down...');
  wss.clients.forEach(ws => ws.close(1001));
  bot.stopPolling();
  db.close();
  process.exit(0);
});   
