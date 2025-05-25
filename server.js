const express = require('express');
const path = require('path');
const http = require('http');
const WebSocket = require('ws');
const mysql = require('mysql2/promise');
const cors = require('cors');
const fs = require('fs');
require('dotenv').config();

const { Kafka } = require('kafkajs');
const startKafkaConsumer = require('./kafkaConsumer');

const app = express();
const PORT = process.env.PORT || 9094;

const kafka = new Kafka({ brokers: [process.env.KAFKA_BROKERS] });
const producer = kafka.producer();

const sessionMap = {};
const clients = new Set();

app.use(cors({
  origin: ['http://localhost:3000', 'http://frontend:3000'],
  credentials: true
}));

app.use(express.json());

// Serve Unity WebGL games
app.use('/games/:gameName', (req, res, next) => {
  const gameName = req.params.gameName.toLowerCase();
  const gamePath = path.join(__dirname, 'games', gameName);
  if (!fs.existsSync(gamePath)) return res.status(404).send('Game not found');
  express.static(gamePath)(req, res, next);
});

app.get('/games/:gameName', (req, res) => {
  const gameName = req.params.gameName.toLowerCase();
  const indexPath = path.join(__dirname, 'games', gameName, 'index.html');
  if (!fs.existsSync(indexPath)) return res.status(404).send('index.html not found');
  res.sendFile(indexPath);
});

app.get('/play/:userId', (req, res) => {
  const userId = req.params.userId;
  const game = sessionMap[userId];
  if (!game) return res.status(404).send('User has no game assigned');
  const indexPath = path.join(__dirname, 'games', game, 'index.html');
  if (!fs.existsSync(indexPath)) return res.status(404).send('Game not found');
  res.sendFile(indexPath);
});

// MySQL
const dbPool = mysql.createPool({
  host: process.env.DB_HOST || 'localhost',
  user: process.env.DB_USER || 'root',
  password: process.env.DB_PASSWORD || 'admin',
  database: process.env.DB_NAME || 'kala',
  port: process.env.DB_PORT || 3306,
});

// WebSocket
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

wss.on('connection', (ws, req) => {
  const params = new URLSearchParams(req.url.replace(/^.*\?/, ''));
  const userId = params.get('userId');
  ws.userId = userId;
  clients.add(ws);
  console.log(`WebSocket client connected (userId: ${userId})`);
  ws.send(JSON.stringify({ type: 'welcome', message: 'WebSocket connected.' }));

  ws.on('message', async (msg) => {
    try {
      const { userId, game } = JSON.parse(msg);
      if (!userId || !game) {
        console.warn('Invalid message format');
        return;
      }
      console.log(`📨 Sending to Kafka: ${userId} → ${game}`);
      await producer.send({
        topic: process.env.KAFKA_TOPIC,
        messages: [{ value: JSON.stringify({ userId, game }) }]
      });
    } catch (err) {
      console.error('Error handling WS message:', err);
    }
  });

  ws.on('close', () => {
    clients.delete(ws);
    console.log(`WebSocket client disconnected (${userId})`);
  });

  ws.on('error', (err) => {
    clients.delete(ws);
    console.error(`WS error (${userId}):`, err);
  });
});

// Save basic metrics
app.post('/send-metrics/:game', async (req, res) => {
  const { userId, tiempo, puntaje, errores } = req.body;
  const game = req.params.game.toLowerCase();
  if (!userId || tiempo == null || puntaje == null || errores == null) {
    return res.status(400).send('Missing metrics fields.');
  }

  const table = `${game}_metrics`;
  try {
    const conn = await dbPool.getConnection();
    await conn.query(
      `INSERT INTO \`${table}\` (user_id, tiempo, puntaje, errores) VALUES (?, ?, ?, ?)`,
      [userId, tiempo, puntaje, errores]
    );
    conn.release();
    console.log(`Metrics inserted into ${table}`);
    res.send('Metrics saved.');
  } catch (err) {
    console.error(`DB error (${table}):`, err);
    res.status(500).send('DB error.');
  }
});

// Save full cajero_actividad session
app.post('/send-cajero-actividad', async (req, res) => {
  const data = req.body;

  if (!data.userId || !data.sessionTimestamp || !data.activityResults) {
    return res.status(400).send('Missing required fields');
  }

  const fields = ['user_id', 'session_timestamp'];
  const values = [data.userId, data.sessionTimestamp];

  for (let i = 1; i <= 5; i++) {
    const key = `interaction_${i}`;
    const result = data.activityResults[key] || {};
    fields.push(`interaction_${i}_time`);
    fields.push(`interaction_${i}_correct_change`);
    fields.push(`interaction_${i}_delta_change`);
    fields.push(`interaction_${i}_identified_well`);

    values.push(result.interactionTime ?? null);
    values.push(result.customerCorrectChange ?? null);
    values.push(result.deltaChange ?? null);
    values.push(result.identifiedWell ?? null);
  }

  const placeholders = fields.map(() => '?').join(',');
  const sql = `INSERT INTO cajero_actividad (${fields.join(',')}) VALUES (${placeholders})`;

  try {
    const conn = await dbPool.getConnection();
    await conn.query(sql, values);
    conn.release();
    console.log(`Saved cajero_actividad for ${data.userId}`);
    res.send('Session saved');
  } catch (err) {
    console.error('[DB ERROR] Failed to insert cajero_actividad:', err);
    res.status(500).send('Database error');
  }
});

// Kafka → WebSocket
startKafkaConsumer(({ userId, game }) => {
  const normalizedGame = game.toLowerCase();
  sessionMap[userId] = normalizedGame;
  console.log(`🎮 Kafka assigned ${userId} → ${normalizedGame}`);

  for (const ws of clients) {
    if (ws.userId === userId) {
      ws.send(JSON.stringify({
        type: 'session-start',
        userId,
        game: normalizedGame
      }));
    }
  }
});

// Startup
(async () => {
  await producer.connect();
  server.listen(PORT, () => {
    console.log(`🚀 Game server running on http://localhost:${PORT}`);
  });
})();

// Shutdown
process.on('SIGINT', async () => {
  console.log('Shutting down...');
  await dbPool.end();
  await producer.disconnect();
  server.close(() => {
    console.log('Server closed cleanly');
  });
});
