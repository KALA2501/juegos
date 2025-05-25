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

// ✅ Serve Unity WebGL games (static folder)
app.use('/games', express.static(path.join(__dirname, 'games')));

// Endpoint to directly open index.html of the game
app.get('/play/:userId', (req, res) => {
  const userId = req.params.userId;
  const game = sessionMap[userId];
  if (!game) return res.status(404).send('User has no game assigned');
  const indexPath = path.join(__dirname, 'games', game, 'index.html');
  if (!fs.existsSync(indexPath)) return res.status(404).send('Game not found');
  res.sendFile(indexPath);
});

// ✅ MySQL
const dbPool = mysql.createPool({
  host: process.env.DB_HOST || 'localhost',
  user: process.env.DB_USER || 'root',
  password: process.env.DB_PASSWORD || 'admin',
  database: process.env.DB_NAME || 'kala',
  port: process.env.DB_PORT || 3306,
});

// ✅ WebSocket
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

// ✅ Basic metrics saving
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

// ✅ Full cajero_actividad session saving
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

app.post('/send-identificacion-metrics', async (req, res) => {
  const data = req.body;

  const columns = [
    'paciente_id', 'tiempo_actividad', 'errores_totales',
    'errores_100', 'tiempo_100',
    'errores_200', 'tiempo_200',
    'errores_500', 'tiempo_500',
    'errores_1000', 'tiempo_1000',
    'errores_2mil', 'tiempo_2mil',
    'errores_5mil', 'tiempo_5mil',
    'errores_10mil', 'tiempo_10mil',
    'errores_20mil', 'tiempo_20mil',
    'errores_50mil', 'tiempo_50mil',
    'errores_100mil', 'tiempo_100mil'
  ];

  const values = columns.map(col => data[col] ?? null);
  const placeholders = columns.map(() => '?').join(',');

  try {
    const conn = await dbPool.getConnection();
    await conn.query(
      `INSERT INTO metricas_identificacion_dinero (${columns.join(',')}) VALUES (${placeholders})`,
      values
    );
    conn.release();
    console.log(`🧠 Identificación metrics saved for ${data.paciente_id}`);
    res.send('OK');
  } catch (err) {
    console.error('❌ DB error:', err);
    res.status(500).send('Error saving metrics');
  }
});

app.post('/send-mercado-metrics', async (req, res) => {
  const {
    user,
    totalActivityDuration,
    listViewCount,
    totalListLookTime,
    correctItemsCount,
    incorrectItemsCount,
    totalQuantityOffBy,
    totalIncorrectQuantity,
    timestamp
  } = req.body;

  const sql = `
    INSERT INTO metricas_mercado (
      user, totalActivityDuration, listViewCount, totalListLookTime,
      correctItemsCount, incorrectItemsCount, totalQuantityOffBy,
      totalIncorrectQuantity, timestamp
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `;

  try {
    const conn = await dbPool.getConnection();
    await conn.query(sql, [
      user,
      totalActivityDuration,
      listViewCount,
      totalListLookTime,
      correctItemsCount,
      incorrectItemsCount,
      totalQuantityOffBy,
      totalIncorrectQuantity,
      timestamp.replace("T", " ").replace("Z", "")
    ]);
    conn.release();
    res.send("Metrics saved");
  } catch (err) {
    console.error("❌ Error saving mercado metrics:", err);
    res.status(500).send("DB error");
  }
});



// ✅ Kafka → WebSocket bridge
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

// ✅ Startup
(async () => {
  await producer.connect();
  server.listen(PORT, () => {
    console.log(`🚀 Game server running on http://localhost:${PORT}`);
  });
})();

// ✅ Shutdown
process.on('SIGINT', async () => {
  console.log('Shutting down...');
  await dbPool.end();
  await producer.disconnect();
  server.close(() => {
    console.log('Server closed cleanly');
  });
});
