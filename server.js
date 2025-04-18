const express = require('express');
const path = require('path');
const http = require('http');
const WebSocket = require('ws');
const { Client } = require('pg'); // PostgreSQL client

const app = express();
const PORT = 9094;

// Serve Unity WebGL build
app.use('/Cubiertos', express.static(path.join(__dirname, 'Cubiertos')));
app.get('/Cubiertos', (req, res) => {
  res.sendFile(path.join(__dirname, 'Cubiertos/index.html'));
});

// Set up PostgreSQL client
const pgClient = new Client({
  user: 'postgres',   // Replace with your PostgreSQL username
  host: 'localhost',      // Replace with your PostgreSQL host (use 'localhost' for local)
  database: 'Data',    // Replace with your PostgreSQL database name
  password: 'PGADMIN',  // Replace with your PostgreSQL password
  port: 5432,             // Default port for PostgreSQL
});

// Connect to PostgreSQL
pgClient.connect()
  .then(() => console.log('Connected to PostgreSQL'))
  .catch((err) => console.error('Failed to connect to PostgreSQL', err));

// Create HTTP server and WebSocket server
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const clients = new Set();

// Track each client's userId from query param
wss.on('connection', (ws, req) => {
  const params = new URLSearchParams(req.url.replace(/^.*\?/, ''));
  const userId = params.get('userId');
  ws.userId = userId;
  clients.add(ws);

  console.log(`Unity client connected for userId: ${userId}`);

  // Send a message to the client when connected
  ws.send(JSON.stringify({ message: 'Connected to WebSocket server.' }));

  ws.on('close', () => {
    clients.delete(ws);
    console.log(`Unity client for ${userId} disconnected`);
  });

  ws.on('error', (error) => {
    console.error(`WebSocket error for userId ${userId}:`, error);
    clients.delete(ws); // Remove the client on error
  });
});

// Endpoint to handle the POST request from Unity and insert data into PostgreSQL
app.use(express.json()); // Middleware to parse JSON bodies

app.post('/send-metrics', (req, res) => {
  console.log('Received request body:', req.body); // Log the received data

  const { userId, tiempo, puntaje, errores } = req.body;

  if (!userId || !tiempo || !puntaje || !errores) {
    return res.status(400).send('Missing required metrics.');
  }

  const query = 'INSERT INTO cubiertos_metrics (user_id, tiempo, puntaje, errores) VALUES ($1, $2, $3, $4)';
  const values = [userId, tiempo, puntaje, errores];

  pgClient.query(query, values)
    .then(() => {
      console.log('Metrics inserted into cubiertos_metrics');
      res.status(200).send('Metrics inserted successfully.');
    })
    .catch((err) => {
      console.error('Error inserting metrics into cubiertos_metrics:', err);
      res.status(500).send('Error inserting metrics into PostgreSQL.');
    });
});

// Graceful server shutdown
process.on('SIGINT', async () => {
  console.log('Shutting down the server gracefully...');
  await pgClient.end(); // Close the PostgreSQL connection
  server.close(() => {
    console.log('Server closed.');
  });
});

server.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
});