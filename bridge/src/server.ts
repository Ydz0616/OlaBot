/**
 * WebSocket server for Python-Node.js bridge communication.
 * HTTP server for frontend QR code and status polling.
 * Security: binds to 127.0.0.1 only; optional BRIDGE_TOKEN auth.
 */

import { WebSocketServer, WebSocket } from 'ws';
import { createServer, IncomingMessage, ServerResponse } from 'http';
import { WhatsAppClient, InboundMessage } from './whatsapp.js';

interface SendCommand {
  type: 'send';
  to: string;
  text: string;
}

interface BridgeMessage {
  type: 'message' | 'status' | 'qr' | 'error';
  [key: string]: unknown;
}

export class BridgeServer {
  private wss: WebSocketServer | null = null;
  private wa: WhatsAppClient | null = null;
  private clients: Set<WebSocket> = new Set();
  
  // State for HTTP API
  private latestQR: string | null = null;
  private connectionStatus: 'disconnected' | 'connecting' | 'connected' = 'disconnected';
  private httpServer: ReturnType<typeof createServer> | null = null;

  constructor(private port: number, private authDir: string, private token?: string) {}

  async start(): Promise<void> {
    // Bind to localhost only — never expose to external network
    this.wss = new WebSocketServer({ host: '127.0.0.1', port: this.port });
    console.log(`🌉 Bridge server listening on ws://127.0.0.1:${this.port}`);
    if (this.token) console.log('🔒 Token authentication enabled');

    // Start HTTP API server for frontend polling
    const httpPort = this.port + 1; // e.g. 3002 if bridge is 3001
    this.httpServer = createServer((req, res) => this.handleHTTP(req, res));
    this.httpServer.listen(httpPort, '127.0.0.1', () => {
      console.log(`🌐 HTTP API listening on http://127.0.0.1:${httpPort}`);
    });

    // Initialize WhatsApp client
    this.wa = new WhatsAppClient({
      authDir: this.authDir,
      onMessage: (msg) => this.broadcast({ type: 'message', ...msg }),
      onQR: (qr) => {
        this.latestQR = qr;
        this.connectionStatus = 'connecting';
        this.broadcast({ type: 'qr', qr });
      },
      onStatus: (status) => {
        if (status === 'connected') {
          this.connectionStatus = 'connected';
          this.latestQR = null; // Clear QR once connected
        } else {
          this.connectionStatus = 'disconnected';
        }
        this.broadcast({ type: 'status', status });
      },
    });

    // Handle WebSocket connections
    this.wss.on('connection', (ws) => {
      if (this.token) {
        // Require auth handshake as first message
        const timeout = setTimeout(() => ws.close(4001, 'Auth timeout'), 5000);
        ws.once('message', (data) => {
          clearTimeout(timeout);
          try {
            const msg = JSON.parse(data.toString());
            if (msg.type === 'auth' && msg.token === this.token) {
              console.log('🔗 Python client authenticated');
              this.setupClient(ws);
            } else {
              ws.close(4003, 'Invalid token');
            }
          } catch {
            ws.close(4003, 'Invalid auth message');
          }
        });
      } else {
        console.log('🔗 Python client connected');
        this.setupClient(ws);
      }
    });

    // Connect to WhatsApp
    await this.wa.connect();
  }

  private handleHTTP(req: IncomingMessage, res: ServerResponse): void {
    // CORS headers for frontend
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    res.setHeader('Content-Type', 'application/json');

    if (req.method === 'OPTIONS') {
      res.writeHead(204);
      res.end();
      return;
    }

    if (req.url === '/qr') {
      res.writeHead(200);
      res.end(JSON.stringify({
        qr: this.latestQR,
        status: this.connectionStatus,
      }));
    } else if (req.url === '/status') {
      res.writeHead(200);
      res.end(JSON.stringify({
        status: this.connectionStatus,
      }));
    } else if (req.url === '/contacts') {
      const contacts = this.wa?.getContacts() || [];
      res.writeHead(200);
      res.end(JSON.stringify({
        count: contacts.length,
        contacts,
      }));
    } else if (req.url === '/contacts/history') {
      const history = this.wa?.getHistoryMessages() || new Map();
      const result: Record<string, any[]> = {};
      history.forEach((msgs, jid) => {
        result[jid] = msgs;
      });
      res.writeHead(200);
      res.end(JSON.stringify({
        count: Object.keys(result).length,
        history: result,
      }));
    } else {
      res.writeHead(404);
      res.end(JSON.stringify({ error: 'Not found' }));
    }
  }

  private setupClient(ws: WebSocket): void {
    this.clients.add(ws);

    ws.on('message', async (data) => {
      try {
        const cmd = JSON.parse(data.toString()) as SendCommand;
        await this.handleCommand(cmd);
        ws.send(JSON.stringify({ type: 'sent', to: cmd.to }));
      } catch (error) {
        console.error('Error handling command:', error);
        ws.send(JSON.stringify({ type: 'error', error: String(error) }));
      }
    });

    ws.on('close', () => {
      console.log('🔌 Python client disconnected');
      this.clients.delete(ws);
    });

    ws.on('error', (error) => {
      console.error('WebSocket error:', error);
      this.clients.delete(ws);
    });
  }

  private async handleCommand(cmd: SendCommand): Promise<void> {
    if (cmd.type === 'send' && this.wa) {
      await this.wa.sendMessage(cmd.to, cmd.text);
    }
  }

  private broadcast(msg: BridgeMessage): void {
    const data = JSON.stringify(msg);
    for (const client of this.clients) {
      if (client.readyState === WebSocket.OPEN) {
        client.send(data);
      }
    }
  }

  async stop(): Promise<void> {
    // Close all client connections
    for (const client of this.clients) {
      client.close();
    }
    this.clients.clear();

    // Close HTTP server
    if (this.httpServer) {
      this.httpServer.close();
      this.httpServer = null;
    }

    // Close WebSocket server
    if (this.wss) {
      this.wss.close();
      this.wss = null;
    }

    // Disconnect WhatsApp
    if (this.wa) {
      await this.wa.disconnect();
      this.wa = null;
    }
  }
}

