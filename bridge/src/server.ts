/**
 * WebSocket server for Python-Node.js bridge communication.
 * HTTP server for frontend QR code, status polling, contact/history APIs.
 * Security: binds to 127.0.0.1 only; optional BRIDGE_TOKEN auth.
 */

import { WebSocketServer, WebSocket } from 'ws';
import { createServer, IncomingMessage, ServerResponse } from 'http';
import { WhatsAppClient, InboundMessage, EnrichedContact } from './whatsapp.js';

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
  private fullSyncComplete: boolean = false;
  private httpServer: ReturnType<typeof createServer> | null = null;

  constructor(private port: number, private authDir: string, private token?: string) {}

  async start(): Promise<void> {
    // Bind host: 127.0.0.1 for local dev, 0.0.0.0 for Docker
    const host = process.env.BRIDGE_HOST || '127.0.0.1';

    this.wss = new WebSocketServer({ host, port: this.port });
    console.log(`🌉 Bridge server listening on ws://${host}:${this.port}`);
    if (this.token) console.log('🔒 Token authentication enabled');

    // Start HTTP API server for frontend polling
    const httpPort = this.port + 1; // e.g. 3002 if bridge is 3001
    this.httpServer = createServer((req, res) => this.handleHTTP(req, res));
    this.httpServer.listen(httpPort, host, () => {
      console.log(`🌐 HTTP API listening on http://${host}:${httpPort}`);
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
          this.latestQR = null;
        } else {
          this.connectionStatus = 'disconnected';
        }
        this.broadcast({ type: 'status', status });
      },
      onHistorySyncComplete: () => {
        this.fullSyncComplete = true;
        console.log('📡 Broadcasting history_sync_complete to all connected clients');
        this.broadcast({
          type: 'message',
          id: '__history_sync__',
          sender: '',
          pn: '',
          content: '__HISTORY_SYNC_COMPLETE__',
          timestamp: Date.now(),
          isGroup: false,
          fromMe: true,
          pushName: '',
          contactName: '',
        });
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

  // ═══════════════════════════════════════════════════════════════
  // HTTP API
  // ═══════════════════════════════════════════════════════════════

  private handleHTTP(req: IncomingMessage, res: ServerResponse): void {
    // CORS headers for frontend
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    res.setHeader('Content-Type', 'application/json');

    if (req.method === 'OPTIONS') {
      res.writeHead(204);
      res.end();
      return;
    }

    const url = req.url || '';

    // ─── Status & QR ─────────────────────────────────────────
    if (url === '/qr') {
      res.writeHead(200);
      res.end(JSON.stringify({
        qr: this.latestQR,
        status: this.connectionStatus,
      }));
      return;
    }

    if (url === '/status') {
      res.writeHead(200);
      res.end(JSON.stringify({
        status: this.connectionStatus,
        historySyncComplete: this.wa?.isHistorySyncComplete() || false,
        fullSyncComplete: this.fullSyncComplete,
        contactCount: this.wa?.getEnrichedContacts().length || 0,
        historyChats: this.wa?.getHistoryMessages().size || 0,
        totalMessages: Array.from(this.wa?.getHistoryMessages().values() || []).reduce((sum, msgs) => sum + msgs.length, 0),
        unresolvedLidChats: Array.from(this.wa?.getHistoryMessages().keys() || []).filter(jid => jid.endsWith('@lid')).length,
        lidMappings: this.wa?.getLidMap().size || 0,
      }));
      return;
    }

    // ─── Contacts (legacy, from contacts.upsert) ─────────────
    if (url === '/contacts') {
      const contacts = this.wa?.getContacts() || [];
      res.writeHead(200);
      res.end(JSON.stringify({
        count: contacts.length,
        contacts,
      }));
      return;
    }

    // ─── Enriched Contacts (NEW — single source of truth) ────
    if (url === '/contacts/enriched') {
      const enriched = this.wa?.getEnrichedContacts() || [];
      res.writeHead(200);
      res.end(JSON.stringify({
        count: enriched.length,
        contacts: enriched,
      }));
      return;
    }

    // ─── History with LID map ────────────────────────────────
    if (url === '/contacts/history') {
      const history = this.wa?.getHistoryMessages() || new Map();
      const histContacts = this.wa?.getHistoryContacts() || [];
      const lidMap = this.wa?.getLidMap() || new Map();

      const historyObj: Record<string, any[]> = {};
      history.forEach((msgs, jid) => {
        historyObj[jid] = msgs;
      });

      const lidMapObj: Record<string, string> = {};
      lidMap.forEach((phone, lid) => {
        lidMapObj[lid] = phone;
      });

      res.writeHead(200);
      res.end(JSON.stringify({
        count: Object.keys(historyObj).length,
        history: historyObj,
        historyContacts: histContacts,
        lidMap: lidMapObj,
        historySyncComplete: this.wa?.isHistorySyncComplete() || false,
      }));
      return;
    }

    // ─── Auth Reset ──────────────────────────────────────────
    if (url === '/auth/reset' && req.method === 'POST') {
      console.log('🔑 Auth reset requested via HTTP API');
      if (this.wa) {
        // Reset all tracking state
        this.connectionStatus = 'disconnected';
        this.latestQR = null;
        this.fullSyncComplete = false;

        // Disconnect → clear auth → reconnect (fresh QR)
        this.wa.disconnect().then(() => {
          this.wa!.clearAuthState();
          // clearAuthState resets reconnectAttempts internally
          return this.wa!.connect();
        }).catch(err => {
          console.error('Failed to reconnect after auth reset:', err);
          // Last resort: try connecting again after a delay
          setTimeout(() => {
            this.wa!.connect().catch(e => {
              console.error('Retry after auth reset also failed:', e);
            });
          }, 3000);
        });
      }
      res.writeHead(200);
      res.end(JSON.stringify({
        ok: true,
        message: 'Auth reset initiated. Scan new QR code to reconnect.',
      }));
      return;
    }

    // ─── Auth Status ─────────────────────────────────────────
    if (url === '/auth/status') {
      const hasAuth = this.wa?.hasAuthState() || false;
      res.writeHead(200);
      res.end(JSON.stringify({
        hasAuth,
        connected: this.connectionStatus === 'connected',
        selfPhone: this.wa?.selfPhone || '',
      }));
      return;
    }

    // ─── Test: inject a fake message ─────────────────────────
    if (url === '/test/inject' && req.method === 'POST') {
      if (process.env.BRIDGE_TEST_MODE !== 'true') {
        res.writeHead(403);
        res.end(JSON.stringify({ error: 'Test mode not enabled' }));
        return;
      }
      this.readBody(req, (body) => {
        try {
          const data = JSON.parse(body);
          const msg: BridgeMessage = {
            type: 'message',
            id: `test_${Date.now()}`,
            sender: data.sender || '15551234567@s.whatsapp.net',
            pn: data.pn || '',
            content: data.content || '',
            timestamp: Date.now(),
            isGroup: data.isGroup || false,
            fromMe: data.fromMe ?? false,
            pushName: data.pushName || 'TestUser',
            contactName: data.contactName || '',
          };
          this.broadcast(msg);
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, injected: msg }));
        } catch {
          res.writeHead(400);
          res.end(JSON.stringify({ error: 'Invalid JSON' }));
        }
      });
      return;
    }

    // ─── Test: inject history messages ────────────────────────
    if (url === '/test/inject-history' && req.method === 'POST') {
      if (process.env.BRIDGE_TEST_MODE !== 'true') {
        res.writeHead(403);
        res.end(JSON.stringify({ error: 'Test mode not enabled' }));
        return;
      }
      this.readBody(req, (body) => {
        try {
          const data = JSON.parse(body);
          const messages: Array<{sender: string; content: string; fromMe?: boolean; pushName?: string; timestamp?: number}> = data.messages || [];
          const injected: BridgeMessage[] = [];
          for (let i = 0; i < messages.length; i++) {
            const m = messages[i];
            const msg: BridgeMessage = {
              type: 'message',
              id: `hist_test_${Date.now()}_${i}`,
              sender: m.sender || '15551234567@s.whatsapp.net',
              pn: '',
              content: m.content || '',
              timestamp: m.timestamp || (Date.now() - (messages.length - i) * 60000),
              isGroup: false,
              fromMe: m.fromMe ?? false,
              pushName: m.pushName || '',
              contactName: '',
            };
            this.broadcast(msg);
            injected.push(msg);
          }
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, count: injected.length }));
        } catch {
          res.writeHead(400);
          res.end(JSON.stringify({ error: 'Invalid JSON' }));
        }
      });
      return;
    }

    // ─── Dashboard chat ──────────────────────────────────────
    if (url === '/chat' && req.method === 'POST') {
      const selfPhone = this.wa?.selfPhone || '';
      if (!selfPhone) {
        res.writeHead(503);
        res.end(JSON.stringify({ error: 'WhatsApp not connected yet' }));
        return;
      }
      this.readBody(req, async (body) => {
        try {
          const data = JSON.parse(body);
          const content = data.content || '';

          // Log the user message to backend for persistence
          const backendUrl = process.env.OLAINTEL_API_URL || 'http://backend:8000';
          try {
            await fetch(`${backendUrl}/api/chat`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ role: 'user', content }),
            });
          } catch (logErr) {
            console.error('Failed to log user chat message:', logErr);
          }

          const msg: BridgeMessage = {
            type: 'message',
            id: `dashboard_${Date.now()}`,
            sender: `${selfPhone}@s.whatsapp.net`,
            pn: '',
            content,
            timestamp: Date.now(),
            isGroup: false,
            fromMe: true,
            pushName: 'Boss',
            contactName: 'Boss',
          };
          this.broadcast(msg);
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, message_id: msg.id }));
        } catch {
          res.writeHead(400);
          res.end(JSON.stringify({ error: 'Invalid JSON' }));
        }
      });
      return;
    }

    // ─── Manual history fetch for specific chat ──────────────
    if (url === '/fetch-history' && req.method === 'POST') {
      this.readBody(req, async (body) => {
        try {
          const data = JSON.parse(body);
          const phone = data.phone || '';
          const count = data.count || 50;
          if (!phone) {
            res.writeHead(400);
            res.end(JSON.stringify({ error: 'Missing phone parameter' }));
            return;
          }
          // Trigger fetch
          const result = await this.wa?.fetchHistoryForChat(phone, count);
          // Wait for responses
          await new Promise(resolve => setTimeout(resolve, 5000));
          // Check messages after fetch
          const phoneJid = phone.includes('@') ? phone : `${phone}@s.whatsapp.net`;
          const after = (this.wa?.getHistoryMessages().get(phoneJid) || []).length;
          res.writeHead(200);
          res.end(JSON.stringify({
            ok: true,
            phone,
            phoneJid,
            ...result,
            after,
            gained: after - (result?.before || 0),
          }));
        } catch (err) {
          res.writeHead(500);
          res.end(JSON.stringify({ error: String(err) }));
        }
      });
      return;
    }

    // ─── 404 ─────────────────────────────────────────────────
    res.writeHead(404);
    res.end(JSON.stringify({ error: 'Not found' }));
  }

  /**
   * Helper to read request body without repeating boilerplate.
   */
  private readBody(req: IncomingMessage, callback: (body: string) => void): void {
    let body = '';
    req.on('data', (chunk) => { body += chunk; });
    req.on('end', () => callback(body));
  }

  // ═══════════════════════════════════════════════════════════════
  // WEBSOCKET CLIENT MANAGEMENT
  // ═══════════════════════════════════════════════════════════════

  private setupClient(ws: WebSocket): void {
    this.clients.add(ws);

    // Send self-phone info to the newly connected client
    if (this.wa) {
      const selfPhone = this.wa.selfPhone || '';
      if (selfPhone) {
        ws.send(JSON.stringify({
          type: 'message',
          id: '__self__',
          sender: `${selfPhone}@s.whatsapp.net`,
          pn: '',
          content: '__SELF_PHONE__',
          timestamp: Date.now(),
          isGroup: false,
          fromMe: true,
          pushName: '',
          contactName: '',
        }));
      }

      // If history sync already completed, send completion signal
      if (this.wa.isHistorySyncComplete()) {
        ws.send(JSON.stringify({
          type: 'message',
          id: '__history_sync__',
          sender: '',
          pn: '',
          content: '__HISTORY_SYNC_COMPLETE__',
          timestamp: Date.now(),
          isGroup: false,
          fromMe: true,
          pushName: '',
          contactName: '',
        }));
      }
    }

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

      // If this message targets the boss (selfPhone), log it as an agent response
      const selfPhone = this.wa.selfPhone || '';
      const targetPhone = cmd.to.replace('@s.whatsapp.net', '');
      if (selfPhone && targetPhone === selfPhone) {
        try {
          const backendUrl = process.env.OLAINTEL_API_URL || 'http://backend:8000';
          const res = await fetch(`${backendUrl}/api/chat`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ role: 'assistant', content: cmd.text }),
          });
          if (!res.ok) console.error('Failed to log agent response to chat:', res.status);
        } catch (err) {
          console.error('Failed to log agent response to chat:', err);
        }
      }
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
