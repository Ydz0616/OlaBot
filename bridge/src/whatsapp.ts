/**
 * WhatsApp client wrapper using Baileys.
 * Based on OpenClaw's working implementation.
 */

/* eslint-disable @typescript-eslint/no-explicit-any */
import makeWASocket, {
  DisconnectReason,
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore,
} from '@whiskeysockets/baileys';

import { Boom } from '@hapi/boom';
import qrcode from 'qrcode-terminal';
import pino from 'pino';

const VERSION = '0.1.0';

export interface InboundMessage {
  id: string;
  sender: string;
  pn: string;
  content: string;
  timestamp: number;
  isGroup: boolean;
  fromMe: boolean;
  pushName: string;
  contactName: string;
}

export interface SyncedContact {
  id: string;
  name: string | null;
  pushName: string | null;
  phone: string | null;
}

export interface SyncedMessage {
  id: string;
  remoteJid: string;
  fromMe: boolean;
  content: string;
  timestamp: number;
}

export interface WhatsAppClientOptions {
  authDir: string;
  onMessage: (msg: InboundMessage) => void;
  onQR: (qr: string) => void;
  onStatus: (status: string) => void;
}

export class WhatsAppClient {
  private sock: any = null;
  private options: WhatsAppClientOptions;
  private reconnecting = false;
  private contacts: Map<string, SyncedContact> = new Map();
  private historyMessages: Map<string, SyncedMessage[]> = new Map();
  private sentMessageIds: Set<string> = new Set();
  private selfPhone: string = '';

  constructor(options: WhatsAppClientOptions) {
    this.options = options;
  }

  async connect(): Promise<void> {
    const logger = pino({ level: 'silent' });
    const { state, saveCreds } = await useMultiFileAuthState(this.options.authDir);
    const { version } = await fetchLatestBaileysVersion();

    console.log(`Using Baileys version: ${version.join('.')}`);

    // Create socket following OpenClaw's pattern
    this.sock = makeWASocket({
      auth: {
        creds: state.creds,
        keys: makeCacheableSignalKeyStore(state.keys, logger),
      },
      version,
      logger,
      printQRInTerminal: false,
      browser: ['nanobot', 'cli', VERSION],
      syncFullHistory: true,
      markOnlineOnConnect: false,
    });

    // Handle WebSocket errors
    if (this.sock.ws && typeof this.sock.ws.on === 'function') {
      this.sock.ws.on('error', (err: Error) => {
        console.error('WebSocket error:', err.message);
      });
    }

    // Handle connection updates
    this.sock.ev.on('connection.update', async (update: any) => {
      const { connection, lastDisconnect, qr } = update;

      if (qr) {
        // Display QR code in terminal
        console.log('\n📱 Scan this QR code with WhatsApp (Linked Devices):\n');
        qrcode.generate(qr, { small: true });
        this.options.onQR(qr);
      }

      if (connection === 'close') {
        const statusCode = (lastDisconnect?.error as Boom)?.output?.statusCode;
        const shouldReconnect = statusCode !== DisconnectReason.loggedOut;

        console.log(`Connection closed. Status: ${statusCode}, Will reconnect: ${shouldReconnect}`);
        this.options.onStatus('disconnected');

        if (shouldReconnect && !this.reconnecting) {
          this.reconnecting = true;
          console.log('Reconnecting in 5 seconds...');
          setTimeout(() => {
            this.reconnecting = false;
            this.connect();
          }, 5000);
        }
      } else if (connection === 'open') {
        console.log('✅ Connected to WhatsApp');
        // Extract and broadcast self phone number for boss identification
        const rawId = this.sock?.user?.id || '';
        this.selfPhone = rawId.split(':')[0].split('@')[0];
        if (this.selfPhone) {
          console.log(`📱 Self phone: ${this.selfPhone}`);
          // Broadcast to connected clients via onMessage with a special 'self' type
          this.options.onStatus('connected');
          // Send self-phone info as a special message
          this.options.onMessage({
            id: '__self__',
            sender: `${this.selfPhone}@s.whatsapp.net`,
            pn: '',
            content: '__SELF_PHONE__',
            timestamp: Date.now(),
            isGroup: false,
            fromMe: true,
            pushName: '',
            contactName: '',
          });
        } else {
          this.options.onStatus('connected');
        }
      }
    });

    // Save credentials on update
    this.sock.ev.on('creds.update', saveCreds);

    // Handle contact syncs
    this.sock.ev.on('contacts.upsert', (contacts: any[]) => {
      console.log(`📇 Synced ${contacts.length} contacts`);
      for (const c of contacts) {
        const id = c.id || '';
        if (!id || id === 'status@broadcast') continue;
        // Extract phone from JID (e.g. 8618936119618@s.whatsapp.net → 8618936119618)
        const phone = id.includes('@') ? id.split('@')[0] : null;
        this.contacts.set(id, {
          id,
          name: c.name || c.verifiedName || null,
          pushName: c.notify || null,
          phone,
        });
      }
    });

    this.sock.ev.on('contacts.update', (updates: any[]) => {
      for (const u of updates) {
        const existing = this.contacts.get(u.id);
        if (existing) {
          if (u.name) existing.name = u.name;
          if (u.notify) existing.pushName = u.notify;
        }
      }
    });

    // Handle incoming messages
    this.sock.ev.on('messages.upsert', async ({ messages, type }: { messages: any[]; type: string }) => {
      for (const msg of messages) {
        // Skip status updates
        if (msg.key.remoteJid === 'status@broadcast') continue;

        const content = this.extractMessageContent(msg);
        if (!content) continue;

        const isGroup = msg.key.remoteJid?.endsWith('@g.us') || false;
        const fromMe = msg.key.fromMe || false;
        const jid = msg.key.remoteJid || '';

        // Store history sync messages (type = 'append' for history)
        if (type === 'append') {
          const existing = this.historyMessages.get(jid) || [];
          if (existing.length < 10) {
            existing.push({
              id: msg.key.id || '',
              remoteJid: jid,
              fromMe,
              content,
              timestamp: msg.messageTimestamp as number,
            });
            this.historyMessages.set(jid, existing);
          }
          continue;
        }

        // Live messages (type = 'notify')
        if (type !== 'notify') continue;

        // Skip agent-sent messages (already logged via _log_outbound)
        const msgId = msg.key.id || '';
        if (fromMe && this.sentMessageIds.has(msgId)) {
          this.sentMessageIds.delete(msgId);  // Clean up
          continue;
        }

        // Extract display names for contact resolution
        const pushName = msg.pushName || '';
        const savedContact = this.contacts.get(jid);
        const contactName = savedContact?.name || savedContact?.pushName || '';

        this.options.onMessage({
          id: msg.key.id || '',
          sender: jid,
          pn: msg.key.remoteJidAlt || '',
          content,
          timestamp: msg.messageTimestamp as number,
          isGroup,
          fromMe,
          pushName,
          contactName,
        });
      }
    });
  }

  private extractMessageContent(msg: any): string | null {
    const message = msg.message;
    if (!message) return null;

    // Text message
    if (message.conversation) {
      return message.conversation;
    }

    // Extended text (reply, link preview)
    if (message.extendedTextMessage?.text) {
      return message.extendedTextMessage.text;
    }

    // Image with caption
    if (message.imageMessage?.caption) {
      return `[Image] ${message.imageMessage.caption}`;
    }

    // Video with caption
    if (message.videoMessage?.caption) {
      return `[Video] ${message.videoMessage.caption}`;
    }

    // Document with caption
    if (message.documentMessage?.caption) {
      return `[Document] ${message.documentMessage.caption}`;
    }

    // Voice/Audio message
    if (message.audioMessage) {
      return `[Voice Message]`;
    }

    return null;
  }

  async sendMessage(to: string, text: string): Promise<void> {
    if (!this.sock) {
      throw new Error('Not connected');
    }

    // Robust JID normalization
    let jid = to.replace(/^\+/, ''); // strip leading +
    if (jid.endsWith('@lid')) {
      // LID addresses cannot receive messages — this is a bug in chat_id resolution
      console.warn(`⚠️ Cannot send to LID address: ${to}. Use phone number instead.`);
      throw new Error(`Cannot send to LID address: ${to}. Use a phone number like 8615653637766@s.whatsapp.net`);
    } else if (!jid.includes('@')) {
      jid = `${jid}@s.whatsapp.net`;
    }

    const result = await this.sock.sendMessage(jid, { text });
    // Track sent message ID for dedup when Baileys fires messages.upsert
    if (result?.key?.id) {
      this.sentMessageIds.add(result.key.id);
      // Prevent memory leak: cap at 200 entries
      if (this.sentMessageIds.size > 200) {
        const first = this.sentMessageIds.values().next().value;
        if (first) this.sentMessageIds.delete(first);
      }
    }
  }

  getContacts(): SyncedContact[] {
    // Return from our contacts map
    if (this.contacts.size > 0) {
      return Array.from(this.contacts.values());
    }
    // Fallback: try to read from socket's internal store
    try {
      const store = this.sock?.store;
      if (store?.contacts) {
        const result: SyncedContact[] = [];
        for (const [id, c] of Object.entries(store.contacts) as [string, any][]) {
          if (!id || id === 'status@broadcast') continue;
          const phone = id.includes('@') ? id.split('@')[0] : null;
          result.push({
            id,
            name: c.name || c.verifiedName || null,
            pushName: c.notify || null,
            phone,
          });
        }
        return result;
      }
    } catch (e) {
      // ignore
    }
    return [];
  }

  getHistoryMessages(): Map<string, SyncedMessage[]> {
    return this.historyMessages;
  }

  async disconnect(): Promise<void> {
    if (this.sock) {
      this.sock.end(undefined);
      this.sock = null;
    }
  }
}
