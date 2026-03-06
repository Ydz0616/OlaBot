/**
 * WhatsApp client wrapper using Baileys.
 *
 * Handles:
 * - WhatsApp Web connection lifecycle (connect, reconnect, auth)
 * - LID ↔ Phone bidirectional mapping (persisted to disk)
 * - History message collection and resolution (LID → phone JID)
 * - Contact enrichment from multiple sources
 * - Message deduplication and sent-message tracking
 * - Auth state health checking and cleanup
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
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';

const VERSION = '0.2.0';

// ─── Types ──────────────────────────────────────────────────────

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

/**
 * An enriched contact that merges data from all available sources:
 * contacts.upsert, messaging-history.set, and LID→Phone resolution.
 */
export interface EnrichedContact {
  phone: string;        // Phone number (digits only, no @suffix)
  phoneJid: string;     // Full phone JID (e.g. 8618936119618@s.whatsapp.net)
  lid: string;          // LID if known, else empty
  name: string;         // Best available display name
  pushName: string;     // WhatsApp push name (notify)
  source: string;       // Where this contact was resolved from
}

export interface WhatsAppClientOptions {
  authDir: string;
  onMessage: (msg: InboundMessage) => void;
  onQR: (qr: string) => void;
  onStatus: (status: string) => void;
  onHistorySyncComplete?: () => void;
}

// ─── Persistence file names ─────────────────────────────────────

const HISTORY_FILE = 'history.json';
const LID_MAP_FILE = 'lid_map.json';

// ─── Constants ──────────────────────────────────────────────────

/** Max history messages stored per chat. */
const MAX_HISTORY_PER_CHAT = 200;

/** Max sent message IDs to track for dedup. */
const MAX_SENT_IDS = 500;

// ─── Main class ─────────────────────────────────────────────────

export class WhatsAppClient {
  private sock: any = null;
  private options: WhatsAppClientOptions;
  private reconnecting = false;
  private reconnectAttempts = 0;
  private readonly maxReconnectAttempts = 5;

  // Contact sources
  private contacts: Map<string, SyncedContact> = new Map();

  // History messages keyed by PHONE JID (resolved)
  private historyMessages: Map<string, SyncedMessage[]> = new Map();

  // LID ↔ Phone bidirectional maps (persisted)
  private lidToPhone: Map<string, string> = new Map(); // LID JID → phone JID
  private phoneToLid: Map<string, string> = new Map(); // phone JID → LID JID

  // Contacts from messaging-history.set (LID-keyed, with push names)
  private historyContacts: Map<string, { id: string; notify?: string; name?: string }> = new Map();

  // Dedup for agent-sent messages
  private sentMessageIds: Map<string, number> = new Map(); // id → timestamp

  // Self phone
  selfPhone: string = '';

  // Persistence paths
  private historyPath: string = '';
  private lidMapPath: string = '';

  // Track whether a full history sync has been received
  private historySyncReceived = false;

  // Debounce timer for history sync complete signal
  // Baileys sends history in multiple batches — we wait for all of them
  private historySyncDebounceTimer: ReturnType<typeof setTimeout> | null = null;
  private readonly HISTORY_SYNC_DEBOUNCE_MS = 5000; // 5 seconds after last batch

  constructor(options: WhatsAppClientOptions) {
    this.options = options;
    this.historyPath = path.join(options.authDir, HISTORY_FILE);
    this.lidMapPath = path.join(options.authDir, LID_MAP_FILE);
    this.loadHistory();
    this.loadLidMap();
  }

  // ═══════════════════════════════════════════════════════════════
  // PERSISTENCE
  // ═══════════════════════════════════════════════════════════════

  /** Load persisted history messages from disk. */
  private loadHistory(): void {
    try {
      if (fs.existsSync(this.historyPath)) {
        const raw = fs.readFileSync(this.historyPath, 'utf-8');
        const data: Record<string, SyncedMessage[]> = JSON.parse(raw);
        for (const [jid, msgs] of Object.entries(data)) {
          this.historyMessages.set(jid, msgs);
        }
        console.log(`📂 Loaded history: ${this.historyMessages.size} chats from disk`);
      }
    } catch (err) {
      console.error('Failed to load history from disk:', err);
    }
  }

  /** Persist history messages to disk. */
  private saveHistory(): void {
    try {
      const obj: Record<string, SyncedMessage[]> = {};
      this.historyMessages.forEach((msgs, jid) => {
        obj[jid] = msgs;
      });
      fs.writeFileSync(this.historyPath, JSON.stringify(obj), 'utf-8');
    } catch (err) {
      console.error('Failed to save history to disk:', err);
    }
  }

  /** Load persisted LID ↔ Phone mappings from disk. */
  private loadLidMap(): void {
    try {
      if (fs.existsSync(this.lidMapPath)) {
        const raw = fs.readFileSync(this.lidMapPath, 'utf-8');
        const data: Record<string, string> = JSON.parse(raw);
        for (const [lid, phone] of Object.entries(data)) {
          this.lidToPhone.set(lid, phone);
          this.phoneToLid.set(phone, lid);
        }
        console.log(`📂 Loaded LID map: ${this.lidToPhone.size} mappings from disk`);
      }
    } catch (err) {
      console.error('Failed to load LID map from disk:', err);
    }
  }

  /** Persist LID → Phone mappings to disk. */
  private saveLidMap(): void {
    try {
      const obj: Record<string, string> = {};
      this.lidToPhone.forEach((phone, lid) => {
        obj[lid] = phone;
      });
      fs.writeFileSync(this.lidMapPath, JSON.stringify(obj), 'utf-8');
    } catch (err) {
      console.error('Failed to save LID map to disk:', err);
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // LID ↔ PHONE RESOLUTION
  // ═══════════════════════════════════════════════════════════════

  /**
   * Register a LID ↔ Phone mapping.
   * Returns true if a new mapping was added.
   */
  private registerLidMapping(lidJid: string, phoneJid: string): boolean {
    if (!lidJid || !phoneJid) return false;

    // Normalize: ensure proper suffixes
    const lid = lidJid.includes('@') ? lidJid : `${lidJid}@lid`;
    const phone = phoneJid.includes('@') ? phoneJid : `${phoneJid}@s.whatsapp.net`;

    if (this.lidToPhone.has(lid) && this.lidToPhone.get(lid) === phone) {
      return false; // Already known
    }

    this.lidToPhone.set(lid, phone);
    this.phoneToLid.set(phone, lid);
    return true;
  }

  /**
   * Resolve a JID to a phone JID.
   * If it's already a phone JID, returns as-is.
   * If it's a LID, looks up the mapping.
   * Returns null if resolution fails.
   */
  resolveToPhoneJid(jid: string): string | null {
    if (!jid) return null;
    if (jid.endsWith('@s.whatsapp.net')) return jid;
    if (jid.endsWith('@lid')) {
      return this.lidToPhone.get(jid) || null;
    }
    // Raw number without suffix — assume phone
    if (/^\d+$/.test(jid)) return `${jid}@s.whatsapp.net`;
    return null;
  }

  /**
   * Extract phone number digits from any JID format.
   */
  private extractPhone(jid: string): string {
    return jid.split('@')[0].split(':')[0];
  }

  /**
   * Normalize a Baileys timestamp to a plain number.
   * Baileys uses protobuf Long objects: { low: number, high: number, unsigned: boolean }
   * This converts them to standard Unix epoch seconds.
   */
  private normalizeTimestamp(ts: any): number {
    if (typeof ts === 'number') return ts;
    if (ts && typeof ts === 'object' && 'low' in ts) {
      // Protobuf Long: combine high and low bits
      // For timestamps, high is usually 0, so low is sufficient
      const result = (ts.high || 0) * 4294967296 + (ts.low || 0);
      if (result > 0) return result;
    }
    if (typeof ts === 'string') {
      const parsed = parseInt(ts, 10);
      if (parsed > 0) return parsed;
    }
    // Fallback: use current time rather than returning 0
    // (0 → to_timestamp(0) = 1970-01-01 → displayed as 12/31 16:00 in UTC-8)
    return Math.floor(Date.now() / 1000);
  }

  /**
   * Re-key history entries stored under LID JIDs to their resolved phone JIDs.
   * Called after new LID mappings become available.
   * Returns the number of entries re-keyed.
   */
  private rekeyHistoryFromLid(): number {
    let reKeyed = 0;
    const toMove: Array<{ oldJid: string; newJid: string }> = [];

    this.historyMessages.forEach((_msgs, jid) => {
      if (!jid.endsWith('@lid')) return;
      const phoneJid = this.lidToPhone.get(jid);
      if (phoneJid) {
        toMove.push({ oldJid: jid, newJid: phoneJid });
      }
    });

    for (const { oldJid, newJid } of toMove) {
      const lidMsgs = this.historyMessages.get(oldJid) || [];
      const existingMsgs = this.historyMessages.get(newJid) || [];

      // Merge, dedup by message ID, sort by timestamp
      const merged = this.mergeAndDedup(existingMsgs, lidMsgs);
      this.historyMessages.set(newJid, merged);
      this.historyMessages.delete(oldJid);
      reKeyed++;
    }

    return reKeyed;
  }

  /**
   * Actively request additional message history for under-synced chats.
   * WhatsApp's passive sync may deliver only 1-2 messages for some chats.
   * Uses Baileys' fetchMessageHistory() to request more messages.
   * Results arrive via messaging-history.set and are processed normally.
   */
  private async fetchAdditionalHistory(): Promise<void> {
    if (!this.sock) return;
    if (typeof this.sock.fetchMessageHistory !== 'function') {
      console.log('⚠️ fetchMessageHistory not available in this Baileys version');
      return;
    }

    const MIN_MESSAGES_THRESHOLD = 10;
    const FETCH_COUNT = 50;
    const underSynced: Array<{ jid: string; count: number; oldestMsg: SyncedMessage }> = [];

    this.historyMessages.forEach((msgs, jid) => {
      // Skip group chats, broadcast, and self-phone
      if (jid.includes('@g.us') || jid === 'status@broadcast' || jid === '0@s.whatsapp.net') return;
      if (msgs.length > 0 && msgs.length < MIN_MESSAGES_THRESHOLD) {
        // Find oldest message as anchor
        const sorted = [...msgs].sort((a, b) => a.timestamp - b.timestamp);
        underSynced.push({ jid, count: msgs.length, oldestMsg: sorted[0] });
      }
    });

    if (underSynced.length === 0) {
      console.log('📥 All chats have sufficient history (≥ 10 msgs), no additional fetch needed');
      return;
    }

    console.log(`📥 Requesting additional history for ${underSynced.length} under-synced chats (< ${MIN_MESSAGES_THRESHOLD} msgs)...`);

    let requested = 0;
    for (const { jid, count, oldestMsg } of underSynced) {
      try {
        // fetchMessageHistory needs: count, oldest message key, oldest timestamp
        const msgKey = {
          remoteJid: jid,
          id: oldestMsg.id,
          fromMe: oldestMsg.fromMe,
        };
        const timestamp = oldestMsg.timestamp || 0;

        await this.sock.fetchMessageHistory(FETCH_COUNT, msgKey, timestamp);
        requested++;
        console.log(`  📥 Requested ${FETCH_COUNT} more msgs for ${this.extractPhone(jid)} (had ${count})`);

        // Small delay between requests to avoid rate limiting
        await new Promise(resolve => setTimeout(resolve, 200));
      } catch (err: any) {
        console.log(`  ⚠️ fetchMessageHistory failed for ${this.extractPhone(jid)}: ${err.message || err}`);
      }
    }

    if (requested > 0) {
      // Wait for responses to arrive via messaging-history.set
      console.log(`📥 Waiting 8s for ${requested} history fetch responses...`);
      await new Promise(resolve => setTimeout(resolve, 8000));
    }
  }

  /**
   * Manually request message history for a specific chat.
   * Tries both phone JID and LID JID to maximize coverage.
   * Returns the number of messages after the request.
   */
  async fetchHistoryForChat(phone: string, count: number = 50): Promise<{ before: number; jidsTried: string[]; error?: string }> {
    if (!this.sock || typeof this.sock.fetchMessageHistory !== 'function') {
      return { before: 0, jidsTried: [], error: 'fetchMessageHistory not available' };
    }

    const phoneJid = phone.includes('@') ? phone : `${phone}@s.whatsapp.net`;
    const lidJid = this.phoneToLid.get(phoneJid) || null;
    const jidsTried: string[] = [];

    // Count messages before fetch
    const before = (this.historyMessages.get(phoneJid) || []).length;
    console.log(`📥 Manual fetch for ${phone}: ${before} msgs before, phoneJid=${phoneJid}, lidJid=${lidJid}`);

    // Try with phone JID first
    try {
      const existing = this.historyMessages.get(phoneJid) || [];
      const anchor = existing.length > 0
        ? existing.sort((a, b) => a.timestamp - b.timestamp)[0]
        : null;

      const msgKey = {
        remoteJid: phoneJid,
        id: anchor?.id || '',
        fromMe: anchor?.fromMe || false,
      };
      const ts = anchor?.timestamp || 0;

      await this.sock.fetchMessageHistory(count, msgKey, ts);
      jidsTried.push(phoneJid);
      console.log(`  📥 Requested via phone JID: ${phoneJid}`);
    } catch (err: any) {
      console.log(`  ⚠️ fetchMessageHistory failed for phone JID: ${err.message || err}`);
    }

    // Also try with LID JID if available
    if (lidJid) {
      try {
        const existing = this.historyMessages.get(lidJid) || this.historyMessages.get(phoneJid) || [];
        const anchor = existing.length > 0
          ? existing.sort((a, b) => a.timestamp - b.timestamp)[0]
          : null;

        const msgKey = {
          remoteJid: lidJid,
          id: anchor?.id || '',
          fromMe: anchor?.fromMe || false,
        };
        const ts = anchor?.timestamp || 0;

        await this.sock.fetchMessageHistory(count, msgKey, ts);
        jidsTried.push(lidJid);
        console.log(`  📥 Requested via LID JID: ${lidJid}`);
      } catch (err: any) {
        console.log(`  ⚠️ fetchMessageHistory failed for LID JID: ${err.message || err}`);
      }
    }

    return { before, jidsTried };
  }

  /**
   * Merge two message arrays, dedup by ID, sort by timestamp, cap to MAX_HISTORY_PER_CHAT.
   */
  private mergeAndDedup(a: SyncedMessage[], b: SyncedMessage[]): SyncedMessage[] {
    const seen = new Set<string>();
    const result: SyncedMessage[] = [];

    for (const msg of [...a, ...b]) {
      if (msg.id && seen.has(msg.id)) continue;
      if (msg.id) seen.add(msg.id);
      result.push(msg);
    }

    result.sort((x, y) => x.timestamp - y.timestamp);

    // Keep last MAX_HISTORY_PER_CHAT messages
    if (result.length > MAX_HISTORY_PER_CHAT) {
      return result.slice(result.length - MAX_HISTORY_PER_CHAT);
    }
    return result;
  }

  // ═══════════════════════════════════════════════════════════════
  // AUTH STATE MANAGEMENT
  // ═══════════════════════════════════════════════════════════════

  /**
   * Check if auth state exists and appears healthy.
   */
  hasAuthState(): boolean {
    const authDir = this.options.authDir;
    try {
      if (!fs.existsSync(authDir)) return false;
      const files = fs.readdirSync(authDir);
      // Baileys stores creds.json as the primary credential file
      return files.some(f => f === 'creds.json');
    } catch {
      return false;
    }
  }

  /**
   * Delete all auth state files so the next connect() starts fresh
   * and triggers QR code generation.
   * Also clears history and LID map since they belong to the old session.
   */
  clearAuthState(): void {
    const authDir = this.options.authDir;
    try {
      if (fs.existsSync(authDir)) {
        const files = fs.readdirSync(authDir);
        for (const file of files) {
          const filePath = path.join(authDir, file);
          fs.rmSync(filePath, { recursive: true, force: true });
        }
        console.log(`🗑️ Cleared ${files.length} auth files from ${authDir}`);
      }
    } catch (err) {
      console.error('Failed to clear auth state:', err);
    }

    // Clear in-memory state
    this.historyMessages.clear();
    this.lidToPhone.clear();
    this.phoneToLid.clear();
    this.contacts.clear();
    this.historyContacts.clear();
    this.selfPhone = '';
    this.historySyncReceived = false;
    this.reconnectAttempts = 0;
    this.reconnecting = false;

    console.log('🗑️ Cleared all in-memory state (history, LID map, contacts, reconnect counters)');
  }

  // ═══════════════════════════════════════════════════════════════
  // CONNECTION
  // ═══════════════════════════════════════════════════════════════

  async connect(): Promise<void> {
    const logger = pino({ level: 'silent' });
    const { state, saveCreds } = await useMultiFileAuthState(this.options.authDir);
    const { version } = await fetchLatestBaileysVersion();

    const hasExisting = this.hasAuthState();
    console.log(hasExisting
      ? `📂 Resuming existing session (Baileys ${version.join('.')})`
      : `🆕 Fresh session — QR code will appear (Baileys ${version.join('.')})`
    );

    // Create socket
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

    // ─── Connection lifecycle ──────────────────────────────────
    this.sock.ev.on('connection.update', async (update: any) => {
      const { connection, lastDisconnect, qr } = update;

      if (qr) {
        console.log('\n📱 Scan this QR code with WhatsApp (Linked Devices):\n');
        qrcode.generate(qr, { small: true });
        this.options.onQR(qr);
      }

      if (connection === 'close') {
        const statusCode = (lastDisconnect?.error as Boom)?.output?.statusCode;
        const isLoggedOut = statusCode === DisconnectReason.loggedOut;

        console.log(`Connection closed. Status: ${statusCode}, loggedOut: ${isLoggedOut} (attempt ${this.reconnectAttempts}/${this.maxReconnectAttempts})`);
        this.options.onStatus('disconnected');

        if (isLoggedOut) {
          // 401 = credentials expired. Full reset.
          console.log('🔑 Auth expired — clearing ALL state for fresh QR code...');
          this.clearAuthState();
          this.reconnectAttempts = 0;
        }

        const shouldReconnect = this.reconnectAttempts < this.maxReconnectAttempts;

        if (shouldReconnect && !this.reconnecting) {
          this.reconnecting = true;
          this.reconnectAttempts++;
          const delay = isLoggedOut ? 2000 : Math.min(3000 * this.reconnectAttempts, 10000);
          console.log(`Reconnecting in ${delay / 1000}s...`);
          setTimeout(() => {
            this.reconnecting = false;
            this.connect();
          }, delay);
        } else if (!shouldReconnect) {
          console.log('⚠️ Max reconnect attempts reached. Restart bridge to try again.');
        }
      } else if (connection === 'open') {
        console.log('✅ Connected to WhatsApp');
        this.reconnectAttempts = 0;

        // Extract and broadcast self phone number
        const rawId = this.sock?.user?.id || '';
        this.selfPhone = rawId.split(':')[0].split('@')[0];
        if (this.selfPhone) {
          console.log(`📱 Self phone: ${this.selfPhone}`);
          this.options.onStatus('connected');
          // Broadcast self-phone as special message
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

    // ─── Contact sync ──────────────────────────────────────────
    this.sock.ev.on('contacts.upsert', (contacts: any[]) => {
      console.log(`📇 Synced ${contacts.length} contacts via contacts.upsert`);

      for (const c of contacts) {
        const id = c.id || '';
        if (!id || id === 'status@broadcast') continue;
        const phone = id.includes('@') ? id.split('@')[0] : null;
        this.contacts.set(id, {
          id,
          name: c.name || c.verifiedName || null,
          pushName: c.notify || null,
          phone,
        });

        // Register LID→Phone mapping from contacts.upsert
        // In Baileys 7.x, contacts have:
        //   id: "12023734274@s.whatsapp.net" (real phone JID)
        //   lid: "103341862457432@lid" (LID JID)
        // This is the AUTHORITATIVE source for LID→Phone mappings.
        if (c.lid && id.endsWith('@s.whatsapp.net')) {
          const lidJid = c.lid.endsWith('@lid') ? c.lid : `${c.lid}@lid`;
          if (this.registerLidMapping(lidJid, id)) {
            // Only log new mappings, not re-registrations
          }
        }
        // Legacy: also check pn field (older Baileys versions)
        if (c.pn && id.endsWith('@lid')) {
          const phoneJid = `${c.pn}@s.whatsapp.net`;
          if (this.registerLidMapping(id, phoneJid)) {
            console.log(`🔗 LID mapping from contacts.upsert: ${id} → ${phoneJid}`);
          }
        }
      }

      // Re-key any history already stored under LID-derived JIDs to real phone JIDs
      const reKeyed = this.rekeyHistoryFromLid();
      this.saveLidMap();
      if (reKeyed > 0) {
        console.log(`🔄 Re-keyed ${reKeyed} history chats from LID → real phone JID (via contacts.upsert)`);
        this.saveHistory();
      }
      console.log(`🔗 contacts.upsert: ${this.lidToPhone.size} total LID→Phone mappings`);
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

    // ─── History sync ──────────────────────────────────────────
    this.sock.ev.on('messaging-history.set', ({ messages: histMsgs, contacts: histContacts, isLatest }: { messages: any[]; chats: any[]; contacts: any[]; isLatest: boolean }) => {
      let newMappings = 0;

      // RAW DUMP: Save every message BEFORE any filtering to /tmp/raw_history_dump.jsonl
      // This helps diagnose if WhatsApp sent messages that our code filters out.
      try {
        const dumpLines: string[] = [];
        for (const msg of histMsgs) {
          const jid = msg.key?.remoteJid || 'unknown';
          const content = this.extractMessageContent(msg);
          const msgType = msg.message ? Object.keys(msg.message).join(',') : 'null';
          dumpLines.push(JSON.stringify({
            jid,
            id: msg.key?.id,
            fromMe: msg.key?.fromMe,
            timestamp: this.normalizeTimestamp(msg.messageTimestamp),
            content: content || null,
            msgType,
            hasMessage: !!msg.message,
            participant: msg.key?.participant,
          }));
        }
        fs.appendFileSync('/tmp/raw_history_dump.jsonl', dumpLines.join('\n') + '\n');
        console.log(`📋 RAW DUMP: ${histMsgs.length} msgs appended to /tmp/raw_history_dump.jsonl (isLatest=${isLatest})`);
      } catch (e) {
        console.log(`⚠️ Raw dump failed: ${e}`);
      }

      // Step 1: Collect all history contacts

      for (const c of histContacts) {
        const cId = c.id || '';
        if (!cId || cId === 'status@broadcast') continue;

        // Store raw history contact
        this.historyContacts.set(cId, {
          id: cId,
          notify: c.notify || undefined,
          name: c.name || c.verifiedName || undefined,
        });


        // Try pn field mapping (may work in future Baileys versions)
        if (c.pn && cId.endsWith('@lid')) {
          const phoneJid = `${c.pn}@s.whatsapp.net`;
          if (this.registerLidMapping(cId, phoneJid)) {
            newMappings++;
          }
        }
      }

      // Note: Step 1.5 (same-numeric-ID matching) has been REMOVED.
      // The authoritative LID→Phone mappings come from contacts.upsert (via c.lid field).
      // The numeric ID in @lid is NOT the same as the phone number, so matching by
      // shared numeric ID produced WRONG mappings.

      // Step 2: Store history messages (resolved to phone JID when possible)
      let msgCount = 0;
      for (const msg of histMsgs) {
        if (!msg.key?.remoteJid || msg.key.remoteJid === 'status@broadcast') continue;
        const rawJid = msg.key.remoteJid;
        if (rawJid.endsWith('@g.us')) continue; // skip groups

        const content = this.extractMessageContent(msg);
        if (!content) continue;

        // Resolve JID: prefer phone JID, fall back to raw JID (may be LID)
        const resolvedJid = this.resolveToPhoneJid(rawJid) || rawJid;

        // Extract LID mapping from message metadata if available
        if (rawJid.endsWith('@lid') && msg.key.remoteJidAlt) {
          const alt = msg.key.remoteJidAlt;
          if (alt.endsWith('@s.whatsapp.net')) {
            if (this.registerLidMapping(rawJid, alt)) {
              newMappings++;
            }
          }
        }

        const existing = this.historyMessages.get(resolvedJid) || [];
        const msgId = msg.key.id || `hist_${crypto.randomBytes(4).toString('hex')}`;

        // Dedup by message ID
        if (existing.some(m => m.id === msgId)) continue;

        existing.push({
          id: msgId,
          remoteJid: resolvedJid,
          fromMe: msg.key.fromMe || false,
          content,
          timestamp: this.normalizeTimestamp(msg.messageTimestamp),
        });
        this.historyMessages.set(resolvedJid, existing);
        msgCount++;
      }

      // Step 3: If we got new mappings, re-key any LID-stored history
      if (newMappings > 0) {
        const reKeyed = this.rekeyHistoryFromLid();
        this.saveLidMap();
        if (reKeyed > 0) {
          console.log(`🔄 Re-keyed ${reKeyed} history chats from LID → phone JID`);
        }
        console.log(`🔗 ${newMappings} new LID mappings (total: ${this.lidToPhone.size})`);
      }

      // Step 4: Enforce per-chat cap, sort, and deduplicate
      this.historyMessages.forEach((msgs, jid) => {
        const deduped = this.mergeAndDedup(msgs, []);
        this.historyMessages.set(jid, deduped);
      });

      if (msgCount > 0) {
        this.saveHistory();
        console.log(`💾 History sync: +${msgCount} msgs across ${this.historyMessages.size} chats (isLatest=${isLatest})`);
      }

      // Step 5: Debounced history sync complete signal
      // Baileys sends history in multiple batches (isLatest=true first for recent,
      // then isLatest=false for older messages). We CANNOT rely on isLatest alone
      // because the bulk of messages arrive AFTER the isLatest=true batch.
      // Instead, debounce: reset a timer on each batch, fire only after 5s of silence.
      if (this.historySyncDebounceTimer) {
        clearTimeout(this.historySyncDebounceTimer);
      }
      this.historySyncDebounceTimer = setTimeout(async () => {
        this.historySyncDebounceTimer = null;

        // Note: fetchAdditionalHistory() has been DISABLED.
        // It sent dozens of fetchMessageHistory() requests which triggered
        // WhatsApp rate-limiting (status 440 disconnect loops).
        // Passive sync via messaging-history.set is sufficient.

        this.historySyncReceived = true;
        this.saveHistory();
        this.saveLidMap();
        const totalMsgs = Array.from(this.historyMessages.values()).reduce((s, m) => s + m.length, 0);
        console.log(`✅ History sync complete (debounced). ${this.historyMessages.size} chats, ${this.lidToPhone.size} LID mappings, ${totalMsgs} total msgs`);
        this.options.onHistorySyncComplete?.();
      }, this.HISTORY_SYNC_DEBOUNCE_MS);
    });

    // ─── Live messages ─────────────────────────────────────────
    this.sock.ev.on('messages.upsert', async ({ messages, type }: { messages: any[]; type: string }) => {
      let historyUpdated = false;

      for (const msg of messages) {
        if (msg.key.remoteJid === 'status@broadcast') continue;

        const content = this.extractMessageContent(msg);
        if (!content) continue;

        const isGroup = msg.key.remoteJid?.endsWith('@g.us') || false;
        const fromMe = msg.key.fromMe || false;
        const jid = msg.key.remoteJid || '';

        // ─── Extract LID mapping from live messages ────────
        if (jid.endsWith('@lid') && msg.key.remoteJidAlt) {
          const alt = msg.key.remoteJidAlt;
          if (alt.endsWith('@s.whatsapp.net')) {
            if (this.registerLidMapping(jid, alt)) {
              console.log(`🔗 LID mapping from live message: ${jid} → ${alt}`);
              this.saveLidMap();
              // Re-key history if this is a new mapping
              const reKeyed = this.rekeyHistoryFromLid();
              if (reKeyed > 0) {
                this.saveHistory();
                console.log(`🔄 Re-keyed ${reKeyed} history chats`);
              }
            }
          }
        }

        // ─── Store history for 'append' type ────────────────
        if (type === 'append') {
          if (jid.endsWith('@g.us')) continue;

          const resolvedJid = this.resolveToPhoneJid(jid) || jid;
          const existing = this.historyMessages.get(resolvedJid) || [];
          const msgId = msg.key.id || '';

          if (!existing.some(m => m.id === msgId)) {
            existing.push({
              id: msgId,
              remoteJid: resolvedJid,
              fromMe,
              content,
              timestamp: this.normalizeTimestamp(msg.messageTimestamp),
            });
            // Enforce cap
            if (existing.length > MAX_HISTORY_PER_CHAT) {
              existing.splice(0, existing.length - MAX_HISTORY_PER_CHAT);
            }
            this.historyMessages.set(resolvedJid, existing);
            historyUpdated = true;
          }
          continue;
        }

        // ─── Live messages (type = 'notify') ────────────────
        if (type !== 'notify') continue;

        // Skip agent-sent messages (already logged via _log_outbound)
        const msgId = msg.key.id || '';
        if (fromMe && this.sentMessageIds.has(msgId)) {
          this.sentMessageIds.delete(msgId);
          continue;
        }

        // Extract display names for contact resolution
        const pushName = msg.pushName || '';
        const savedContact = this.contacts.get(jid);
        const contactName = savedContact?.name || savedContact?.pushName || '';

        // Resolve sender JID for the gateway
        const resolvedSender = this.resolveToPhoneJid(jid) || jid;
        const pn = msg.key.remoteJidAlt || '';

        this.options.onMessage({
          id: msgId,
          sender: resolvedSender,
          pn,
          content,
          timestamp: this.normalizeTimestamp(msg.messageTimestamp),
          isGroup,
          fromMe,
          pushName,
          contactName,
        });
      }

      if (historyUpdated) {
        this.saveHistory();
      }
    });
  }

  // ═══════════════════════════════════════════════════════════════
  // MESSAGE CONTENT EXTRACTION
  // ═══════════════════════════════════════════════════════════════

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
    // Image without caption
    if (message.imageMessage) {
      return '[Image]';
    }
    // Video with caption
    if (message.videoMessage?.caption) {
      return `[Video] ${message.videoMessage.caption}`;
    }
    // Video without caption
    if (message.videoMessage) {
      return '[Video]';
    }
    // Document
    if (message.documentMessage) {
      const fileName = message.documentMessage.fileName || 'file';
      const caption = message.documentMessage.caption;
      return caption ? `[Document: ${fileName}] ${caption}` : `[Document: ${fileName}]`;
    }
    // Voice/Audio message
    if (message.audioMessage) {
      return '[Voice Message]';
    }
    // Sticker
    if (message.stickerMessage) {
      return '[Sticker]';
    }
    // Contact card
    if (message.contactMessage) {
      return `[Contact: ${message.contactMessage.displayName || 'Unknown'}]`;
    }
    // Location
    if (message.locationMessage) {
      return `[Location: ${message.locationMessage.degreesLatitude}, ${message.locationMessage.degreesLongitude}]`;
    }

    return null;
  }

  // ═══════════════════════════════════════════════════════════════
  // SENDING
  // ═══════════════════════════════════════════════════════════════

  async sendMessage(to: string, text: string): Promise<void> {
    if (!this.sock) {
      throw new Error('Not connected');
    }

    // Robust JID normalization
    let jid = to.replace(/^\+/, ''); // strip leading +
    if (jid.endsWith('@lid')) {
      // Attempt LID resolution before giving up
      const resolved = this.lidToPhone.get(jid);
      if (resolved) {
        console.log(`🔗 Resolved LID for send: ${jid} → ${resolved}`);
        jid = resolved;
      } else {
        console.warn(`⚠️ Cannot send to LID address: ${to}. No phone mapping found.`);
        throw new Error(`Cannot send to LID address: ${to}. Use a phone number like 8615653637766@s.whatsapp.net`);
      }
    } else if (!jid.includes('@')) {
      jid = `${jid}@s.whatsapp.net`;
    }

    const result = await this.sock.sendMessage(jid, { text });

    // Track sent message ID for dedup
    if (result?.key?.id) {
      this.sentMessageIds.set(result.key.id, Date.now());

      // Evict oldest entries when exceeding max
      if (this.sentMessageIds.size > MAX_SENT_IDS) {
        const entries = Array.from(this.sentMessageIds.entries());
        entries.sort((a, b) => a[1] - b[1]);
        const toDelete = entries.slice(0, entries.length - MAX_SENT_IDS);
        for (const [id] of toDelete) {
          this.sentMessageIds.delete(id);
        }
      }
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // ACCESSORS (used by server.ts HTTP endpoints)
  // ═══════════════════════════════════════════════════════════════

  /** Get contacts from contacts.upsert (may be empty on existing sessions). */
  getContacts(): SyncedContact[] {
    if (this.contacts.size > 0) {
      return Array.from(this.contacts.values());
    }
    // Fallback: try socket's internal store
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
    } catch {
      // ignore
    }
    return [];
  }

  /** Get history messages (keyed by resolved phone JID when possible). */
  getHistoryMessages(): Map<string, SyncedMessage[]> {
    return this.historyMessages;
  }

  /** Get raw history contacts from messaging-history.set. */
  getHistoryContacts(): Array<{ id: string; notify?: string; name?: string }> {
    return Array.from(this.historyContacts.values());
  }

  /** Get LID → Phone mapping. */
  getLidMap(): Map<string, string> {
    return this.lidToPhone;
  }

  /** Whether history sync has completed. */
  isHistorySyncComplete(): boolean {
    return this.historySyncReceived;
  }

  /**
   * Get enriched contacts merged from all available sources.
   * This is the SINGLE SOURCE OF TRUTH for the contact list.
   *
   * Sources (in priority order):
   * 1. contacts.upsert (phone-keyed, with verified names)
   * 2. messaging-history.set contacts (LID-keyed, with push names)
   * 3. History messages (JIDs that have messages but no explicit contact record)
   */
  getEnrichedContacts(): EnrichedContact[] {
    const result = new Map<string, EnrichedContact>(); // keyed by phone

    // Source 1: contacts.upsert
    this.contacts.forEach((c) => {
      const phone = c.phone || this.extractPhone(c.id);
      if (!phone || phone === 'status') return;
      const phoneJid = `${phone}@s.whatsapp.net`;

      result.set(phone, {
        phone,
        phoneJid,
        lid: this.phoneToLid.get(phoneJid) || '',
        name: c.name || '',
        pushName: c.pushName || '',
        source: 'contacts.upsert',
      });
    });

    // Source 2: messaging-history.set contacts
    this.historyContacts.forEach((hc) => {
      let phone = '';
      let lid = '';

      if (hc.id.endsWith('@s.whatsapp.net')) {
        phone = this.extractPhone(hc.id);
      } else if (hc.id.endsWith('@lid')) {
        lid = hc.id;
        const resolved = this.lidToPhone.get(hc.id);
        if (resolved) {
          phone = this.extractPhone(resolved);
        }
      } else {
        phone = this.extractPhone(hc.id);
      }

      if (!phone) return;

      // Only add if not already present from a higher-priority source
      if (!result.has(phone)) {
        const phoneJid = `${phone}@s.whatsapp.net`;
        result.set(phone, {
          phone,
          phoneJid,
          lid: lid || this.phoneToLid.get(phoneJid) || '',
          name: hc.name || '',
          pushName: hc.notify || '',
          source: 'history.contacts',
        });
      } else {
        // Merge: fill in missing fields
        const existing = result.get(phone)!;
        if (!existing.name && hc.name) existing.name = hc.name;
        if (!existing.pushName && hc.notify) existing.pushName = hc.notify;
        if (!existing.lid && lid) existing.lid = lid;
      }
    });

    // Source 3: History messages (JIDs that have messages but no contact record)
    this.historyMessages.forEach((_msgs, jid) => {
      if (jid.endsWith('@g.us')) return;
      const phone = this.extractPhone(jid);
      if (!phone || result.has(phone)) return;

      const phoneJid = `${phone}@s.whatsapp.net`;
      result.set(phone, {
        phone,
        phoneJid,
        lid: this.phoneToLid.get(phoneJid) || '',
        name: '',
        pushName: '',
        source: 'history.messages',
      });
    });

    return Array.from(result.values()).filter(c => {
      if (c.phone === 'status') return false;
      if (c.phoneJid.endsWith('@g.us')) return false;
      // Filter out group community IDs masquerading as phone numbers
      // WhatsApp group community IDs start with 120363 and are very long
      if (c.phone.startsWith('120363') && c.phone.length > 15) return false;
      return true;
    });
  }

  async disconnect(): Promise<void> {
    if (this.sock) {
      this.sock.end(undefined);
      this.sock = null;
    }
  }
}
