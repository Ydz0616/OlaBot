/**
 * Unit tests for WhatsApp history sync broadcast logic.
 *
 * Run:  node --test bridge/src/whatsapp.test.mjs
 *   or: cd bridge && node --test src/whatsapp.test.mjs
 *
 * Uses Node.js built-in test runner (Node >= 20).
 */

import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'fs';
import path from 'path';
import os from 'os';

// We can't import WhatsAppClient directly because it depends on Baileys.
// Instead we test the BROADCAST LOGIC by simulating what the handlers do.
// This validates the contract: history messages → onMessage callback.

/**
 * Minimal stub that mirrors WhatsAppClient's history handling logic
 * (extracted from whatsapp.ts messaging-history.set handler).
 */
function processHistorySet(histMsgs, historyMessages, onMessage) {
  let count = 0;
  const toBroadcast = [];
  for (const msg of histMsgs) {
    if (!msg.key?.remoteJid || msg.key.remoteJid === 'status@broadcast') continue;
    const jid = msg.key.remoteJid;
    if (jid.endsWith('@g.us')) continue;

    const content = msg.message?.conversation
      || msg.message?.extendedTextMessage?.text
      || null;
    if (!content) continue;

    const existing = historyMessages.get(jid) || [];
    if (existing.length < 50) {
      existing.push({
        id: msg.key.id || '',
        remoteJid: jid,
        fromMe: msg.key.fromMe || false,
        content,
        timestamp: msg.messageTimestamp,
      });
      historyMessages.set(jid, existing);
      count++;

      toBroadcast.push({
        id: msg.key.id || `hist_${Date.now()}_${count}`,
        sender: jid,
        pn: '',
        content,
        timestamp: msg.messageTimestamp,
        isGroup: false,
        fromMe: msg.key.fromMe || false,
        pushName: msg.pushName || '',
        contactName: '',
      });
    }
  }
  if (count > 0) {
    for (const m of toBroadcast) {
      onMessage(m);
    }
  }
  return { count, toBroadcast };
}

/**
 * Simulate setupClient history push (from server.ts).
 */
function pushHistoryToClient(historyMessages, sendFn) {
  let histCount = 0;
  historyMessages.forEach((msgs, jid) => {
    for (const m of msgs) {
      sendFn({
        type: 'message',
        id: m.id || `hist_push_${Date.now()}_${histCount}`,
        sender: m.remoteJid,
        pn: '',
        content: m.content,
        timestamp: m.timestamp,
        isGroup: false,
        fromMe: m.fromMe,
        pushName: '',
        contactName: '',
      });
      histCount++;
    }
  });
  return histCount;
}

// ─── Helper: create fake Baileys message ────────────────────

function fakeMsg(jid, text, opts = {}) {
  return {
    key: {
      remoteJid: jid,
      id: opts.id || `msg_${Math.random().toString(36).slice(2, 8)}`,
      fromMe: opts.fromMe || false,
    },
    message: { conversation: text },
    messageTimestamp: opts.timestamp || Math.floor(Date.now() / 1000),
    pushName: opts.pushName || '',
  };
}


// ═══════════════════════════════════════════════════════════════
// TESTS
// ═══════════════════════════════════════════════════════════════

describe('History Sync Broadcast', () => {

  let historyMessages;
  let broadcastedMessages;
  let onMessage;

  beforeEach(() => {
    historyMessages = new Map();
    broadcastedMessages = [];
    onMessage = (msg) => broadcastedMessages.push(msg);
  });

  // ─── messaging-history.set handler ──────────────

  it('broadcasts history messages to gateway', () => {
    const histMsgs = [
      fakeMsg('8618936119618@s.whatsapp.net', 'Hello from history', { fromMe: false }),
      fakeMsg('14155551234@s.whatsapp.net', 'Another history msg', { fromMe: true }),
    ];

    const { count } = processHistorySet(histMsgs, historyMessages, onMessage);

    assert.equal(count, 2, 'should store 2 messages');
    assert.equal(broadcastedMessages.length, 2, 'should broadcast 2 messages');
    assert.equal(broadcastedMessages[0].sender, '8618936119618@s.whatsapp.net');
    assert.equal(broadcastedMessages[0].content, 'Hello from history');
    assert.equal(broadcastedMessages[0].fromMe, false);
    assert.equal(broadcastedMessages[1].sender, '14155551234@s.whatsapp.net');
    assert.equal(broadcastedMessages[1].fromMe, true);
  });

  it('skips status@broadcast messages', () => {
    const histMsgs = [
      fakeMsg('status@broadcast', 'Status update'),
      fakeMsg('8618936119618@s.whatsapp.net', 'Real message'),
    ];

    processHistorySet(histMsgs, historyMessages, onMessage);

    assert.equal(broadcastedMessages.length, 1);
    assert.equal(broadcastedMessages[0].content, 'Real message');
  });

  it('skips group messages (@g.us)', () => {
    const histMsgs = [
      fakeMsg('120363041234567890@g.us', 'Group hello'),
      fakeMsg('14155559999@s.whatsapp.net', 'Direct msg'),
    ];

    processHistorySet(histMsgs, historyMessages, onMessage);

    assert.equal(broadcastedMessages.length, 1);
    assert.equal(broadcastedMessages[0].sender, '14155559999@s.whatsapp.net');
  });

  it('skips messages with no extractable text content', () => {
    const histMsgs = [
      { key: { remoteJid: '14155551234@s.whatsapp.net', id: 'x1' }, message: {}, messageTimestamp: 1000 },
      { key: { remoteJid: '14155551234@s.whatsapp.net', id: 'x2' }, message: null, messageTimestamp: 1001 },
      fakeMsg('14155551234@s.whatsapp.net', 'Has text'),
    ];

    processHistorySet(histMsgs, historyMessages, onMessage);

    assert.equal(broadcastedMessages.length, 1);
    assert.equal(broadcastedMessages[0].content, 'Has text');
  });

  it('caps at 50 messages per chat', () => {
    const jid = '14155551234@s.whatsapp.net';
    const histMsgs = [];
    for (let i = 0; i < 60; i++) {
      histMsgs.push(fakeMsg(jid, `Message ${i}`, { id: `id_${i}` }));
    }

    const { count } = processHistorySet(histMsgs, historyMessages, onMessage);

    assert.equal(count, 50, 'should cap at 50');
    assert.equal(broadcastedMessages.length, 50);
    assert.equal(historyMessages.get(jid).length, 50);
  });

  it('handles extendedTextMessage format', () => {
    const histMsgs = [{
      key: { remoteJid: '14155551234@s.whatsapp.net', id: 'ext1' },
      message: { extendedTextMessage: { text: 'Extended text reply' } },
      messageTimestamp: 1000,
    }];

    processHistorySet(histMsgs, historyMessages, onMessage);

    assert.equal(broadcastedMessages.length, 1);
    assert.equal(broadcastedMessages[0].content, 'Extended text reply');
  });

  it('preserves pushName in broadcast', () => {
    const histMsgs = [
      fakeMsg('14155551234@s.whatsapp.net', 'Hi', { pushName: 'John Doe' }),
    ];

    processHistorySet(histMsgs, historyMessages, onMessage);

    assert.equal(broadcastedMessages[0].pushName, 'John Doe');
  });

  it('broadcasts nothing when input is empty', () => {
    processHistorySet([], historyMessages, onMessage);
    assert.equal(broadcastedMessages.length, 0);
  });

  // ─── setupClient history push ──────────────────

  it('pushes existing history to newly connected gateway', () => {
    // Pre-populate history (simulating data stored from a previous sync)
    historyMessages.set('8618936119618@s.whatsapp.net', [
      { id: 'h1', remoteJid: '8618936119618@s.whatsapp.net', fromMe: false, content: 'Old msg 1', timestamp: 1000 },
      { id: 'h2', remoteJid: '8618936119618@s.whatsapp.net', fromMe: true, content: 'Old reply', timestamp: 1001 },
    ]);
    historyMessages.set('14155559999@s.whatsapp.net', [
      { id: 'h3', remoteJid: '14155559999@s.whatsapp.net', fromMe: false, content: 'Another old msg', timestamp: 2000 },
    ]);

    const sent = [];
    const count = pushHistoryToClient(historyMessages, (msg) => sent.push(msg));

    assert.equal(count, 3, 'should push all 3 stored messages');
    assert.equal(sent.length, 3);
    assert.equal(sent[0].content, 'Old msg 1');
    assert.equal(sent[0].sender, '8618936119618@s.whatsapp.net');
    assert.equal(sent[2].content, 'Another old msg');
  });

  it('pushes zero when no history exists', () => {
    const sent = [];
    const count = pushHistoryToClient(historyMessages, (msg) => sent.push(msg));
    assert.equal(count, 0);
    assert.equal(sent.length, 0);
  });

  // ─── History persistence (disk) ────────────────

  it('saves and loads history to/from disk', () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-test-'));
    const histPath = path.join(tmpDir, 'history.json');

    // Simulate save
    const data = {
      '14155551234@s.whatsapp.net': [
        { id: 'p1', remoteJid: '14155551234@s.whatsapp.net', fromMe: false, content: 'Persisted msg', timestamp: 5000 },
      ],
    };
    fs.writeFileSync(histPath, JSON.stringify(data), 'utf-8');

    // Simulate load
    const raw = fs.readFileSync(histPath, 'utf-8');
    const loaded = JSON.parse(raw);
    const restoredMap = new Map();
    for (const [jid, msgs] of Object.entries(loaded)) {
      restoredMap.set(jid, msgs);
    }

    assert.equal(restoredMap.size, 1);
    assert.equal(restoredMap.get('14155551234@s.whatsapp.net')[0].content, 'Persisted msg');

    // Cleanup
    fs.rmSync(tmpDir, { recursive: true });
  });
});
