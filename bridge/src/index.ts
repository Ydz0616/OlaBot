#!/usr/bin/env node
/**
 * nanobot WhatsApp Bridge
 *
 * This bridge connects WhatsApp Web to nanobot's Python backend
 * via WebSocket. It handles authentication, message forwarding,
 * and reconnection logic.
 *
 * Usage:
 *   npm run build && npm start
 *   npm run dev                  # Build + run in one step
 *   npm run dev -- --fresh       # Force fresh QR (wipe auth)
 *
 * Flags:
 *   --fresh   Wipe existing auth state before connecting.
 *             Forces a new QR code scan (clean cold start).
 */

// Polyfill crypto for Baileys in ESM
import { webcrypto } from 'crypto';
if (!globalThis.crypto) {
  (globalThis as any).crypto = webcrypto;
}

import { BridgeServer } from './server.js';
import { homedir } from 'os';
import { join } from 'path';
import fs from 'fs';

const PORT = parseInt(process.env.BRIDGE_PORT || '3001', 10);
const AUTH_DIR = process.env.AUTH_DIR || join(homedir(), '.nanobot', 'whatsapp-auth');
const TOKEN = process.env.BRIDGE_TOKEN || undefined;

// Parse CLI flags
const args = process.argv.slice(2);
const freshMode = args.includes('--fresh');

console.log('🐈 nanobot WhatsApp Bridge');
console.log('========================\n');

// --fresh flag: wipe auth state for a clean cold start
if (freshMode) {
  console.log('⚠️  --fresh flag detected: wiping auth state for clean cold start');
  try {
    if (fs.existsSync(AUTH_DIR)) {
      fs.rmSync(AUTH_DIR, { recursive: true, force: true });
      console.log(`🗑️  Removed auth directory: ${AUTH_DIR}`);
    }
  } catch (err) {
    console.error('Failed to wipe auth state:', err);
  }
}

// Ensure auth directory exists
if (!fs.existsSync(AUTH_DIR)) {
  fs.mkdirSync(AUTH_DIR, { recursive: true });
  console.log(`📁 Created auth directory: ${AUTH_DIR}`);
}

const server = new BridgeServer(PORT, AUTH_DIR, TOKEN);

// Handle graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n\nShutting down...');
  await server.stop();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  await server.stop();
  process.exit(0);
});

// Start the server
server.start().catch((error) => {
  console.error('Failed to start bridge:', error);
  process.exit(1);
});
