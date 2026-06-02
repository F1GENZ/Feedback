const dns = require('dns');
dns.setDefaultResultOrder('ipv4first');
const https = require('https');

const express = require('express');
const cors = require('cors');
const sheetsClient = require('./sheetsClient');
const crypto = require('crypto');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
require('dotenv').config();

const app = express();

// Performance: gzip compression
try { app.use(require('compression')()); } catch(e) { /* compression not installed */ }

// Middleware
app.use(cors());
app.use(express.json({ limit: '10mb' }));

// Telegram User ID to Host mapping (ID is more reliable than username)
// To get your ID: send /myid to the bot
const TELEGRAM_ID_TO_HOST = {
  '814408956': 'Quốc',
  '852487488': 'Taiz',
  '642649821': 'Lâm',
  '801593125': 'Nghĩa',  // Note: Tuan và Nghĩa có cùng ID
  // '801593125': 'Tuan', // Tuan có cùng ID với Nghĩa - cần xác nhận lại
};

// Reverse mapping for notifications
const HOST_TO_TELEGRAM_ID = Object.fromEntries(
  Object.entries(TELEGRAM_ID_TO_HOST).map(([id, host]) => [host, id])
);

// Cache for R2 URLs (to avoid re-uploading same images)
const imageUrlCache = new Map();
let r2UploadDisabledUntil = 0;
let lastR2AuthWarningAt = 0;
const R2_AUTH_BACKOFF_MS = 10 * 60 * 1000;
let r2AuthDisabled = false;
const telegramMediaGroups = new Map();
const TELEGRAM_MEDIA_GROUP_DELAY_MS = 1200;
// Helper: Format Vietnam timezone timestamp
function formatVNTimestamp(format = 'full') {
  const now = new Date();
  const vn = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Ho_Chi_Minh' }));
  const d = vn.getDate().toString().padStart(2, '0');
  const m = (vn.getMonth() + 1).toString().padStart(2, '0');
  const y = vn.getFullYear();
  const h = vn.getHours().toString().padStart(2, '0');
  const min = vn.getMinutes().toString().padStart(2, '0');
  const s = vn.getSeconds().toString().padStart(2, '0');
  return format === 'short' ? `${h}:${min} ${d}/${m}/${y}` : `${h}:${min}:${s} ${d}/${m}/${y}`;
}

// Helper: Parse comments from JSON string
function parseComments(rawString) {
  if (!rawString) return [];
  const trimmed = rawString.trim();
  if (trimmed.startsWith('[') && trimmed.endsWith(']')) {
    try {
      const parsed = JSON.parse(trimmed);
      return Array.isArray(parsed) ? parsed : [];
    } catch (e) { return []; }
  }
  // Legacy text → wrap as comment
  return [{ text: rawString, time: 'Note cũ', author: 'System' }];
}

function extractCommentImageUrls(text) {
  if (!text) return [];

  const urls = [];
  const telegramIdRegex = /\[TG_IMAGE\]\s*(\S+)/g;
  let idMatch;
  while ((idMatch = telegramIdRegex.exec(text)) !== null) {
    urls.push(idMatch[1].trim());
  }

  const markerRegex = /\[IMAGE\]\s*(https?:\/\/\S+)/g;
  let match;
  while ((match = markerRegex.exec(text)) !== null) {
    urls.push(match[1].trim());
  }

  const urlRegex = /https?:\/\/\S+/g;
  while ((match = urlRegex.exec(text)) !== null) {
    const url = match[0].trim();
    const lower = url.toLowerCase();
    const looksLikeImage =
      lower.includes('images.f1genz.dev/') ||
      lower.includes('api.telegram.org/file/') ||
      /\.(png|jpe?g|gif|webp)(\?|#|$)/i.test(lower);
    if (looksLikeImage) urls.push(url);
  }

  return [...new Set(urls)];
}

function stripCommentImageData(text) {
  if (!text) return '';
  return text
    .replace(/\[TG_IMAGE\]\s*\S+/g, '')
    .replace(/\[IMAGE\]\s*https?:\/\/\S+/g, '')
    .replace(/https?:\/\/\S*(?:images\.f1genz\.dev|api\.telegram\.org\/file)\S*/g, '')
    .replace(/https?:\/\/\S+\.(?:png|jpe?g|gif|webp)(?:\?\S*)?/gi, '')
    .trim();
}

function getFeedbackCommentImages(feedback) {
  const comments = parseComments(feedback.devNote || '');
  return comments.flatMap(comment => extractCommentImageUrls(comment.text || ''));
}

function formatTelegramImageComment(firstName, text, imageUrl, fileId = '') {
  const prefix = `[Telegram] ${firstName}: ${text || ''}`.trim();
  const imageRef = formatTelegramImageReference(imageUrl, fileId);
  return imageRef ? `${prefix}\n${imageRef}` : prefix;
}

function formatTelegramImageReference(imageUrl, fileId) {
  if (imageUrl && !imageUrl.startsWith('Error:')) return `[IMAGE]${imageUrl}`;
  return fileId ? `[TG_IMAGE]${fileId}` : '';
}

// Singleton S3 client (reused across all uploads)
let _s3Client = null;
function getS3Client() {
  if (!_s3Client) {
    _s3Client = new S3Client({
      region: 'auto',
      endpoint: `https://${process.env.CLOUDFLARE_ACCOUNT_ID}.r2.cloudflarestorage.com`,
      credentials: {
        accessKeyId: process.env.CLOUDFLARE_R2_ACCESS_KEY_ID,
        secretAccessKey: process.env.CLOUDFLARE_R2_SECRET_ACCESS_KEY,
      },
    });
  }
  return _s3Client;
}

function isR2AuthError(error) {
  const message = String(error?.message || error || '').toLowerCase();
  const name = String(error?.name || '').toLowerCase();
  return message.includes('unauthorized') || message.includes('access denied') || name.includes('forbidden');
}

function shouldSkipR2Upload() {
  return r2AuthDisabled || Date.now() < r2UploadDisabledUntil;
}

function handleR2UploadError(error, context = 'R2 upload') {
  if (!isR2AuthError(error)) {
    console.error(`${context}:`, error.message || error);
    return;
  }

  if (r2AuthDisabled) return;
  r2AuthDisabled = true;
  r2UploadDisabledUntil = Date.now() + R2_AUTH_BACKOFF_MS;
  if (Date.now() - lastR2AuthWarningAt > R2_AUTH_BACKOFF_MS) {
    lastR2AuthWarningAt = Date.now();
    console.error(`${context}: Unauthorized. R2 upload disabled until server restart. Check CLOUDFLARE_R2_ACCESS_KEY_ID / CLOUDFLARE_R2_SECRET_ACCESS_KEY / bucket permissions.`);
  }
}

// In-memory cache for getAllData (reduces Google Sheets API calls)
let _dataCache = null;
let _dataCacheTime = 0;
const DATA_CACHE_TTL = 10000; // 10 seconds
const DONE_AUTO_DELETE_AFTER_DAYS = 30;
let _lastDoneCleanupTime = 0;
const DONE_CLEANUP_INTERVAL_MS = 60 * 60 * 1000; // 1 hour
const OVERDUE_CHECK_INTERVAL_MS = 60 * 60 * 1000; // 1 hour

async function getCachedData(forceRefresh = false) {
  if (!forceRefresh && _dataCache && (Date.now() - _dataCacheTime < DATA_CACHE_TTL)) {
    return _dataCache;
  }
  _dataCache = await sheetsClient.getAllData();
  _dataCacheTime = Date.now();
  return _dataCache;
}

function invalidateDataCache() {
  _dataCache = null;
  _dataCacheTime = 0;
}

function parseVNTimestamp(value) {
  if (!value) return 0;
  const match = String(value).match(/(?:(\d{1,2}):(\d{2})(?::(\d{2}))?\s+)?(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (!match) return 0;
  const [, hh = '0', mm = '0', ss = '0', dd, mo, yyyy] = match;
  return new Date(Number(yyyy), Number(mo) - 1, Number(dd), Number(hh), Number(mm), Number(ss)).getTime();
}

function formatTodayDateInput() {
  const vn = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Ho_Chi_Minh' }));
  const y = vn.getFullYear();
  const m = (vn.getMonth() + 1).toString().padStart(2, '0');
  const d = vn.getDate().toString().padStart(2, '0');
  return `${y}-${m}-${d}`;
}

function parseDeadlineDate(value) {
  if (!value) return 0;
  const raw = String(value).trim();
  let match = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (match) return new Date(Number(match[1]), Number(match[2]) - 1, Number(match[3])).getTime();

  match = raw.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (match) return new Date(Number(match[3]), Number(match[2]) - 1, Number(match[1])).getTime();

  return 0;
}

function getTodayStartTime() {
  return parseDeadlineDate(formatTodayDateInput());
}

function getPriorityValue(row) {
  const priority = parseInt(row.priority, 10);
  return Number.isNaN(priority) ? 999999 : priority;
}

function sortFeedbackByPriority(rows) {
  return [...rows].sort((a, b) => {
    const pa = getPriorityValue(a);
    const pb = getPriorityValue(b);
    if (pa !== pb) return pa - pb;
    return parseVNTimestamp(b.time) - parseVNTimestamp(a.time);
  });
}

function hasHotTag(row) {
  return String(row.tags || '')
    .split(/[,\s]+/)
    .some(tag => normalizeTagToken(tag) === 'gap');
}

function normalizeText(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/đ/g, 'd');
}

function normalizeTagToken(value) {
  const normalized = normalizeText(value);
  if (normalized === 'gap' || normalized === 'hot') return 'gap';
  return normalized;
}

function normalizeTags(tags) {
  return [...new Set(String(tags || '')
    .split(/[,\s]+/)
    .map(normalizeTagToken)
    .filter(tag => TAGS_LIST.includes(tag)))]
    .join(', ');
}

function hasUrgentTag(row) {
  return String(row.tags || '')
    .split(/[,\s]+/)
    .map(normalizeTagToken)
    .some(tag => tag === 'gap');
}

function addUrgentTag(tags) {
  const normalized = normalizeTags(tags);
  const parts = normalized ? normalized.split(',').map(t => t.trim()).filter(Boolean) : [];
  if (!parts.includes('gap')) parts.push('gap');
  return parts.join(', ');
}

function getAvailableHosts(rows = []) {
  const hosts = [...Object.values(TELEGRAM_ID_TO_HOST), ...rows.map(row => row.host).filter(Boolean)];
  return [...new Set(hosts)].filter(Boolean);
}

function resolveHostName(input, rows = []) {
  const wanted = normalizeText(input);
  if (!wanted) return '';

  const alias = Object.entries(HOST_ALIAS_MAP).find(([key]) => normalizeText(key) === wanted);
  if (alias) return alias[1];

  return getAvailableHosts(rows).find(host => normalizeText(host) === wanted) || '';
}

async function cleanupOldDoneFeedbacks(rows, force = false) {
  const now = Date.now();
  if (!force && now - _lastDoneCleanupTime < DONE_CLEANUP_INTERVAL_MS) return 0;
  _lastDoneCleanupTime = now;

  const cutoff = now - DONE_AUTO_DELETE_AFTER_DAYS * 24 * 60 * 60 * 1000;
  const rowsToDelete = rows
    .filter(row => row.stage === 'Done')
    .filter(row => {
      const doneTime = parseVNTimestamp(row.updatedAt || row.time);
      return doneTime > 0 && doneTime < cutoff;
    })
    .map(row => row.rowNumber)
    .sort((a, b) => b - a);

  for (const rowNumber of rowsToDelete) {
    await sheetsClient.deleteRow(rowNumber);
  }

  if (rowsToDelete.length > 0) {
    invalidateDataCache();
    console.log(`[Cleanup] Deleted ${rowsToDelete.length} Done feedback older than ${DONE_AUTO_DELETE_AFTER_DAYS} days`);
  }

  return rowsToDelete.length;
}

async function runDoneCleanupJob() {
  try {
    const data = await sheetsClient.getAllData();
    await cleanupOldDoneFeedbacks(data.rows || [], true);
  } catch (error) {
    console.error('[Cleanup] Done feedback cleanup failed:', error);
  }
}

async function runOverdueFeedbackJob() {
  try {
    const data = await sheetsClient.getAllData();
    const todayStart = getTodayStartTime();
    const overdueRows = (data.rows || []).filter(row => {
      if (row.stage !== 'Feedback') return false;
      if (hasUrgentTag(row)) return false;
      const deadlineTime = parseDeadlineDate(row.deadline);
      return deadlineTime > 0 && deadlineTime < todayStart;
    });

    if (overdueRows.length === 0) return;

    const timestamp = formatVNTimestamp();
    const updates = [];
    overdueRows.forEach(row => {
      updates.push({ rowNumber: row.rowNumber, colLetter: 'G', value: addUrgentTag(row.tags) });
      updates.push({ rowNumber: row.rowNumber, colLetter: 'O', value: timestamp });
    });

    await sheetsClient.batchUpdateCells(updates);
    invalidateDataCache();

    const groupChatId = process.env.TELEGRAM_GROUP_CHAT_ID;
    if (groupChatId) {
      const lines = overdueRows.slice(0, 10).map(row => `#${row.rowNumber} ${row.host || '-'} - ${row.shop || 'N/A'}`);
      const more = overdueRows.length > 10 ? `\n...và ${overdueRows.length - 10} feedback khác` : '';
      await sendTelegramMessage(
        groupChatId,
        `🔥 ${overdueRows.length} feedback quá deadline đã gắn tag Gấp:\n${lines.join('\n')}${more}`,
        { disable_web_page_preview: true }
      );
    }
  } catch (error) {
    console.error('[Deadline] Overdue feedback job failed:', error);
  }
}


// ==================== SMART PARSING CONSTANTS ====================
const VALID_DOMAINS = ['.com', '.vn', '.com.vn', '.myharavan.com', '.mysapo.net', '.net', '.asia', '.org', '.group', '.top', '.online', '.dev'];
const LINK_FEEDBACK_FORMATS = ['docs.google.com', 'drive.google.com', 'onedrive.live.com', 'figma.com', 'canva.com', 'trello.com'];
const TAGS_LIST = ['hrv', 'haravan', 'gap', 'note', 'baogia', 'sapo', 'hot'];
const HOST_ALIAS_MAP = {
  'quoc': 'Quốc', 'quốc': 'Quốc',
  'taiz': 'Taiz', 'tai': 'Taiz', 'tài': 'Taiz',
  'lam': 'Lâm', 'lâm': 'Lâm',
  'nghia': 'Nghĩa', 'nghĩa': 'Nghĩa',
  'tuan': 'Tuan', 'tuấn': 'Tuan'
};

function parseMessageContent(text, firstName) {
  const message = text || '';
  const cleaned = message.replace(/[^\p{L}\p{N}\s]/gu, ' ').trim();
  const words = message.split(/\s+/);
  const HOSTS = Object.values(TELEGRAM_ID_TO_HOST);

  // Shop: first URL with valid domain that's NOT a feedback link
  const shopDomains = words.filter(w =>
    !LINK_FEEDBACK_FORMATS.some(f => w.includes(f)) && VALID_DOMAINS.some(d => w.includes(d))
  ).map(u => (u.match(/https?:\/\/([^\/]+)/)?.[1] || u.split('/')[0]).replace(/^www\./, '').trim());
  const shop = shopDomains[0] || '';

  // Link: first URL matching feedback formats
  const allUrls = message.match(/https?:\/\/\S+/g) || [];
  const link = allUrls.find(u => LINK_FEEDBACK_FORMATS.some(f => u.includes(f))) || '';

  // Host: from message keywords or fallback to firstName
  let hostMatch = cleaned.split(/\s+/).filter(w => HOSTS.some(k => k.toLowerCase() === w.toLowerCase()));
  if (hostMatch.length === 0) hostMatch = [firstName.trim()];
  hostMatch = hostMatch.map(h => HOSTS.find(k => k.toLowerCase() === h.toLowerCase()) || h);
  const host = [...new Set(hostMatch)].join(';');

  // Tags
  const tags = normalizeTags(cleaned);

  // Note: remaining text after removing bare domain URLs, host, tags
  // Keep URLs that have path or query; remove bare domains only
  let content = message;
  words.filter(u => VALID_DOMAINS.some(d => u.includes(d))).forEach(u => {
    try {
      const urlStr = u.startsWith('http') ? u : 'https://' + u;
      const urlObj = new URL(urlStr);
      const hasPath = urlObj.pathname !== '/' && urlObj.pathname !== '';
      const hasQuery = urlObj.search !== '';
      if (!hasPath && !hasQuery) {
        content = content.replace(u, '').trim();
      }
    } catch (e) {
      // URL parsing failed → remove it (likely just a bare domain)
      content = content.replace(u, '').trim();
    }
  });
  hostMatch.forEach(h => { content = content.replace(new RegExp(`\\b${h}\\b`, 'gi'), '').trim(); });
  cleaned.split(/[\s,]+/).filter(w => TAGS_LIST.includes(normalizeTagToken(w))).forEach(t => { content = content.replace(new RegExp(`\\b${t}\\b`, 'gi'), '').trim(); });
  content = content.replace(/[.,;]{2,}/g, ' ').replace(/\s+/g, ' ').trim();

  return { shop, link, host, tags, note: content, message };
}

async function handleCreateFromTelegram(chatId, firstName, text, photoId, userId, chatType, extraPhotoIds = []) {
  try {
    const parsed = parseMessageContent(text || '', firstName);
    const ts = formatVNTimestamp();
    const deadline = hasUrgentTag({ tags: parsed.tags }) ? formatTodayDateInput() : '';
    const feedbackId = Date.now().toString();

    const row = [
      feedbackId, deadline, parsed.host, parsed.shop, parsed.link,
      'Feedback', parsed.tags, '', '', parsed.note,
      ts, parsed.message, userId, photoId || '', ''
    ];
    await sheetsClient.appendRow(row);
    invalidateDataCache();

    const data = await getCachedData(true);
    const createdRow = (data.rows || []).find(r => r.id === feedbackId);

    if (createdRow && extraPhotoIds.length > 0) {
      const imageLines = [];
      for (let i = 0; i < extraPhotoIds.length; i++) {
        const photoUrl = await uploadTelegramPhotoToR2(extraPhotoIds[i]);
        const imageRef = formatTelegramImageReference(photoUrl, extraPhotoIds[i]);
        if (imageRef) imageLines.push(imageRef);
      }
      if (imageLines.length > 0) {
        await addCommentToFeedback(createdRow.rowNumber, `[Telegram] ${firstName}: ảnh bổ sung\n${imageLines.join('\n')}`);
      }
    }

    const feedbackCount = (data.rows || []).filter(r => r.host === parsed.host && r.stage === 'Feedback').length;
    await sendTelegramMessage(chatId, `✅ ${parsed.host} có ${feedbackCount} feedback`);

    // Always notify group
    const groupChatId = process.env.TELEGRAM_GROUP_CHAT_ID;
    if (groupChatId) {
      await sendTelegramMessage(groupChatId, `📬 ${parsed.host} có ${feedbackCount} feedback`);
    }
  } catch (error) {
    console.error('Create from Telegram error:', error);
    await sendTelegramMessage(chatId, `❌ Lỗi: ${error.message}`);
  }
}

function getTelegramMessageImageId(message) {
  if (message.document && message.document.mime_type && message.document.mime_type.startsWith('image/')) {
    return message.document.file_id;
  }
  if (message.photo && message.photo.length > 0) {
    return message.photo[message.photo.length - 1].file_id;
  }
  return '';
}

function isTelegramCreateCommandText(text) {
  return /^\/f(?:@\S+)?(?:\s|$)/.test(text || '');
}

function stripTelegramCreateCommand(text) {
  return (text || '').replace(/^\/f(?:@\S+)?\s*/, '').trim();
}

function queueTelegramMediaGroup({ message, chatId, firstName, userId, chatType, text, photoId }) {
  const key = `${chatId}:${message.media_group_id}`;
  const originalText = message.reply_to_message?.text || message.reply_to_message?.caption || '';
  const replyMatch = originalText.match(/#(\d+)/);
  const existing = telegramMediaGroups.get(key) || {
    chatId,
    firstName,
    userId,
    chatType,
    text: '',
    photoIds: [],
    replyRowNumber: replyMatch ? parseInt(replyMatch[1], 10) : null,
    timer: null
  };

  if (text && !existing.text) existing.text = text;
  if (photoId && !existing.photoIds.includes(photoId)) existing.photoIds.push(photoId);
  if (!existing.replyRowNumber && replyMatch) existing.replyRowNumber = parseInt(replyMatch[1], 10);

  clearTimeout(existing.timer);
  existing.timer = setTimeout(() => {
    processTelegramMediaGroup(key).catch(error => {
      console.error('Telegram media group error:', error);
      sendTelegramMessage(chatId, `❌ Lỗi album ảnh: ${error.message}`).catch(() => {});
    });
  }, TELEGRAM_MEDIA_GROUP_DELAY_MS);

  telegramMediaGroups.set(key, existing);
}

async function buildTelegramAlbumComment(firstName, text, photoIds) {
  const imageLines = [];
  for (const photoId of photoIds) {
    const photoUrl = await uploadTelegramPhotoToR2(photoId);
    const imageRef = formatTelegramImageReference(photoUrl, photoId);
    if (imageRef) imageLines.push(imageRef);
  }

  const prefix = `[Telegram] ${firstName}: ${text || 'ảnh'}`;
  return imageLines.length > 0 ? `${prefix}\n${imageLines.join('\n')}` : prefix;
}

async function processTelegramMediaGroup(key) {
  const group = telegramMediaGroups.get(key);
  if (!group) return;
  telegramMediaGroups.delete(key);

  if (group.replyRowNumber) {
    const commentText = await buildTelegramAlbumComment(group.firstName, group.text, group.photoIds);
    await addCommentToFeedback(group.replyRowNumber, commentText);
    await sendTelegramMessage(group.chatId, `✅ Đã thêm ${group.photoIds.length} ảnh vào #${group.replyRowNumber}`);
    return;
  }

  const isCreateCommand = isTelegramCreateCommandText(group.text);
  if (group.chatType !== 'private' && !isCreateCommand) return;

  const createText = isCreateCommand
    ? stripTelegramCreateCommand(group.text)
    : group.text;
  await handleCreateFromTelegram(
    group.chatId,
    group.firstName,
    createText,
    group.photoIds[0] || '',
    group.userId,
    group.chatType,
    group.photoIds.slice(1)
  );
}

// ==================== TELEGRAM BOT WEBHOOK ====================
app.post('/api/telegram-webhook', async (req, res) => {
  try {
    const update = req.body;
    
    // Only process message updates
    if (!update.message) {
      return res.json({ ok: true });
    }
    
    const message = update.message;
    const chatId = message.chat.id;
    const userId = message.from.id.toString();
    const text = (message.text || message.caption || '').trim();
    const username = message.from.username || '';
    const firstName = message.from.first_name || 'User';
    const chatType = message.chat.type;
    
    const photoId = getTelegramMessageImageId(message);

    if (message.media_group_id && photoId) {
      queueTelegramMediaGroup({ message, chatId, firstName, userId, chatType, text, photoId });
      return res.json({ ok: true });
    }
    
    // Handle /myid command - show user's Telegram ID
    if (text === '/myid') {
      await sendTelegramMessage(chatId, 
        `🆔 *Thông tin của bạn:*\n\n` +
        `• User ID: \`${userId}\`\n` +
        `• Chat ID: \`${chatId}\`\n` +
        `• Username: @${username}`,
        { parse_mode: 'Markdown' }
      );
      return res.json({ ok: true });
    }
    
    // Handle /groupid command - show group chat ID
    if (text === '/groupid') {
      const chatTitle = message.chat.title || 'N/A';
      
      await sendTelegramMessage(chatId, 
        `🆔 *Thông tin chat:*\n\n` +
        `• Chat ID: \`${chatId}\`\n` +
        `• Type: ${chatType}\n` +
        `• Title: ${chatTitle}\n\n` +
        `${chatType !== 'private' ? '✅ Đây là Group ID, copy vào .env!' : '⚠️ Đây là chat riêng, không phải group'}`,
        { parse_mode: 'Markdown' }
      );
      return res.json({ ok: true });
    }
    
    // Handle // command - Read feedbacks (//, // all, // Tên, // shop)
    if (!message.reply_to_message && (text === '//' || text.startsWith('// '))) {
      const keyword = text.startsWith('// ') ? text.substring(3).trim().toLowerCase() : '';
      const isAll = keyword === 'all';
      
      const currentUserHost = TELEGRAM_ID_TO_HOST[userId] || firstName;
      const data = await getCachedData();
      const rows = data.rows || [];
      
      let filtered;
      if (isAll) {
        filtered = rows.filter(r => r.stage === 'Feedback');
      } else if (keyword) {
        // Check if keyword matches a host name
        const matchedHost = HOST_ALIAS_MAP[keyword];
        if (matchedHost) {
          filtered = rows.filter(r => r.host === matchedHost && r.stage === 'Feedback');
        } else {
          // Search by shop name
          filtered = rows.filter(r => r.stage === 'Feedback' && r.shop && r.shop.toLowerCase().includes(keyword));
        }
      } else {
        // // without keyword → own feedbacks
        filtered = rows.filter(r => r.host === currentUserHost && r.stage === 'Feedback');
      }
      filtered = sortFeedbackByPriority(filtered);
      
      const MAX_ITEMS = 20;
      if (filtered.length === 0) {
        await sendTelegramMessage(chatId, '🎉🎉 Hết Feedback! 🎉🎉');
        return res.json({ ok: true });
      }
      
      if (filtered.length > MAX_ITEMS) {
        await sendTelegramMessage(chatId, `📊 Hiện ${MAX_ITEMS}/${filtered.length} feedback ưu tiên`);
        filtered = filtered.slice(0, MAX_ITEMS);
      }
      
      telegramRequest(`https://api.telegram.org/bot${process.env.TELEGRAM_BOT_TOKEN}/sendChatAction`, { chat_id: chatId, action: 'typing' });
      
      for (const fb of filtered) {
        await sendFeedbackToTelegram(chatId, fb);
      }
      
      return res.json({ ok: true });
    }
    
    // Handle reply to feedback messages (with ID in text or caption for photos)
    if (message.reply_to_message) {
      const originalText = message.reply_to_message.text || message.reply_to_message.caption || '';
      
      // Extract rowNumber from #123 pattern
      const match = originalText.match(/#(\d+)/);
      if (match) {
        const rowNumber = parseInt(match[1]);
        
        // Get text from either message.text or message.caption (for photo replies)
        const replyText = text || message.caption || '';
        const lowerText = replyText.toLowerCase();
        
        try {
          // Reply "// Tên" to move this feedback to another host.
          if (lowerText.startsWith('//')) {
            const targetInput = replyText.replace(/^\/\/\s*/, '').trim();
            const data = await getCachedData(true);
            const rows = data.rows || [];
            const targetHost = resolveHostName(targetInput, rows);

            if (!targetHost) {
              await sendTelegramMessage(chatId, `⚠️ Không tìm thấy host "${targetInput}". Host hiện có: ${getAvailableHosts(rows).join(', ')}`);
              return res.json({ ok: true });
            }

            const timestamp = formatVNTimestamp();
            await sheetsClient.updateCell(rowNumber, 'C', targetHost);
            await sheetsClient.updateCell(rowNumber, 'O', timestamp);
            invalidateDataCache();
            await sendTelegramMessage(chatId, `✅ #${rowNumber} đã chuyển sang ${targetHost}`);
            if (targetHost) await notifyHostFeedbackCount(targetHost);
            return res.json({ ok: true });
          }

          // Check if message starts with "Done"
          if (lowerText === 'done' || lowerText.startsWith('done ') || lowerText.startsWith('done-') || lowerText.startsWith('done:')) {
            // Update stage to Done
            const now = new Date();
            const vnTime = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Ho_Chi_Minh' }));
            const d = vnTime.getDate().toString().padStart(2, '0');
            const m = (vnTime.getMonth() + 1).toString().padStart(2, '0');
            const y = vnTime.getFullYear();
            const h = vnTime.getHours().toString().padStart(2, '0');
            const min = vnTime.getMinutes().toString().padStart(2, '0');
            const s = vnTime.getSeconds().toString().padStart(2, '0');
            const timestamp = `${h}:${min}:${s} ${d}/${m}/${y}`;
            
            await sheetsClient.updateCell(rowNumber, 'F', 'Done');
            await sheetsClient.updateCell(rowNumber, 'O', timestamp);
            
            // If there's additional text after "Done" or photo, add as comment
            const extraText = replyText.replace(/^done[\s\-:]*/i, '').trim();
            let commentText = '';
            
            // Check for document (uncompressed image) first, then photo
            if (message.document && message.document.mime_type && message.document.mime_type.startsWith('image/')) {
              const photoUrl = await uploadTelegramPhotoToR2(message.document.file_id);
              commentText = formatTelegramImageComment(firstName, extraText || 'Done', photoUrl, message.document.file_id);
            } else if (message.photo && message.photo.length > 0) {
              // Get largest photo (compressed)
              const photo = message.photo[message.photo.length - 1];
              const photoUrl = await uploadTelegramPhotoToR2(photo.file_id);
              commentText = formatTelegramImageComment(firstName, extraText || 'Done', photoUrl, photo.file_id);
            } else if (extraText) {
              commentText = `[Telegram] ${firstName}: ${extraText}`;
            }
            
            if (commentText) {
              await addCommentToFeedback(rowNumber, commentText);
            }
            
            await sendTelegramMessage(chatId, `✅ #${rowNumber} → Done!`);
            
            // Auto-refresh: send remaining feedbacks
            const data = await sheetsClient.getAllData();
            const rows = data.rows || [];
            const remainingFeedbacks = sortFeedbackByPriority(rows.filter(r => 
              r.host === TELEGRAM_ID_TO_HOST[userId] && r.stage === 'Feedback'
            ));
            
            
            if (remainingFeedbacks.length > 0) {
              await sendTelegramMessage(chatId, `📋 Còn ${remainingFeedbacks.length} feedback:`);
              
              // Send messages sequentially to avoid ETIMEDOUT
              for (const fb of remainingFeedbacks) {
                await sendFeedbackToTelegram(chatId, fb);
              }
            } else {
              await sendTelegramMessage(chatId, `🎉 Không còn feedback nào!`);
            }
          } else if (lowerText === 'del' || lowerText.startsWith('del ')) {
            // Delete feedback (set Stage = Deleted)
            const delNow = new Date();
            const delVn = new Date(delNow.toLocaleString('en-US', { timeZone: 'Asia/Ho_Chi_Minh' }));
            const delTs = `${delVn.getHours().toString().padStart(2,'0')}:${delVn.getMinutes().toString().padStart(2,'0')}:${delVn.getSeconds().toString().padStart(2,'0')} ${delVn.getDate().toString().padStart(2,'0')}/${(delVn.getMonth()+1).toString().padStart(2,'0')}/${delVn.getFullYear()}`;
            await sheetsClient.updateCell(rowNumber, 'F', 'Deleted');
            await sheetsClient.updateCell(rowNumber, 'O', delTs);
            await sendTelegramMessage(chatId, `🗑️ Đã xóa ID #${rowNumber}`);
          } else {
            // Any other reply → add as comment
            let commentText = `[Telegram] ${firstName}: ${replyText}`;
            
            // Check for document (uncompressed) first, then photo
            if (message.document && message.document.mime_type && message.document.mime_type.startsWith('image/')) {
              const photoUrl = await uploadTelegramPhotoToR2(message.document.file_id);
              commentText = formatTelegramImageComment(firstName, replyText, photoUrl, message.document.file_id);
            } else if (message.photo && message.photo.length > 0) {
              const photo = message.photo[message.photo.length - 1];
              const photoUrl = await uploadTelegramPhotoToR2(photo.file_id);
              commentText = formatTelegramImageComment(firstName, replyText, photoUrl, photo.file_id);
            }
            
            await addCommentToFeedback(rowNumber, commentText);
          }
        } catch (error) {
          await sendTelegramMessage(chatId, `❌ Lỗi: ${error.message}`);
        }
        return res.json({ ok: true });
      }
    }
    
    // Handle /start command
    if (text === '/start') {
      await sendTelegramMessage(chatId, 
        `👋 Xin chào ${firstName}!\n\n` +
        `🔹 /f <nội dung> - Tạo feedback\n` +
        `🔹 // - Xem feedback\n` +
        `🔹 /help - Xem hướng dẫn`
      );
      return res.json({ ok: true });
    }
    
    // Handle /help command
    if (text === '/help') {
      await sendTelegramMessage(chatId,
        `📚 *Hướng dẫn sử dụng Bot*\n\n` +
        `*🆕 Tạo feedback:*\n` +
        `• \`/f <nội dung>\` - Tạo feedback mới\n` +
        `• Chat riêng: gửi trực tiếp, bot tự tạo\n\n` +
        `*📋 Xem feedback:*\n` +
        `• \`//\` - Xem feedback của mình\n` +
        `• \`// Tên\` - Xem theo host\n` +
        `• \`// all\` - Xem tất cả\n` +
        `• \`// shop\` - Xem theo shop\n\n` +
        `*✅ Xử lý (Reply tin nhắn):*\n` +
        `• \`done\` - Hoàn thành\n` +
        `• \`done <ghi chú>\` - Done + note\n` +
        `• \`del\` - Xóa feedback\n` +
        `• text/ảnh khác - Thêm comment\n\n` +
        `*🔧 Khác:* \`/myid\` \`/groupid\``,
        { parse_mode: 'Markdown' }
      );
      return res.json({ ok: true });
    }
    
    // Handle /f command - Create feedback (group + private)
    if (isTelegramCreateCommandText(text)) {
      const createText = stripTelegramCreateCommand(text);
      if (!createText && !photoId) {
        await sendTelegramMessage(chatId, '⚠️ Cần nội dung.\nVD: `/f neymarsport.com fix lỗi`', { parse_mode: 'Markdown' });
        return res.json({ ok: true });
      }
      await handleCreateFromTelegram(chatId, firstName, createText, photoId, userId, chatType);
      return res.json({ ok: true });
    }
    
    // Direct message in PRIVATE chat → auto create feedback
    if (chatType === 'private' && (text || photoId)) {
      await handleCreateFromTelegram(chatId, firstName, text, photoId, userId, chatType);
      return res.json({ ok: true });
    }
    
    res.json({ ok: true });
  } catch (error) {
    console.error('Telegram webhook error:', error);
    res.json({ ok: true }); // Always return ok to Telegram
  }
});

// Low-level HTTPS request to Telegram (uses system TCP stack like curl, forces IPv4)
function telegramRequest(url, data, timeoutMs = 15000) {
  return new Promise((resolve, reject) => {
    const postData = JSON.stringify(data);
    const urlObj = new URL(url);
    const options = {
      hostname: urlObj.hostname,
      path: urlObj.pathname + urlObj.search,
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(postData) },
      timeout: timeoutMs,
      family: 4 // Force IPv4 only
    };
    const req = https.request(options, (res) => {
      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => {
        try { resolve(JSON.parse(body)); } catch (e) { resolve({ ok: true }); }
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('HTTPS request timeout')); });
    req.write(postData);
    req.end();
  });
}

// Send message to Telegram (using https module, forced IPv4)
async function sendTelegramMessage(chatId, text, options = {}) {
  const botToken = process.env.TELEGRAM_BOT_TOKEN;
  if (!botToken) { console.error('TELEGRAM_BOT_TOKEN not configured'); return; }
  
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const result = await telegramRequest(
        `https://api.telegram.org/bot${botToken}/sendMessage`,
        { chat_id: chatId, text: text, ...options }
      );
      return result;
    } catch (error) {
      console.error(`[Telegram] sendMessage attempt ${attempt}/3 FAILED | ${error.message}`);
      if (attempt < 3) await new Promise(r => setTimeout(r, 1000 * attempt));
      else console.error(`[Telegram] GAVE UP for chat ${chatId}`);
    }
  }
}

// Send photo to Telegram (fast: send file_id directly, R2 upload in background)
async function sendTelegramPhoto(chatId, fileId, caption = '', options = {}, rowNumber = null) {
  const botToken = process.env.TELEGRAM_BOT_TOKEN;
  if (!botToken) { console.error('TELEGRAM_BOT_TOKEN not configured'); return; }
  
  try {
    // Determine what to send: URL, cached R2, or raw file_id
    let photoToSend = fileId;
    if (fileId.startsWith('http://') || fileId.startsWith('https://')) {
      photoToSend = fileId;
    } else if (imageUrlCache.has(fileId)) {
      photoToSend = imageUrlCache.get(fileId);
    }
    // Otherwise use file_id directly (Telegram handles it natively - FAST)
    
    // Send photo immediately
    let result;
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        result = await telegramRequest(
          `https://api.telegram.org/bot${botToken}/sendPhoto`,
          { chat_id: chatId, photo: photoToSend, caption: caption, ...options },
          20000
        );
        if (result.ok) break;
        throw new Error(result.description || 'Failed to send photo');
      } catch (err) {
        console.error(`[Telegram] sendPhoto attempt ${attempt}/3 FAILED | ${err.message}`);
        if (attempt < 3) await new Promise(r => setTimeout(r, 1000 * attempt));
        else throw err;
      }
    }
    
    // Upload to R2 in background (don't block response) for uncached file_ids
    if (!fileId.startsWith('http') && !imageUrlCache.has(fileId) && !shouldSkipR2Upload()) {
      uploadToR2InBackground(botToken, fileId, rowNumber).catch(err =>
        handleR2UploadError(err, '[R2] Background upload failed')
      );
    }
  } catch (error) {
    console.error('Failed to send Telegram photo:', error);
    throw error;
  }
}

function buildFeedbackCaption(fb, noteLimit = 500) {
  const shop = fb.shop || 'N/A';
  let note = fb.note || fb.message || '';
  if (note.length > noteLimit) note = note.substring(0, noteLimit) + '...';

  let caption = hasHotTag(fb) ? `🔥🔥🔥 HOT 🔥🔥🔥\n` : '';
  caption += `• ID: #${fb.rowNumber}\n`;
  caption += `• Shop: ${shop}\n`;
  caption += `• File: ${fb.link || 'KHÔNG có file'}`;
  if (note) caption += `\n• Note: ${note}`;
  return caption;
}

async function sendFeedbackToTelegram(chatId, fb, options = {}) {
  const caption = buildFeedbackCaption(fb, options.noteLimit || 500);
  const sendOptions = { disable_web_page_preview: true };

  try {
    if (fb.imageId) {
      await sendTelegramPhoto(chatId, fb.imageId, caption, sendOptions, fb.rowNumber);
    } else {
      await sendTelegramMessage(chatId, caption, sendOptions);
    }
  } catch (err) {
    console.error('Send error:', err.message);
    if (fb.imageId) {
      await sendTelegramMessage(chatId, caption + '\n\n📷 (Không thể tải ảnh)', sendOptions).catch(() => {});
    }
  }

  const commentImages = getFeedbackCommentImages(fb);
  for (let i = 0; i < commentImages.length; i++) {
    const imageCaption = `📎 #${fb.rowNumber} ảnh comment ${i + 1}/${commentImages.length}`;
    try {
      await sendTelegramPhoto(chatId, commentImages[i], imageCaption, sendOptions, null);
    } catch (err) {
      console.error('Send comment image error:', err.message);
    }
  }
}

// Background R2 upload (non-blocking)
async function uploadToR2InBackground(botToken, fileId, rowNumber) {
  if (shouldSkipR2Upload()) return;

  try {
    const fileResponse = await fetch(`https://api.telegram.org/bot${botToken}/getFile?file_id=${fileId}`);
    const fileData = await fileResponse.json();
    if (!fileData.ok || !fileData.result?.file_path) return;
    
    const filePath = fileData.result.file_path;
    const fileUrl = `https://api.telegram.org/file/bot${botToken}/${filePath}`;
    const downloadResponse = await fetch(fileUrl);
    if (!downloadResponse.ok) return;
    
    const buffer = Buffer.from(await downloadResponse.arrayBuffer());
    const fileName = `telegram/${Date.now()}_${filePath.split('/').pop()}`;
    await getS3Client().send(new PutObjectCommand({
      Bucket: process.env.CLOUDFLARE_R2_BUCKET_NAME,
      Key: fileName,
      Body: buffer,
      ContentType: 'image/jpeg',
    }));
    
    const r2Url = `${process.env.CLOUDFLARE_CDN_URL}/${fileName}`;
    imageUrlCache.set(fileId, r2Url);
    
    if (rowNumber) {
      await sheetsClient.updateCell(rowNumber, 'N', r2Url);
      console.log(`[R2] Background upload done: row ${rowNumber} → ${r2Url}`);
    }
  } catch (err) {
    handleR2UploadError(err, '[R2] Background upload error');
  }
}

// Upload Telegram photo to R2 and return CDN URL
async function uploadTelegramPhotoToR2(fileId) {
  const botToken = process.env.TELEGRAM_BOT_TOKEN;
  if (!botToken) {
    return 'Error: Bot token not configured';
  }
  if (shouldSkipR2Upload()) {
    return 'Error: R2 upload temporarily paused due to authorization error';
  }
  
  try {
    // Step 1: Get file path from Telegram
    const fileResponse = await fetch(`https://api.telegram.org/bot${botToken}/getFile?file_id=${fileId}`);
    const fileData = await fileResponse.json();
    
    if (!fileData.ok || !fileData.result || !fileData.result.file_path) {
      return 'Error: Could not get file from Telegram';
    }
    
    const filePath = fileData.result.file_path;
    const fileUrl = `https://api.telegram.org/file/bot${botToken}/${filePath}`;
    
    // Step 2: Download file from Telegram
    const downloadResponse = await fetch(fileUrl);
    if (!downloadResponse.ok) {
      return 'Error: Could not download file';
    }
    
    const arrayBuffer = await downloadResponse.arrayBuffer();
    const buffer = Buffer.from(arrayBuffer);
    
    // Step 3: Upload to R2
    
    // Generate unique filename
    const ext = filePath.split('.').pop() || 'jpg';
    const filename = `telegram/${Date.now()}-${crypto.randomUUID()}.${ext}`;
    
    await getS3Client().send(new PutObjectCommand({
      Bucket: process.env.CLOUDFLARE_R2_BUCKET_NAME.trim(),
      Key: filename,
      Body: buffer,
      ContentType: `image/${ext}`
    }));
    
    // Return CDN URL
    return `${process.env.CLOUDFLARE_CDN_URL}/${filename}`;
  } catch (error) {
    handleR2UploadError(error, 'Failed to upload to R2');
    return 'Error: ' + error.message;
  }
}

// Notify host about feedback count via Telegram Group
async function notifyHostFeedbackCount(host) {
  const groupChatId = process.env.TELEGRAM_GROUP_CHAT_ID;
  if (!groupChatId) {
    console.warn('[Notification] TELEGRAM_GROUP_CHAT_ID not configured');
    return;
  }
  
  console.log(`[Notification] Checking feedback count for host: ${host}`);
  
  try {
    const data = await getCachedData();
    const rows = data.rows || [];
    const feedbackCount = rows.filter(r => r.host === host && r.stage === 'Feedback').length;
    
    console.log(`[Notification] ${host} has ${feedbackCount} feedback(s)`);
    
    if (feedbackCount > 0) {
      const message = `📬 ${host} có ${feedbackCount} feedback`;
      console.log(`[Notification] Sending to group ${groupChatId}: ${message}`);
      await sendTelegramMessage(groupChatId, message);
      console.log(`[Notification] Message sent successfully`);
    } else {
      console.log(`[Notification] No feedback to notify`);
    }
  } catch (error) {
    console.error('[Notification] Failed to notify group:', error);
  }
}


// Add comment to feedback (for Telegram integration)
async function addCommentToFeedback(rowNumber, commentText) {
  const currentRow = await sheetsClient.getRow(rowNumber);
  let comments = [];
  
  const currentContent = currentRow[7] || '';
  if (currentContent) {
    if (currentContent.trim().startsWith('[') && currentContent.trim().endsWith(']')) {
      try {
        comments = JSON.parse(currentContent);
        if (!Array.isArray(comments)) comments = [];
      } catch (e) {
        comments = [];
      }
    } else {
      comments.push({
        text: currentContent,
        time: 'Note cũ',
        author: 'System'
      });
    }
  }

  const timestamp = formatVNTimestamp('short');
  
  comments.push({
    text: commentText,
    time: timestamp,
    author: 'Telegram'
  });

  await sheetsClient.updateCell(rowNumber, 'H', JSON.stringify(comments));
}

// API Key Middleware
app.use('/api', (req, res, next) => {
  // Skip API key check for Telegram endpoints
  if (req.path.startsWith('/telegram-')) {
    return next();
  }
  
  const apiKey = req.headers['x-api-key'];
  const validKey = process.env.API_KEY;
  
  if (!validKey) {
    console.warn('API_KEY not set in .env! Securing is disabled.');
    return next();
  }
  
  if (apiKey !== validKey) {
    return res.status(403).json({ success: false, message: 'Forbidden: Invalid API Key' });
  }
  next();
});

// Telegram Webhook Setup Endpoint
app.get('/api/telegram-setup', async (req, res) => {
  const botToken = process.env.TELEGRAM_BOT_TOKEN;
  if (!botToken) {
    return res.json({ success: false, message: 'TELEGRAM_BOT_TOKEN not configured' });
  }
  
  // Get the webhook URL from query or use default
  const webhookUrl = req.query.url || `https://api-feedback.f1genz.dev/api/telegram-webhook`;
  
  try {
    // Set webhook
    const response = await fetch(`https://api.telegram.org/bot${botToken}/setWebhook`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url: webhookUrl })
    });
    const result = await response.json();
    
    if (result.ok) {
      return res.json({ 
        success: true, 
        message: `Webhook đã được thiết lập: ${webhookUrl}`,
        result 
      });
    } else {
      return res.json({ success: false, message: result.description || 'Failed to set webhook', result });
    }
  } catch (error) {
    return res.json({ success: false, message: error.message });
  }
});

// Get Telegram Webhook Info
app.get('/api/telegram-info', async (req, res) => {
  const botToken = process.env.TELEGRAM_BOT_TOKEN;
  if (!botToken) {
    return res.json({ success: false, message: 'TELEGRAM_BOT_TOKEN not configured' });
  }
  
  try {
    const response = await fetch(`https://api.telegram.org/bot${botToken}/getWebhookInfo`);
    const result = await response.json();
    return res.json({ success: true, webhookInfo: result.result });
  } catch (error) {
    return res.json({ success: false, message: error.message });
  }
});

// Routes
// POST /exec endpoint to mimic Google Apps Script Web App
// Expects body: { action: '...', ...data }
app.post('/api/exec', async (req, res) => {
  const { action } = req.body;
  
  try {
    let result;
    switch (action) {
      case 'getDashboardData':
        result = await getDashboardData();
        break;
      case 'getGuidesData':
        result = await getGuidesData();
        break;
      case 'createFeedback':
        result = await createFeedback(req.body.feedback);
        break;
      case 'updateFeedback':
        result = await updateFeedback(req.body.rowNumber, req.body.updates);
        break;
      case 'updateStage':
        result = await updateStage(req.body.rowNumber, req.body.newStage);
        break;
      case 'deleteFeedback':
        result = await deleteFeedback(req.body.rowNumber);
        break;
      case 'bulkUpdateStage':
        result = await bulkUpdateStage(req.body.rowNumbers, req.body.newStage);
        break;
      case 'bulkDelete':
        result = await bulkDelete(req.body.rowNumbers);
        break;
      case 'updateFeedbackPriority':
        result = await updateFeedbackPriority(req.body.orderedRowNumbers);
        break;
      case 'addComment':
        result = await addComment(req.body.rowNumber, req.body.commentText);
        break;
      case 'getComments':
        result = await getComments(req.body.rowNumber);
        break;
      case 'createGuide':
        result = await createGuide(req.body.guide);
        break;
      case 'updateGuide':
        result = await updateGuide(req.body.rowNumber, req.body.updates);
        break;
      case 'deleteGuide':
        result = await deleteGuide(req.body.rowNumber);
        break;
      case 'deleteComment':
        result = await deleteComment(req.body.rowNumber, req.body.commentIndex);
        break;
      case 'deleteFeedbackImage':
        result = await deleteFeedbackImage(req.body.rowNumber);
        break;
      default:
        result = { success: false, message: 'Unknown action: ' + action };
    }
    
    // Mimic Apps Script output format (often implicit)
    // Here we just send JSON
    res.json(result);
    
  } catch (error) {
    console.error('API Error:', error);
    res.json({ success: false, message: error.message });
  }
});

// GET handler for simple read operations
app.get('/api/exec', async (req, res) => {
  const action = req.query.action;
  
  try {
    let result;
    switch (action) {
      case 'getDashboardData':
        result = await getDashboardData();
        break;
      case 'getGuidesData':
        result = await getGuidesData();
        break;
      case 'getHistory':
        result = await getHistory();
        break;
      default:
        result = { success: false, message: 'Unknown action or use POST: ' + action };
    }
    res.json(result);
  } catch (error) {
    console.error('API Error:', error);
    res.json({ success: false, message: error.message });
  }
});

function buildTelegramImageProxyUrl(fileId) {
  return `/api/telegram-image?raw=1&fileId=${encodeURIComponent(fileId)}`;
}

// Telegram Image Proxy Endpoint
app.get('/api/telegram-image', async (req, res) => {
  const fileId = req.query.fileId;
  const rowNumber = req.query.rowNumber; // Optional: to update Sheet
  const raw = req.query.raw === '1';
  
  if (!fileId) {
    return res.json({ success: false, message: 'Missing fileId parameter' });
  }
  
  // If already a URL, return it directly
  if (fileId.startsWith('http://') || fileId.startsWith('https://')) {
    if (raw) return res.redirect(fileId);
    return res.json({ success: true, url: fileId });
  }
  
  // Check cache first
  if (imageUrlCache.has(fileId)) {
    if (raw) return res.redirect(imageUrlCache.get(fileId));
    return res.json({ success: true, url: imageUrlCache.get(fileId) });
  }
  
  const botToken = process.env.TELEGRAM_BOT_TOKEN;
  if (!botToken) {
    return res.json({ success: false, message: 'Telegram bot token not configured' });
  }
  
  try {
    // Get file info from Telegram
    const response = await fetch(`https://api.telegram.org/bot${botToken}/getFile?file_id=${fileId}`);
    const data = await response.json();
    
    if (!data.ok) {
      return res.json({ success: false, message: 'Failed to get file from Telegram: ' + (data.description || 'Unknown error') });
    }
    
    const filePath = data.result.file_path;
    const fileUrl = `https://api.telegram.org/file/bot${botToken}/${filePath}`;

    if (raw) {
      const imageResponse = await fetch(fileUrl);
      if (!imageResponse.ok) {
        return res.status(502).send('Could not download Telegram image');
      }

      const contentType = imageResponse.headers.get('content-type') || 'image/jpeg';
      const buffer = Buffer.from(await imageResponse.arrayBuffer());
      res.setHeader('Content-Type', contentType);
      res.setHeader('Cache-Control', 'public, max-age=3600');
      return res.send(buffer);
    }
    
    // Download and upload to R2 for permanent URL
    try {
      if (shouldSkipR2Upload()) {
        return res.json({ success: true, url: buildTelegramImageProxyUrl(fileId), temporary: true });
      }

      const downloadResponse = await fetch(fileUrl);
      if (downloadResponse.ok) {
        const arrayBuffer = await downloadResponse.arrayBuffer();
        const buffer = Buffer.from(arrayBuffer);
        
        // Upload to R2
        
        const fileName = `telegram/${Date.now()}_${filePath.split('/').pop()}`;
        await getS3Client().send(new PutObjectCommand({
          Bucket: process.env.CLOUDFLARE_R2_BUCKET_NAME,
          Key: fileName,
          Body: buffer,
          ContentType: 'image/jpeg',
        }));
        
        const r2Url = `${process.env.CLOUDFLARE_CDN_URL}/${fileName}`;
        
        // Cache it
        imageUrlCache.set(fileId, r2Url);
        
        // Update Sheet if rowNumber provided
        if (rowNumber) {
          await sheetsClient.updateCell(parseInt(rowNumber), 'N', r2Url);
          console.log(`Updated Sheet row ${rowNumber} with R2 URL`);
        }
        
        return res.json({ success: true, url: r2Url });
      }
    } catch (uploadError) {
      handleR2UploadError(uploadError, 'Failed to upload to R2');
    }
    
    // Fallback to proxy URL (temporary, does not expose bot token)
    return res.json({ 
      success: true, 
      url: buildTelegramImageProxyUrl(fileId),
      temporary: true
    });
  } catch (error) {
    console.error('Telegram API Error:', error);
    return res.json({ success: false, message: error.message });
  }
});

// Helper Functions

async function getDashboardData() {
  let data = await getCachedData();
  let rows = data.rows || [];

  const deletedDoneRows = await cleanupOldDoneFeedbacks(rows);
  if (deletedDoneRows > 0) {
    data = await getCachedData(true);
    rows = data.rows || [];
  }

  // Calculate stats
  const hostStats = {};
  const stageStats = {};
  let pendingFeedback = 0;
  let doneFeedback = 0;

  rows.forEach(row => {
    if (row.host) hostStats[row.host] = (hostStats[row.host] || 0) + 1;
    if (row.stage) stageStats[row.stage] = (stageStats[row.stage] || 0) + 1;
    if (row.stage === 'Feedback') pendingFeedback++;
    if (row.stage === 'Done') doneFeedback++;
  });

  // Unique filter options
  const hosts = [...new Set(rows.map(r => r.host).filter(Boolean))].sort();
  const stages = [...new Set(rows.map(r => r.stage).filter(Boolean))].sort();
  
  return {
    success: true,
    stats: {
      pending: pendingFeedback,
      done: doneFeedback,
      byHost: hostStats,
      byStage: stageStats
    },
    filterOptions: { hosts, stages },
    feedback: rows
  };
}

async function getGuidesData() {
  const data = await sheetsClient.getGuidesData();
  const rows = data.rows || [];

  // Group by type
  const grouped = {};
  rows.forEach(row => {
    if (!grouped[row.type]) grouped[row.type] = [];
    grouped[row.type].push(row);
  });

  return {
    success: true,
    guides: rows,
    groupedGuides: grouped
  };
}

async function getHistory() {
  const data = await sheetsClient.getHistoryData();
  return {
    success: true,
    history: data.rows || []
  };
}

async function createFeedback(feedback) {
  if (!feedback) throw new Error('No feedback data');
  
  const timestamp = formatVNTimestamp();
  const tags = normalizeTags(feedback.tags || '');
  const deadline = hasUrgentTag({ tags }) ? formatTodayDateInput() : (feedback.deadline || '');
  
  // Prepare row data (Columns A-O)
  // A: ID, B: Deadline, C: Host, D: Shop, E: Link, F: Stage, G: Tags, H: Dev_note, I: Image_note, J: Note, K: Time, L: Message, M: MessageID, N: ImageID, O: UpdatedAt
  const row = [
    Date.now().toString(),          // ID (timestamp)
    deadline,                       // Deadline
    feedback.host || '',            // Host
    feedback.shop || '',            // Shop
    feedback.link || '',            // Link
    feedback.stage || 'Feedback',   // Stage
    tags,                           // Tags
    feedback.devNote || '',         // Dev_note
    '',                             // Image_note
    feedback.note || '',            // Note
    timestamp,                      // Time (created)
    feedback.note || '',            // Message (copy of note for now)
    '',                             // MessageID
    '',                             // ImageID
    timestamp,                      // UpdatedAt
    ''                              // Priority
  ];

  const success = await sheetsClient.appendRow(row);
  invalidateDataCache();
  if (success) {
    // Notify host if stage is Feedback
    if ((feedback.stage || 'Feedback') === 'Feedback' && feedback.host) {
      await notifyHostFeedbackCount(feedback.host);
    }
    // await sheetsClient.logHistory('CREATE', `Tạo feedback: ${feedback.shop || 'N/A'}`); // DISABLED
    return { success: true, message: 'Đã tạo feedback thành công!' };
  } else {
    throw new Error('Failed to append row');
  }
}

async function updateFeedback(rowNumber, updates) {
  if (!rowNumber) throw new Error('Missing rowNumber');
  
  const currentRowRaw = await sheetsClient.getRow(rowNumber); 
  // Expecting array of values A-N
  
  if (!currentRowRaw) throw new Error('Row not found');
  
  // Merge logic
  // Columns: A-P (0-15)
  // A:ID, B:Deadline, C:Host, D:Shop, E:Link, F:Stage, G:Tags, H:Dev, I:ImgN, J:Note, K:Time, L:Msg, M:MsgID, N:ImgID, O:UpdatedAt, P:Priority
  
  const newRow = [...currentRowRaw];
  // Ensure we have enough empty strings (16 columns: A-P)
  while(newRow.length < 16) newRow.push('');
  
  // Keep ID (0) same
  if (updates.deadline !== undefined) newRow[1] = updates.deadline;
  if (updates.host !== undefined) newRow[2] = updates.host;
  if (updates.shop !== undefined) newRow[3] = updates.shop;
  if (updates.link !== undefined) newRow[4] = updates.link;
  if (updates.stage !== undefined) newRow[5] = updates.stage;
  if (updates.tags !== undefined) newRow[6] = updates.tags;
  if (updates.devNote !== undefined) newRow[7] = updates.devNote;
  if (updates.note !== undefined) newRow[9] = updates.note;
  if (updates.message !== undefined) newRow[11] = updates.message;
  // Keep Time (10) same
  newRow[6] = normalizeTags(newRow[6]);
  if (hasUrgentTag({ tags: newRow[6] })) newRow[1] = formatTodayDateInput();
  
  newRow[14] = formatVNTimestamp();
  
  await sheetsClient.updateRow(rowNumber, newRow);
  invalidateDataCache();
  
  // Notify host if stage changed to Feedback
  if (updates.stage === 'Feedback' && newRow[2]) {
    await notifyHostFeedbackCount(newRow[2]);
  }
  
  return { success: true, message: 'Cập nhật thành công!' };
}

async function updateStage(rowNumber, newStage) {
  // Get current row to check host
  const currentRow = await sheetsClient.getRow(rowNumber);
  const host = currentRow[2]; // Column C = Host
  
  const timestamp = formatVNTimestamp();
  
  await sheetsClient.updateCell(rowNumber, 'F', newStage);
  await sheetsClient.updateCell(rowNumber, 'O', timestamp);
  invalidateDataCache();
  
  // Notify host if stage changed to Feedback
  if (newStage === 'Feedback' && host) {
    console.log(`[Notification] Stage changed to Feedback for host: ${host}`);
    try {
      await notifyHostFeedbackCount(host);
    } catch (notifyError) {
      console.error('[Notification] Error calling notifyHostFeedbackCount:', notifyError);
    }
  }
  
  // await sheetsClient.logHistory('UPDATE_STAGE', `Row ${rowNumber} -> ${newStage}`); // DISABLED
  return { success: true, message: `Đã cập nhật Stage thành "${newStage}"` };
}

async function deleteFeedback(rowNumber) {
  if (!rowNumber) throw new Error('Missing rowNumber');
  
  await sheetsClient.deleteRow(rowNumber);
  invalidateDataCache();
  // await sheetsClient.logHistory('DELETE', `Xóa row ${rowNumber}`); // DISABLED
  return { success: true, message: 'Đã xóa feedback!' };
}

// ==================== BULK ACTIONS ====================

async function bulkUpdateStage(rowNumbers, newStage) {
  if (!rowNumbers || !Array.isArray(rowNumbers) || rowNumbers.length === 0) {
    throw new Error('Missing or invalid rowNumbers');
  }
  if (!newStage) throw new Error('Missing newStage');

  const timestamp = formatVNTimestamp();

  for (const rowNumber of rowNumbers) {
    await sheetsClient.updateCell(rowNumber, 'F', newStage);
    await sheetsClient.updateCell(rowNumber, 'O', timestamp);
  }

  return { success: true, message: `Đã cập nhật ${rowNumbers.length} mục thành "${newStage}"` };
}

async function bulkDelete(rowNumbers) {
  if (!rowNumbers || !Array.isArray(rowNumbers) || rowNumbers.length === 0) {
    throw new Error('Missing or invalid rowNumbers');
  }

  // Sort descending to delete from bottom up (prevents row shifting issues)
  const sorted = [...rowNumbers].sort((a, b) => b - a);
  
  for (const rowNumber of sorted) {
    await sheetsClient.deleteRow(rowNumber);
  }

  return { success: true, message: `Đã xóa ${rowNumbers.length} mục` };
}

async function updateFeedbackPriority(orderedRowNumbers) {
  if (!orderedRowNumbers || !Array.isArray(orderedRowNumbers) || orderedRowNumbers.length === 0) {
    throw new Error('Missing or invalid orderedRowNumbers');
  }

  const normalizedOrdered = [...new Set(orderedRowNumbers.map(n => parseInt(n, 10)).filter(Number.isInteger))];
  if (normalizedOrdered.length === 0) throw new Error('No valid row numbers');

  const data = await sheetsClient.getAllData();
  const rows = data.rows || [];
  const orderedSet = new Set(normalizedOrdered);
  const rest = rows
    .filter(row => !orderedSet.has(row.rowNumber))
    .sort((a, b) => {
      const pa = parseInt(a.priority, 10);
      const pb = parseInt(b.priority, 10);
      if (!Number.isNaN(pa) || !Number.isNaN(pb)) return (Number.isNaN(pa) ? 999999 : pa) - (Number.isNaN(pb) ? 999999 : pb);
      return parseVNTimestamp(b.time) - parseVNTimestamp(a.time);
    })
    .map(row => row.rowNumber);

  const fullOrder = [...normalizedOrdered, ...rest];
  await sheetsClient.batchUpdateCells(
    fullOrder.map((rowNumber, index) => ({
      rowNumber,
      colLetter: 'P',
      value: index + 1
    }))
  );

  invalidateDataCache();
  return { success: true, message: 'Saved priority order!' };
}

// ==================== COMMENTS ====================

async function addComment(rowNumber, commentText) {
  if (!rowNumber) throw new Error('Missing rowNumber');
  if (!commentText || !commentText.trim()) throw new Error('Missing comment text');

  const currentRow = await sheetsClient.getRow(rowNumber);
  let comments = [];
  
  // Parse existing comments or preserve legacy text
  const currentContent = currentRow[7] || '';
  if (currentContent) {
    if (currentContent.trim().startsWith('[') && currentContent.trim().endsWith(']')) {
      try {
        comments = JSON.parse(currentContent);
        if (!Array.isArray(comments)) comments = [];
      } catch (e) {
        comments = [];
      }
    } else {
      // Treat properly as legacy note
      comments.push({
        text: currentContent,
        time: 'Note cũ',
        author: 'System'
      });
    }
  }

  const timestamp = formatVNTimestamp('short');
  
  comments.push({
    text: commentText.trim(),
    time: timestamp,
    author: 'User'
  });

  await sheetsClient.updateCell(rowNumber, 'H', JSON.stringify(comments));
  
  return { success: true, message: 'Đã thêm comment!', comments };
}

async function getComments(rowNumber) {
  if (!rowNumber) throw new Error('Missing rowNumber');

  const currentRow = await sheetsClient.getRow(rowNumber);
  let comments = [];
  
  comments = parseComments(currentRow[7] || '');

  return { success: true, comments };
}

async function deleteComment(rowNumber, commentIndex) {
  if (!rowNumber) throw new Error('Missing rowNumber');
  if (commentIndex === undefined || commentIndex === null) throw new Error('Missing commentIndex');

  const currentRow = await sheetsClient.getRow(rowNumber);
  let comments = parseComments(currentRow[7] || '');

  if (commentIndex >= 0 && commentIndex < comments.length) {
    comments.splice(commentIndex, 1);
    await sheetsClient.updateCell(rowNumber, 'H', JSON.stringify(comments));
    return { success: true, message: 'Deleted comment!', comments };
  } else {
    return { success: false, message: 'Invalid comment index' };
  }
}

async function deleteFeedbackImage(rowNumber) {
  if (!rowNumber) throw new Error('Missing rowNumber');

  await sheetsClient.batchUpdateCells([
    { rowNumber, colLetter: 'I', value: '' },
    { rowNumber, colLetter: 'N', value: '' },
    { rowNumber, colLetter: 'O', value: formatVNTimestamp() }
  ]);
  invalidateDataCache();
  return { success: true, message: 'Deleted feedback image!' };
}

// ==================== GUIDES CRUD ====================

async function createGuide(guide) {
  if (!guide) throw new Error('No guide data');
  
  // Columns: A: Type, B: Template, C: Link, D: App
  const row = [
    guide.type || 'Hướng dẫn',
    guide.template || '',
    guide.link || '',
    guide.app || ''
  ];

  const success = await sheetsClient.appendGuideRow(row);
  if (success) {
    return { success: true, message: 'Đã tạo hướng dẫn thành công!' };
  } else {
    throw new Error('Failed to append row');
  }
}

async function updateGuide(rowNumber, updates) {
  if (!rowNumber) throw new Error('Missing rowNumber');
  
  const newRow = [
    updates.type || '',
    updates.template || '',
    updates.link || '',
    updates.app || ''
  ];
  
  await sheetsClient.updateGuideRow(rowNumber, newRow);
  return { success: true, message: 'Cập nhật thành công!' };
}

async function deleteGuide(rowNumber) {
  if (!rowNumber) throw new Error('Missing rowNumber');
  
  await sheetsClient.deleteGuideRow(rowNumber);
  return { success: true, message: 'Đã xóa hướng dẫn!' };
}

// Start server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  setTimeout(runDoneCleanupJob, 10 * 1000);
  setTimeout(runOverdueFeedbackJob, 20 * 1000);
  setInterval(runDoneCleanupJob, DONE_CLEANUP_INTERVAL_MS);
  setInterval(runOverdueFeedbackJob, OVERDUE_CHECK_INTERVAL_MS);
});
