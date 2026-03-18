const dns = require('dns');
dns.setDefaultResultOrder('ipv4first');

const express = require('express');
const cors = require('cors');
const sheetsClient = require('./sheetsClient');
const crypto = require('crypto');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
require('dotenv').config();

const app = express();

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

// ==================== SMART PARSING CONSTANTS ====================
const VALID_DOMAINS = ['.com', '.vn', '.com.vn', '.myharavan.com', '.mysapo.net', '.net', '.asia', '.org', '.group', '.top', '.online'];
const LINK_FEEDBACK_FORMATS = ['docs.google.com', 'drive.google.com', 'onedrive.live.com', 'figma.com', 'canva.com', 'trello.com'];
const TAGS_LIST = ['hrv', 'haravan', 'gap', 'note', 'baogia', 'sapo'];
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
  const tags = [...new Set(cleaned.split(/[\s,]+/).filter(w => TAGS_LIST.includes(w.toLowerCase())).map(t => t.toLowerCase()))].join(', ');

  // Note: remaining text after removing shop URLs, link, host, tags
  let content = message;
  words.filter(u => VALID_DOMAINS.some(d => u.includes(d))).forEach(u => { content = content.replace(u, '').trim(); });
  if (link) content = content.replace(link, '').trim();
  hostMatch.forEach(h => { content = content.replace(new RegExp(`\\b${h}\\b`, 'gi'), '').trim(); });
  cleaned.split(/[\s,]+/).filter(w => TAGS_LIST.includes(w.toLowerCase())).forEach(t => { content = content.replace(new RegExp(`\\b${t}\\b`, 'gi'), '').trim(); });
  content = content.replace(/[.,;]{2,}/g, ' ').replace(/\s+/g, ' ').trim();

  return { shop, link, host, tags, note: content, message };
}

async function handleCreateFromTelegram(chatId, firstName, text, photoId, userId, chatType) {
  try {
    const parsed = parseMessageContent(text || '', firstName);
    const now = new Date();
    const vn = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Ho_Chi_Minh' }));
    const ts = `${vn.getHours().toString().padStart(2,'0')}:${vn.getMinutes().toString().padStart(2,'0')}:${vn.getSeconds().toString().padStart(2,'0')} ${vn.getDate().toString().padStart(2,'0')}/${(vn.getMonth()+1).toString().padStart(2,'0')}/${vn.getFullYear()}`;

    let imageId = photoId || '';
    if (photoId) {
      try {
        const r2Url = await uploadTelegramPhotoToR2(photoId);
        if (r2Url && !r2Url.startsWith('Error')) imageId = r2Url;
      } catch (e) { console.error('Photo upload failed:', e.message); }
    }

    const row = [
      Date.now().toString(), '', parsed.host, parsed.shop, parsed.link,
      'Feedback', parsed.tags, '', '', parsed.note,
      ts, parsed.message, userId, imageId, ''
    ];
    await sheetsClient.appendRow(row);

    const data = await sheetsClient.getAllData();
    const feedbackCount = (data.rows || []).filter(r => r.host === parsed.host && r.stage === 'Feedback').length;
    await sendTelegramMessage(chatId, `✅ ${parsed.host} có ${feedbackCount} feedback`);

    // Notify group if creating from private chat
    const groupChatId = process.env.TELEGRAM_GROUP_CHAT_ID;
    if (groupChatId && String(chatId) !== String(groupChatId)) {
      await sendTelegramMessage(groupChatId, `📬 ${parsed.host} có ${feedbackCount} feedback`);
    }
  } catch (error) {
    console.error('Create from Telegram error:', error);
    await sendTelegramMessage(chatId, `❌ Lỗi: ${error.message}`);
  }
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
    
    // Extract photo if present
    let photoId = '';
    if (message.photo && message.photo.length > 0) {
      photoId = message.photo[message.photo.length - 1].file_id;
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
      const chatType = message.chat.type; // 'private', 'group', 'supergroup'
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
    
    // Handle /r commands - Read feedbacks (/r, /rall, /r<name>, /r<shop>)
    if (text.startsWith('/r') && !text.startsWith('/restart') && text !== '/r@' && !text.startsWith('/reply')) {
      const cmdRaw = text.replace(/@\S+/, '').trim();
      const cmd = cmdRaw.toLowerCase();
      const isAll = cmd === '/rall';
      let searchKeyword = '';
      if (!isAll && cmdRaw.length > 2) searchKeyword = cmdRaw.substring(2).trim().toLowerCase();
      
      const currentUserHost = TELEGRAM_ID_TO_HOST[userId] || firstName;
      const data = await sheetsClient.getAllData();
      const rows = data.rows || [];
      
      let filtered;
      if (isAll) {
        filtered = rows.filter(r => r.stage === 'Feedback');
      } else if (searchKeyword) {
        const matchedHost = HOST_ALIAS_MAP[searchKeyword];
        if (matchedHost) {
          filtered = rows.filter(r => r.host === matchedHost && r.stage === 'Feedback');
        } else {
          filtered = rows.filter(r => r.stage === 'Feedback' && r.shop && r.shop.toLowerCase().includes(searchKeyword));
        }
      } else {
        filtered = rows.filter(r => r.host === currentUserHost && r.stage === 'Feedback');
      }
      
      if (filtered.length === 0) {
        await sendTelegramMessage(chatId, '🎉🎉 Hết Feedback! 🎉🎉');
        return res.json({ ok: true });
      }
      
      fetch(`https://api.telegram.org/bot${process.env.TELEGRAM_BOT_TOKEN}/sendChatAction`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: chatId, action: 'typing' })
      });
      
      for (const fb of filtered) {
        let noteText = fb.note || fb.message || '';
        if (noteText.length > 500) noteText = noteText.substring(0, 500) + '...';
        let caption = `• ID: #${fb.rowNumber}\n• Shop: ${fb.shop || 'N/A'}\n• File:\n${fb.link || 'KHÔNG có file'}`;
        if (noteText) caption += `\n• Note: ${noteText}`;
        try {
          if (fb.imageId && !fb.imageId.startsWith('http')) {
            await sendTelegramPhoto(chatId, fb.imageId, caption, { disable_web_page_preview: true }, fb.rowNumber);
          } else {
            await sendTelegramMessage(chatId, caption, { disable_web_page_preview: true });
          }
        } catch (err) {
          await sendTelegramMessage(chatId, caption + '\n\n📷 (Không thể tải ảnh)', { disable_web_page_preview: true }).catch(() => {});
        }
      }
      return res.json({ ok: true });
    }
    
    // Handle // command - show user's feedbacks or // <Host> for specific host
    if (text === '//' || text.startsWith('// ')) {
      // Find current user's host
      const currentUserHost = TELEGRAM_ID_TO_HOST[userId] || null;
      
      if (!currentUserHost) {
        await sendTelegramMessage(chatId, 
          `⚠️ Chưa được đăng ký trong hệ thống\n\n` +
          `🆔 User ID của bạn: \`${userId}\`\n\n` +
          `Gửi ID này cho admin để được thêm vào.`,
          { parse_mode: 'Markdown' }
        );
        return res.json({ ok: true });
      }
      
      // Determine which host to query
      let targetHost = currentUserHost;
      if (text.startsWith('// ')) {
        const requestedHost = text.substring(3).trim();
        // Map common variations
        const hostMap = {
          'quoc': 'Quốc',
          'quốc': 'Quốc',
          'taiz': 'Taiz',
          'tai': 'Taiz',
          'tài': 'Taiz',
          'lam': 'Lâm',
          'lâm': 'Lâm',
          'nghia': 'Nghĩa',
          'nghĩa': 'Nghĩa',
          'tuan': 'Tuan',
          'tuấn': 'Tuan'
        };
        targetHost = hostMap[requestedHost.toLowerCase()] || requestedHost;
      }
      
      // Get feedbacks for target host
      const data = await sheetsClient.getAllData();
      const rows = data.rows || [];
      
      // Filter feedbacks: stage = "Feedback" only
      const userFeedbacks = rows.filter(r => 
        r.host === targetHost && r.stage === 'Feedback'
      );
      
      if (userFeedbacks.length === 0) {
        await sendTelegramMessage(chatId, `✅ Không có feedback nào cho ${targetHost}`);
        return res.json({ ok: true });
      }
      
      // Show typing indicator
      fetch(`https://api.telegram.org/bot${process.env.TELEGRAM_BOT_TOKEN}/sendChatAction`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: chatId, action: 'typing' })
      });
      
      
      
      // Send messages sequentially to avoid ETIMEDOUT
      for (const fb of userFeedbacks) {
        const shop = fb.shop || 'N/A';
        let note = fb.note || fb.message || '';
        if (note.length > 500) note = note.substring(0, 500) + '... (quá dài, xem trên Dashboard)';
        
        let caption = `• ID: #${fb.rowNumber}\n`;
        caption += `• Shop: ${shop}\n`;
        caption += `• File: ${fb.link || 'KHÔNG có file'}`;
        if (note) caption += `\n• Note: ${note}`;
        
        try {
          if (fb.imageId) {
            await sendTelegramPhoto(chatId, fb.imageId, caption, { disable_web_page_preview: true }, fb.rowNumber);
          } else {
            await sendTelegramMessage(chatId, caption, { disable_web_page_preview: true });
          }
        } catch (err) {
          console.error('Send error:', err.message);
          if (fb.imageId) {
            await sendTelegramMessage(chatId, caption + '\n\n📷 (Không thể tải ảnh)', { disable_web_page_preview: true }).catch(() => {});
          }
        }
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
              commentText = `[Telegram] ${firstName}: ${extraText || 'Done'}\n${photoUrl}`;
            } else if (message.photo && message.photo.length > 0) {
              // Get largest photo (compressed)
              const photo = message.photo[message.photo.length - 1];
              const photoUrl = await uploadTelegramPhotoToR2(photo.file_id);
              commentText = `[Telegram] ${firstName}: ${extraText || 'Done'}\n${photoUrl}`;
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
            const remainingFeedbacks = rows.filter(r => 
              r.host === TELEGRAM_ID_TO_HOST[userId] && r.stage === 'Feedback'
            );
            
            
            if (remainingFeedbacks.length > 0) {
              await sendTelegramMessage(chatId, `📋 Còn ${remainingFeedbacks.length} feedback:`);
              
              // Send messages sequentially to avoid ETIMEDOUT
              for (const fb of remainingFeedbacks) {
                const shopName = fb.shop || 'N/A';
                let noteText = fb.note || fb.message || '';
                if (noteText.length > 500) noteText = noteText.substring(0, 500) + '... (quá dài, xem trên Dashboard)';
                
                let caption = `• ID: #${fb.rowNumber}\n`;
                caption += `• Shop: ${shopName}\n`;
                caption += `• File: ${fb.link || 'KHÔNG có file'}`;
                if (noteText) caption += `\n• Note: ${noteText}`;
                
                try {
                  if (fb.imageId) {
                    await sendTelegramPhoto(chatId, fb.imageId, caption, { disable_web_page_preview: true }, fb.rowNumber);
                  } else {
                    await sendTelegramMessage(chatId, caption, { disable_web_page_preview: true });
                  }
                } catch (err) {
                  console.error('Send error:', err.message);
                  if (fb.imageId) {
                    await sendTelegramMessage(chatId, caption + '\n\n📷 (Không thể tải ảnh)', { disable_web_page_preview: true }).catch(() => {});
                  }
                }
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
              commentText += `\n${photoUrl}`;
            } else if (message.photo && message.photo.length > 0) {
              const photo = message.photo[message.photo.length - 1];
              const photoUrl = await uploadTelegramPhotoToR2(photo.file_id);
              commentText += `\n${photoUrl}`;
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
        `🔹 // hoặc /r - Xem feedback\n` +
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
        `• \`//\` hoặc \`/r\` - Xem của mình\n` +
        `• \`// Tên\` hoặc \`/rTên\` - Theo host\n` +
        `• \`/rall\` - Xem tất cả\n` +
        `• \`/r<shop>\` - Theo shop\n\n` +
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
    if (text.startsWith('/f ') || text.startsWith('/f@')) {
      const createText = text.replace(/^\/f(@\S+)?\s*/, '').trim();
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

// Send message to Telegram (with retry)
async function sendTelegramMessage(chatId, text, options = {}) {
  const botToken = process.env.TELEGRAM_BOT_TOKEN;
  if (!botToken) { console.error('TELEGRAM_BOT_TOKEN not configured'); return; }
  
  const MAX_RETRIES = 3;
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const response = await fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: chatId, text: text, ...options }),
        signal: AbortSignal.timeout(10000)
      });
      return response;
    } catch (error) {
      const ips = error.cause?.errors?.map(e => e.address + ':' + e.port).join(', ') || 'unknown';
      console.error(`[Telegram] sendMessage attempt ${attempt}/${MAX_RETRIES} FAILED | IPs tried: ${ips} | ${error.cause?.code || error.message}`);
      if (attempt < MAX_RETRIES) {
        await new Promise(r => setTimeout(r, 1000 * attempt));
      } else {
        console.error(`[Telegram] GAVE UP after ${MAX_RETRIES} attempts for chat ${chatId}`);
      }
    }
  }
}

// Send photo to Telegram
async function sendTelegramPhoto(chatId, fileId, caption = '', options = {}, rowNumber = null) {
  const botToken = process.env.TELEGRAM_BOT_TOKEN;
  const oldBotToken = process.env.TELEGRAM_IMAGE_BOT_TOKEN;
  
  if (!botToken) {
    console.error('TELEGRAM_BOT_TOKEN not configured');
    return;
  }
  
  try {
    let photoToSend = fileId;
    let needsSheetUpdate = false;
    
    // If imageId is already a URL, use it directly
    if (fileId.startsWith('http://') || fileId.startsWith('https://')) {
      photoToSend = fileId;
      console.log('Using existing URL:', fileId);
    }
    // Check cache first
    else if (imageUrlCache.has(fileId)) {
      photoToSend = imageUrlCache.get(fileId);
      console.log('Using cached R2 URL:', photoToSend);
    }
    // Otherwise, download from old bot and upload to R2
    else if (oldBotToken) {
      try {
        const fileResponse = await fetch(`https://api.telegram.org/bot${oldBotToken}/getFile?file_id=${fileId}`);
        const fileData = await fileResponse.json();
        
        if (fileData.ok && fileData.result && fileData.result.file_path) {
          const filePath = fileData.result.file_path;
          const fileUrl = `https://api.telegram.org/file/bot${oldBotToken}/${filePath}`;
          
          // Download and upload to R2
          const downloadResponse = await fetch(fileUrl);
          if (downloadResponse.ok) {
            const arrayBuffer = await downloadResponse.arrayBuffer();
            const buffer = Buffer.from(arrayBuffer);
            
            // Upload to R2
            const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
            const s3Client = new S3Client({
              region: 'auto',
              endpoint: `https://${process.env.CLOUDFLARE_ACCOUNT_ID}.r2.cloudflarestorage.com`,
              credentials: {
                accessKeyId: process.env.CLOUDFLARE_R2_ACCESS_KEY_ID,
                secretAccessKey: process.env.CLOUDFLARE_R2_SECRET_ACCESS_KEY,
              },
            });
            
            const fileName = `telegram/${Date.now()}_${filePath.split('/').pop()}`;
            await s3Client.send(new PutObjectCommand({
              Bucket: process.env.CLOUDFLARE_R2_BUCKET_NAME,
              Key: fileName,
              Body: buffer,
              ContentType: 'image/jpeg',
            }));
            
            // Use R2 CDN URL
            photoToSend = `${process.env.CLOUDFLARE_CDN_URL}/${fileName}`;
            
            // Cache it
            imageUrlCache.set(fileId, photoToSend);
            needsSheetUpdate = true;
            console.log('Uploaded to R2 and cached:', photoToSend);
          }
        }
      } catch (err) {
        console.log('Could not process with old bot, using file_id:', err.message);
      }
    }
    
    // Send photo using CURRENT bot (with retry)
    let result;
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        const response = await fetch(`https://api.telegram.org/bot${botToken}/sendPhoto`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ chat_id: chatId, photo: photoToSend, caption: caption, ...options }),
          signal: AbortSignal.timeout(15000)
        });
        result = await response.json();
        if (result.ok) break;
        throw new Error(result.description || 'Failed to send photo');
      } catch (err) {
        const ips = err.cause?.errors?.map(e => e.address + ':' + e.port).join(', ') || '';
        console.error(`[Telegram] sendPhoto attempt ${attempt}/3 FAILED | ${ips} | ${err.cause?.code || err.message}`);
        if (attempt < 3) await new Promise(r => setTimeout(r, 1000 * attempt));
        else throw err;
      }
    }
    
    // Update Sheet with R2 URL if we uploaded a new image
    if (needsSheetUpdate && rowNumber && photoToSend.startsWith('http')) {
      try {
        await sheetsClient.updateCell(rowNumber, 'N', photoToSend);
        console.log(`Updated Sheet row ${rowNumber} with R2 URL`);
      } catch (err) {
        console.error('Failed to update Sheet:', err.message);
      }
    }
  } catch (error) {
    console.error('Failed to send Telegram photo:', error);
    throw error;
  }
}

// Upload Telegram photo to R2 and return CDN URL
async function uploadTelegramPhotoToR2(fileId) {
  const botToken = process.env.TELEGRAM_BOT_TOKEN;
  if (!botToken) {
    return 'Error: Bot token not configured';
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
    const s3Client = new S3Client({
      region: 'auto',
      endpoint: `https://${process.env.CLOUDFLARE_ACCOUNT_ID}.r2.cloudflarestorage.com`,
      credentials: {
        accessKeyId: process.env.CLOUDFLARE_R2_ACCESS_KEY_ID,
        secretAccessKey: process.env.CLOUDFLARE_R2_SECRET_ACCESS_KEY
      }
    });
    
    // Generate unique filename
    const ext = filePath.split('.').pop() || 'jpg';
    const filename = `telegram/${Date.now()}-${crypto.randomUUID()}.${ext}`;
    
    await s3Client.send(new PutObjectCommand({
      Bucket: process.env.CLOUDFLARE_R2_BUCKET_NAME.trim(),
      Key: filename,
      Body: buffer,
      ContentType: `image/${ext}`
    }));
    
    // Return CDN URL
    return `${process.env.CLOUDFLARE_CDN_URL}/${filename}`;
  } catch (error) {
    console.error('Failed to upload to R2:', error);
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
    // Get current feedback count for this host
    const data = await sheetsClient.getAllData();
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

  // Create new comment with VN Timezone
  const now = new Date();
  const vnTime = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Ho_Chi_Minh' }));
  const d = vnTime.getDate().toString().padStart(2, '0');
  const m = (vnTime.getMonth() + 1).toString().padStart(2, '0');
  const y = vnTime.getFullYear();
  const h = vnTime.getHours().toString().padStart(2, '0');
  const min = vnTime.getMinutes().toString().padStart(2, '0');
  const timestamp = `${h}:${min} ${d}/${m}/${y}`;
  
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

// Telegram Image Proxy Endpoint (uses old bot token for image access)
app.get('/api/telegram-image', async (req, res) => {
  const fileId = req.query.fileId;
  const rowNumber = req.query.rowNumber; // Optional: to update Sheet
  
  if (!fileId) {
    return res.json({ success: false, message: 'Missing fileId parameter' });
  }
  
  // If already a URL, return it directly
  if (fileId.startsWith('http://') || fileId.startsWith('https://')) {
    return res.json({ success: true, url: fileId });
  }
  
  // Check cache first
  if (imageUrlCache.has(fileId)) {
    return res.json({ success: true, url: imageUrlCache.get(fileId) });
  }
  
  // Use separate token for image proxy (old bot that has access to images)
  const botToken = process.env.TELEGRAM_IMAGE_BOT_TOKEN || process.env.TELEGRAM_BOT_TOKEN;
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
    
    // Download and upload to R2 for permanent URL
    try {
      const downloadResponse = await fetch(fileUrl);
      if (downloadResponse.ok) {
        const arrayBuffer = await downloadResponse.arrayBuffer();
        const buffer = Buffer.from(arrayBuffer);
        
        // Upload to R2
        const s3Client = new S3Client({
          region: 'auto',
          endpoint: `https://${process.env.CLOUDFLARE_ACCOUNT_ID}.r2.cloudflarestorage.com`,
          credentials: {
            accessKeyId: process.env.CLOUDFLARE_R2_ACCESS_KEY_ID,
            secretAccessKey: process.env.CLOUDFLARE_R2_SECRET_ACCESS_KEY,
          },
        });
        
        const fileName = `telegram/${Date.now()}_${filePath.split('/').pop()}`;
        await s3Client.send(new PutObjectCommand({
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
      console.error('Failed to upload to R2:', uploadError.message);
    }
    
    // Fallback to Telegram URL (temporary)
    return res.json({ 
      success: true, 
      url: fileUrl,
      temporary: true
    });
  } catch (error) {
    console.error('Telegram API Error:', error);
    return res.json({ success: false, message: error.message });
  }
});

// Helper Functions

async function getDashboardData() {
  const data = await sheetsClient.getAllData();
  const rows = data.rows || [];

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
  
  // Use Vietnam timezone
  const now = new Date();
  const vnTime = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Ho_Chi_Minh' }));
  const d = vnTime.getDate().toString().padStart(2, '0');
  const m = (vnTime.getMonth() + 1).toString().padStart(2, '0');
  const y = vnTime.getFullYear();
  const h = vnTime.getHours().toString().padStart(2, '0');
  const min = vnTime.getMinutes().toString().padStart(2, '0');
  const s = vnTime.getSeconds().toString().padStart(2, '0');
  const timestamp = `${h}:${min}:${s} ${d}/${m}/${y}`;
  
  // Prepare row data (Columns A-O)
  // A: ID, B: Deadline, C: Host, D: Shop, E: Link, F: Stage, G: Tags, H: Dev_note, I: Image_note, J: Note, K: Time, L: Message, M: MessageID, N: ImageID, O: UpdatedAt
  const row = [
    Date.now().toString(),          // ID (timestamp)
    feedback.deadline || '',        // Deadline
    feedback.host || '',            // Host
    feedback.shop || '',            // Shop
    feedback.link || '',            // Link
    feedback.stage || 'Feedback',   // Stage
    feedback.tags || '',            // Tags
    feedback.devNote || '',         // Dev_note
    '',                             // Image_note
    feedback.note || '',            // Note
    timestamp,                      // Time (created)
    feedback.note || '',            // Message (copy of note for now)
    '',                             // MessageID
    '',                             // ImageID
    timestamp                       // UpdatedAt
  ];

  const success = await sheetsClient.appendRow(row);
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
  
  // We need to fetch current data first to merge updates properly?
  // Or we modify sheetsClient to update specific cells?
  // Code.gs gets existing row, modifies it, and overwrites.
  
  // Optimized approach: 
  // Read specific row using getAllData is inefficient if we just need one row.
  // But getAllData reads everything anyway.
  
  // Better: read just that row from sheet?
  // Let's implement reading a single row in sheetsClient if needed, or just iterate `getAllData` since it's cached in memory usually?
  // No, on serverless, no cache.
  // Actually, updateRow expects full row array.
  // Let's implement `getRow` in sheetsClient. NO wait, I can just use getValues for that specific range.
  
  // Since sheetsClient doesn't have getRow, I'll use getAllData logic inside here or add getRow to client.
  // Adding getRow to client is better. But for now I will cheat:
  // Since our `updates` object only has specific fields, we need to know the OLD values to preserve them.
  // BUT Code.gs `updateFeedback` logic reads the row first.
  
  // I will add `getRow(rowNumber)` to `sheetsClient` now. Or inline it.
  // Let's assume I'll add it.
  
  // For now, let's just implement `getRow` logic here using sheetsClient instance if I can access sheets object? No it's encapsulated.
  // I should update sheetsClient.js to include getRow.
  
  // Wait, I already wrote sheetsClient.js without getRow. I should add it.
  
  // Re-reading sheetsClient.js content... it doesn't have getRow.
  // I'll assume for this file that `sheetsClient.getRow(rowNumber)` exists and I'll update sheetsClient.js in next step.
  
  const currentRowRaw = await sheetsClient.getRow(rowNumber); 
  // Expecting array of values A-N
  
  if (!currentRowRaw) throw new Error('Row not found');
  
  // Merge logic
  // Columns: A-O (0-14)
  // A:ID, B:Deadline, C:Host, D:Shop, E:Link, F:Stage, G:Tags, H:Dev, I:ImgN, J:Note, K:Time, L:Msg, M:MsgID, N:ImgID, O:UpdatedAt
  
  const newRow = [...currentRowRaw];
  // Ensure we have enough empty strings (15 columns: A-O)
  while(newRow.length < 15) newRow.push('');
  
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
  
  // Update the updated_at timestamp (column O, index 14)
  const now = new Date();
  const vnTime = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Ho_Chi_Minh' }));
  const d = vnTime.getDate().toString().padStart(2, '0');
  const m = (vnTime.getMonth() + 1).toString().padStart(2, '0');
  const y = vnTime.getFullYear();
  const h = vnTime.getHours().toString().padStart(2, '0');
  const min = vnTime.getMinutes().toString().padStart(2, '0');
  const s = vnTime.getSeconds().toString().padStart(2, '0');
  newRow[14] = `${h}:${min}:${s} ${d}/${m}/${y}`;
  
  await sheetsClient.updateRow(rowNumber, newRow);
  
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
  
  // Update Column F (Stage) and Column O (UpdatedAt)
  const now = new Date();
  const vnTime = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Ho_Chi_Minh' }));
  const d = vnTime.getDate().toString().padStart(2, '0');
  const m = (vnTime.getMonth() + 1).toString().padStart(2, '0');
  const y = vnTime.getFullYear();
  const h = vnTime.getHours().toString().padStart(2, '0');
  const min = vnTime.getMinutes().toString().padStart(2, '0');
  const s = vnTime.getSeconds().toString().padStart(2, '0');
  const timestamp = `${h}:${min}:${s} ${d}/${m}/${y}`;
  
  await sheetsClient.updateCell(rowNumber, 'F', newStage);
  await sheetsClient.updateCell(rowNumber, 'O', timestamp);
  
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
  // await sheetsClient.logHistory('DELETE', `Xóa row ${rowNumber}`); // DISABLED
  return { success: true, message: 'Đã xóa feedback!' };
}

// ==================== BULK ACTIONS ====================

async function bulkUpdateStage(rowNumbers, newStage) {
  if (!rowNumbers || !Array.isArray(rowNumbers) || rowNumbers.length === 0) {
    throw new Error('Missing or invalid rowNumbers');
  }
  if (!newStage) throw new Error('Missing newStage');

  const now = new Date();
  const vnTime = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Ho_Chi_Minh' }));
  const d = vnTime.getDate().toString().padStart(2, '0');
  const m = (vnTime.getMonth() + 1).toString().padStart(2, '0');
  const y = vnTime.getFullYear();
  const h = vnTime.getHours().toString().padStart(2, '0');
  const min = vnTime.getMinutes().toString().padStart(2, '0');
  const s = vnTime.getSeconds().toString().padStart(2, '0');
  const timestamp = `${h}:${min}:${s} ${d}/${m}/${y}`;

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

  // Create new comment with VN Timezone
  const now = new Date();
  const options = { timeZone: 'Asia/Ho_Chi_Minh', hour: '2-digit', minute: '2-digit', day: '2-digit', month: '2-digit', year: 'numeric', hour12: false };
  // Format: "22:30 09/01/2026"
  // toLocaleString returns "22:30:00 09/01/2026" or similar depending on locale, let's normalize
  const timeStr = now.toLocaleString('vi-VN', options).replace(/:\d{2} /, ' '); 
  // Simplified manual formatting to ensure consistency if needed, but locale vi-VN is usually good.
  // Actually, let's stick to the previous format "HH:mm dd/MM/yyyy"
  const vnTime = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Ho_Chi_Minh' }));
  const d = vnTime.getDate().toString().padStart(2, '0');
  const m = (vnTime.getMonth() + 1).toString().padStart(2, '0');
  const y = vnTime.getFullYear();
  const h = vnTime.getHours().toString().padStart(2, '0');
  const min = vnTime.getMinutes().toString().padStart(2, '0');
  const timestamp = `${h}:${min} ${d}/${m}/${y}`;
  
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

  return { success: true, comments };
}

async function deleteComment(rowNumber, commentIndex) {
  if (!rowNumber) throw new Error('Missing rowNumber');
  if (commentIndex === undefined || commentIndex === null) throw new Error('Missing commentIndex');

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
    }
  }

  if (commentIndex >= 0 && commentIndex < comments.length) {
    comments.splice(commentIndex, 1);
    await sheetsClient.updateCell(rowNumber, 'H', JSON.stringify(comments));
    return { success: true, message: 'Deleted comment!', comments };
  } else {
    return { success: false, message: 'Invalid comment index' };
  }
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
});
