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
    const text = (message.text || '').trim();
    const username = message.from.username || '';
    const firstName = message.from.first_name || 'User';
    
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
      
      // Send each feedback as a separate message with rowNumber for reply tracking
      for (const fb of userFeedbacks) {
        const shop = fb.shop || 'N/A';
        const note = fb.note || fb.message || '';
        
        // Check if has link - make File Feedback clickable
        const fileStatus = fb.link ? `[File Feedback](${fb.link})` : 'KHÔNG có file';
        
        // Make shop name clickable (assuming shop is a URL)
        const shopLink = shop.startsWith('http') ? `[${shop}](https://${shop})` : `[${shop}](https://${shop})`;
        
        // Simple format with ID at top
        let msg = `• ID: #${fb.rowNumber}\n`;
        msg += `• Shop: ${shopLink}\n`;
        msg += `• File: ${fileStatus}`;
        if (note) {
          msg += `\n• Note: ${note}`;
        }
        
        await sendTelegramMessage(chatId, msg, { 
          parse_mode: 'Markdown', 
          disable_web_page_preview: true
        });
      }
      
      return res.json({ ok: true });
    }
    
    // Handle reply to feedback messages (with ID in text)
    if (message.reply_to_message) {
      const originalText = message.reply_to_message.text || '';
      
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
              
              for (const fb of remainingFeedbacks) {
                const shopName = fb.shop || 'N/A';
                const noteText = fb.note || fb.message || '';
                const fileStatus = fb.link ? `[File Feedback](${fb.link})` : 'KHÔNG có file';
                
                // Make shop name clickable
                const shopLink = shopName.startsWith('http') ? `[${shopName}](https://${shopName})` : `[${shopName}](https://${shopName})`;
                
                let msg = `• ID: #${fb.rowNumber}\n`;
                msg += `• Shop: ${shopLink}\n`;
                msg += `• File: ${fileStatus}`;
                if (noteText) {
                  msg += `\n• Note: ${noteText}`;
                }
                
                await sendTelegramMessage(chatId, msg, { 
                  parse_mode: 'Markdown', 
                  disable_web_page_preview: true
                });
              }
            } else {
              await sendTelegramMessage(chatId, `🎉 Không còn feedback nào!`);
            }
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
        `🔹 Gõ // để xem feedback của bạn\n` +
        `🔹 Gõ /help để xem hướng dẫn`
      );
      return res.json({ ok: true });
    }
    
    // Handle /help command
    if (text === '/help') {
      await sendTelegramMessage(chatId,
        `📚 *Hướng dẫn sử dụng Bot*\n\n` +
        `\`//\` - Xem danh sách feedback của bạn\n` +
        `\`/start\` - Bắt đầu\n` +
        `\`/help\` - Xem hướng dẫn`,
        { parse_mode: 'Markdown' }
      );
      return res.json({ ok: true });
    }
    
    res.json({ ok: true });
  } catch (error) {
    console.error('Telegram webhook error:', error);
    res.json({ ok: true }); // Always return ok to Telegram
  }
});

// Send message to Telegram
async function sendTelegramMessage(chatId, text, options = {}) {
  const botToken = process.env.TELEGRAM_BOT_TOKEN;
  if (!botToken) {
    console.error('TELEGRAM_BOT_TOKEN not configured');
    return;
  }
  
  try {
    await fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chat_id: chatId,
        text: text,
        ...options
      })
    });
  } catch (error) {
    console.error('Failed to send Telegram message:', error);
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
    console.warn('TELEGRAM_GROUP_CHAT_ID not configured');
    return;
  }
  
  try {
    // Get current feedback count for this host
    const data = await sheetsClient.getAllData();
    const rows = data.rows || [];
    const feedbackCount = rows.filter(r => r.host === host && r.stage === 'Feedback').length;
    
    if (feedbackCount > 0) {
      await sendTelegramMessage(groupChatId, `📬 ${host} có ${feedbackCount} feedback`);
    }
  } catch (error) {
    console.error('Failed to notify group:', error);
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
  
  if (!fileId) {
    return res.json({ success: false, message: 'Missing fileId parameter' });
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
    
    return res.json({ 
      success: true, 
      url: fileUrl,
      fileId: fileId
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
    await notifyHostFeedbackCount(host);
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
