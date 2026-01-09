const express = require('express');
const cors = require('cors');
const sheetsClient = require('./sheetsClient');
const crypto = require('crypto');
require('dotenv').config();

const app = express();

// Middleware
app.use(cors());
app.use(express.json());

// API Key Middleware
app.use('/api', (req, res, next) => {
  const apiKey = req.headers['x-api-key'];
  const validKey = process.env.API_KEY;
  
  if (!validKey) {
    // If no key configured on server, warning but allow? 
    // Or better: Fail closed. But for now let's blocking if key IS configured.
    // If .env has key, we require it.
    console.warn('API_KEY not set in .env! Securing is disabled.');
    return next();
  }
  
  if (apiKey !== validKey) {
    return res.status(403).json({ success: false, message: 'Forbidden: Invalid API Key' });
  }
  next();
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
  
  const now = new Date();
  // Format: HH:mm:ss dd/MM/yyyy (Manual formatting to ensure consistency)
  const d = now.getDate().toString().padStart(2, '0');
  const m = (now.getMonth() + 1).toString().padStart(2, '0');
  const y = now.getFullYear();
  const h = now.getHours().toString().padStart(2, '0');
  const min = now.getMinutes().toString().padStart(2, '0');
  const s = now.getSeconds().toString().padStart(2, '0');
  const timestamp = `${h}:${min}:${s} ${d}/${m}/${y}`;
  
  // Prepare row data (Columns A-O)
  // A: Deadline, B: Host, C: Shop, D: Link, E: Stage, F: Tags, G: Dev_note, H: Image_note, I: Note, J: Time, K: Message, L: MessageID, M: ImageID, N: ID, O: UpdatedAt
  const row = [
    feedback.deadline || '',        
    feedback.host || '',            
    feedback.shop || '',            
    feedback.link || '',            
    feedback.stage || 'Feedback',   
    feedback.tags || '',            
    feedback.devNote || '',         
    '',                             // Image_note
    feedback.note || '',            
    timestamp,                      // Time (created)
    feedback.note || '',            // Message (copy of note for now)
    '',                             // MessageID
    '',                             // ImageID
    crypto.randomUUID(),            // ID
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
  // Columns: A-N (0-13)
  // A:Deadline, B:Host, C:Shop, D:Link, E:Stage, F:Tags, G:Dev, H:ImgN, I:Note, J:Time, K:Msg, L:MsgID, M:ImgID, N:ID
  
  const newRow = [...currentRowRaw];
  // Ensure we have enough empty strings (15 columns: A-O)
  while(newRow.length < 15) newRow.push('');
  
  if (updates.deadline !== undefined) newRow[0] = updates.deadline;
  if (updates.host !== undefined) newRow[1] = updates.host;
  if (updates.shop !== undefined) newRow[2] = updates.shop;
  if (updates.link !== undefined) newRow[3] = updates.link;
  if (updates.stage !== undefined) newRow[4] = updates.stage;
  if (updates.tags !== undefined) newRow[5] = updates.tags;
  if (updates.devNote !== undefined) newRow[6] = updates.devNote;
  if (updates.note !== undefined) newRow[8] = updates.note;
  if (updates.message !== undefined) newRow[10] = updates.message;
  // Keep ID (13) and Time (9) same
  
  // Update the updated_at timestamp (column O, index 14)
  const now = new Date();
  const d = now.getDate().toString().padStart(2, '0');
  const m = (now.getMonth() + 1).toString().padStart(2, '0');
  const y = now.getFullYear();
  const h = now.getHours().toString().padStart(2, '0');
  const min = now.getMinutes().toString().padStart(2, '0');
  const s = now.getSeconds().toString().padStart(2, '0');
  newRow[14] = `${h}:${min}:${s} ${d}/${m}/${y}`;
  
  await sheetsClient.updateRow(rowNumber, newRow);
  return { success: true, message: 'Cập nhật thành công!' };
}

async function updateStage(rowNumber, newStage) {
  // Update Column E (Stage) and Column O (UpdatedAt)
  const now = new Date();
  const d = now.getDate().toString().padStart(2, '0');
  const m = (now.getMonth() + 1).toString().padStart(2, '0');
  const y = now.getFullYear();
  const h = now.getHours().toString().padStart(2, '0');
  const min = now.getMinutes().toString().padStart(2, '0');
  const s = now.getSeconds().toString().padStart(2, '0');
  const timestamp = `${h}:${min}:${s} ${d}/${m}/${y}`;
  
  await sheetsClient.updateCell(rowNumber, 'E', newStage);
  await sheetsClient.updateCell(rowNumber, 'O', timestamp);
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
  const d = now.getDate().toString().padStart(2, '0');
  const m = (now.getMonth() + 1).toString().padStart(2, '0');
  const y = now.getFullYear();
  const h = now.getHours().toString().padStart(2, '0');
  const min = now.getMinutes().toString().padStart(2, '0');
  const s = now.getSeconds().toString().padStart(2, '0');
  const timestamp = `${h}:${min}:${s} ${d}/${m}/${y}`;

  for (const rowNumber of rowNumbers) {
    await sheetsClient.updateCell(rowNumber, 'E', newStage);
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

  // Read current devNote (column G, index 6)
  const currentRow = await sheetsClient.getRow(rowNumber);
  let comments = [];
  
  try {
    if (currentRow[6]) {
      comments = JSON.parse(currentRow[6]);
      if (!Array.isArray(comments)) comments = [];
    }
  } catch (e) {
    comments = []; // Invalid JSON, start fresh
  }

  // Create new comment
  const now = new Date();
  const d = now.getDate().toString().padStart(2, '0');
  const m = (now.getMonth() + 1).toString().padStart(2, '0');
  const y = now.getFullYear();
  const h = now.getHours().toString().padStart(2, '0');
  const min = now.getMinutes().toString().padStart(2, '0');
  
  comments.push({
    text: commentText.trim(),
    time: `${h}:${min} ${d}/${m}/${y}`,
    author: 'User'
  });

  // Write back
  await sheetsClient.updateCell(rowNumber, 'G', JSON.stringify(comments));
  
  return { success: true, message: 'Đã thêm comment!', comments };
}

async function getComments(rowNumber) {
  if (!rowNumber) throw new Error('Missing rowNumber');

  const currentRow = await sheetsClient.getRow(rowNumber);
  let comments = [];
  
  try {
    if (currentRow[6]) {
      comments = JSON.parse(currentRow[6]);
      if (!Array.isArray(comments)) comments = [];
    }
  } catch (e) {
    comments = [];
  }

  return { success: true, comments };
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
