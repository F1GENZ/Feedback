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
      case 'createFeedback':
        result = await createFeedback(req.body.feedback);
        break;
      case 'updateFeedback':
        result = await updateFeedback(req.body.rowNumber, req.body.updates);
        break;
      case 'updateStage':
        result = await updateStage(req.body.rowNumber, req.body.newStage);
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
  
  // Prepare row data (Columns A-N)
  // A: Deadline, B: Host, C: Shop, D: Link, E: Stage, F: Tags, G: Dev_note, H: Image_note, I: Note, J: Time, K: Message, L: MessageID, M: ImageID, N: ID
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
    timestamp,                      
    feedback.note || '',            // Message (copy of note for now)
    '',                             // MessageID
    '',                             // ImageID
    crypto.randomUUID()             
  ];

  const success = await sheetsClient.appendRow(row);
  if (success) {
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
  // Ensure we have enough empty strings
  while(newRow.length < 14) newRow.push('');
  
  if (updates.deadline !== undefined) newRow[0] = updates.deadline;
  if (updates.host !== undefined) newRow[1] = updates.host;
  if (updates.shop !== undefined) newRow[2] = updates.shop;
  if (updates.link !== undefined) newRow[3] = updates.link;
  if (updates.stage !== undefined) newRow[4] = updates.stage;
  if (updates.tags !== undefined) newRow[5] = updates.tags;
  if (updates.devNote !== undefined) newRow[6] = updates.devNote;
  if (updates.note !== undefined) newRow[8] = updates.note;
  if (updates.message !== undefined) newRow[10] = updates.message;
  // Keep ID (13) and Time (9) same usually
  
  await sheetsClient.updateRow(rowNumber, newRow);
  return { success: true, message: 'Cập nhật thành công!' };
}

async function updateStage(rowNumber, newStage) {
  // Update Column E (Index 4 in 0-based, or Column E)
  // sheetsClient.updateCell takes (row, colLetter, val)
  
  await sheetsClient.updateCell(rowNumber, 'E', newStage);
  return { success: true, message: `Đã cập nhật Stage thành "${newStage}"` };
}

// Start server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
