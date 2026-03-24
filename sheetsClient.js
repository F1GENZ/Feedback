const { google } = require('googleapis');
const path = require('path');
require('dotenv').config();

const SCOPES = ['https://www.googleapis.com/auth/spreadsheets'];
const SHEET_ID = process.env.SHEET_ID;
const SHEET_NAME = 'Feedback';

const MAX_RETRIES = 3;
const BASE_DELAY_MS = 1000;

/**
 * Retry wrapper for Google API calls that may fail with transient 500/503 errors.
 * Uses exponential backoff: 1s, 2s, 4s
 */
async function withRetry(fn, label = 'API call') {
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      return await fn();
    } catch (error) {
      const status = error?.code || error?.response?.status;
      const isRetryable = [500, 503, 429].includes(status);

      if (isRetryable && attempt < MAX_RETRIES) {
        const delay = BASE_DELAY_MS * Math.pow(2, attempt - 1);
        console.warn(`[Retry] ${label} failed (HTTP ${status}), attempt ${attempt}/${MAX_RETRIES}. Retrying in ${delay}ms...`);
        await new Promise(resolve => setTimeout(resolve, delay));
      } else {
        throw error;
      }
    }
  }
}

class SheetsClient {
  constructor() {
    const authOptions = {
      scopes: SCOPES,
    };

    if (process.env.GOOGLE_JSON_KEY) {
      // Support raw JSON string from Env Var (Best for App Platform)
      try {
        authOptions.credentials = JSON.parse(process.env.GOOGLE_JSON_KEY);
      } catch (e) {
        console.error('Failed to parse GOOGLE_JSON_KEY:', e);
      }
    } else {
      // Fallback to file path
      authOptions.keyFile = process.env.GOOGLE_APPLICATION_CREDENTIALS || 'credentials.json';
    }

    this.auth = new google.auth.GoogleAuth(authOptions);
    this.sheets = google.sheets({ version: 'v4', auth: this.auth });
  }

  async getAllData() {
    return withRetry(async () => {
      const response = await this.sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range: SHEET_NAME,
      });

      const data = response.data.values || [];
      if (data.length < 3) return { headers: [], rows: [] };

      const headers = data[0]; 
      const rowsRaw = data.slice(2); 

      const rows = rowsRaw.map((row, index) => ({
        rowNumber: index + 3,
        deadline: row[0] || '',
        host: row[1] || '',
        shop: row[2] || '',
        link: row[3] || '',
        stage: row[4] || '',
        tags: row[5] || '',
        devNote: row[6] || '',
        imageNote: row[7] || '',
        note: row[8] || '',
        time: row[9] || '',
        message: row[10] || '',
        messageId: row[11] || '',
        imageId: row[12] || '',
        id: row[13] || '',
        updatedAt: row[14] || ''
      })).filter(row => row.shop || row.host);

      return { headers, rows };
    }, 'getAllData');
  }

  async getRow(rowNumber) {
    return withRetry(async () => {
      const range = `${SHEET_NAME}!A${rowNumber}:O${rowNumber}`;
      const response = await this.sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range: range,
      });
      return response.data.values ? response.data.values[0] : [];
    }, `getRow(${rowNumber})`);
  }

  async appendRow(rowArray) {
    return withRetry(async () => {
      await this.sheets.spreadsheets.values.append({
        spreadsheetId: SHEET_ID,
        range: SHEET_NAME,
        valueInputOption: 'USER_ENTERED',
        resource: { values: [rowArray] },
      });
      return true;
    }, 'appendRow');
  }

  async updateRow(rowNumber, rowArray) {
    return withRetry(async () => {
      const range = `${SHEET_NAME}!A${rowNumber}:O${rowNumber}`;
      await this.sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: range,
        valueInputOption: 'USER_ENTERED',
        resource: { values: [rowArray] },
      });
      return true;
    }, `updateRow(${rowNumber})`);
  }

  async updateCell(rowNumber, colLetter, value) {
    return withRetry(async () => {
      const range = `${SHEET_NAME}!${colLetter}${rowNumber}`;
      await this.sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: range,
        valueInputOption: 'USER_ENTERED',
        resource: { values: [[value]] },
      });
      return true;
    }, `updateCell(${colLetter}${rowNumber})`);
  }

  async deleteRow(rowNumber) {
    return withRetry(async () => {
      const spreadsheet = await this.sheets.spreadsheets.get({
        spreadsheetId: SHEET_ID,
      });
      const sheet = spreadsheet.data.sheets.find(s => s.properties.title === SHEET_NAME);
      const sheetId = sheet.properties.sheetId;

      await this.sheets.spreadsheets.batchUpdate({
        spreadsheetId: SHEET_ID,
        resource: {
          requests: [{
            deleteDimension: {
              range: {
                sheetId: sheetId,
                dimension: 'ROWS',
                startIndex: rowNumber - 1,
                endIndex: rowNumber
              }
            }
          }]
        }
      });
      return true;
    }, `deleteRow(${rowNumber})`);
  }

  async getGuidesData() {
    return withRetry(async () => {
      const response = await this.sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range: 'Intruction',
      });

      const data = response.data.values || [];
      if (data.length < 2) return { rows: [] };

      const rowsRaw = data.slice(1);

      const rows = rowsRaw.map((row, index) => ({
        rowNumber: index + 2,
        type: row[0] || '',
        template: row[1] || '',
        link: row[2] || '',
        app: row[3] || ''
      })).filter(row => row.template || row.link);

      return { rows };
    }, 'getGuidesData');
  }

  async appendGuideRow(rowArray) {
    return withRetry(async () => {
      await this.sheets.spreadsheets.values.append({
        spreadsheetId: SHEET_ID,
        range: 'Intruction',
        valueInputOption: 'USER_ENTERED',
        resource: { values: [rowArray] },
      });
      return true;
    }, 'appendGuideRow');
  }

  async updateGuideRow(rowNumber, rowArray) {
    return withRetry(async () => {
      const range = `Intruction!A${rowNumber}:D${rowNumber}`;
      await this.sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: range,
        valueInputOption: 'USER_ENTERED',
        resource: { values: [rowArray] },
      });
      return true;
    }, `updateGuideRow(${rowNumber})`);
  }

  async deleteGuideRow(rowNumber) {
    return withRetry(async () => {
      const spreadsheet = await this.sheets.spreadsheets.get({
        spreadsheetId: SHEET_ID,
      });
      const sheet = spreadsheet.data.sheets.find(s => s.properties.title === 'Intruction');
      if (!sheet) {
        throw new Error('Sheet "Intruction" not found');
      }
      const sheetId = sheet.properties.sheetId;

      await this.sheets.spreadsheets.batchUpdate({
        spreadsheetId: SHEET_ID,
        resource: {
          requests: [{
            deleteDimension: {
              range: {
                sheetId: sheetId,
                dimension: 'ROWS',
                startIndex: rowNumber - 1,
                endIndex: rowNumber
              }
            }
          }]
        }
      });
      return true;
    }, `deleteGuideRow(${rowNumber})`);
  }

  // --- HISTORY LOG ---
  async logHistory(action, content) {
    try {
      const now = new Date();
      const timestamp = now.toLocaleString('vi-VN', { timeZone: 'Asia/Ho_Chi_Minh' });
      await this.sheets.spreadsheets.values.append({
        spreadsheetId: SHEET_ID,
        range: 'History',
        valueInputOption: 'USER_ENTERED',
        resource: { values: [[timestamp, action, content]] },
      });
      return true;
    } catch (error) {
      console.error('SheetsClient: logHistory Error:', error);
      return false;
    }
  }

  async getHistoryData() {
    return withRetry(async () => {
      const response = await this.sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range: 'History',
      });

      const data = response.data.values || [];
      if (data.length < 1) return { rows: [] };

      const rows = data.map((row, index) => ({
        rowNumber: index + 1,
        time: row[0] || '',
        action: row[1] || '',
        content: row[2] || ''
      })).reverse();

      return { rows };
    }, 'getHistoryData');
  }
}

module.exports = new SheetsClient();
