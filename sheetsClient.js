const { google } = require('googleapis');
const path = require('path');
require('dotenv').config();

const SCOPES = ['https://www.googleapis.com/auth/spreadsheets'];
const SHEET_ID = process.env.SHEET_ID;
const SHEET_NAME = 'Feedback';

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
    try {
      const response = await this.sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range: SHEET_NAME, // Read entire sheet
      });

      const data = response.data.values || [];
      if (data.length < 3) return { headers: [], rows: [] };

      // Row 1 & 2 are headers (indices 0, 1) in Code.gs logic
      const headers = data[0]; 
      const rowsRaw = data.slice(2); 

      // Map rows - Column order: A:ID, B:Deadline, C:Host, D:Shop, E:Link, F:Stage, G:Tags, H:Dev_note, I:Image_note, J:Note, K:Time, L:Message, M:MessageID, N:ImageID, O:UpdatedAt, P:Priority
      const rows = rowsRaw.map((row, index) => ({
        rowNumber: index + 3, // 1-based index, skipping 2 header rows
        id: row[0] || '',
        deadline: row[1] || '',
        host: row[2] || '',
        shop: row[3] || '',
        link: row[4] || '',
        stage: row[5] || '',
        tags: row[6] || '',
        devNote: row[7] || '',
        imageNote: row[8] || '',
        note: row[9] || '',
        time: row[10] || '',
        message: row[11] || '',
        messageId: row[12] || '',
        imageId: row[13] || '',
        updatedAt: row[14] || '',
        priority: row[15] || ''
      })).filter(row => row.shop || row.host);

      return { headers, rows };
    } catch (error) {
      console.error('SheetsClient: getAllData Error:', error);
      throw error;
    }
  }

  async getRow(rowNumber) {
    try {
      const range = `${SHEET_NAME}!A${rowNumber}:P${rowNumber}`;
      const response = await this.sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range: range,
      });
      return response.data.values ? response.data.values[0] : [];
    } catch (error) {
      console.error('SheetsClient: getRow Error:', error);
      throw error;
    }
  }

  async appendRow(rowArray) {
    try {
      const rowNumber = await this.getNextFeedbackRowNumber();
      const normalizedRow = (Array.isArray(rowArray) ? [...rowArray] : []).slice(0, 16);
      while (normalizedRow.length < 16) normalizedRow.push('');

      await this.sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: `${SHEET_NAME}!A${rowNumber}:P${rowNumber}`,
        valueInputOption: 'USER_ENTERED',
        resource: { values: [normalizedRow] },
      });
      return true;
    } catch (error) {
      console.error('SheetsClient: appendRow Error:', error);
      throw error;
    }
  }

  isCanonicalFeedbackRow(row) {
    if (!row || row.length === 0) return false;

    const id = String(row[0] || '').trim();
    const host = String(row[2] || '').trim();
    const shop = String(row[3] || '').trim();
    const stage = String(row[5] || '').trim();

    return /^\d{12,16}$/.test(id) || Boolean(host || shop || stage);
  }

  async getNextFeedbackRowNumber() {
    const response = await this.sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: `${SHEET_NAME}!A:P`,
    });
    const values = response.data.values || [];
    let lastCanonicalRow = 2; // Row 1-2 are headers/legacy header area.

    for (let index = values.length - 1; index >= 2; index--) {
      if (this.isCanonicalFeedbackRow(values[index])) {
        lastCanonicalRow = index + 1;
        break;
      }
    }

    return lastCanonicalRow + 1;
  }

  async getRawFeedbackData() {
    try {
      const response = await this.sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range: `${SHEET_NAME}!A:Z`,
      });
      const values = response.data.values || [];
      return {
        values,
        lastUsedRow: values.length
      };
    } catch (error) {
      console.error('SheetsClient: getRawFeedbackData Error:', error);
      throw error;
    }
  }

  async deleteFeedbackRowRange(startRowNumber, endRowNumber) {
    try {
      if (!startRowNumber || !endRowNumber || endRowNumber < startRowNumber) return true;

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
                sheetId,
                dimension: 'ROWS',
                startIndex: startRowNumber - 1,
                endIndex: endRowNumber
              }
            }
          }]
        }
      });
      return true;
    } catch (error) {
      console.error('SheetsClient: deleteFeedbackRowRange Error:', error);
      throw error;
    }
  }

  async updateRow(rowNumber, rowArray) {
    // rowNumber is 1-based
    try {
      const range = `${SHEET_NAME}!A${rowNumber}:P${rowNumber}`;
      await this.sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: range,
        valueInputOption: 'USER_ENTERED',
        resource: { values: [rowArray] },
      });
      return true;
    } catch (error) {
      console.error('SheetsClient: updateRow Error:', error);
      throw error;
    }
  }

  async updateCell(rowNumber, colLetter, value) {
    try {
      const range = `${SHEET_NAME}!${colLetter}${rowNumber}`;
      await this.sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: range,
        valueInputOption: 'USER_ENTERED',
        resource: { values: [[value]] },
      });
      return true;
    } catch (error) {
      console.error('SheetsClient: updateCell Error:', error);
      throw error;
    }
  }

  async batchUpdateCells(updates) {
    try {
      if (!updates || updates.length === 0) return true;
      await this.sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: SHEET_ID,
        resource: {
          valueInputOption: 'USER_ENTERED',
          data: updates.map(update => ({
            range: `${SHEET_NAME}!${update.colLetter}${update.rowNumber}`,
            values: [[update.value]]
          }))
        },
      });
      return true;
    } catch (error) {
      console.error('SheetsClient: batchUpdateCells Error:', error);
      throw error;
    }
  }

  async deleteRow(rowNumber) {
    try {
      // Get sheet ID first
      const spreadsheet = await this.sheets.spreadsheets.get({
        spreadsheetId: SHEET_ID,
      });
      const sheet = spreadsheet.data.sheets.find(s => s.properties.title === SHEET_NAME);
      const sheetId = sheet.properties.sheetId;

      // Delete row using batchUpdate
      await this.sheets.spreadsheets.batchUpdate({
        spreadsheetId: SHEET_ID,
        resource: {
          requests: [{
            deleteDimension: {
              range: {
                sheetId: sheetId,
                dimension: 'ROWS',
                startIndex: rowNumber - 1, // 0-based
                endIndex: rowNumber
              }
            }
          }]
        }
      });
      return true;
    } catch (error) {
      console.error('SheetsClient: deleteRow Error:', error);
      throw error;
    }
  }

  async deleteRows(rowNumbers) {
    try {
      if (!rowNumbers || !Array.isArray(rowNumbers) || rowNumbers.length === 0) return true;

      const normalized = [...new Set(rowNumbers.map(n => parseInt(n, 10)).filter(Number.isInteger))]
        .sort((a, b) => b - a);
      if (normalized.length === 0) return true;

      const spreadsheet = await this.sheets.spreadsheets.get({
        spreadsheetId: SHEET_ID,
      });
      const sheet = spreadsheet.data.sheets.find(s => s.properties.title === SHEET_NAME);
      const sheetId = sheet.properties.sheetId;

      await this.sheets.spreadsheets.batchUpdate({
        spreadsheetId: SHEET_ID,
        resource: {
          requests: normalized.map(rowNumber => ({
            deleteDimension: {
              range: {
                sheetId: sheetId,
                dimension: 'ROWS',
                startIndex: rowNumber - 1,
                endIndex: rowNumber
              }
            }
          }))
        }
      });
      return true;
    } catch (error) {
      console.error('SheetsClient: deleteRows Error:', error);
      throw error;
    }
  }

  async getGuidesData() {
    try {
      const response = await this.sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range: 'Intruction',
      });

      const data = response.data.values || [];
      if (data.length < 2) return { rows: [] };

      // Row 1 is header
      const rowsRaw = data.slice(1);

      // Map rows: Type, Template, Link, App
      const rows = rowsRaw.map((row, index) => ({
        rowNumber: index + 2,
        type: row[0] || '',
        template: row[1] || '',
        link: row[2] || '',
        app: row[3] || ''
      })).filter(row => row.template || row.link);

      return { rows };
    } catch (error) {
      console.error('SheetsClient: getGuidesData Error:', error);
      throw error;
    }
  }

  async appendGuideRow(rowArray) {
    try {
      await this.sheets.spreadsheets.values.append({
        spreadsheetId: SHEET_ID,
        range: 'Intruction',
        valueInputOption: 'USER_ENTERED',
        resource: { values: [rowArray] },
      });
      return true;
    } catch (error) {
      console.error('SheetsClient: appendGuideRow Error:', error);
      throw error;
    }
  }

  async updateGuideRow(rowNumber, rowArray) {
    try {
      const range = `Intruction!A${rowNumber}:D${rowNumber}`;
      await this.sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: range,
        valueInputOption: 'USER_ENTERED',
        resource: { values: [rowArray] },
      });
      return true;
    } catch (error) {
      console.error('SheetsClient: updateGuideRow Error:', error);
      throw error;
    }
  }

  async deleteGuideRow(rowNumber) {
    try {
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
    } catch (error) {
      console.error('SheetsClient: deleteGuideRow Error:', error);
      throw error;
    }
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
      // Don't throw - logging should not break main operations
      return false;
    }
  }

  async getHistoryData() {
    try {
      const response = await this.sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range: 'History',
      });

      const data = response.data.values || [];
      if (data.length < 1) return { rows: [] };

      // Assume first row is header or data starts from row 1
      const rows = data.map((row, index) => ({
        rowNumber: index + 1,
        time: row[0] || '',
        action: row[1] || '',
        content: row[2] || ''
      })).reverse(); // Latest first

      return { rows };
    } catch (error) {
      console.error('SheetsClient: getHistoryData Error:', error);
      throw error;
    }
  }
}

module.exports = new SheetsClient();
