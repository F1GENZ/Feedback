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

      // Map rows roughly matching Code.gs logic
      const rows = rowsRaw.map((row, index) => ({
        rowNumber: index + 3, // 1-based index, skipping 2 header rows
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
        id: row[13] || ''
      })).filter(row => row.shop || row.host);

      return { headers, rows };
    } catch (error) {
      console.error('SheetsClient: getAllData Error:', error);
      throw error;
    }
  }

  async getRow(rowNumber) {
    try {
      const range = `${SHEET_NAME}!A${rowNumber}:N${rowNumber}`;
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
    // rowArray should match the columns order A-N
    try {
      await this.sheets.spreadsheets.values.append({
        spreadsheetId: SHEET_ID,
        range: SHEET_NAME,
        valueInputOption: 'USER_ENTERED',
        resource: { values: [rowArray] },
      });
      return true;
    } catch (error) {
      console.error('SheetsClient: appendRow Error:', error);
      throw error;
    }
  }

  async updateRow(rowNumber, rowArray) {
    // rowNumber is 1-based
    try {
      const range = `${SHEET_NAME}!A${rowNumber}:N${rowNumber}`;
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

  async getGuidesData() {
    try {
      const response = await this.sheets.spreadsheets.values.get({
        spreadsheetId: SHEET_ID,
        range: 'File Hướng Dẫn',
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
}

module.exports = new SheetsClient();
