/**
 * ==============================================
 * async/await不使用版 Google Trends GAS
 * ==============================================
 */

// Flask APIのURL
const FLASK_API_URL = "https://google-trends-api-bxio.onrender.com/trend";
const FLASK_HEALTH_URL = "https://google-trends-api-bxio.onrender.com/health";

/**
 * メイン関数：サーバー起動確認付き（同期版）
 */
function fetchGoogleTrendsData() {
  const sheet = SpreadsheetApp.getActiveSheet();
  
  try {
    // サーバー状態確認
    const serverReady = ensureServerReady();
    if (!serverReady) {
      SpreadsheetApp.getUi().alert('サーバーエラー', 'Renderサーバーが応答しません。しばらく待ってから再実行してください。', SpreadsheetApp.getUi().ButtonSet.OK);
      return;
    }
    
    // 現在の設定を読み取り
    const currentConfig = readConfiguration(sheet);
    
    // バリデーション
    if (!validateConfig(currentConfig)) {
      SpreadsheetApp.getUi().alert('設定エラー', 'キーワード、日付、期間を正しく入力してください。', SpreadsheetApp.getUi().ButtonSet.OK);
      return;
    }
    
    // 前回設定をシートから読み取り
    const previousConfig = readPreviousConfigFromSheet(sheet);
    
    // 実行モードを判定
    const executionMode = determineMode(currentConfig, previousConfig);
    
    Logger.log('実行モード: ' + executionMode.mode);
    Logger.log('理由: ' + executionMode.reason);
    
    // モード別実行（同期版）
    switch (executionMode.mode) {
      case 'NEW':
        executeNewAnalysis(sheet, currentConfig);
        break;
      case 'EXPAND':
        executeExpansion(sheet, currentConfig, previousConfig);
        break;
      case 'SAME':
        const rerun = SpreadsheetApp.getUi().alert(
          '変更なし',
          '設定に変更がありません。再実行しますか？',
          SpreadsheetApp.getUi().ButtonSet.YES_NO
        );
        if (rerun === SpreadsheetApp.getUi().Button.YES) {
          executeNewAnalysis(sheet, currentConfig);
        }
        break;
    }
    
  } catch (error) {
    Logger.log('エラー: ' + error.toString());
    SpreadsheetApp.getUi().alert('エラー', error.toString(), SpreadsheetApp.getUi().ButtonSet.OK);
  }
}

/**
 * サーバー起動確認（Keep-Alive機能）
 */
function ensureServerReady() {
  Logger.log('サーバー状態確認開始...');
  
  const maxAttempts = 3;
  
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      Logger.log(`サーバーチェック試行 ${attempt}/${maxAttempts}`);
      
      const options = {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
          'User-Agent': 'Google-Apps-Script'
        },
        muteHttpExceptions: true
      };
      
      const response = UrlFetchApp.fetch(FLASK_HEALTH_URL, options);
      const responseCode = response.getResponseCode();
      
      if (responseCode === 200) {
        const healthData = JSON.parse(response.getContentText());
        Logger.log('サーバー状態: ' + JSON.stringify(healthData));
        
        // ライブラリ可用性チェック
        if (healthData.libraries && healthData.libraries.pandas && healthData.libraries.pytrends) {
          Logger.log('✅ サーバー準備完了');
          return true;
        } else {
          Logger.log('⚠️ 必要なライブラリが利用できません');
          return false;
        }
      } else {
        Logger.log(`❌ サーバー応答エラー: ${responseCode}`);
        
        if (attempt < maxAttempts) {
          // Cold start対応：最初の試行で失敗した場合は長めに待機
          const waitTime = attempt === 1 ? 30 : 15;
          Logger.log(`${waitTime}秒待機してリトライします...`);
          Utilities.sleep(waitTime * 1000);
        }
      }
      
    } catch (error) {
      Logger.log(`サーバーチェック失敗 (試行${attempt}): ${error.toString()}`);
      
      if (attempt < maxAttempts) {
        const waitTime = 15;
        Logger.log(`${waitTime}秒待機してリトライします...`);
        Utilities.sleep(waitTime * 1000);
      }
    }
  }
  
  Logger.log('❌ サーバーが応答しません');
  return false;
}

/**
 * サーバーウォームアップ（手動実行用）
 */
function warmupServer() {
  Logger.log('サーバーウォームアップ開始...');
  
  try {
    const options = {
      method: 'GET',
      headers: { 'Content-Type': 'application/json' },
      muteHttpExceptions: true
    };
    
    // ホームエンドポイントにアクセス
    const homeResponse = UrlFetchApp.fetch(FLASK_API_URL.replace('/trend', ''), options);
    Logger.log('ホームエンドポイント応答: ' + homeResponse.getResponseCode());
    
    // ヘルスチェックエンドポイントにアクセス
    const healthResponse = UrlFetchApp.fetch(FLASK_HEALTH_URL, options);
    Logger.log('ヘルスチェック応答: ' + healthResponse.getResponseCode());
    
    if (healthResponse.getResponseCode() === 200) {
      const healthData = JSON.parse(healthResponse.getContentText());
      Logger.log('サーバー情報: ' + JSON.stringify(healthData, null, 2));
      
      SpreadsheetApp.getUi().alert(
        '✅ ウォームアップ完了',
        'サーバーが起動しました！\n' +
        'ライブラリ状態:\n' +
        '- Pandas: ' + (healthData.libraries?.pandas ? '✅' : '❌') + '\n' +
        '- Pytrends: ' + (healthData.libraries?.pytrends ? '✅' : '❌') + '\n\n' +
        '今すぐデータ取得を実行できます。',
        SpreadsheetApp.getUi().ButtonSet.OK
      );
    } else {
      throw new Error('サーバーが正常に応答しません');
    }
    
  } catch (error) {
    Logger.log('ウォームアップエラー: ' + error.toString());
    SpreadsheetApp.getUi().alert(
      '❌ ウォームアップ失敗',
      'サーバーの起動に失敗しました:\n' + error.toString() + '\n\n' +
      'Renderダッシュボードでサービス状態を確認してください。',
      SpreadsheetApp.getUi().ButtonSet.OK
    );
  }
}

/**
 * 設定読み取り
 */
function readConfiguration(sheet) {
  const keywords = sheet.getRange('B2:E2').getValues()[0]
    .map(k => k.toString().trim())
    .filter(k => k.length > 0);
  
  return {
    keywords: keywords,
    startDate: sheet.getRange('B3').getValue(),
    endDate: sheet.getRange('B4').getValue(),
    frequency: sheet.getRange('B5').getValue().toString().trim().toLowerCase(),
    baseKeyword: keywords.length > 0 ? keywords[0] : null
  };
}

/**
 * 前回設定をシートから読み取り（隠し行使用）
 */
function readPreviousConfigFromSheet(sheet) {
  try {
    const hiddenRow = sheet.getRange('B6:F6').getValues()[0];
    
    if (!hiddenRow[0]) return null;
    
    const keywords = hiddenRow[0].split(',').filter(k => k.trim().length > 0);
    
    return {
      keywords: keywords,
      startDate: new Date(hiddenRow[1]),
      endDate: new Date(hiddenRow[2]),
      frequency: hiddenRow[3],
      baseKeyword: hiddenRow[4] || (keywords.length > 0 ? keywords[0] : null)
    };
  } catch (error) {
    Logger.log('前回設定読み取りエラー: ' + error.toString());
    return null;
  }
}

/**
 * 設定をシートに保存（隠し行使用）
 */
function saveConfigToSheet(sheet, config) {
  try {
    const configRow = [
      config.keywords.join(','),
      config.startDate.toISOString(),
      config.endDate.toISOString(),
      config.frequency,
      config.baseKeyword || ''
    ];
    
    sheet.getRange('B6:F6').setValues([configRow]);
    sheet.hideRows(6);
    
    Logger.log('設定保存完了（シート）');
  } catch (error) {
    Logger.log('設定保存エラー: ' + error.toString());
  }
}

/**
 * データ情報をシートに保存
 */
function saveDataInfoToSheet(sheet, data, baseKeyword) {
  try {
    const keywords = Object.keys(data.keywords);
    const info = [
      keywords.length + 1,
      calculateAverage(data.keywords[baseKeyword]),
      data.dates.length
    ];
    
    sheet.getRange('H6:J6').setValues([info]);
    Logger.log('データ情報保存完了（シート）');
  } catch (error) {
    Logger.log('データ情報保存エラー: ' + error.toString());
  }
}

/**
 * データ情報をシートから読み取り
 */
function readDataInfoFromSheet(sheet) {
  try {
    const hiddenRow = sheet.getRange('H6:J6').getValues()[0];
    
    if (!hiddenRow[0]) return null;
    
    return {
      lastColumn: hiddenRow[0],
      baseAverage: hiddenRow[1],
      dataRows: hiddenRow[2]
    };
  } catch (error) {
    Logger.log('データ情報読み取りエラー: ' + error.toString());
    return null;
  }
}

/**
 * 設定検証（強化版）
 */
function validateConfig(config) {
  if (!config.keywords || config.keywords.length === 0) {
    return false;
  }
  if (config.keywords.length > 4) {
    return false;
  }
  if (!config.startDate || !config.endDate) {
    return false;
  }
  if (!['daily', 'weekly', 'monthly'].includes(config.frequency)) {
    return false;
  }
  
  const start = new Date(config.startDate);
  const end = new Date(config.endDate);
  const daysDiff = Math.ceil((end - start) / (1000 * 60 * 60 * 24));
  
  if (config.frequency === 'daily' && daysDiff > 270) {
    SpreadsheetApp.getUi().alert('期間エラー', 'Daily（日次）は最大9ヶ月までです。期間を短縮するか、WeeklyまたはMonthlyを選択してください。', SpreadsheetApp.getUi().ButtonSet.OK);
    return false;
  }
  if (config.frequency === 'weekly' && daysDiff < 7) {
    SpreadsheetApp.getUi().alert('期間エラー', 'Weekly（週次）は最低1週間以上必要です。期間を延長するか、Dailyを選択してください。', SpreadsheetApp.getUi().ButtonSet.OK);
    return false;
  }
  if (config.frequency === 'monthly' && daysDiff < 30) {
    SpreadsheetApp.getUi().alert('期間エラー', 'Monthly（月次）は最低1ヶ月以上必要です。期間を延長するか、DailyまたはWeeklyを選択してください。', SpreadsheetApp.getUi().ButtonSet.OK);
    return false;
  }
  
  return true;
}

/**
 * 実行モード判定
 */
function determineMode(current, previous) {
  if (!previous) {
    return { mode: 'NEW', reason: '初回実行' };
  }
  
  if (current.baseKeyword !== previous.baseKeyword) {
    return { mode: 'NEW', reason: '基準キーワード（B2）が変更されました' };
  }
  
  if (current.startDate.getTime() !== previous.startDate.getTime() ||
      current.endDate.getTime() !== previous.endDate.getTime() ||
      current.frequency !== previous.frequency) {
    return { mode: 'NEW', reason: '日付または期間が変更されました' };
  }
  
  const currentOthers = current.keywords.slice(1).sort();
  const previousOthers = previous.keywords.slice(1).sort();
  
  if (JSON.stringify(currentOthers) !== JSON.stringify(previousOthers)) {
    return { mode: 'EXPAND', reason: 'キーワード（C2-E2）が変更されました - 横展開実行' };
  }
  
  return { mode: 'SAME', reason: '設定に変更なし' };
}

/**
 * 新規分析実行（同期版）
 */
function executeNewAnalysis(sheet, config) {
  Logger.log('新規分析開始');
  
  try {
    clearDataArea(sheet);
    const data = fetchFromAPI(config);  // awaitを削除
    outputData(sheet, data, 1, false);
    saveConfigToSheet(sheet, config);
    saveDataInfoToSheet(sheet, data, config.baseKeyword);
    
    SpreadsheetApp.getUi().alert('完了', '新規分析が完了しました！', SpreadsheetApp.getUi().ButtonSet.OK);
  } catch (error) {
    Logger.log('新規分析エラー: ' + error.toString());
    SpreadsheetApp.getUi().alert('エラー', '新規分析に失敗しました: ' + error.toString(), SpreadsheetApp.getUi().ButtonSet.OK);
  }
}

/**
 * 横展開実行（同期版）
 */
function executeExpansion(sheet, current, previous) {
  Logger.log('横展開開始');
  
  try {
    const existingInfo = readDataInfoFromSheet(sheet);
    if (!existingInfo) {
      throw new Error('既存データが見つかりません。新規分析として実行します。');
    }
    
    const newData = fetchFromAPI(current);  // awaitを削除
    const normalized = normalizeData(newData, existingInfo, current.baseKeyword);
    const nextCol = existingInfo.lastColumn + 1;
    
    outputData(sheet, normalized, nextCol, true);
    updateDataInfoInSheet(sheet, existingInfo, normalized, nextCol);
    saveConfigToSheet(sheet, current);
    
    SpreadsheetApp.getUi().alert('完了', `キーワードが横展開されました！\n新しいデータは${getColumnLetter(nextCol)}列から追加されています。`, SpreadsheetApp.getUi().ButtonSet.OK);
    
  } catch (error) {
    Logger.log('横展開エラー: ' + error.toString());
    
    const retry = SpreadsheetApp.getUi().alert(
      'エラー',
      '横展開に失敗しました: ' + error.toString() + '\n\n新規分析として実行しますか？',
      SpreadsheetApp.getUi().ButtonSet.YES_NO
    );
    
    if (retry === SpreadsheetApp.getUi().Button.YES) {
      executeNewAnalysis(sheet, current);
    }
  }
}

/**
 * API呼び出し（同期版・Render最適化）
 */
function fetchFromAPI(config) {
  const timeframe = formatDate(config.startDate) + ' ' + formatDate(config.endDate);
  
  Logger.log('API呼び出し: ' + JSON.stringify(config.keywords));
  
  const maxRetries = 4;
  let lastError = null;
  
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      if (attempt > 1) {
        const waitTime = Math.pow(2, attempt - 1) * 15;
        Logger.log(`リトライ前待機: ${waitTime}秒`);
        Utilities.sleep(waitTime * 1000);
      }
      
      // Cold start対応の初期遅延
      const delay = attempt === 1 ? Math.random() * 10 + 5 : Math.random() * 5 + 3;
      Logger.log(`初期待機: ${delay.toFixed(1)}秒`);
      Utilities.sleep(delay * 1000);
      
      const payload = {
        keywords: config.keywords,
        timeframe: timeframe,
        frequency: config.frequency,
        geo: "JP"
      };
      
      const options = {
        method: 'POST',
        headers: { 
          'Content-Type': 'application/json',
          'User-Agent': 'Google-Apps-Script-Smart-GAS'
        },
        payload: JSON.stringify(payload),
        muteHttpExceptions: true
      };
      
      const response = UrlFetchApp.fetch(FLASK_API_URL, options);
      const responseCode = response.getResponseCode();
      
      if (responseCode === 200) {
        const result = JSON.parse(response.getContentText());
        return processResponse(result);
      } else if (responseCode === 429) {
        lastError = new Error('Rate limit exceeded');
        if (attempt < maxRetries) {
          Logger.log('Rate limit検出、長時間待機します...');
          Utilities.sleep(120 * 1000); // 2分待機
          continue;
        }
      } else if (responseCode === 500) {
        lastError = new Error('Server internal error');
        if (attempt < maxRetries) {
          Logger.log('サーバーエラー検出、サーバー回復を待機...');
          Utilities.sleep(60 * 1000); // 1分待機
          continue;
        }
      } else {
        throw new Error(`API Error (${responseCode}): ${response.getContentText()}`);
      }
      
    } catch (error) {
      lastError = error;
      Logger.log(`試行${attempt}失敗: ${error.toString()}`);
    }
  }
  
  throw new Error(`API呼び出し失敗 (${maxRetries}回試行): ${lastError ? lastError.toString() : 'Unknown error'}`);
}

/**
 * レスポンス処理
 */
function processResponse(responseData) {
  const records = responseData.data;
  const firstRecord = records[0];
  const keywords = Object.keys(firstRecord).filter(key => key !== 'date' && key !== 'index');
  
  const dates = records.map(record => new Date(record.date || record.index));
  const keywordData = {};
  
  keywords.forEach(keyword => {
    keywordData[keyword] = records.map(record => record[keyword] || 0);
  });
  
  return { dates: dates, keywords: keywordData };
}

/**
 * データ正規化
 */
function normalizeData(newData, existingInfo, baseKeyword) {
  Logger.log('データ正規化開始 - 基準: ' + baseKeyword);
  
  if (!newData.keywords[baseKeyword]) {
    throw new Error('新データに基準キーワードが含まれていません');
  }
  
  const newBaseAvg = calculateAverage(newData.keywords[baseKeyword]);
  const existingBaseAvg = existingInfo.baseAverage;
  
  Logger.log(`新データ平均: ${newBaseAvg}, 既存データ平均: ${existingBaseAvg}`);
  
  if (newBaseAvg === 0) {
    throw new Error('新データの基準キーワード平均値が0です');
  }
  
  const scalingFactor = existingBaseAvg / newBaseAvg;
  Logger.log('スケーリング係数: ' + scalingFactor);
  
  const normalized = {
    dates: newData.dates,
    keywords: {}
  };
  
  Object.keys(newData.keywords).forEach(keyword => {
    if (keyword === baseKeyword) {
      normalized.keywords[keyword] = newData.keywords[keyword].map(() => Math.round(existingBaseAvg));
    } else {
      normalized.keywords[keyword] = newData.keywords[keyword].map(value => 
        Math.round(value * scalingFactor)
      );
    }
  });
  
  return normalized;
}

/**
 * データ出力（横展開対応）
 */
function outputData(sheet, data, startColumn, isExpansion = false) {
  const startRow = 8;
  const keywords = Object.keys(data.keywords);
  
  let headers, dataToWrite;
  
  if (isExpansion) {
    const nonBaseKeywords = keywords.slice(1);
    headers = nonBaseKeywords;
    
    dataToWrite = [];
    for (let i = 0; i < data.dates.length; i++) {
      const row = [];
      nonBaseKeywords.forEach(keyword => {
        row.push(data.keywords[keyword][i] || 0);
      });
      dataToWrite.push(row);
    }
  } else {
    headers = ['日付', ...keywords];
    
    dataToWrite = [];
    for (let i = 0; i < data.dates.length; i++) {
      const row = [formatDate(data.dates[i])];
      keywords.forEach(keyword => {
        row.push(data.keywords[keyword][i] || 0);
      });
      dataToWrite.push(row);
    }
  }
  
  if (headers.length === 0) return;
  
  sheet.getRange(startRow, startColumn, 1, headers.length).setValues([headers]);
  
  if (dataToWrite.length > 0) {
    sheet.getRange(startRow + 1, startColumn, dataToWrite.length, headers.length).setValues(dataToWrite);
  }
  
  sheet.getRange(startRow, startColumn, 1, headers.length)
    .setFontWeight('bold')
    .setBackground('#E8F0FE');
  
  Logger.log(`データ出力完了: ${headers.length}列, ${dataToWrite.length}行 (列${startColumn}から)`);
}

/**
 * データエリアクリア
 */
function clearDataArea(sheet) {
  const startRow = 8;
  const lastRow = sheet.getLastRow();
  const lastCol = sheet.getLastColumn();
  
  if (lastRow >= startRow && lastCol > 0) {
    sheet.getRange(startRow, 1, lastRow - startRow + 1, lastCol).clear();
    Logger.log('データエリアクリア完了');
  }
}

/**
 * データ情報更新（シート版）
 */
function updateDataInfoInSheet(sheet, existing, newData, newStartColumn) {
  const newKeywords = Object.keys(newData.keywords).slice(1);
  const updated = [
    newStartColumn + newKeywords.length - 1,
    existing.baseAverage,
    existing.dataRows
  ];
  
  sheet.getRange('H6:J6').setValues([updated]);
}

/**
 * 平均値計算
 */
function calculateAverage(array) {
  if (!array || array.length === 0) return 0;
  const sum = array.reduce((acc, val) => acc + (typeof val === 'number' ? val : 0), 0);
  return sum / array.length;
}

/**
 * 日付フォーマット
 */
function formatDate(date) {
  return Utilities.formatDate(new Date(date), Session.getScriptTimeZone(), 'yyyy-MM-dd');
}

/**
 * 列番号をアルファベットに変換
 */
function getColumnLetter(column) {
  let result = '';
  while (column > 0) {
    column--;
    result = String.fromCharCode(65 + (column % 26)) + result;
    column = Math.floor(column / 26);
  }
  return result;
}

/**
 * テンプレート作成（GAS対応版）
 */
function createTemplate() {
  const sheet = SpreadsheetApp.getActiveSheet();
  
  sheet.getRange('A1').setValue('📊 Smart Google Trends分析（GAS最適化版）');
  sheet.getRange('A1').setFontSize(14).setFontWeight('bold');
  
  sheet.getRange('A2').setValue('キーワード（最大4個）');
  sheet.getRange('A3').setValue('開始日');
  sheet.getRange('A4').setValue('終了日');
  sheet.getRange('A5').setValue('期間');
  
  sheet.getRange('B2').setValue('タイミー');
  sheet.getRange('C2').setValue('リクナビ');
  sheet.getRange('D2').setValue('バイトル');
  sheet.getRange('E2').setValue('');
  
  sheet.getRange('B3').setValue(new Date('2025-02-01'));
  sheet.getRange('B4').setValue(new Date('2025-03-31'));
  sheet.getRange('B5').setValue('Weekly');
  
  sheet.getRange('A2:A5').setFontWeight('bold').setBackground('#F8F9FA');
  sheet.getRange('B2:E2').setBackground('#E8F0FE');
  
  sheet.hideRows(6);
  
  sheet.getRange('A7').setValue('📈 データ出力エリア（A8以降に自動出力）');
  sheet.getRange('A7').setFontWeight('bold').setBackground('#FFF3CD');
  
  SpreadsheetApp.getUi().alert(
    '✅ GAS最適化版テンプレート作成完了',
    '🚀 使い方:\n' +
    '1. サーバーが停止している場合は「warmupServer()」を実行\n' +
    '2. B2-E2にキーワード入力 → 実行\n' +
    '3. 横展開: B2固定、C2-E2変更 → 実行\n\n' +
    '💡 async/awaitを使わない同期処理版です！\n' +
    '⚡ Cold start & エラーハンドリング対応済み',
    SpreadsheetApp.getUi().ButtonSet.OK
  );
}

/**
 * 保存データクリア（トラブルシューティング用）
 */
function clearStoredData() {
  const sheet = SpreadsheetApp.getActiveSheet();
  
  sheet.getRange('B6:J6').clear();
  sheet.showRows(6);
  sheet.hideRows(6);
  
  Logger.log('保存データをクリアしました（シート版）');
  SpreadsheetApp.getUi().alert('完了', '保存データをクリアしました', SpreadsheetApp.getUi().ButtonSet.OK);
}
