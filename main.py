from flask import Flask, request, jsonify
import time
import random
import logging
from datetime import datetime, timedelta

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# 段階的インポート
PANDAS_AVAILABLE = False
PYTRENDS_AVAILABLE = False

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
    logger.info("✅ Pandas imported successfully")
except Exception as e:
    logger.error(f"❌ Pandas import failed: {e}")

try:
    from pytrends.request import TrendReq
    PYTRENDS_AVAILABLE = True
    logger.info("✅ Pytrends imported successfully")
except Exception as e:
    logger.error(f"❌ Pytrends import failed: {e}")

@app.route("/")
def home():
    status = {
        "service": "Google Trends API for Smart GAS",
        "pandas": "✅ Available" if PANDAS_AVAILABLE else "❌ Not Available",
        "pytrends": "✅ Available" if PYTRENDS_AVAILABLE else "❌ Not Available",
        "version": "2.0.0",
        "timestamp": datetime.now().isoformat()
    }
    
    return jsonify(status)

@app.route("/health")
def health_check():
    """ヘルスチェック用エンドポイント"""
    return jsonify({
        "status": "healthy",
        "libraries": {
            "pandas": PANDAS_AVAILABLE,
            "pytrends": PYTRENDS_AVAILABLE
        },
        "timestamp": datetime.now().isoformat()
    })

@app.route("/trend", methods=["POST"])
def trend():
    # ライブラリ可用性チェック
    if not PANDAS_AVAILABLE or not PYTRENDS_AVAILABLE:
        return jsonify({
            "error": "Required libraries not available",
            "details": {
                "pandas": PANDAS_AVAILABLE,
                "pytrends": PYTRENDS_AVAILABLE
            }
        }), 500
    
    try:
        # リクエストデータの取得と検証
        data = request.json
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400
        
        validation_result = validate_request(data)
        if not validation_result["valid"]:
            return jsonify({"error": validation_result["message"]}), 400
        
        keywords = data.get("keywords", [])
        timeframe = data.get("timeframe", "today 12-m")
        frequency = data.get("frequency", "weekly")
        geo = data.get("geo", "JP")
        
        logger.info(f"Processing request: {len(keywords)} keywords, {frequency} frequency, geo: {geo}")
        
        # Google Trendsデータ取得
        result = fetch_trends_with_enhanced_retry(keywords, timeframe, frequency, geo)
        
        if "error" in result:
            return jsonify(result), 429 if "rate limit" in result["error"].lower() else 500
        
        logger.info(f"Successfully processed {len(keywords)} keywords")
        return jsonify(result)
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Unexpected error: {error_msg}")
        return jsonify({
            "error": "Internal server error",
            "details": error_msg
        }), 500

def validate_request(data):
    """リクエストデータの詳細検証"""
    keywords = data.get("keywords", [])
    
    # キーワード検証
    if not keywords:
        return {"valid": False, "message": "Keywords are required"}
    
    if not isinstance(keywords, list):
        return {"valid": False, "message": "Keywords must be a list"}
    
    if len(keywords) > 4:
        return {"valid": False, "message": "Maximum 4 keywords allowed"}
    
    # 空のキーワードをチェック
    valid_keywords = [k for k in keywords if k and str(k).strip()]
    if len(valid_keywords) != len(keywords):
        return {"valid": False, "message": "Empty keywords are not allowed"}
    
    # 期間検証
    frequency = data.get("frequency", "weekly").lower()
    valid_frequencies = ["daily", "weekly", "monthly"]
    if frequency not in valid_frequencies:
        return {"valid": False, "message": f"Frequency must be one of: {valid_frequencies}"}
    
    # タイムフレーム検証
    timeframe = data.get("timeframe", "")
    if timeframe and not validate_timeframe(timeframe, frequency):
        return {"valid": False, "message": "Invalid timeframe for specified frequency"}
    
    return {"valid": True}

def validate_timeframe(timeframe, frequency):
    """タイムフレームと期間の整合性をチェック"""
    try:
        # "YYYY-MM-DD YYYY-MM-DD" 形式を想定
        if " " in timeframe:
            start_str, end_str = timeframe.split(" ", 1)
            start_date = datetime.strptime(start_str, "%Y-%m-%d")
            end_date = datetime.strptime(end_str, "%Y-%m-%d")
            
            days_diff = (end_date - start_date).days
            
            # Google Trendsの制約をチェック
            if frequency == "daily" and days_diff > 270:
                return False
            elif frequency == "weekly" and days_diff < 7:
                return False
            elif frequency == "monthly" and days_diff < 30:
                return False
                
        return True
    except:
        return True  # パースできない場合はGoogle Trendsに任せる

def fetch_trends_with_enhanced_retry(keywords, timeframe, frequency, geo):
    """強化されたリトライ機能付きデータ取得"""
    max_retries = 3
    base_delay = 10
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempt {attempt + 1}/{max_retries} for keywords: {keywords}")
            
            # 試行間の待機
            if attempt > 0:
                wait_time = base_delay * (2 ** (attempt - 1)) + random.uniform(5, 15)
                logger.info(f"Waiting {wait_time:.1f} seconds before retry...")
                time.sleep(wait_time)
            
            # 初期ランダム遅延
            initial_delay = random.uniform(3, 8)
            logger.info(f"Initial delay: {initial_delay:.1f} seconds")
            time.sleep(initial_delay)
            
            # pytrends インスタンス作成
            pytrends = TrendReq(
                hl="ja-JP", 
                tz=540,
                timeout=(10, 25),  # 接続タイムアウト, 読み取りタイムアウト
                retries=2,
                backoff_factor=0.1
            )
            
            # ペイロード構築
            build_payload_safely(pytrends, keywords, timeframe, geo)
            
            # データ取得
            df = pytrends.interest_over_time()
            
            if df.empty:
                if attempt == max_retries - 1:
                    return {"error": "No data available from Google Trends after all retries"}
                logger.warning(f"Empty data on attempt {attempt + 1}, retrying...")
                continue
            
            # データ処理
            processed_data = process_trends_dataframe(df, frequency)
            
            logger.info(f"Success on attempt {attempt + 1}")
            return {"data": processed_data}
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Attempt {attempt + 1} failed: {error_msg}")
            
            # Rate limitエラーの特別処理
            if any(keyword in error_msg.lower() for keyword in ["429", "rate limit", "too many requests"]):
                if attempt < max_retries - 1:
                    long_wait = 60 + random.uniform(30, 90)  # 1.5-2.5分
                    logger.info(f"Rate limit detected, waiting {long_wait:.1f} seconds...")
                    time.sleep(long_wait)
                    continue
                else:
                    return {"error": "Rate limit exceeded. Please try again later."}
            
            # その他のエラー
            if attempt == max_retries - 1:
                return {"error": f"Failed after {max_retries} attempts: {error_msg}"}
    
    return {"error": "Unknown error occurred"}

def build_payload_safely(pytrends, keywords, timeframe, geo):
    """安全なペイロード構築"""
    try:
        # 基本的なペイロード構築
        if geo and geo.strip() and geo.upper() != "NONE":
            pytrends.build_payload(
                kw_list=keywords,
                timeframe=timeframe,
                geo=geo.upper()
            )
        else:
            pytrends.build_payload(
                kw_list=keywords,
                timeframe=timeframe
            )
            
        logger.info("Payload built successfully")
        
    except Exception as e:
        logger.error(f"Payload build error: {e}")
        # フォールバック: より基本的なペイロード
        pytrends.build_payload(kw_list=keywords)

def process_trends_dataframe(df, frequency):
    """データフレーム処理（エラー耐性強化版）"""
    try:
        # isPartial列の削除
        if 'isPartial' in df.columns:
            df = df.drop(columns=['isPartial'])
        
        # 期間変換
        if frequency == "daily":
            df = convert_to_daily_safe(df)
        elif frequency == "monthly":
            df = convert_to_monthly_safe(df)
        # weeklyはそのまま（デフォルト）
        
        # インデックスを列に変換
        df_result = df.reset_index()
        
        # 日付列の処理
        date_column = find_date_column(df_result)
        if date_column and date_column != 'date':
            df_result = df_result.rename(columns={date_column: 'date'})
        
        # 日付フォーマット
        if 'date' in df_result.columns:
            df_result['date'] = format_dates_safely(df_result['date'])
        
        # NaN値を0で置換
        df_result = df_result.fillna(0)
        
        # 辞書形式で返却
        records = df_result.to_dict(orient="records")
        
        logger.info(f"Processed {len(records)} records with {len(df_result.columns)} columns")
        return records
        
    except Exception as e:
        logger.error(f"DataFrame processing error: {e}")
        # フォールバック: 最小限の処理
        try:
            df_simple = df.reset_index().fillna(0)
            return df_simple.to_dict(orient="records")
        except:
            raise Exception(f"Data processing failed: {e}")

def convert_to_daily_safe(df):
    """安全な日次変換"""
    try:
        if not isinstance(df.index, pd.DatetimeIndex):
            return df
        
        # 日次インデックス作成
        start_date = df.index.min()
        end_date = df.index.max()
        daily_index = pd.date_range(start=start_date, end=end_date, freq='D')
        
        # 線形補間
        df_reindexed = df.reindex(df.index.union(daily_index))
        df_interpolated = df_reindexed.interpolate(method='linear')
        df_daily = df_interpolated.reindex(daily_index)
        
        logger.info(f"Converted to daily: {len(df_daily)} days")
        return df_daily
        
    except Exception as e:
        logger.warning(f"Daily conversion failed: {e}, returning original data")
        return df

def convert_to_monthly_safe(df):
    """安全な月次変換"""
    try:
        if not isinstance(df.index, pd.DatetimeIndex):
            return df
        
        # 月次リサンプル
        df_monthly = df.resample('M').mean()
        df_monthly.index = df_monthly.index.to_period('M').to_timestamp()
        
        logger.info(f"Converted to monthly: {len(df_monthly)} months")
        return df_monthly
        
    except Exception as e:
        logger.warning(f"Monthly conversion failed: {e}, returning original data")
        return df

def find_date_column(df):
    """日付列を特定"""
    for col in df.columns:
        if any(word in col.lower() for word in ['date', 'time', 'index']):
            return col
        
        # データ型チェック
        try:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                return col
        except:
            continue
    
    return None

def format_dates_safely(date_series):
    """安全な日付フォーマット"""
    try:
        # datetime型に変換
        date_series = pd.to_datetime(date_series)
        # 文字列フォーマット
        return date_series.dt.strftime('%Y-%m-%d')
    except Exception as e:
        logger.warning(f"Date formatting failed: {e}, returning original")
        return date_series

# エラーハンドラー
@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(405)
def method_not_allowed(error):
    return jsonify({"error": "Method not allowed"}), 405

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal server error"}), 500

if __name__ == "__main__":
    logger.info("Starting Google Trends API server (Smart GAS optimized version)...")
    app.run(host="0.0.0.0", port=10000, debug=False)
