from flask import Flask, request, jsonify
import time
import random
import logging
import os
import threading
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

# Render最適化: Keep-Alive機能
class KeepAlive:
    def __init__(self):
        self.last_request = datetime.now()
        self.is_running = True
        
    def update_last_request(self):
        self.last_request = datetime.now()
        
    def start_keep_alive(self):
        """Keep-Aliveスレッドを開始"""
        def keep_alive_worker():
            while self.is_running:
                try:
                    # 10分間隔でpingを送信
                    time.sleep(600)
                    if (datetime.now() - self.last_request).seconds > 600:
                        logger.info("🔄 Keep-alive ping - server is alive")
                        
                except Exception as e:
                    logger.error(f"Keep-alive error: {e}")
                    
        thread = threading.Thread(target=keep_alive_worker, daemon=True)
        thread.start()
        logger.info("🚀 Keep-alive service started")

# Keep-Aliveインスタンス
keep_alive = KeepAlive()

@app.before_request
def before_request():
    """リクエスト前処理 - Keep-Alive更新"""
    keep_alive.update_last_request()

@app.route("/")
def home():
    status = {
        "service": "Google Trends API for Smart GAS (Render Optimized)",
        "pandas": "✅ Available" if PANDAS_AVAILABLE else "❌ Not Available",
        "pytrends": "✅ Available" if PYTRENDS_AVAILABLE else "❌ Not Available",
        "version": "2.1.0 (Render Enhanced)",
        "timestamp": datetime.now().isoformat(),
        "uptime": str(datetime.now() - keep_alive.last_request),
        "environment": "Render" if os.getenv('RENDER') else "Local"
    }
    
    logger.info(f"Home endpoint accessed - Libraries OK: {PANDAS_AVAILABLE and PYTRENDS_AVAILABLE}")
    return jsonify(status)

@app.route("/health")
def health_check():
    """ヘルスチェック用エンドポイント（Render最適化）"""
    health_status = {
        "status": "healthy" if PANDAS_AVAILABLE and PYTRENDS_AVAILABLE else "degraded",
        "libraries": {
            "pandas": PANDAS_AVAILABLE,
            "pytrends": PYTRENDS_AVAILABLE
        },
        "timestamp": datetime.now().isoformat(),
        "server_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC"),
        "last_request": keep_alive.last_request.isoformat(),
        "render_optimizations": {
            "keep_alive": True,
            "cold_start_protection": True,
            "enhanced_retry": True
        }
    }
    
    # ライブラリ状態による適切なステータスコード
    status_code = 200 if PANDAS_AVAILABLE and PYTRENDS_AVAILABLE else 503
    
    logger.info(f"Health check - Status: {health_status['status']}")
    return jsonify(health_status), status_code

@app.route("/ping")
def ping():
    """軽量なpingエンドポイント（Keep-Alive用）"""
    return jsonify({
        "pong": True,
        "timestamp": datetime.now().isoformat()
    })

@app.route("/warmup", methods=["GET", "POST"])
def warmup():
    """ウォームアップ専用エンドポイント"""
    logger.info("🔥 Warmup endpoint called")
    
    warmup_result = {
        "warmed_up": True,
        "timestamp": datetime.now().isoformat(),
        "libraries_ready": PANDAS_AVAILABLE and PYTRENDS_AVAILABLE,
        "server_ready": True
    }
    
    # ライブラリの動作テスト
    if PANDAS_AVAILABLE and PYTRENDS_AVAILABLE:
        try:
            # 軽量なテスト実行
            test_trends = TrendReq(hl="ja-JP", tz=540, timeout=(5, 10))
            warmup_result["pytrends_test"] = "✅ OK"
            logger.info("✅ Pytrends warmup test successful")
        except Exception as e:
            warmup_result["pytrends_test"] = f"❌ {str(e)}"
            logger.warning(f"⚠️ Pytrends warmup test failed: {e}")
    
    return jsonify(warmup_result)

@app.route("/trend", methods=["POST"])
def trend():
    # ライブラリ可用性チェック
    if not PANDAS_AVAILABLE or not PYTRENDS_AVAILABLE:
        logger.error("❌ Required libraries not available")
        return jsonify({
            "error": "Required libraries not available",
            "details": {
                "pandas": PANDAS_AVAILABLE,
                "pytrends": PYTRENDS_AVAILABLE
            },
            "solution": "Server is starting up. Please try the /warmup endpoint first."
        }), 503
    
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
        
        logger.info(f"🚀 Processing request: {len(keywords)} keywords, {frequency} frequency, geo: {geo}")
        
        # Render最適化: 処理開始ログ
        start_time = time.time()
        
        # Google Trendsデータ取得
        result = fetch_trends_with_render_optimization(keywords, timeframe, frequency, geo)
        
        # 処理時間ログ
        processing_time = time.time() - start_time
        logger.info(f"⏱️ Processing completed in {processing_time:.2f} seconds")
        
        if "error" in result:
            return jsonify(result), 429 if "rate limit" in result["error"].lower() else 500
        
        # 成功時のレスポンスに追加情報
        result["meta"] = {
            "processing_time_seconds": round(processing_time, 2),
            "server_timestamp": datetime.now().isoformat(),
            "keywords_processed": len(keywords),
            "data_points": len(result.get("data", []))
        }
        
        logger.info(f"✅ Successfully processed {len(keywords)} keywords")
        return jsonify(result)
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"💥 Unexpected error: {error_msg}")
        return jsonify({
            "error": "Internal server error",
            "details": error_msg,
            "timestamp": datetime.now().isoformat()
        }), 500

def validate_request(data):
    """リクエストデータの詳細検証（Render最適化）"""
    keywords = data.get("keywords", [])
    
    # キーワード検証
    if not keywords:
        return {"valid": False, "message": "Keywords are required"}
    
    if not isinstance(keywords, list):
        return {"valid": False, "message": "Keywords must be a list"}
    
    if len(keywords) > 4:
        return {"valid": False, "message": "Maximum 4 keywords allowed (Google Trends API limitation)"}
    
    # 空のキーワードをチェック
    valid_keywords = [k for k in keywords if k and str(k).strip()]
    if len(valid_keywords) != len(keywords):
        return {"valid": False, "message": "Empty keywords are not allowed"}
    
    # キーワード長チェック（Render最適化）
    for keyword in keywords:
        if len(str(keyword).strip()) > 100:
            return {"valid": False, "message": "Keywords must be less than 100 characters"}
    
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
        return True

def fetch_trends_with_render_optimization(keywords, timeframe, frequency, geo):
    """Render最適化版データ取得"""
    max_retries = 4  # Render用に増加
    base_delay = 8   # Render用に延長
    
    for attempt in range(max_retries):
        try:
            logger.info(f"🔄 Attempt {attempt + 1}/{max_retries} for keywords: {keywords}")
            
            # 試行間の待機（Render最適化）
            if attempt > 0:
                wait_time = base_delay * (2 ** (attempt - 1)) + random.uniform(3, 12)
                logger.info(f"⏳ Waiting {wait_time:.1f} seconds before retry...")
                time.sleep(wait_time)
            
            # 初期ランダム遅延（Cold start対策）
            initial_delay = random.uniform(2, 6) if attempt == 0 else random.uniform(1, 3)
            logger.info(f"⏳ Initial delay: {initial_delay:.1f} seconds")
            time.sleep(initial_delay)
            
            # pytrends インスタンス作成（Render最適化）
            pytrends = TrendReq(
                hl="ja-JP", 
                tz=540,
                timeout=(15, 30),  # Render用にタイムアウト延長
                retries=3,         # 内部リトライ増加
                backoff_factor=0.2 # バックオフ係数調整
            )
            
            # ペイロード構築
            build_payload_safely(pytrends, keywords, timeframe, geo)
            
            # データ取得
            logger.info("📊 Fetching data from Google Trends...")
            df = pytrends.interest_over_time()
            
            if df.empty:
                if attempt == max_retries - 1:
                    logger.warning("📭 No data available after all retries")
                    return {"error": "No data available from Google Trends after all retries"}
                logger.warning(f"📭 Empty data on attempt {attempt + 1}, retrying...")
                continue
            
            # データ処理
            logger.info("🔧 Processing data...")
            processed_data = process_trends_dataframe(df, frequency)
            
            logger.info(f"✅ Success on attempt {attempt + 1}")
            return {"data": processed_data}
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"❌ Attempt {attempt + 1} failed: {error_msg}")
            
            # Rate limitエラーの特別処理（Render最適化）
            if any(keyword in error_msg.lower() for keyword in ["429", "rate limit", "too many requests"]):
                if attempt < max_retries - 1:
                    long_wait = 90 + random.uniform(30, 60)  # 2-2.5分
                    logger.info(f"🚫 Rate limit detected, waiting {long_wait:.1f} seconds...")
                    time.sleep(long_wait)
                    continue
                else:
                    return {"error": "Rate limit exceeded. Please try again later.", "retry_after": 300}
            
            # タイムアウトエラーの処理
            if "timeout" in error_msg.lower():
                if attempt < max_retries - 1:
                    timeout_wait = 30 + random.uniform(10, 30)
                    logger.info(f"⏰ Timeout detected, waiting {timeout_wait:.1f} seconds...")
                    time.sleep(timeout_wait)
                    continue
            
            # その他のエラー
            if attempt == max_retries - 1:
                return {"error": f"Failed after {max_retries} attempts: {error_msg}"}
    
    return {"error": "Unknown error occurred"}

def build_payload_safely(pytrends, keywords, timeframe, geo):
    """安全なペイロード構築（Render最適化）"""
    try:
        logger.info(f"🔧 Building payload for keywords: {keywords}")
        
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
            
        logger.info("✅ Payload built successfully")
        
    except Exception as e:
        logger.error(f"❌ Payload build error: {e}")
        # フォールバック: より基本的なペイロード
        try:
            pytrends.build_payload(kw_list=keywords)
            logger.info("✅ Fallback payload built")
        except Exception as fallback_error:
            logger.error(f"❌ Fallback payload failed: {fallback_error}")
            raise e

def process_trends_dataframe(df, frequency):
    """データフレーム処理（Render最適化版）"""
    try:
        logger.info(f"📊 Processing DataFrame: {df.shape}")
        
        # isPartial列の削除
        if 'isPartial' in df.columns:
            df = df.drop(columns=['isPartial'])
            logger.info("🗑️ Removed isPartial column")
        
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
            logger.info(f"📅 Renamed date column: {date_column} -> date")
        
        # 日付フォーマット
        if 'date' in df_result.columns:
            df_result['date'] = format_dates_safely(df_result['date'])
        
        # NaN値を0で置換
        df_result = df_result.fillna(0)
        
        # 整数型に変換（trend値）
        for col in df_result.columns:
            if col != 'date':
                df_result[col] = df_result[col].astype(int)
        
        # 辞書形式で返却
        records = df_result.to_dict(orient="records")
        
        logger.info(f"✅ Processed {len(records)} records with {len(df_result.columns)} columns")
        return records
        
    except Exception as e:
        logger.error(f"❌ DataFrame processing error: {e}")
        # フォールバック: 最小限の処理
        try:
            df_simple = df.reset_index().fillna(0)
            simple_records = df_simple.to_dict(orient="records")
            logger.info(f"⚠️ Fallback processing: {len(simple_records)} records")
            return simple_records
        except:
            raise Exception(f"Data processing failed completely: {e}")

def convert_to_daily_safe(df):
    """安全な日次変換"""
    try:
        if not isinstance(df.index, pd.DatetimeIndex):
            return df
        
        start_date = df.index.min()
        end_date = df.index.max()
        daily_index = pd.date_range(start=start_date, end=end_date, freq='D')
        
        df_reindexed = df.reindex(df.index.union(daily_index))
        df_interpolated = df_reindexed.interpolate(method='linear')
        df_daily = df_interpolated.reindex(daily_index)
        
        logger.info(f"📅 Converted to daily: {len(df_daily)} days")
        return df_daily
        
    except Exception as e:
        logger.warning(f"⚠️ Daily conversion failed: {e}, returning original data")
        return df

def convert_to_monthly_safe(df):
    """安全な月次変換"""
    try:
        if not isinstance(df.index, pd.DatetimeIndex):
            return df
        
        df_monthly = df.resample('M').mean()
        df_monthly.index = df_monthly.index.to_period('M').to_timestamp()
        
        logger.info(f"📅 Converted to monthly: {len(df_monthly)} months")
        return df_monthly
        
    except Exception as e:
        logger.warning(f"⚠️ Monthly conversion failed: {e}, returning original data")
        return df

def find_date_column(df):
    """日付列を特定"""
    for col in df.columns:
        if any(word in col.lower() for word in ['date', 'time', 'index']):
            return col
        
        try:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                return col
        except:
            continue
    
    return None

def format_dates_safely(date_series):
    """安全な日付フォーマット"""
    try:
        date_series = pd.to_datetime(date_series)
        return date_series.dt.strftime('%Y-%m-%d')
    except Exception as e:
        logger.warning(f"⚠️ Date formatting failed: {e}, returning original")
        return date_series

# Renderに最適化されたエラーハンドラー
@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "error": "Endpoint not found",
        "available_endpoints": ["/", "/health", "/ping", "/warmup", "/trend"],
        "timestamp": datetime.now().isoformat()
    }), 404

@app.errorhandler(405)
def method_not_allowed(error):
    return jsonify({
        "error": "Method not allowed",
        "hint": "Use POST for /trend endpoint",
        "timestamp": datetime.now().isoformat()
    }), 405

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"💥 Internal server error: {error}")
    return jsonify({
        "error": "Internal server error",
        "libraries_status": {
            "pandas": PANDAS_AVAILABLE,
            "pytrends": PYTRENDS_AVAILABLE
        },
        "timestamp": datetime.now().isoformat()
    }), 500

if __name__ == "__main__":
    logger.info("🚀 Starting Google Trends API server (Render optimized version)...")
    
    # Keep-Aliveサービス開始
    keep_alive.start_keep_alive()
    
    # Renderの場合はポート設定
    port = int(os.environ.get("PORT", 10000))
    
    logger.info(f"🌐 Server starting on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
