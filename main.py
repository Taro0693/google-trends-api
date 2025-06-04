from flask import Flask, request, jsonify
import time
import random

app = Flask(__name__)

# 段階的インポート（エラー時にも動作を継続）
PANDAS_AVAILABLE = False
PYTRENDS_AVAILABLE = False

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
    print("✅ Pandas imported successfully")
except Exception as e:
    print(f"❌ Pandas import failed: {e}")

try:
    from pytrends.request import TrendReq
    PYTRENDS_AVAILABLE = True
    print("✅ Pytrends imported successfully")
except Exception as e:
    print(f"❌ Pytrends import failed: {e}")

@app.route("/")
def home():
    status = {
        "pandas": "✅" if PANDAS_AVAILABLE else "❌",
        "pytrends": "✅" if PYTRENDS_AVAILABLE else "❌"
    }
    return f"Google Trends API Status - Pandas: {status['pandas']} Pytrends: {status['pytrends']}"

@app.route("/trend", methods=["POST"])
def trend():
    # 必要なライブラリがない場合はエラー
    if not PANDAS_AVAILABLE or not PYTRENDS_AVAILABLE:
        return jsonify({
            "error": "Required libraries not available",
            "pandas": PANDAS_AVAILABLE,
            "pytrends": PYTRENDS_AVAILABLE
        }), 500
    
    try:
        data = request.json
        keywords = data.get("keywords", [])
        timeframe = data.get("timeframe", "today 12-m")
        frequency = data.get("frequency", "weekly")
        geo = data.get("geo", "")
        
        # バリデーション
        if not keywords or len(keywords) < 2:
            return jsonify({"error": "At least 2 keywords required"}), 400
        
        if len(keywords) > 4:
            return jsonify({
                "error": "Maximum 4 keywords supported",
                "reason": "To avoid Google Trends rate limits"
            }), 400
        
        # データ取得
        result = fetch_google_trends(keywords, timeframe, frequency, geo)
        return jsonify(result)
        
    except Exception as e:
        return jsonify({"error": f"Request processing error: {str(e)}"}), 500

def fetch_google_trends(keywords, timeframe, frequency, geo):
    """Google Trendsデータ取得（エラー耐性最優先）"""
    try:
        # 初期遅延
        time.sleep(random.uniform(2, 5))
        
        # pytrendsインスタンス作成（最小限のパラメータ）
        pytrends = TrendReq(hl="ja-JP", tz=540)
        
        # データ取得
        if geo and geo.strip():
            pytrends.build_payload(keywords, timeframe=timeframe, geo=geo)
        else:
            pytrends.build_payload(keywords, timeframe=timeframe)
        
        # データフレーム取得
        df = pytrends.interest_over_time()
        
        if df.empty:
            return {"error": "No data available from Google Trends"}
        
        # isPartial列削除
        if 'isPartial' in df.columns:
            df = df.drop(columns=['isPartial'])
        
        # データ処理（エラー耐性重視）
        result_df = process_dataframe(df, frequency)
        
        # 辞書形式で返す
        data_records = result_df.fillna(0).to_dict(orient="records")
        
        return {"data": data_records}
        
    except Exception as e:
        error_msg = str(e)
        if "429" in error_msg or "Too Many Requests" in error_msg:
            return {"error": "Rate limit exceeded. Please wait and try again."}
        else:
            return {"error": f"Google Trends API error: {error_msg}"}

def process_dataframe(df, frequency):
    """データフレーム処理（安全版）"""
    try:
        # インデックスを列に変換
        df_result = df.reset_index()
        
        # 日付列の特定と名前変更
        date_column = None
        for col in df_result.columns:
            if any(word in col.lower() for word in ['date', 'time', 'index']) or pd.api.types.is_datetime64_any_dtype(df_result[col]):
                date_column = col
                break
        
        if date_column and date_column != 'date':
            df_result = df_result.rename(columns={date_column: 'date'})
        
        # 日付フォーマット（エラーが発生しても継続）
        if 'date' in df_result.columns:
            try:
                df_result['date'] = pd.to_datetime(df_result['date']).dt.strftime('%Y-%m-%d')
            except:
                pass  # 日付変換失敗時はそのまま
        
        # frequency処理（簡単な処理のみ）
        if frequency == "daily":
            df_result = interpolate_daily(df_result)
        elif frequency == "monthly":
            df_result = aggregate_monthly(df_result)
        
        return df_result
        
    except Exception as e:
        print(f"DataFrame processing error: {e}")
        return df.reset_index()  # 最低限の処理

def interpolate_daily(df):
    """日次補間（簡易版）"""
    try:
        if 'date' not in df.columns:
            return df
        
        # 日付列をDatetimeIndexに設定
        df['date'] = pd.to_datetime(df['date'])
        df_indexed = df.set_index('date')
        
        # 日次リサンプリング
        start_date = df_indexed.index.min()
        end_date = df_indexed.index.max()
        daily_index = pd.date_range(start=start_date, end=end_date, freq='D')
        
        # 補間
        df_reindexed = df_indexed.reindex(daily_index)
        df_interpolated = df_reindexed.interpolate(method='linear')
        
        # インデックスを列に戻す
        return df_interpolated.reset_index().rename(columns={'index': 'date'})
        
    except Exception as e:
        print(f"Daily interpolation error: {e}")
        return df

def aggregate_monthly(df):
    """月次集約（簡易版）"""
    try:
        if 'date' not in df.columns:
            return df
        
        # 日付列をDatetimeIndexに設定
        df['date'] = pd.to_datetime(df['date'])
        df_indexed = df.set_index('date')
        
        # 月次リサンプリング
        df_monthly = df_indexed.resample('M').mean()
        
        # インデックスを列に戻す
        return df_monthly.reset_index()
        
    except Exception as e:
        print(f"Monthly aggregation error: {e}")
        return df

if __name__ == "__main__":
    print("Starting Google Trends API server...")
    app.run(host="0.0.0.0", port=10000, debug=False)
