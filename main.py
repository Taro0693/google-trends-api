from flask import Flask, request, jsonify
import pandas as pd
import time
import random

# pytrends のインポートをtry-catchで保護
try:
    from pytrends.request import TrendReq
    PYTRENDS_AVAILABLE = True
except ImportError as e:
    print(f"Pytrends import error: {e}")
    PYTRENDS_AVAILABLE = False

app = Flask(__name__)

@app.route("/")
def home():
    status = "✅ Available" if PYTRENDS_AVAILABLE else "❌ Import Error"
    return f"Google Trends API is running! Pytrends: {status}"

@app.route("/trend", methods=["POST"])
def trend():
    if not PYTRENDS_AVAILABLE:
        return jsonify({"error": "Pytrends library not available"}), 500
    
    try:
        data = request.json
        keywords = data.get("keywords", [])
        timeframe = data.get("timeframe", "today 12-m")
        frequency = data.get("frequency", "weekly")
        geo = data.get("geo", "")
        
        if not keywords or len(keywords) < 2:
            return jsonify({"error": "At least 2 keywords required"}), 400
        
        # 4個以下の場合のみ処理（429エラー回避）
        if len(keywords) <= 4:
            return get_trends_data(keywords, timeframe, frequency, geo)
        else:
            return jsonify({"error": "Maximum 4 keywords supported to avoid rate limits"}), 400
        
    except Exception as e:
        error_msg = str(e)
        return jsonify({"error": f"Server error: {error_msg}"}), 500

def get_trends_data(keywords, timeframe, frequency, geo):
    """トレンドデータ取得（シンプル版）"""
    try:
        # 初期遅延
        time.sleep(random.uniform(2, 5))
        
        # pytrends インスタンス作成（最小限のパラメータ）
        pytrends = TrendReq(hl="ja-JP", tz=540)
        
        # データ取得
        if geo:
            pytrends.build_payload(keywords, timeframe=timeframe, geo=geo)
        else:
            pytrends.build_payload(keywords, timeframe=timeframe)
        
        df = pytrends.interest_over_time()
        
        if df.empty:
            return jsonify({"error": "No data available from Google Trends"}), 404
        
        # isPartial列削除
        if 'isPartial' in df.columns:
            df = df.drop(columns=['isPartial'])
        
        # frequency対応
        if frequency == "daily":
            df = convert_to_daily(df)
        elif frequency == "monthly":
            df = convert_to_monthly(df)
        
        # データ整理
        df_result = df.reset_index()
        
        # 日付列の処理
        if 'date' not in df_result.columns:
            # インデックスが日付の場合
            if hasattr(df_result, 'index') and len(df_result.columns) > 0:
                first_col = df_result.columns[0]
                if 'date' in first_col.lower() or first_col == 'index':
                    df_result = df_result.rename(columns={first_col: 'date'})
        
        # 日付フォーマット
        if 'date' in df_result.columns:
            try:
                df_result['date'] = pd.to_datetime(df_result['date']).dt.strftime('%Y-%m-%d')
            except:
                pass  # 日付変換に失敗した場合はそのまま
        
        # データを辞書形式で返す
        result_data = df_result.fillna(0).to_dict(orient="records")
        
        return jsonify({"data": result_data})
        
    except Exception as e:
        error_msg = str(e)
        if "429" in error_msg:
            return jsonify({"error": "Rate limit exceeded. Please try again later."}), 429
        else:
            return jsonify({"error": f"Data fetch error: {error_msg}"}), 500

def convert_to_daily(df):
    """日次変換（簡易版）"""
    try:
        if not isinstance(df.index, pd.DatetimeIndex):
            return df
        
        # 日次インデックス作成
        start_date = df.index[0]
        end_date = df.index[-1]
        daily_index = pd.date_range(start=start_date, end=end_date, freq='D')
        
        # 線形補間
        df_reindexed = df.reindex(df.index.union(daily_index))
        df_interpolated = df_reindexed.interpolate(method='linear')
        df_daily = df_interpolated.reindex(daily_index)
        
        return df_daily
    except:
        return df

def convert_to_monthly(df):
    """月次変換（簡易版）"""
    try:
        if not isinstance(df.index, pd.DatetimeIndex):
            return df
        
        # 月次リサンプル
        df_monthly = df.resample('M').mean()
        df_monthly.index = df_monthly.index.to_period('M').to_timestamp()
        
        return df_monthly
    except:
        return df

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000, debug=False)
