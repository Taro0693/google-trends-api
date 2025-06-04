from flask import Flask, request, jsonify
from pytrends.request import TrendReq
import pandas as pd
import time
import random

app = Flask(__name__)

@app.route("/")
def home():
    return "✅ Google Trends API is running!"

@app.route("/trend", methods=["POST"])
def trend():
    try:
        data = request.json
        keywords = data.get("keywords", [])
        timeframe = data.get("timeframe", "today 12-m")
        frequency = data.get("frequency", "weekly")
        geo = data.get("geo", "")
        
        if not keywords or len(keywords) < 2:
            return jsonify({"error": "At least 2 keywords required"}), 400
        
        # キーワードが4個以下の場合は単純処理
        if len(keywords) <= 4:
            return get_simple_trends(keywords, timeframe, frequency, geo)
        
        # 5個以上の場合のスケーリング処理（429エラー対策強化）
        return get_scaled_trends(keywords, timeframe, frequency, geo)
        
    except Exception as e:
        error_msg = str(e)
        if "429" in error_msg or "Too Many Requests" in error_msg:
            return jsonify({"error": "Rate limit exceeded. Please try again later."}), 429
        elif "quota" in error_msg.lower():
            return jsonify({"error": "API quota exceeded"}), 429
        else:
            return jsonify({"error": f"Unexpected error: {error_msg}"}), 500

def get_simple_trends(keywords, timeframe, frequency, geo):
    """4個以下のキーワード用シンプル処理"""
    try:
        # ランダム遅延追加（429エラー対策）
        time.sleep(random.uniform(1, 3))
        
        pytrends = TrendReq(hl="ja-JP", tz=540, retries=2, backoff_factor=0.1)
        
        if geo:
            pytrends.build_payload(keywords, timeframe=timeframe, geo=geo)
        else:
            pytrends.build_payload(keywords, timeframe=timeframe)
        
        df = pytrends.interest_over_time()
        
        if df.empty:
            return jsonify({"error": "No data available"}), 404
        
        # isPartial列削除
        if 'isPartial' in df.columns:
            df = df.drop(columns=['isPartial'])
        
        # frequency対応
        if frequency == "daily":
            df = interpolate_to_daily(df)
        elif frequency == "monthly":
            df = aggregate_to_monthly(df)
        
        # データフレーム整理
        df = df.reset_index()
        if 'date' not in df.columns and df.columns[0] in ['index', 'Date']:
            df = df.rename(columns={df.columns[0]: 'date'})
        
        # 日付フォーマット統一
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        
        return jsonify({"data": df.fillna(0).to_dict(orient="records")})
        
    except Exception as e:
        return jsonify({"error": f"Simple trends error: {str(e)}"}), 500

def get_scaled_trends(keywords, timeframe, frequency, geo):
    """5個以上のキーワード用スケーリング処理（429エラー対策強化）"""
    try:
        pivot = keywords[0]
        
        # 4個ずつのグループに分割（共通キーワード含む）
        group1 = keywords[:4]  # 最初の4個
        group2 = [pivot] + keywords[4:]  # 共通キーワード + 残り
        
        # グループ1のデータ取得
        print(f"Group 1: {group1}")
        df1 = fetch_trends_with_retry(group1, timeframe, geo, retry_count=3)
        
        if df1.empty:
            return jsonify({"error": "No data available for group 1"}), 404
        
        # 長い待機時間（429エラー対策）
        wait_time = random.uniform(45, 75)  # 45-75秒のランダム待機
        print(f"Waiting {wait_time:.1f} seconds before second request...")
        time.sleep(wait_time)
        
        # グループ2のデータ取得
        print(f"Group 2: {group2}")
        df2 = fetch_trends_with_retry(group2, timeframe, geo, retry_count=3)
        
        if df2.empty:
            return jsonify({"error": "No data available for group 2"}), 404
        
        # スケーリング処理
        df_final = scale_and_merge_data(df1, df2, pivot)
        
        # frequency対応
        if frequency == "daily":
            df_final = interpolate_to_daily(df_final)
        elif frequency == "monthly":
            df_final = aggregate_to_monthly(df_final)
        
        # データフレーム整理
        df_final = df_final.reset_index()
        if 'date' not in df_final.columns and df_final.columns[0] in ['index', 'Date']:
            df_final = df_final.rename(columns={df_final.columns[0]: 'date'})
        
        # 日付フォーマット統一
        if 'date' in df_final.columns:
            df_final['date'] = pd.to_datetime(df_final['date']).dt.strftime('%Y-%m-%d')
        
        return jsonify({"data": df_final.fillna(0).to_dict(orient="records")})
        
    except Exception as e:
        return jsonify({"error": f"Scaled trends error: {str(e)}"}), 500

def fetch_trends_with_retry(keywords, timeframe, geo, retry_count=3):
    """リトライ機能付きトレンド取得"""
    for attempt in range(retry_count):
        try:
            # 初回以外は長い待機
            if attempt > 0:
                wait_time = random.uniform(60, 120)  # 1-2分待機
                print(f"Retry attempt {attempt + 1}, waiting {wait_time:.1f} seconds...")
                time.sleep(wait_time)
            
            pytrends = TrendReq(hl="ja-JP", tz=540, retries=1, backoff_factor=0.5)
            
            if geo:
                pytrends.build_payload(keywords, timeframe=timeframe, geo=geo)
            else:
                pytrends.build_payload(keywords, timeframe=timeframe)
            
            df = pytrends.interest_over_time()
            
            if not df.empty:
                # isPartial列削除
                if 'isPartial' in df.columns:
                    df = df.drop(columns=['isPartial'])
                return df
            else:
                print(f"Empty data on attempt {attempt + 1}")
                
        except Exception as e:
            error_msg = str(e)
            print(f"Attempt {attempt + 1} failed: {error_msg}")
            
            if "429" in error_msg or "Too Many Requests" in error_msg:
                if attempt == retry_count - 1:  # 最後の試行
                    raise Exception("Rate limit exceeded after all retries")
                continue
            else:
                raise e
    
    raise Exception("Failed to fetch data after all retries")

def scale_and_merge_data(df1, df2, pivot):
    """データスケーリングとマージ"""
    pivot_mean1 = df1[pivot].mean()
    pivot_mean2 = df2[pivot].mean()
    
    if pivot_mean2 == 0:
        raise Exception("Cannot scale: pivot mean is zero in group 2")
    
    scale_factor = pivot_mean1 / pivot_mean2
    print(f"Scale factor: {scale_factor:.3f} (Group1 mean: {pivot_mean1:.1f}, Group2 mean: {pivot_mean2:.1f})")
    
    # group2のpivot以外をスケーリング
    df2_scaled = df2.copy()
    for col in df2.columns:
        if col != pivot:
            df2_scaled[col] = df2[col] * scale_factor
    
    # データ統合
    final_columns = {}
    
    # group1のデータを追加
    for col in df1.columns:
        final_columns[col] = df1[col]
    
    # group2のスケーリング済みデータを追加（pivot除く）
    for col in df2_scaled.columns:
        if col != pivot:
            final_columns[col] = df2_scaled[col]
    
    df_final = pd.DataFrame(final_columns, index=df1.index)
    return df_final

def interpolate_to_daily(df):
    """週次データを日次に線形補間"""
    if df.empty:
        return df
    
    try:
        if not isinstance(df.index, pd.DatetimeIndex):
            return df
        
        start_date = df.index[0]
        end_date = df.index[-1]
        daily_index = pd.date_range(start=start_date, end=end_date, freq='D')
        
        df_reindexed = df.reindex(df.index.union(daily_index))
        df_interpolated = df_reindexed.interpolate(method='linear')
        df_daily = df_interpolated.reindex(daily_index)
        
        return df_daily
        
    except Exception as e:
        print(f"Daily interpolation error: {e}")
        return df

def aggregate_to_monthly(df):
    """週次データを月次に集約"""
    if df.empty:
        return df
    
    try:
        if not isinstance(df.index, pd.DatetimeIndex):
            return df
        
        df_monthly = df.resample('M').mean()
        df_monthly.index = df_monthly.index.to_period('M').to_timestamp()
        
        return df_monthly
        
    except Exception as e:
        print(f"Monthly aggregation error: {e}")
        return df

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
