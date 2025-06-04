from flask import Flask, request, jsonify
from pytrends.request import TrendReq
import pandas as pd
import time

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
        frequency = data.get("frequency", "weekly")  # 新規追加
        geo = data.get("geo", "")  # 新規追加（空文字列は全世界）
        
        if not keywords or len(keywords) < 2:
            return jsonify({"error": "At least 2 keywords required"}), 400
        
        # キーワードが5個以下の場合は単純処理
        if len(keywords) <= 5:
            pytrends = TrendReq(hl="ja-JP", tz=540)
            
            # geo パラメータを追加
            if geo:
                pytrends.build_payload(keywords, timeframe=timeframe, geo=geo)
            else:
                pytrends.build_payload(keywords, timeframe=timeframe)
            
            df = pytrends.interest_over_time()
            
            if df.empty:
                return jsonify({"error": "No data available"}), 404
            
            # isPartial列のみ削除
            if 'isPartial' in df.columns:
                df = df.drop(columns=['isPartial'])
            
            # frequency対応（日次の場合は補間処理）
            if frequency == "daily":
                df = interpolate_to_daily(df)
            elif frequency == "monthly":
                df = aggregate_to_monthly(df)
            
            df.reset_index(inplace=True)
            return jsonify({"data": df.fillna(0).to_dict(orient="records")})
        
        # 6個以上の場合のスケーリング処理
        pivot = keywords[0]
        group1 = keywords[:5]
        group2 = [pivot] + keywords[5:]
        
        pytrends = TrendReq(hl="ja-JP", tz=540)
        
        # グループ1のデータ取得
        if geo:
            pytrends.build_payload(group1, timeframe=timeframe, geo=geo)
        else:
            pytrends.build_payload(group1, timeframe=timeframe)
        
        df1 = pytrends.interest_over_time()
        
        if df1.empty:
            return jsonify({"error": "No data available for group 1"}), 404
        
        # 待機（429エラー対策）
        time.sleep(30)  # 15秒から30秒に延長
        
        # グループ2のデータ取得
        if geo:
            pytrends.build_payload(group2, timeframe=timeframe, geo=geo)
        else:
            pytrends.build_payload(group2, timeframe=timeframe)
        
        df2 = pytrends.interest_over_time()
        
        if df2.empty:
            return jsonify({"error": "No data available for group 2"}), 404
        
        # スケーリング処理（改良版）
        pivot_mean1 = df1[pivot].mean()
        pivot_mean2 = df2[pivot].mean()
        
        # ゼロ除算防止
        if pivot_mean2 == 0:
            return jsonify({"error": "Cannot scale: pivot mean is zero in group 2"}), 400
        
        scale_factor = pivot_mean1 / pivot_mean2
        
        # group2のpivot以外の列をスケーリング
        df2_scaled = df2.copy()
        for col in df2.columns:
            if col not in [pivot, 'isPartial']:
                df2_scaled[col] = df2[col] * scale_factor
        
        # データ統合（pivotキーワードはgroup1のデータを使用）
        final_columns = {}
        
        # group1のデータを追加（isPartial除く）
        for col in df1.columns:
            if col != 'isPartial':
                final_columns[col] = df1[col]
        
        # group2のスケーリング済みデータを追加（pivotとisPartial除く）
        for col in df2_scaled.columns:
            if col not in [pivot, 'isPartial']:
                final_columns[col] = df2_scaled[col]
        
        df_final = pd.DataFrame(final_columns, index=df1.index)
        
        # frequency対応
        if frequency == "daily":
            df_final = interpolate_to_daily(df_final)
        elif frequency == "monthly":
            df_final = aggregate_to_monthly(df_final)
        
        df_final.reset_index(inplace=True)
        
        return jsonify({"data": df_final.fillna(0).to_dict(orient="records")})
        
    except Exception as e:
        error_msg = str(e)
        if "429" in error_msg or "Too Many Requests" in error_msg:
            return jsonify({"error": "Rate limit exceeded. Please try again later."}), 429
        elif "quota" in error_msg.lower():
            return jsonify({"error": "API quota exceeded"}), 429
        else:
            return jsonify({"error": f"Unexpected error: {error_msg}"}), 500

def interpolate_to_daily(df):
    """週次データを日次に線形補間"""
    if df.empty:
        return df
    
    # インデックスが日付であることを確認
    if not isinstance(df.index, pd.DatetimeIndex):
        return df
    
    # 日次インデックスを作成
    start_date = df.index[0]
    end_date = df.index[-1]
    daily_index = pd.date_range(start=start_date, end=end_date, freq='D')
    
    # 線形補間でリサンプリング
    df_daily = df.reindex(daily_index).interpolate(method='linear')
    
    return df_daily

def aggregate_to_monthly(df):
    """週次データを月次に集約"""
    if df.empty:
        return df
    
    # インデックスが日付であることを確認
    if not isinstance(df.index, pd.DatetimeIndex):
        return df
    
    # 月次で集約（平均値）
    df_monthly = df.resample('M').mean()
    
    # 月初の日付に変更
    df_monthly.index = df_monthly.index.to_period('M').to_timestamp()
    
    return df_monthly

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
