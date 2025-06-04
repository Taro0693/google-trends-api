from flask import Flask, request, jsonify
from pytrends.request import TrendReq
import pandas as pd
import time  # ← 追加

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
        freq = data.get("frequency", "weekly")

        if not keywords or len(keywords) < 2:
            return jsonify({"error": "At least 2 keywords required"}), 400

        pivot = keywords[0]
        group1 = keywords[:5]
        group2 = [pivot] + keywords[5:]

        pytrends = TrendReq(hl="ja-JP", tz=540)

        # --- グループ1 ---
        pytrends.build_payload(group1, timeframe=timeframe)
        df1 = pytrends.interest_over_time()

        # ✅ ここで待機を入れる（429対策）
        time.sleep(15)

        # --- グループ2 ---
        pytrends.build_payload(group2, timeframe=timeframe)
        df2 = pytrends.interest_over_time()

        # --- スケーリング処理 ---
        scale = df1[pivot].mean() / df2[pivot].mean()
        df2_scaled = df2.copy()
        for col in df2.columns:
            if col != pivot:
                df2_scaled[col] = df2[col] * scale

        df_final = pd.concat([
            df1.drop(columns=["isPartial", pivot]),
            df2_scaled.drop(columns=["isPartial", pivot])
        ], axis=1)

        df_final.reset_index(inplace=True)
        return jsonify({"data": df_final.fillna(0).to_dict(orient="records")})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
