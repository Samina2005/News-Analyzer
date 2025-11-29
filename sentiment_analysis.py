import os
import pandas as pd
from textblob import TextBlob

csv_files = [
    "ndtv_general_news.csv",
    "ndtv_cricket_news.csv",
    "ndtv_education_news.csv",
    "ndtv_health_news.csv",
    "ndtv_entertainment_news.csv",
    "ndtv_science_news.csv",
    "ndtv_world_news.csv"
]

def get_sentiment(text):
    try:
        analysis = TextBlob(str(text))
        polarity = analysis.sentiment.polarity
        if polarity > 0:
            return "Positive"
        elif polarity < 0:
            return "Negative"
        else:
            return "Neutral"
    except Exception:
        return "Unknown"

def analyze_sentiments_in_csv(csv_path):
    if not os.path.exists(csv_path):
        print(f"[ERROR] File not found: {csv_path}")
        return

    try:
        df = pd.read_csv(csv_path)

        if 'content' not in df.columns:
            print(f"[SKIPPED] No 'content' column in {csv_path}")
            return

        # Always (re)calculate the sentiment results
        df['analysis_result'] = df['content'].apply(get_sentiment)

        df.to_csv(csv_path, index=False, encoding='utf-8')
        print(f"[UPDATED] Sentiment analysis saved to {csv_path}")
    except Exception as e:
        print(f"[ERROR] Processing {csv_path}: {str(e)}")

def sentiment_analysis():
    for csv_file in csv_files:
        analyze_sentiments_in_csv(csv_file)

if __name__ == "__main__":
    sentiment_analysis()
