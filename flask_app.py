from flask import Flask, jsonify
from threading import Thread
import time
from flask_cors import CORS
from datetime import datetime as dt
import csv
import pandas as pd
from collections import defaultdict
import os
from Scraping import (
    getgeneralarticlendtv,
    geteduarticlendtv,
    gethealtharticlendtv,
    getcricketarticlendtv,
    getsciencearticlendtv,
    getworldarticlendtv,
    getenterarticlendtv,
    combine_category_csvs
)
from sentiment_analysis import sentiment_analysis


app = Flask(__name__)
CORS(app)


# Background scraping loop
def start_scraping_loop():
    while True:
        print("Starting scraping at", dt.now().strftime("%Y-%m-%d %H:%M:%S"))

        try:
            # Latest news from NDTV
            print("Scraping latest news from NDTV...")
            latest_data = getgeneralarticlendtv()
            print(
                f"Scraped {len(latest_data) if latest_data else 0} latest news articles"
            )

            # Education news from NDTV
            print("Scraping education news from NDTV...")
            edu_data = geteduarticlendtv()
            print(f"Scraped {len(edu_data) if edu_data else 0} education news articles")

            # Health news from NDTV
            print("Scraping health news from NDTV...")
            health_data = gethealtharticlendtv()
            print(
                f"Scraped {len(health_data) if health_data else 0} health news articles"
            )

            # Cricket news from NDTV
            print("Scraping cricket news from NDTV...")
            cricket_data = getcricketarticlendtv()
            print(
                f"Scraped {len(cricket_data) if cricket_data else 0} cricket news articles"
            )

            # Science news from NDTV
            print("Scraping science news from NDTV...")
            science_data = getsciencearticlendtv()
            print(
                f"Scraped {len(science_data) if science_data else 0} science news articles"
            )

            # World news from NDTV
            print("Scraping world news from NDTV...")
            world_data = getworldarticlendtv()
            print(f"Scraped {len(world_data) if world_data else 0} world news articles")

            # Entertainment news from NDTV
            print("Scraping entertainment news from NDTV...")
            enter_data = getenterarticlendtv()
            print(
                f"Scraped {len(enter_data) if enter_data else 0} entertainment news articles"
            )

            print("All scraping completed at", dt.now().strftime("%Y-%m-%d %H:%M:%S"))

            #combine category csvs
            combine_category_csvs()

            # Run sentiment analysis
            sentiment_analysis()

        except Exception as e:
            print(f"Error during scraping: {e}")

        print("Sleeping for 1 hour...\n")
        time.sleep(3600)  # 1 hour


def start_background_thread():
    thread = Thread(target=start_scraping_loop)
    thread.daemon = True  # Thread will exit when Flask exits
    thread.start()

# Route for General news from NDTV
@app.route("/api/generalndtv", methods=["GET"])
def fetch_latestndtv():
    try:
        with open("ndtv_general_news.csv", "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            articles = [row for row in reader]

        if not articles:
            return jsonify({"message": "No articles found."}), 404

        return jsonify(articles)

    except FileNotFoundError:
        return jsonify({"error": "CSV file not found."}), 500


# Route of education articles from Ndtv
@app.route("/api/edundtv", methods=["GET"])
def fetch_edundtv():
    try:
        with open("ndtv_education_news.csv", "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            articles = [row for row in reader]

        if not articles:
            return jsonify({"message": "No articles found."}), 404

        return jsonify(articles)

    except FileNotFoundError:
        return jsonify({"error": "CSV file not found."}), 500


# Route of health from Ndtv
@app.route("/api/healthndtv", methods=["GET"])
def fetch_healthndtv():
    try:
        with open("ndtv_health_news.csv", "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            articles = [row for row in reader]

        if not articles:
            return jsonify({"message": "No articles found."}), 404

        return jsonify(articles)

    except FileNotFoundError:
        return jsonify({"error": "CSV file not found."}), 500


# Route of cricket from Ndtv
@app.route("/api/cricketndtv", methods=["GET"])
def fetch_cricketndtv():
    try:
        with open("ndtv_cricket_news.csv", "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            articles = [row for row in reader]

        if not articles:
            return jsonify({"message": "No articles found."}), 404

        return jsonify(articles)

    except FileNotFoundError:
        return jsonify({"error": "CSV file not found."}), 500


# Route of science news from Ndtv
@app.route("/api/sciencendtv", methods=["GET"])
def fetch_sciencendtv():
    try:
        with open("ndtv_science_news.csv", "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            articles = [row for row in reader]

        if not articles:
            return jsonify({"message": "No articles found."}), 404

        return jsonify(articles)

    except FileNotFoundError:
        return jsonify({"error": "CSV file not found."}), 500


# Route of world news from Ndtv
@app.route("/api/worldndtv", methods=["GET"])
def fetch_worldndtv():
    try:
        with open("ndtv_world_news.csv", "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            articles = [row for row in reader]

        if not articles:
            return jsonify({"message": "No articles found."}), 404

        return jsonify(articles)

    except FileNotFoundError:
        return jsonify({"error": "CSV file not found."}), 500


# Route of entertainment news from Ndtv
@app.route("/api/enterndtv", methods=["GET"])
def fetch_enterndtv():
    try:
        with open("ndtv_entertainment_news.csv", "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            articles = [row for row in reader]

        if not articles:
            return jsonify({"message": "No articles found."}), 404

        return jsonify(articles)

    except FileNotFoundError:
        return jsonify({"error": "CSV file not found."}), 500


@app.route("/api/allndtv", methods=["GET"])
def fetch_all_ndtv():
    try:
        # Open the combined CSV file
        with open("ndtv_all_news.csv", "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            all_articles = [row for row in reader]

        if not all_articles:
            return jsonify({"message": "No articles found."}), 404

        return jsonify(all_articles)

    except FileNotFoundError:
        return jsonify({"error": "Combined CSV file not found."}), 500
    except Exception as e:
        return jsonify({"error": f"An error occurred: {e}"}), 500


@app.route("/api/sentiment-trends", methods=["GET"])
def fetch_sentiment_trends():
    try:
        # Open the combined CSV file
        df = pd.read_csv("ndtv_all_news.csv", encoding="utf-8")

        if df.empty:
            return jsonify({"message": "No articles found."}), 404

        print(f"Total articles in ndtv_all_news.csv: {len(df)}")
        print(f"Columns in CSV: {df.columns.tolist()}")

        # Check for missing values in key columns
        print(f"Missing values in datetime_posted: {df['datetime_posted'].isna().sum()}")
        print(f"Missing values in analysis_result: {df['analysis_result'].isna().sum()}")

        # Print sample of datetime values
        print("Sample datetime values:")
        for dt_str in df['datetime_posted'].dropna().sample(min(5, len(df))).tolist():
            print(f"  {dt_str}")

        # Preprocess datetime strings to handle IST timezone
        def clean_datetime(dt_str):
            if pd.isna(dt_str):
                return dt_str

            # Convert to string if not already
            dt_str = str(dt_str)

            # Remove IST and other timezone indicators
            if "IST" in dt_str:
                dt_str = dt_str.replace("IST", "").strip()

            # Handle common prefixes in NDTV dates
            prefixes = ["Updated:", "Published On:", "Posted on:"]
            for prefix in prefixes:
                if dt_str.startswith(prefix):
                    dt_str = dt_str[len(prefix):].strip()

            return dt_str

        # Clean datetime strings before parsing
        df['cleaned_datetime'] = df['datetime_posted'].apply(clean_datetime)

        # Try to parse dates with multiple formats
        def parse_date_with_formats(date_str):
            if pd.isna(date_str):
                return None

            formats = [
                '%B %d, %Y %I:%M %p',  # May 03, 2025 04:10 am
                '%B %d, %Y',           # May 03, 2025
                '%d %B %Y',            # 03 May 2025
                '%Y-%m-%d',            # 2025-05-03
                '%d/%m/%Y',            # 03/05/2025
                '%m/%d/%Y'             # 05/03/2025
            ]

            for fmt in formats:
                try:
                    return pd.to_datetime(date_str, format=fmt).date()
                except:
                    continue

            # If all formats fail, try pandas default parser
            try:
                return pd.to_datetime(date_str).date()
            except:
                return None

        # Apply custom date parsing
        df['date'] = df['cleaned_datetime'].apply(parse_date_with_formats)

        # Count valid dates
        valid_dates = df['date'].notna().sum()
        print(f"Successfully parsed {valid_dates} dates out of {len(df)} articles ({valid_dates/len(df)*100:.1f}%)")

        # Group by date and sentiment, count articles
        sentiment_counts = defaultdict(lambda: {'Positive': 0, 'Negative': 0, 'Neutral': 0})

        # Count of articles by sentiment
        sentiment_totals = {'Positive': 0, 'Negative': 0, 'Neutral': 0}

        for _, row in df.iterrows():
            # Skip rows where date parsing failed
            if pd.isna(row['date']):
                continue

            date_str = str(row['date'])
            sentiment = row['analysis_result']

            # Handle missing or unknown sentiment
            if pd.isna(sentiment) or sentiment == '' or sentiment == 'Unknown':
                sentiment = 'Neutral'

            # Ensure sentiment is one of our expected values
            if sentiment not in ['Positive', 'Negative', 'Neutral']:
                sentiment = 'Neutral'

            sentiment_counts[date_str][sentiment] += 1
            sentiment_totals[sentiment] += 1

        # Print overall sentiment distribution
        total_processed = sum(sentiment_totals.values())
        print(f"\nOverall sentiment distribution (from {total_processed} articles with valid dates):")
        for sentiment, count in sentiment_totals.items():
            percentage = (count / total_processed * 100) if total_processed > 0 else 0
            print(f"{sentiment}: {count} articles ({percentage:.1f}%)")

        # Convert to list format for the API with percentages
        result = []

        # Set minimum articles threshold for a date to be included
        min_articles_threshold = 1

        # Get all dates in sorted order
        sorted_dates = sorted(sentiment_counts.keys())

        for date in sorted_dates:
            counts = sentiment_counts[date]

            # Calculate total articles for this date
            total = counts['Positive'] + counts['Negative'] + counts['Neutral']

            # Skip dates with insufficient articles
            if total < min_articles_threshold:
                print(f"Skipping date {date} with only {total} articles (below threshold of {min_articles_threshold})")
                continue

            # Calculate percentages
            positive_pct = round((counts['Positive'] / total) * 100, 1)
            negative_pct = round((counts['Negative'] / total) * 100, 1)
            neutral_pct = round((counts['Neutral'] / total) * 100, 1)

            # Ensure total is exactly 100% (handle rounding errors)
            total_pct = positive_pct + negative_pct + neutral_pct
            if total_pct != 100:
                # Adjust the largest value to make total exactly 100%
                diff = 100 - total_pct
                if positive_pct >= negative_pct and positive_pct >= neutral_pct:
                    positive_pct += diff
                elif negative_pct >= positive_pct and negative_pct >= neutral_pct:
                    negative_pct += diff
                else:
                    neutral_pct += diff

            # Print debug info for this date
            print(f"Date: {date}, Total: {total}, Positive: {positive_pct}%, Negative: {negative_pct}%, Neutral: {neutral_pct}%")

            result.append({
                'date': date,
                'positive': positive_pct,
                'negative': negative_pct,
                'neutral': neutral_pct,
                'total_articles': total  # Include total for reference
            })

        return jsonify(result)

    except FileNotFoundError:
        return jsonify({"error": "Combined CSV file not found."}), 500
    except Exception as e:
        return jsonify({"error": f"An error occurred: {e}"}), 500


# Run the Flask app
if __name__ == "__main__":
    start_background_thread()
    app.run()
