# Scraping.py (updated)
import requests
import random
import time
import os
import csv
from datetime import datetime as dt
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------
# Helpers & Session
# ---------------------------
def create_session():
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:116.0) Gecko/20100101 Firefox/116.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15",
    ]
    headers = {
        "User-Agent": random.choice(user_agents),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
    }
    s = requests.Session()
    s.headers.update(headers)
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504], raise_on_status=False)
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

SESSION = create_session()

def make_full(href):
    if not href:
        return None
    href = href.strip()
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("//"):
        return "https:" + href
    if href.startswith("/"):
        return "https://www.ndtv.com" + href
    return "https://www.ndtv.com/" + href

# ---------------------------
# CSV helpers (kept logic same)
# ---------------------------
def get_existing_links(filename):
    existing_links = set()
    if os.path.exists(filename):
        try:
            with open(filename, "r", encoding="utf-8") as file:
                reader = csv.DictReader(file)
                for row in reader:
                    # protect against missing 'link' column
                    if "link" in row:
                        existing_links.add(row["link"])
        except Exception as e:
            print(f"[get_existing_links] error reading {filename}: {e}")
    return existing_links

def save_to_csv(data, filename):
    if not data:
        print("No new articles to save.")
        return
    fieldnames = ["link", "headline", "datetime_posted", "content", "scraped_at", "analysis_result"]
    file_exists = os.path.exists(filename)
    try:
        with open(filename, "a", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            for row in data:
                if "analysis_result" not in row:
                    row["analysis_result"] = ""
                writer.writerow(row)
        print(f"Saved {len(data)} new articles to {filename}")
    except Exception as e:
        print(f"Error writing to CSV: {e}")

# ---------------------------
# Category list fetchers (use session + debug dumps)
# ---------------------------

def getgeneralndtv():
    URL = "https://www.ndtv.com/latest"
    try:
        time.sleep(random.uniform(0.8, 1.6))
        resp = SESSION.get(URL, timeout=15)
    except requests.RequestException as e:
        print(f"[getgeneralndtv] request failed: {e}")
        return []



    soup = BeautifulSoup(resp.content, "html.parser")
    generalndtv_list = []

    articles = soup.find_all("div", class_="NwsLstPg_txt-wrp")
    if articles:
        for article in articles:
            h2_tag = article.find("h2", class_="NwsLstPg_ttl")
            a_tag = h2_tag.find("a") if h2_tag else None
            if a_tag and a_tag.get("href"):
                generalndtv_list.append(make_full(a_tag["href"]))
    else:
        # fallback: harvest h2 anchors and reasonable anchors
        for h2 in soup.find_all("h2"):
            a = h2.find("a", href=True)
            if a:
                generalndtv_list.append(make_full(a["href"]))
        if not generalndtv_list:
            for a in soup.select("a[href]"):
                href = a["href"]
                text = a.get_text(strip=True)
                if text and any(x in href for x in ["/news/", "/latest", "/india-news/", "/world-news/"]):
                    generalndtv_list.append(make_full(href))

    # dedupe
    cleaned = []
    seen = set()
    for u in generalndtv_list:
        if not u:
            continue
        if u in seen:
            continue
        seen.add(u)
        cleaned.append(u)

    print(f"[getgeneralndtv] returning {len(cleaned)} links (sample 5): {cleaned[:5]}")
    return cleaned

def getedundtv():
    URL = "https://www.ndtv.com/education"
    try:
        time.sleep(random.uniform(0.8, 1.6))
        resp = SESSION.get(URL, timeout=15)
    except requests.RequestException as e:
        print(f"[getedundtv] request failed: {e}")
        return []

    soup = BeautifulSoup(resp.content, "html.parser")
    edundtv_list = []
    articles = soup.find_all("a", class_="crd_lnk")
    for article in articles:
        link = article.get("href")
        if link:
            edundtv_list.append(make_full(link))
    print(f"[getedundtv] returning {len(edundtv_list)} links")
    return list(dict.fromkeys(edundtv_list))

def gethealthndtv():
    URL = "https://doctor.ndtv.com/top-stories"
    try:
        time.sleep(random.uniform(0.8, 1.6))
        resp = SESSION.get(URL, timeout=15)
    except requests.RequestException as e:
        print(f"[gethealthndtv] request failed: {e}")
        return []

    soup = BeautifulSoup(resp.content, "html.parser")
    healthndtv_list = []
    articles = soup.find_all("div", class_="stry-cont")
    for article in articles:
        a_tag = article.find("a", href=True)
        if a_tag:
            link = a_tag["href"]
            if link.startswith("/"):
                link = "https://doctor.ndtv.com" + link
            healthndtv_list.append(link)
    print(f"[gethealthndtv] returning {len(healthndtv_list)} links")
    return list(dict.fromkeys(healthndtv_list))

def getcricketndtv():
    URL = "https://sports.ndtv.com/cricket"
    try:
        time.sleep(random.uniform(0.8, 1.6))
        resp = SESSION.get(URL, timeout=15)
    except requests.RequestException as e:
        print(f"[getcricketndtv] request failed: {e}")
        return []

    soup = BeautifulSoup(resp.content, "html.parser")
    cricketndtv_list = []
    articles = soup.find_all("div", class_="crd_txt-wrp")
    for article in articles:
        h3_tag = article.find("h3", class_="crd_ttl")
        a_tag = h3_tag.find("a", class_="crd_lnk") if h3_tag else None
        if a_tag and a_tag.get("href"):
            link = a_tag["href"]
            if link.startswith("/"):
                link = "https://sports.ndtv.com" + link
            cricketndtv_list.append(link)
    print(f"[getcricketndtv] returning {len(cricketndtv_list)} links")
    return list(dict.fromkeys(cricketndtv_list))

def getsciencendtv():
    URL = "https://www.ndtv.com/science"
    try:
        time.sleep(random.uniform(0.8, 1.6))
        resp = SESSION.get(URL, timeout=15)
    except requests.RequestException as e:
        print(f"[getsciencendtv] request failed: {e}")
        return []

    soup = BeautifulSoup(resp.content, "html.parser")
    sciencendtv_list = []
    articles = soup.find_all("div", class_="NwsLstPg_txt-wrp")
    for article in articles:
        h2_tag = article.find("h2", class_="NwsLstPg_ttl")
        a_tag = h2_tag.find("a") if h2_tag else None
        if a_tag and a_tag.get("href"):
            link = a_tag["href"]
            if link.startswith("/"):
                link = "https://www.ndtv.com" + link
            sciencendtv_list.append(link)
    print(f"[getsciencendtv] returning {len(sciencendtv_list)} links")
    return list(dict.fromkeys(sciencendtv_list))

def getworldndtv():
    URL = "https://www.ndtv.com/world"
    try:
        time.sleep(random.uniform(0.8, 1.6))
        resp = SESSION.get(URL, timeout=15)
    except requests.RequestException as e:
        print(f"[getworldndtv] request failed: {e}")
        return []

    soup = BeautifulSoup(resp.content, "html.parser")
    worldndtv_list = []
    articles = soup.find_all("div", class_="crd_txt-wrp")
    for article in articles:
        h3_tag = article.find("h3", class_="crd_ttl8")
        if not h3_tag:
            continue
        a_tag = h3_tag.find("a", href=True)
        if a_tag:
            link = a_tag["href"].strip()
            worldndtv_list.append(make_full(link))
    print(f"[getworldndtv] returning {len(worldndtv_list)} links")
    return list(dict.fromkeys(worldndtv_list))

def getenterndtv():
    URL = "https://www.ndtv.com/entertainment/latest"
    try:
        time.sleep(random.uniform(0.8, 1.6))
        resp = SESSION.get(URL, timeout=15)
    except requests.RequestException as e:
        print(f"[getenterndtv] request failed: {e}")
        return []

    soup = BeautifulSoup(resp.content, "html.parser")
    enterndtv_list = []
    articles = soup.find_all("div", class_="NwsLstPg_txt-wrp")
    for article in articles:
        h2_tag = article.find("h2", class_="NwsLstPg_ttl")
        a_tag = h2_tag.find("a", class_="NwsLstPg_ttl-lnk") if h2_tag else None
        if a_tag and a_tag.get("href"):
            link = a_tag["href"]
            if link.startswith("/"):
                link = "https://www.ndtv.com" + link
            enterndtv_list.append(link)
    print(f"[getenterndtv] returning {len(enterndtv_list)} links")
    return list(dict.fromkeys(enterndtv_list))

# ---------------------------
# Article scrapers (kept your logic, use SESSION & dedupe)
# ---------------------------

FILENAME = "ndtv_general_news.csv"

def getgeneralarticlendtv():
    links = getgeneralndtv()
    scraped_data = []
    existing_links = get_existing_links(FILENAME)

    for link in links:
        if link in existing_links:
            continue
        try:
            time.sleep(random.uniform(0.6, 1.2))
            res = SESSION.get(link, timeout=15)
            soup = BeautifulSoup(res.content, "html.parser")

            headline_tag = soup.find("h1", class_="sp-ttl")
            headline = headline_tag.get_text(strip=True) if headline_tag else None

            date_tag = soup.find("span", class_="pst-by_lnk")
            datetime_posted = date_tag.get_text(strip=True) if date_tag else None

            content_div = soup.find("div", id="ignorediv")
            if content_div:
                paragraphs = content_div.find_all("p")
                content = " ".join(p.get_text(strip=True) for p in paragraphs)
            else:
                # fallback: gather all <p> inside article area
                paragraphs = soup.find_all("p")
                content = " ".join(p.get_text(strip=True) for p in paragraphs) if paragraphs else None

            scraped_data.append({
                "link": link,
                "headline": headline,
                "datetime_posted": datetime_posted,
                "content": content,
                "scraped_at": dt.now().strftime("%Y-%m-%d %H:%M:%S"),
            })
        except Exception as e:
            print(f"Error scraping {link}: {e}")

    save_to_csv(scraped_data, FILENAME)
    return scraped_data

# Education
def geteduarticlendtv():
    FILENAME = "ndtv_education_news.csv"
    links = getedundtv()
    scraped_data = []
    existing_links = get_existing_links(FILENAME)

    for link in links:
        if link in existing_links:
            continue
        try:
            time.sleep(random.uniform(0.6, 1.2))
            response = SESSION.get(link, timeout=15)
            soup = BeautifulSoup(response.content, "html.parser")

            headline_tag = soup.find("h1", class_="sp-ttl", itemprop="headline")
            headline = headline_tag.get_text(strip=True) if headline_tag else "No headline"

            datetime_tag = soup.find("span", class_="pst-by_lnk", itemprop="dateModified")
            datetime_text = datetime_tag.get_text(strip=True) if datetime_tag else "No date/time"

            article_section = soup.find("div", class_="Art-exp_wr", id="ignorediv")
            paragraphs = article_section.find_all("p") if article_section else []
            article_text = "\n".join(p.get_text(strip=True) for p in paragraphs)

            scraped_data.append({
                "link": link,
                "headline": headline,
                "datetime_posted": datetime_text,
                "content": article_text,
                "scraped_at": dt.now().strftime("%Y-%m-%d %H:%M:%S"),
            })
        except Exception as e:
            print(f"Error processing {link}: {e}")

    save_to_csv(scraped_data, FILENAME)
    return scraped_data

# Health
def gethealtharticlendtv():
    FILENAME = "ndtv_health_news.csv"
    links = gethealthndtv()
    scraped_data = []
    existing_links = get_existing_links(FILENAME)

    for link in links:
        if link in existing_links:
            continue
        try:
            time.sleep(random.uniform(0.6, 1.2))
            response = SESSION.get(link, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")

            headline_tag = soup.find("div", class_="__sslide")
            headline = headline_tag.find("h1").text.strip() if headline_tag else "No headline found"

            datetime_tag = soup.find("div", class_="article-author tpStl1")
            datetime_text = datetime_tag.find("span").text.strip() if datetime_tag else "No datetime found"

            content_div = soup.find("div", class_="article_storybody")
            paragraphs = content_div.find_all("p") if content_div else []
            article_text = " ".join(p.text.strip() for p in paragraphs)

            scraped_data.append({
                "link": link,
                "headline": headline,
                "datetime_posted": datetime_text,
                "content": article_text,
                "scraped_at": dt.now().strftime("%Y-%m-%d %H:%M:%S"),
            })
        except Exception as e:
            print(f"Error processing {link}: {e}")

    save_to_csv(scraped_data, FILENAME)
    return scraped_data

# Cricket
def getcricketarticlendtv():
    FILENAME = "ndtv_cricket_news.csv"
    links = getcricketndtv()
    scraped_data = []
    existing_links = get_existing_links(FILENAME)

    for link in links:
        if link in existing_links:
            continue
        try:
            time.sleep(random.uniform(0.6, 1.2))
            response = SESSION.get(link, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")

            headline_tag = soup.find("h2", class_="sp-descp")
            headline = headline_tag.get_text(strip=True) if headline_tag else "No headline found"

            datetime_text = "No date/time found"
            meta_tag = soup.find("meta", {"itemprop": "datePublished"})
            if meta_tag and "content" in meta_tag.attrs:
                datetime_text = meta_tag["content"].replace("T", " ").replace("+05:30", "")

            article_section = soup.find("div", class_="story__content")
            paragraphs = article_section.find_all("p") if article_section else []
            article_text = "\n".join(p.get_text(strip=True) for p in paragraphs)

            scraped_data.append({
                "link": link,
                "headline": headline,
                "datetime_posted": datetime_text,
                "content": article_text,
                "scraped_at": dt.now().strftime("%Y-%m-%d %H:%M:%S"),
            })
        except Exception as e:
            print(f"Error processing {link}: {e}")

    save_to_csv(scraped_data, FILENAME)
    return scraped_data

# Science
def getsciencearticlendtv():
    FILENAME = "ndtv_science_news.csv"
    links = getsciencendtv()
    scraped_data = []

    for link in links:
        # dedupe check
        existing_links = get_existing_links(FILENAME)
        if link in existing_links:
            continue
        try:
            time.sleep(random.uniform(0.6, 1.2))
            response = SESSION.get(link, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")

            headline_tag = soup.find("h1", class_="sp-ttl", itemprop="headline")
            headline = headline_tag.get_text(strip=True) if headline_tag else "No headline found"

            datetime_tag = soup.find("span", class_="pst-by_lnk", itemprop="dateModified")
            if datetime_tag and datetime_tag.has_attr("content"):
                datetime_text = datetime_tag["content"]
            elif datetime_tag:
                datetime_text = datetime_tag.get_text(strip=True)
            else:
                datetime_text = "No date/time found"

            paragraphs = soup.find_all("p")
            article_text = "\n".join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))

            scraped_data.append({
                "link": link,
                "headline": headline,
                "datetime_posted": datetime_text,
                "content": article_text,
                "scraped_at": dt.now().strftime("%Y-%m-%d %H:%M:%S"),
            })
        except Exception as e:
            print(f"Error processing {link}: {e}")

    save_to_csv(scraped_data, FILENAME)
    return scraped_data

# World
def getworldarticlendtv():
    FILENAME = "ndtv_world_news.csv"
    links = getworldndtv()
    scraped_data = []

    for link in links:
        existing_links = get_existing_links(FILENAME)
        if link in existing_links:
            continue
        try:
            time.sleep(random.uniform(0.6, 1.2))
            response = SESSION.get(link, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")

            headline_tag = soup.find("h1", class_="sp-ttl", itemprop="headline")
            headline = headline_tag.get_text(strip=True) if headline_tag else "No headline found"

            datetime_tag = soup.find("span", class_="pst-by_lnk", itemprop="dateModified")
            if datetime_tag and datetime_tag.has_attr("content"):
                datetime_text = datetime_tag["content"]
            elif datetime_tag:
                datetime_text = datetime_tag.get_text(strip=True)
            else:
                datetime_text = "No date/time found"

            ai_summary_tag = soup.find("div", class_="AiSum_tx")
            ai_summary = ai_summary_tag.get_text(strip=True) if ai_summary_tag else ""

            paragraphs = soup.find_all("p")
            body_text = "\n".join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
            article_text = ai_summary + "\n" + body_text if ai_summary else body_text

            scraped_data.append({
                "link": link,
                "headline": headline,
                "datetime_posted": datetime_text,
                "content": article_text,
                "scraped_at": dt.now().strftime("%Y-%m-%d %H:%M:%S"),
            })
        except Exception as e:
            print(f"Error processing {link}: {e}")

    save_to_csv(scraped_data, FILENAME)
    return scraped_data

# Entertainment
def getenterarticlendtv():
    FILENAME = "ndtv_entertainment_news.csv"
    links = getenterndtv()
    scraped_data = []

    for link in links:
        existing_links = get_existing_links(FILENAME)
        if link in existing_links:
            continue
        try:
            time.sleep(random.uniform(0.6, 1.2))
            response = SESSION.get(link, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")

            headline_tag = soup.find("h1", class_="sp-ttl", itemprop="headline")
            headline = headline_tag.get_text(strip=True) if headline_tag else "No headline found"

            datetime_tag = soup.find("span", class_="pst-by_lnk", itemprop="dateModified")
            if datetime_tag and datetime_tag.has_attr("content"):
                datetime_text = datetime_tag["content"]
            elif datetime_tag:
                datetime_text = datetime_tag.get_text(strip=True)
            else:
                datetime_text = "No date/time found"

            paragraphs = soup.find_all("p")
            full_text = "\n".join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))

            scraped_data.append({
                "link": link,
                "headline": headline,
                "datetime_posted": datetime_text,
                "content": full_text,
                "scraped_at": dt.now().strftime("%Y-%m-%d %H:%M:%S"),
            })
        except Exception as e:
            print(f"Error fetching {link}: {e}")

    save_to_csv(scraped_data, FILENAME)
    return scraped_data

# ---------------------------
# Combine category CSVs (kept behavior same)
# ---------------------------
category_files = [
    "ndtv_general_news.csv",
    "ndtv_education_news.csv",
    "ndtv_health_news.csv",
    "ndtv_cricket_news.csv",
    "ndtv_science_news.csv",
    "ndtv_world_news.csv",
    "ndtv_entertainment_news.csv"
]

def combine_category_csvs():
    all_data = []
    for category_file in category_files:
        category = category_file.split('_')[1] if '_' in category_file else category_file
        try:
            with open(category_file, "r", encoding="utf-8") as file:
                reader = csv.DictReader(file)
                for row in reader:
                    row["category"] = category
                    all_data.append(row)
            print(f"Successfully read data from {category_file}")
        except Exception as e:
            print(f"Error reading {category_file}: {e}")

    save_combined_csv(all_data)

def save_combined_csv(data, filename="ndtv_all_news.csv"):
    if not data:
        print("No data to save.")
        return
    fieldnames = ["link", "headline", "datetime_posted", "content", "scraped_at", "category", "analysis_result"]
    try:
        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in data:
                if "analysis_result" not in row:
                    row["analysis_result"] = ""
            writer.writerows(data)
        print(f"Successfully saved combined data to {filename}")
    except Exception as e:
        print(f"Error saving to CSV: {e}")

# Keep the original behavior: combine on import (as your original file did)
combine_category_csvs()
