#!/usr/bin/env python3
"""
スクレイピング -> SQLite保存 -> SELECT表示 のフルワークフロー
対象: https://github.com/google (例)
注意: GitHubの構造が変わることがあります。selectorはサンプルです。
"""

import requests
from bs4 import BeautifulSoup
import sqlite3
import time
import logging
import re

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

BASE_ORG = "https://github.com/google"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; scraping-exercise/1.0; +https://example.com)"
}
SLEEP_SECONDS = 1.0  # ← 必ず入れる

def normalize_stars(stars_str: str) -> int:
    """ "1,234" や "4.5k" を整数に変換 """
    if not stars_str:
        return 0
    s = stars_str.strip()
    s = s.replace(",", "")
    # match 1.2k, 12k, 900
    m = re.match(r"^([\d\.]+)\s*([kK]?)$", s)
    if not m:
        try:
            return int(float(s))
        except Exception:
            return 0
    val, suffix = m.groups()
    val_f = float(val)
    if suffix.lower() == "k":
        return int(val_f * 1000)
    return int(val_f)

def fetch_url(session: requests.Session, url: str) -> BeautifulSoup:
    logging.info("GET %s", url)
    r = session.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

def scrape_org_repos(org_url: str, max_repos: int = 30):
    """ 組織ページからリポジトリ一覧を取得し、各リポジトリの詳細をスクレイピングする """
    session = requests.Session()
    soup = fetch_url(session, org_url)

    # GitHub org page のリポジトリリストのセレクタ（変わることがある）
    repo_items = soup.select('li[itemprop="owns"]')  # うまく行かないときは下の代替セレクタへ
    if not repo_items:
        repo_items = soup.select('div.org-repos li')  # 代替

    data = []
    for li in repo_items[:max_repos]:
        name_tag = li.select_one('a[itemprop="name codeRepository"]')
        if not name_tag:
            # 代替パターン：リンク先が /google/xxx の aタグ
            name_tag = li.select_one("a[href^='/google/']")
        if not name_tag:
            continue

        repo_name = name_tag.text.strip()
        repo_href = name_tag.get("href")
        repo_url = "https://github.com" + repo_href

        # 各リポジトリの個別ページを開いて言語・スター数を取得
        time.sleep(SLEEP_SECONDS)
        try:
            repo_soup = fetch_url(session, repo_url)
        except Exception as e:
            logging.error("Failed to fetch %s: %s", repo_url, e)
            continue

        # 主要言語の取得（複数ある場合は最初の主要な言語）
        lang_tag = repo_soup.select_one('span[itemprop="programmingLanguage"]')
        language = lang_tag.text.strip() if lang_tag else None

        # スター数（stargazers へのリンクテキスト）
        star_tag = repo_soup.select_one("a[href$='/stargazers']")
        stars_text = star_tag.text.strip() if star_tag else "0"
        stars = normalize_stars(stars_text)

        logging.info("Found repo: %s | lang=%s | stars=%d", repo_name, language, stars)
        data.append((repo_name, language or "N/A", stars))

    return data

def save_to_sqlite(db_path: str, repos):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS repositories (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE,
        language TEXT,
        stars INTEGER,
        scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    # upsert: 同じ name があれば更新する
    cur.executemany("""
    INSERT INTO repositories (name, language, stars)
    VALUES (?, ?, ?)
    ON CONFLICT(name) DO UPDATE SET
        language=excluded.language,
        stars=excluded.stars,
        scraped_at=CURRENT_TIMESTAMP
    """, repos)
    conn.commit()
    conn.close()
    logging.info("Saved %d rows to %s", len(repos), db_path)

def show_select_examples(db_path: str):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    print("\n-- 全件表示 --")
    for row in cur.execute("SELECT id, name, language, stars, scraped_at FROM repositories ORDER BY id"):
        print(row)

    print("\n-- スター数で上位10件 --")
    for row in cur.execute("SELECT name, stars FROM repositories ORDER BY stars DESC LIMIT 10"):
        print(row)

    print("\n-- 言語ごとの件数（多い順） --")
    for row in cur.execute("SELECT language, COUNT(*) AS cnt FROM repositories GROUP BY language ORDER BY cnt DESC"):
        print(row)

    conn.close()

if __name__ == "__main__":
    db_file = "google_repos.sqlite"
    repos = scrape_org_repos(BASE_ORG, max_repos=20)  # max_repos は必要に応じて
    if repos:
        save_to_sqlite(db_file, repos)
        show_select_examples(db_file)
    else:
        logging.warning("No repositories scraped.")