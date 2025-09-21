#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import json
import time
import random
import difflib
from datetime import datetime
from pathlib import Path

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import requests

# ────────────────────────────────
# Конфиг
# ────────────────────────────────
DEBUG_DUMPS = Path("debug_dumps")
DEBUG_DUMPS.mkdir(exist_ok=True)
DATA_DIR = Path("hltb_data")
DATA_DIR.mkdir(exist_ok=True)

HLTB_BASE = "https://howlongtobeat.com"

# ────────────────────────────────
# Логгер
# ────────────────────────────────
def log(msg: str):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def dump_json(data, filename):
    with open(DEBUG_DUMPS / filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def dump_html(content, filename):
    with open(DEBUG_DUMPS / filename, "w", encoding="utf-8") as f:
        f.write(content)

# ────────────────────────────────
# Нормализация строк
# ────────────────────────────────
def sanitize_query(q: str) -> str:
    q = q.replace("&", "and")
    q = re.sub(r"[/:()]", " ", q)
    q = re.sub(r"\s+", " ", q).strip()
    return q

def generate_search_variants(name: str):
    base = sanitize_query(name)
    variants = [base]

    # Разделение по /
    if "/" in base:
        parts = [p.strip() for p in base.split("/") if p.strip()]
        variants.extend(parts)

    # Цифры в римские
    variants = list(set(variants))
    return variants

# ────────────────────────────────
# Поиск на HLTB
# ────────────────────────────────
def search_game_on_hltb(page, query: str):
    log(f"🔍 Ищем: '{query}'")
    page.goto(f"{HLTB_BASE}/search_results?page=1&query={query}", timeout=60000)
    page.wait_for_timeout(2000)

    candidates = []
    try:
        rows = page.query_selector_all("li.back_gray.shadow_box")
        for row in rows:
            title_el = row.query_selector("a.text_blue")
            year_el = row.query_selector("div.search_list_tidbit")
            if not title_el:
                continue
            title = title_el.inner_text().strip()
            href = title_el.get_attribute("href")
            year = None
            if year_el:
                txt = year_el.inner_text().strip()
                m = re.search(r"\d{4}", txt)
                if m:
                    year = int(m.group(0))
            candidates.append({
                "title": title,
                "href": href,
                "year": year
            })
    except Exception as e:
        log(f"⚠️ Ошибка парсинга кандидатов: {e}")

    dump_json(candidates, f"{query}_candidates_{int(time.time())}.json")
    log(f"🔎 Найдено кандидатов: {len(candidates)}")
    return candidates

# ────────────────────────────────
# Парсинг страницы игры
# ────────────────────────────────
def extract_game_times(page, game_url: str):
    url = f"{HLTB_BASE}{game_url}"
    page.goto(url, timeout=60000)
    page.wait_for_timeout(2000)

    html = page.content()
    dump_html(html, f"{Path(game_url).name}_game_{int(time.time())}.html")

    results = {}
    try:
        blocks = page.query_selector_all("div.GameStats_game_time__*")
        for b in blocks:
            text = b.inner_text().strip()
            if not text:
                continue
            if ":" in text:
                key, val = text.split(":", 1)
                results[key.strip()] = val.strip()
            else:
                parts = text.split()
                if len(parts) >= 2:
                    key = " ".join(parts[:-1])
                    val = parts[-1]
                    results[key.strip()] = val.strip()
    except Exception as e:
        log(f"⚠️ Ошибка парсинга игры {url}: {e}")
    return results

# ────────────────────────────────
# Основной процесс
# ────────────────────────────────
def process_game(page, title: str, year: int):
    variants = generate_search_variants(title)
    best_result = None
    best_score = 0.0

    for var in variants:
        candidates = search_game_on_hltb(page, var)
        if not candidates:
            continue

        # фильтрация по году
        filtered = [c for c in candidates if c["year"] == year]
        pool = filtered if filtered else candidates

        # ищем ближайшее совпадение по названию
        for cand in pool:
            score = difflib.SequenceMatcher(None, cand["title"].lower(), title.lower()).ratio()
            if score > best_score:
                best_score = score
                best_result = cand

        if best_score >= 0.95:
            break

    if best_result:
        log(f"🎯 Выбрано: '{best_result['title']}' (score {best_score:.2f})")
        times = extract_game_times(page, best_result["href"])
        return {
            "title": best_result["title"],
            "year": best_result["year"],
            "times": times
        }
    else:
        log(f"⚠️ Данные не найдены для: {title}")
        return None

# ────────────────────────────────
# Main
# ────────────────────────────────
def main():
    log("🚀 Запуск HLTB Worker")
    games_list = [
        {"title": "Sonic the Hedgehog 3 (& Knuckles)", "year": 1994},
        {"title": "Ultima / Ultima I: The First Age of Darkness", "year": 1981},
        {"title": "Doom II", "year": 1994},
        {"title": "Pokémon Red/Blue/Yellow", "year": 1996},
        {"title": "Tetris", "year": 1985},
        {"title": "Half-Life 2", "year": 2004},
        {"title": "God of War", "year": 2018},
    ]

    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        for idx, g in enumerate(games_list, 1):
            log(f"🎮 Обрабатываю {idx}/{len(games_list)}: {g['title']} ({g['year']})")
            result = process_game(page, g["title"], g["year"])
            if result:
                results.append(result)
            time.sleep(random.uniform(2, 4))

        browser.close()

    outfile = DATA_DIR / f"hltb_data_{int(time.time())}.json"
    with open(outfile, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    log(f"✅ Данные сохранены: {outfile}")

if __name__ == "__main__":
    main()
