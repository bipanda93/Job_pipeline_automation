"""
Scraper France Travail avec stratégie multi-recherches
Pour contourner la limite de pagination (~1000 offres)
"""
from __future__ import annotations

import asyncio
import json
import re
import hashlib
import csv
from datetime import datetime
from pathlib import Path
from typing import Optional

from playwright.async_api import async_playwright

# ============================================================
# CONFIG - MULTI RECHERCHES
# ============================================================
BASE = "https://candidat.francetravail.fr"

# Stratégie: diviser "data" en plusieurs recherches plus spécifiques
SEARCH_CONFIGS = [
    {"keywords": "data scientist", "label": "data_scientist"},
    {"keywords": "data analyst", "label": "data_analyst"},
    {"keywords": "data engineer", "label": "data_engineer"},
    {"keywords": "data architecte", "label": "data_architect"},
    {"keywords": "machine learning", "label": "ml"},
    {"keywords": "big data", "label": "bigdata"},
    {"keywords": "business intelligence", "label": "bi"},
    {"keywords": "analyste données", "label": "analyste"},
]

OUT_DIR = Path("data/raw/france_travail")
OUT_DIR.mkdir(parents=True, exist_ok=True)

URLS_FILE = OUT_DIR / "urls.jsonl"
DETAILS_FILE = OUT_DIR / "details.jsonl"
CSV_FILE = OUT_DIR / "details.csv"
STATE_FILE = OUT_DIR / "state_multi.json"

HEADLESS = False
MAX_PAGES_PER_SEARCH = 50  # 50 x 20 = 1000 max par recherche


# ============================================================
# UTILS
# ============================================================
def now_iso() -> str:
    return datetime.now().isoformat()


def sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()


def extract_offer_id_from_url(url: str) -> Optional[str]:
    m = re.search(r"/detail/([^/?#]+)", url)
    return m.group(1) if m else None


def append_jsonl(path: Path, data: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(data, ensure_ascii=False) + "\n")


def read_seen_offer_ids(path: Path) -> set[str]:
    seen = set()
    if not path.exists():
        return seen
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                obj = json.loads(line)
                oid = obj.get("offer_id")
                if oid:
                    seen.add(oid)
            except Exception:
                pass
    return seen


def read_urls_jsonl_unique(path: Path) -> list[str]:
    if not path.exists():
        return []
    seen = set()
    out = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                obj = json.loads(line)
                u = obj.get("url")
                if u and u not in seen:
                    seen.add(u)
                    out.append(u)
            except Exception:
                continue
    return out


def load_state() -> dict:
    if not STATE_FILE.exists():
        return {
            "searches_done": [],  # Liste des labels terminés
            "current_search_idx": 0,
            "current_page": 0,
        }
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {
            "searches_done": [],
            "current_search_idx": 0,
            "current_page": 0,
        }


def save_state(state: dict):
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def export_to_csv(jsonl_path: Path, csv_path: Path):
    if not jsonl_path.exists():
        print("⚠️ Aucun details.jsonl")
        return

    by_id: dict[str, dict] = {}
    with jsonl_path.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                obj = json.loads(line)
                oid = obj.get("offer_id")
                if oid:
                    by_id[oid] = obj
            except Exception:
                continue

    rows = list(by_id.values())
    if not rows:
        return

    fieldnames = [
        "offer_id", "title", "url", "contract_type", "salary",
        "working_time", "published_at", "scraped_at", "source",
        "error", "raw_text"
    ]
    extra = sorted({k for r in rows for k in r.keys() if k not in fieldnames})
    fieldnames.extend(extra)

    with csv_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

    print(f"📤 CSV exporté: {len(rows)} offres uniques")


async def accept_cookies(page):
    try:
        btn = page.locator("#pecookies-accept-all")
        if await btn.count():
            await btn.click(timeout=5000)
            await page.wait_for_timeout(800)
            print("✓ Cookies acceptés")
    except Exception:
        pass


# ============================================================
# SCRAPING
# ============================================================
async def collect_urls_from_page(page) -> list[str]:
    urls = []
    selectors = [
        'a[href*="/offres/recherche/detail/"]',
        'a[href*="/detail/"]',
    ]

    for selector in selectors:
        links = await page.locator(selector).all()
        for link in links:
            try:
                href = await link.get_attribute("href")
                if href and "/detail/" in href:
                    full_url = href if href.startswith("http") else BASE + href
                    urls.append(full_url)
            except Exception:
                continue
        if urls:
            break

    return list(set(urls))


async def scrape_one_search(page, search_config: dict, state: dict, seen_urls: set) -> int:
    """Scrape une recherche spécifique, retourne le nombre de nouvelles URLs"""
    keywords = search_config["keywords"]
    label = search_config["label"]

    # URL de base pour cette recherche
    search_url_base = (
        f"https://candidat.francetravail.fr/offres/recherche"
        f"?motsCles={keywords.replace(' ', '+')}&offresPartenaires=true&rayon=10&tri=0"
    )

    start_page = state.get("current_page", 0)
    new_urls_count = 0

    print(f"\n🔍 Recherche: '{keywords}' (label: {label})")
    print(f"   Reprise à la page {start_page + 1}")

    for page_idx in range(start_page, MAX_PAGES_PER_SEARCH):
        start = page_idx * 20
        end = start + 19
        url = f"{search_url_base}&range={start}-{end}"

        try:
            await page.goto(url, timeout=60000)
            await page.wait_for_load_state("networkidle", timeout=60000)

            if page_idx == 0:
                await accept_cookies(page)

            await page.wait_for_timeout(800)

            urls = await collect_urls_from_page(page)

            if not urls:
                print(f"   Page {page_idx + 1}: 0 URL -> Fin pour '{label}'")
                break

            page_new = 0
            for u in urls:
                if u not in seen_urls:
                    seen_urls.add(u)
                    append_jsonl(URLS_FILE, {
                        "url": u,
                        "collected_at": now_iso(),
                        "source": "france_travail",
                        "search_label": label,
                        "search_keywords": keywords,
                        "range": f"{start}-{end}",
                    })
                    page_new += 1
                    new_urls_count += 1

            print(f"   Page {page_idx + 1}/{MAX_PAGES_PER_SEARCH} ({start}-{end}): +{page_new} URLs")

            # Sauvegarder la progression
            state["current_page"] = page_idx + 1
            save_state(state)

            # Pause entre les pages
            await page.wait_for_timeout(500)

        except Exception as e:
            print(f"   ⚠️  Erreur page {page_idx + 1}: {e}")
            continue

    # Marquer cette recherche comme terminée
    if label not in state["searches_done"]:
        state["searches_done"].append(label)
    state["current_page"] = 0  # Reset pour la prochaine recherche
    save_state(state)

    print(f"   ✅ '{label}': {new_urls_count} nouvelles URLs collectées")
    return new_urls_count


async def safe_text(page, selector: str, timeout=5000):
    try:
        loc = page.locator(selector).first
        await loc.wait_for(state="visible", timeout=timeout)
        t = await loc.text_content()
        return t.strip() if t else None
    except Exception:
        return None


async def scrape_detail(page, url: str) -> dict:
    try:
        await page.goto(url, timeout=60000)
        await page.wait_for_timeout(600)
    except Exception as e:
        return {
            "offer_id": sha1(url),
            "url": url,
            "scraped_at": now_iso(),
            "source": "france_travail",
            "error": str(e),
        }

    offer_id = extract_offer_id_from_url(url) or sha1(url)
    title = await safe_text(page, "h1", timeout=15000)
    main = await safe_text(page, "main", timeout=8000)
    aside = await safe_text(page, "aside", timeout=5000)

    blob = (main or "") + "\n" + (aside or "")

    def rgx(p: str):
        m = re.search(p, blob, re.IGNORECASE)
        return m.group(1).strip() if m else None

    return {
        "offer_id": offer_id,
        "title": title,
        "url": url,
        "contract_type": rgx(r"\b(CDI|CDD|Alternance|Stage|Intérim)\b"),
        "salary": rgx(r"Salaire\s*.*?:\s*(.+)"),
        "working_time": rgx(r"(\d{2}H/\s*semaine.*)"),
        "published_at": rgx(r"Publié(?:e)?\s+le\s+(\d{1,2}\s+\w+\s+\d{4})"),
        "raw_text": main,
        "scraped_at": now_iso(),
        "source": "france_travail",
    }


# ============================================================
# MAIN
# ============================================================
async def main():
    state = load_state()

    print("=" * 70)
    print("🚀 SCRAPER MULTI-RECHERCHES FRANCE TRAVAIL")
    print("=" * 70)

    # Phase 1: Collecter les URLs via plusieurs recherches
    existing_urls = read_urls_jsonl_unique(URLS_FILE)
    seen_urls = set(existing_urls)
    print(f"📦 URLs déjà collectées: {len(existing_urls)}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        page = await browser.new_page()

        start_idx = state.get("current_search_idx", 0)

        for i in range(start_idx, len(SEARCH_CONFIGS)):
            config = SEARCH_CONFIGS[i]
            label = config["label"]

            # Skip si déjà fait
            if label in state.get("searches_done", []):
                print(f"⊘ '{label}' déjà terminée, skip")
                continue

            state["current_search_idx"] = i
            save_state(state)

            await scrape_one_search(page, config, state, seen_urls)

        print(f"\n✅ Collecte URLs terminée: {len(seen_urls)} URLs uniques")

        # Phase 2: Scraper les détails
        all_urls = read_urls_jsonl_unique(URLS_FILE)
        seen_details = read_seen_offer_ids(DETAILS_FILE)

        print(f"\n📊 Phase 2: Scraping des détails")
        print(f"   URLs à scraper: {len(all_urls)}")
        print(f"   Déjà scrapées: {len(seen_details)}")

        scraped = 0
        skipped = 0

        for i, url in enumerate(all_urls, 1):
            oid = extract_offer_id_from_url(url)

            if oid and oid in seen_details:
                skipped += 1
                continue

            data = await scrape_detail(page, url)
            append_jsonl(DETAILS_FILE, data)

            if data.get("offer_id"):
                seen_details.add(data["offer_id"])

            scraped += 1
            title = (data.get("title") or "Sans titre")[:50]
            print(f"✓ {i}/{len(all_urls)} - {data.get('offer_id')} - {title}")

            await page.wait_for_timeout(300)

            if scraped % 50 == 0:
                print(f"💾 Checkpoint: {scraped} offres scrapées")

        await browser.close()

    # Export CSV
    export_to_csv(DETAILS_FILE, CSV_FILE)

    print("\n" + "=" * 70)
    print("✅ TERMINÉ")
    print(f"   📊 {scraped} nouvelles offres scrapées")
    print(f"   ⊘  {skipped} déjà existantes")
    print(f"   📁 Total URLs: {len(all_urls)}")
    print(f"   💾 Fichiers:")
    print(f"      - {URLS_FILE}")
    print(f"      - {DETAILS_FILE}")
    print(f"      - {CSV_FILE}")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())