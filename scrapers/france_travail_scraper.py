from __future__ import annotations

import asyncio
import json
import re
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Optional

from playwright.async_api import async_playwright
from db import get_connection

# ============================================================
# CONFIG
# ============================================================
SEARCH_URL = (
    "https://candidat.francetravail.fr/offres/recherche"
    "?motsCles=data&offresPartenaires=true&rayon=10&tri=0"
)
BASE = "https://candidat.francetravail.fr"

OUT_DIR = Path("data/raw/france_travail")
OUT_DIR.mkdir(parents=True, exist_ok=True)

URLS_FILE = OUT_DIR / "urls.jsonl"
DETAILS_FILE = OUT_DIR / "details.jsonl"

HEADLESS = False
MAX_PAGES = 150  # 2955 offres / 20 par page ≈ 148 pages

# France Travail offer id: 201MFRL, 201QXVB, etc.
OFFER_ID_RE = re.compile(r"\b[0-9A-Z]{7}\b")


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


async def accept_cookies(page):
    try:
        btn = page.locator("#pecookies-accept-all")
        if await btn.count():
            await btn.click(timeout=5000)
            await page.wait_for_timeout(800)
            print("✓ Cookies accepted")
    except Exception:
        pass


# ============================================================
# SCRAPING HTML - Collecte des URLs depuis les pages
# ============================================================
async def collect_urls_from_page(page) -> list[str]:
    """Extrait les URLs des offres depuis la page actuelle"""
    urls = []

    # Sélecteur pour les liens d'offres (à ajuster selon la structure HTML)
    selectors = [
        'a[href*="/offres/recherche/detail/"]',
        '[data-testid*="offer"] a',
        'article a[href*="/detail/"]'
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

    return list(set(urls))  # Dédupliquer


async def go_to_next_page(page) -> bool:
    """Clique sur le bouton 'Suivant' et retourne True si succès"""
    selectors = [
        'button:has-text("Suivant")',
        'button[aria-label*="suivant"]',
        'a:has-text("Suivant")',
        '[data-testid="pagination-next"]',
        'button.pagination-next'
    ]

    for selector in selectors:
        try:
            btn = page.locator(selector).first
            if await btn.count() and await btn.is_enabled():
                await btn.click(timeout=5000)
                await page.wait_for_timeout(2000)
                return True
        except Exception:
            continue

    return False


async def fetch_all_urls_via_html(page, max_pages: int = 150) -> list[str]:
    """
    Collecte toutes les URLs en naviguant page par page via HTML
    """
    await page.goto(SEARCH_URL, timeout=60000)
    await page.wait_for_load_state("networkidle", timeout=60000)
    await accept_cookies(page)

    all_urls = []
    seen_urls = set()

    for page_num in range(max_pages):
        # Attendre que le contenu se charge
        await page.wait_for_timeout(1500)

        # Collecter les URLs de cette page
        urls = await collect_urls_from_page(page)

        new_count = 0
        for url in urls:
            if url not in seen_urls:
                seen_urls.add(url)
                all_urls.append(url)
                new_count += 1

        print(f"[HTML] Page {page_num + 1} -> +{new_count} nouvelles URLs (total={len(all_urls)})")

        # Si aucune URL trouvée, essayer de scroller
        if not urls:
            await page.mouse.wheel(0, 3000)
            await page.wait_for_timeout(1000)
            urls = await collect_urls_from_page(page)
            if not urls:
                print(f"[HTML] ⚠️  Aucune URL trouvée sur cette page, arrêt")
                break

        # Progression
        if (page_num + 1) % 10 == 0:
            print(f"🔄 Progression: {page_num + 1} pages, {len(all_urls)} URLs collectées")

        # Aller à la page suivante
        success = await go_to_next_page(page)
        if not success:
            print(f"[HTML] ✓ Plus de bouton 'Suivant', fin de pagination")
            break

    print(f"✅ Collecte terminée: {len(all_urls)} URLs uniques")
    return all_urls


# ============================================================
# SCRAPE DETAIL
# ============================================================
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
        print(f"  ⚠️  Erreur chargement: {e}")
        return {
            "offer_id": sha1(url),
            "url": url,
            "scraped_at": now_iso(),
            "source": "france_travail",
            "error": str(e)
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
# Connexion PostgreSQL
# ============================================================

def insert_job_to_db(job: dict):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO raw_france_travail_jobs
            (offer_id, title, company, location,
             contract_type, salary, raw_text, url, scraped_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (offer_id) DO NOTHING
        """, (
            job.get("offer_id"),
            job.get("title"),
            job.get("company"),
            job.get("location"),
            job.get("contract_type"),
            job.get("salary"),
            job.get("raw_text"),
            job.get("url"),
        ))
        conn.commit()
    except Exception as e:
        print(f"Erreur insertion DB: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
# ============================================================
# MAIN
# ============================================================
async def main():
    seen = read_seen_offer_ids(DETAILS_FILE)
    print(f"🔁 Offres déjà scrapées: {len(seen)}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        page = await browser.new_page()

        # Collecter toutes les URLs via HTML
        urls = await fetch_all_urls_via_html(page, max_pages=MAX_PAGES)
        print(f"📌 URLs collectées: {len(urls)}")

        # Sauvegarder les URLs
        for u in urls:
            append_jsonl(URLS_FILE, {
                "url": u,
                "collected_at": now_iso(),
                "source": "france_travail"
            })

        # Scraper les détails
        scraped_count = 0
        skipped_count = 0

        for i, u in enumerate(urls, 1):
            oid = extract_offer_id_from_url(u)
            if oid and oid in seen:
                skipped_count += 1
                if skipped_count <= 5:  # Afficher les 5 premiers
                    print(f"⊘ {i}/{len(urls)} - {oid} déjà scrapée (skip)")
                elif skipped_count == 6:
                    print(f"⊘ ... ({len(urls) - i} autres déjà scrapées)")
                continue

            data = await scrape_detail(page, u)
            insert_job_to_db(data)

            if data.get("offer_id"):
                seen.add(data["offer_id"])

            scraped_count += 1
            title_preview = (data.get("title") or "Sans titre")[:60]
            print(f"✓ {i}/{len(urls)} - {data.get('offer_id')} {title_preview}")

            # Pause pour éviter les bans
            await page.wait_for_timeout(300)

            # Progression
            if scraped_count % 50 == 0:
                print(f"💾 {scraped_count} offres scrapées...")

        await browser.close()

    print(f"\n✅ TERMINÉ")
    print(f"   📊 {scraped_count} nouvelles offres scrapées")
    print(f"   ⊘  {skipped_count} offres déjà existantes")
    print(f"   📁 Total URLs collectées: {len(urls)}")


if __name__ == "__main__":
    asyncio.run(main())