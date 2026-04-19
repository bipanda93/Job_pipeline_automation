from __future__ import annotations

import asyncio
import csv
import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from urllib.parse import quote_plus, urlparse, parse_qs

from playwright.async_api import async_playwright
from db import get_connection

# ============================================================
# CONFIG
# ============================================================
BASE = "https://www.linkedin.com"
JOBS_SEARCH = "https://www.linkedin.com/jobs/search/"

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

LOCATION = "France"
GEO_ID = ""
TIME_RANGE = ""

RESULTS_PER_PAGE = 25
MAX_PAGES_PER_SEARCH = 40

HEADLESS = False
SLOW_MO_MS = 0

OUT_DIR = Path("data/raw/linkedin")
OUT_DIR.mkdir(parents=True, exist_ok=True)

URLS_FILE = OUT_DIR / "linkedin_urls.jsonl"
DETAILS_FILE = OUT_DIR / "linkedin_details.jsonl"
CSV_FILE = OUT_DIR / "linkedin_details.csv"
STATE_FILE = OUT_DIR / "linkedin_state.json"
DEBUG_DIR = OUT_DIR / "debug"
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

# Anti-ban / stabilité
WAIT_AFTER_PAGE_MS = 2000
WAIT_AFTER_CARD_CLICK_MS = 500


# ============================================================
# UTILS
# ============================================================
def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()


def append_jsonl(path: Path, obj: dict):
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def read_jsonl(path: Path) -> List[dict]:
    if not path.exists():
        return []
    out = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                out.append(json.loads(line))
            except Exception:
                continue
    return out


def read_seen_offer_ids(path: Path) -> Set[str]:
    seen: Set[str] = set()
    for row in read_jsonl(path):
        oid = row.get("offer_id")
        if oid:
            seen.add(str(oid))
    return seen


def export_details_jsonl_to_csv(details_path: Path, csv_path: Path):
    rows = read_jsonl(details_path)
    if not rows:
        print("⚠️ Aucun détail à exporter.")
        return

    # dédup par offer_id
    by_id: Dict[str, dict] = {}
    for r in rows:
        oid = str(r.get("offer_id") or sha1(r.get("url", "")))
        by_id[oid] = r
    final = list(by_id.values())

    fieldnames = sorted({k for r in final for k in r.keys()})
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(final)

    print(f"📤 CSV exporté: {csv_path} ({len(final)} offres uniques)")


def load_state() -> dict:
    if not STATE_FILE.exists():
        return {"search_idx": 0, "page_idx": 0, "phase": "collect", "detail_idx": 0}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"search_idx": 0, "page_idx": 0, "phase": "collect", "detail_idx": 0}


def save_state(state: dict):
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


# ============================================================
# LOGIN / CHALLENGE (MANUEL)
# ============================================================
def looks_like_login_or_challenge(url: str, html: str) -> bool:
    u = (url or "").lower()
    h = (html or "").lower()
    if "linkedin.com/login" in u:
        return True
    if "checkpoint" in u or "challenge" in u:
        return True
    if "captcha" in h or "verify" in h and "linkedin" in h:
        return True
    return False


async def require_manual_if_needed(page, target_url: str, label: str):
    try:
        html = await page.content()
    except Exception:
        html = ""

    if not looks_like_login_or_challenge(page.url, html):
        return

    print("\n" + "=" * 90)
    print(f"🛑 LinkedIn demande une connexion / vérification ({label}).")
    print(f"➡️ URL actuelle: {page.url}")
    print("➡️ Connecte-toi / valide la vérification dans le navigateur.")
    print("➡️ Ensuite reviens au terminal et appuie sur Entrée.")
    print(f"🎯 Je relancerai ensuite l'URL cible:\n   {target_url}")
    print("=" * 90 + "\n")

    # debug
    try:
        (DEBUG_DIR / "challenge.html").write_text(await page.content(), encoding="utf-8")
        await page.screenshot(path=str(DEBUG_DIR / "challenge.png"), full_page=True)
        print("🧩 Debug: debug/challenge.html + debug/challenge.png")
    except Exception:
        pass

    await asyncio.to_thread(input, "✅ Entrée pour reprendre... ")

    # relance cible
    await page.goto(target_url, wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_timeout(1200)


# ============================================================
# BUILD SEARCH URL (start pagination)
# ============================================================
def build_search_url(keywords: str, start: int = 0) -> str:
    q = quote_plus(keywords)
    loc = quote_plus(LOCATION) if LOCATION else ""
    params = [f"keywords={q}"]
    if start > 0:
        params.append(f"start={start}")
    if loc:
        params.append(f"location={loc}")
    if GEO_ID:
        params.append(f"geoId={quote_plus(GEO_ID)}")

    return JOBS_SEARCH + "?" + "&".join(params)


# ============================================================
# EXTRACT: collect job URLs from listing
# ============================================================
async def collect_job_urls_from_listing(page) -> List[str]:
    """
    Stratégie:
    - attendre qu'un container de résultats apparaisse
    - extraire tous les liens vers /jobs/view/...
    - normaliser
    """
    candidates = [
        "a[href*='/jobs/view/']",
        "a.job-card-list__title",
        "a.jobs-search-results__list-item-link",
        "a[data-control-id*='job_card']",
    ]

    ok = False
    for sel in candidates:
        try:
            await page.wait_for_selector(sel, timeout=15000)
            ok = True
            break
        except Exception:
            continue
    if not ok:
        return []

    links = await page.locator("a[href*='/jobs/view/']").all()
    urls: Set[str] = set()
    for a in links:
        try:
            href = await a.get_attribute("href")
            if not href:
                continue
            if href.startswith("/"):
                href = BASE + href
            href = href.split("?")[0]
            if "/jobs/view/" in href:
                urls.add(href)
        except Exception:
            continue
    return list(urls)


def extract_offer_id_from_job_url(url: str) -> Optional[str]:
    m = re.search(r"/jobs/view/(\d+)", url)
    return m.group(1) if m else None


# ============================================================
# PAGINATION AUTOMATIQUE
# ============================================================
async def click_next_button(page) -> bool:
    """
    Clique sur le bouton "Suivant" s'il existe et n'est pas désactivé
    Retourne True si succès, False sinon
    """
    next_button_selectors = [
        'button[aria-label="Suivant"]',
        'button:has-text("Suivant")',
        'button .artdeco-button__text:has-text("Suivant")',
        'button[aria-label="Next"]',
        'button:has-text("Next")',
        '.artdeco-pagination__button--next',
    ]

    for selector in next_button_selectors:
        try:
            button = page.locator(selector).first
            count = await button.count()

            if count > 0:
                # Vérifier si le bouton n'est pas désactivé
                is_disabled = await button.get_attribute("disabled")
                if is_disabled:
                    return False

                # Cliquer sur le bouton
                await button.click(timeout=5000)
                await page.wait_for_timeout(WAIT_AFTER_PAGE_MS)
                return True
        except Exception:
            continue

    return False


# ============================================================
# DETAIL scrape (optionnel)
# ============================================================
async def safe_text(page, selector: str, timeout=8000) -> Optional[str]:
    try:
        loc = page.locator(selector).first
        await loc.wait_for(state="visible", timeout=timeout)
        txt = await loc.text_content()
        return txt.strip() if txt else None
    except Exception:
        return None


async def scrape_job_detail(page, url: str, meta: dict) -> dict:
    """
    Scrape les détails d'une offre - VERSION AUTOMATIQUE
    Continue automatiquement même si certains champs sont manquants
    """
    offer_id = extract_offer_id_from_job_url(url) or sha1(url)

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(1500)

        # Vérification login (mais sans bloquer)
        try:
            html = await page.content()
            if looks_like_login_or_challenge(page.url, html):
                # Auto-retry une fois
                print(f"   ⚠️ Challenge détecté pour {offer_id}, retry...")
                await page.wait_for_timeout(2000)
                await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_timeout(1500)
        except Exception:
            pass

        # Extraction avec timeouts réduits (pas de blocage)
        title = await safe_text(page, "h1", timeout=5000)
        company = await safe_text(page, "a.topcard__org-name-link, .topcard__org-name-link", timeout=3000)
        location = await safe_text(page, ".topcard__flavor--bullet, .jobs-unified-top-card__bullet", timeout=3000)
        posted = await safe_text(page, "span.posted-time-ago__text, span.jobs-unified-top-card__posted-date",
                                 timeout=3000)
        description = await safe_text(page, "#job-details, .jobs-description__content, .jobs-box__html-content",
                                      timeout=5000)

        return {
            "offer_id": offer_id,
            "url": url,
            "title": title or "N/A",
            "company": company or "N/A",
            "location": location or "N/A",
            "posted_at": posted or "N/A",
            "raw_text": description or "",
            "scraped_at": now_iso(),
            "source": "linkedin",
            **meta,
        }
    except Exception as e:
        # Retourner un objet minimal même en cas d'erreur
        return {
            "offer_id": offer_id,
            "url": url,
            "title": "ERROR",
            "company": "ERROR",
            "location": "N/A",
            "posted_at": "N/A",
            "raw_text": "",
            "error": str(e)[:200],
            "scraped_at": now_iso(),
            "source": "linkedin",
            **meta,
        }

#============================================================
# Connexion PostgreSQL
# ============================================================

def insert_job_to_db(job: dict):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO raw_linkedin_jobs
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
    state = load_state()
    phase = state.get("phase", "collect")

    seen_urls: Set[str] = set()
    for row in read_jsonl(URLS_FILE):
        u = row.get("url")
        if u:
            seen_urls.add(u)

    seen_details = read_seen_offer_ids(DETAILS_FILE)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS, slow_mo=SLOW_MO_MS)
        page = await browser.new_page()
        await page.set_extra_http_headers({"Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8"})

        await page.goto(BASE, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(1000)

        # ===== Phase 1: Collect URLs avec PAGINATION AUTOMATIQUE
        if phase == "collect":
            for sidx in range(int(state.get("search_idx", 0)), len(SEARCH_CONFIGS)):
                cfg = SEARCH_CONFIGS[sidx]
                label = cfg["label"]
                kw = cfg["keywords"]

                print("\n" + "=" * 70)
                print(f"📍 {sidx + 1}/{len(SEARCH_CONFIGS)}: {label}")
                print("=" * 70)

                # Aller sur la première page
                search_url = build_search_url(kw, start=0)
                await page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_timeout(1500)
                await require_manual_if_needed(page, target_url=search_url, label=f"SEARCH {label}")

                pidx = 0

                # Boucle de pagination automatique
                while pidx < MAX_PAGES_PER_SEARCH:
                    state.update({"search_idx": sidx, "page_idx": pidx, "phase": "collect"})
                    save_state(state)

                    # Collecter les URLs de la page actuelle
                    urls = await collect_job_urls_from_listing(page)

                    if not urls:
                        dbg = DEBUG_DIR / f"{label}_p{pidx + 1}.html"
                        try:
                            dbg.write_text(await page.content(), encoding="utf-8")
                            await page.screenshot(path=str(DEBUG_DIR / f"{label}_p{pidx + 1}.png"), full_page=True)
                        except Exception:
                            pass
                        print(f"⚠️ {label} page {pidx + 1}: 0 url (stop pagination)")
                        break

                    # Sauvegarder les URLs
                    new_count = 0
                    for u in urls:
                        if u not in seen_urls:
                            seen_urls.add(u)
                            append_jsonl(URLS_FILE, {
                                "url": u,
                                "offer_id": extract_offer_id_from_job_url(u) or sha1(u),
                                "search_label": label,
                                "search_keywords": kw,
                                "page": pidx + 1,
                                "collected_at": now_iso(),
                                "source": "linkedin",
                            })
                            new_count += 1

                    print(f"✅ {label} page {pidx + 1}: +{new_count} nouvelles offres (total: {len(seen_urls)})")

                    # Essayer de cliquer sur "Suivant"
                    has_next = await click_next_button(page)

                    if not has_next:
                        print(f"✓ {label}: Fin de la pagination (plus de bouton 'Suivant')")
                        break

                    pidx += 1
                    print(f"➡️  Passage à la page {pidx + 1}...")

                # Reset page_idx pour le prochain keyword
                state["page_idx"] = 0
                save_state(state)

            # Phase suivante
            state["phase"] = "detail"
            state["detail_idx"] = 0
            save_state(state)

        # ===== Phase 2: Details (AUTOMATIQUE)
        urls_rows = read_jsonl(URLS_FILE)
        all_urls = [r["url"] for r in urls_rows if r.get("url")]

        print("\n" + "=" * 70)
        print(f"🧾 Phase DETAIL: {len(all_urls)} urls à scraper")
        print(f"📊 Déjà scrapés: {len(seen_details)}")
        print(f"📊 Restants: {len(all_urls) - len(seen_details)}")
        print("=" * 70)

        start_detail = int(state.get("detail_idx", 0))
        success_count = 0
        error_count = 0

        for i in range(start_detail, len(all_urls)):
            url = all_urls[i]
            offer_id = extract_offer_id_from_job_url(url) or sha1(url)

            state["detail_idx"] = i
            state["phase"] = "detail"
            save_state(state)

            if offer_id in seen_details:
                print(f"⊘ {i + 1}/{len(all_urls)} - {offer_id} déjà scrapé, skip")
                continue

            meta = {}
            try:
                meta = next(r for r in urls_rows if r.get("url") == url)
            except StopIteration:
                meta = {}

            try:
                data = await scrape_job_detail(page, url, {
                    "search_label": meta.get("search_label"),
                    "search_keywords": meta.get("search_keywords"),
                })
                insert_job_to_db(data)
                seen_details.add(data["offer_id"])
                success_count += 1

                title = (data.get('title') or 'Sans titre')[:60]
                company = (data.get('company') or 'N/A')[:30]
                print(f"✅ {i + 1}/{len(all_urls)} - {offer_id} - {title} @ {company}")

                # Pause automatique entre chaque job
                await page.wait_for_timeout(WAIT_AFTER_CARD_CLICK_MS)

            except Exception as e:
                error_count += 1
                print(f"❌ {i + 1}/{len(all_urls)} - Erreur sur {offer_id}: {str(e)[:100]}")

                # Sauvegarder quand même avec erreur
                insert_job_to_db({
                    "offer_id": offer_id,
                    "url": url,
                    "error": str(e),
                    "scraped_at": now_iso(),
                    "source": "linkedin",
                    "search_label": meta.get("search_label"),
                    "search_keywords": meta.get("search_keywords"),
                })
                seen_details.add(offer_id)

                # Continuer automatiquement même en cas d'erreur
                await page.wait_for_timeout(1000)

        print("\n" + "=" * 70)
        print(f"✅ Phase DETAIL terminée:")
        print(f"   📊 Succès: {success_count}")
        print(f"   ❌ Erreurs: {error_count}")
        print(f"   📊 Total: {len(seen_details)}")
        print("=" * 70)

        await browser.close()

    print("✅ Terminé")


if __name__ == "__main__":
    asyncio.run(main())