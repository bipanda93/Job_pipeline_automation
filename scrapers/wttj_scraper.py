from __future__ import annotations

import asyncio
import csv
import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Set
from urllib.parse import quote_plus

from playwright.async_api import async_playwright
from db import get_connection

# ============================================================
# CONFIG
# ============================================================
BASE = "https://www.welcometothejungle.com"
JOBS_SEARCH = f"{BASE}/fr/jobs"

SEARCH_CONFIGS = [
    {"keywords": "data scientist", "label": "data_scientist"},
    {"keywords": "data analyst", "label": "data_analyst"},
    {"keywords": "data engineer", "label": "data_engineer"},
    {"keywords": "data architect", "label": "data_architect"},
    {"keywords": "machine learning", "label": "ml"},
    {"keywords": "big data", "label": "bigdata"},
    {"keywords": "business intelligence", "label": "bi"},
    {"keywords": "analyste données", "label": "analyste"},
    {"keywords": "stage data", "label": "stage_data"},
    {"keywords": "alternance data", "label": "alternance_data"},
]

LOCATION = "France"
CONTRACT_TYPES = ["INTERNSHIP", "APPRENTICESHIP", "FULL_TIME"]  # Stage, Alternance, CDI

HEADLESS = False
SLOW_MO_MS = 0

OUT_DIR = Path("data/raw/wttj")
OUT_DIR.mkdir(parents=True, exist_ok=True)

URLS_FILE = OUT_DIR / "wttj_urls.jsonl"
DETAILS_FILE = OUT_DIR / "wttj_details.jsonl"
CSV_FILE = OUT_DIR / "wttj_details.csv"
STATE_FILE = OUT_DIR / "wttj_state.json"

WAIT_AFTER_PAGE_MS = 2000
MAX_PAGES = 20  # WTTJ a généralement moins de pages que LinkedIn


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


def load_state() -> dict:
    if not STATE_FILE.exists():
        return {"search_idx": 0, "page": 1, "phase": "collect", "detail_idx": 0}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"search_idx": 0, "page": 1, "phase": "collect", "detail_idx": 0}


def save_state(state: dict):
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def export_details_to_csv(details_path: Path, csv_path: Path):
    rows = read_jsonl(details_path)
    if not rows:
        print("⚠️ Aucun détail à exporter.")
        return

    # Dédup par offer_id
    by_id = {}
    for r in rows:
        oid = str(r.get("offer_id") or sha1(r.get("url", "")))
        by_id[oid] = r
    final = list(by_id.values())

    # Export CSV
    import csv as csv_module
    all_keys = set()
    for r in final:
        all_keys.update(r.keys())

    fieldnames = sorted(all_keys)

    with csv_path.open("w", encoding="utf-8", newline="") as f:
        w = csv_module.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(final)

    print(f"📤 CSV exporté: {csv_path} ({len(final)} offres uniques)")


# ============================================================
# BUILD SEARCH URL
# ============================================================
def build_search_url(keywords: str, page: int = 1) -> str:
    """
    WTTJ utilise une structure d'URL différente de LinkedIn
    Format: /fr/jobs?query=data%20scientist&page=1
    """
    params = []

    if keywords:
        params.append(f"query={quote_plus(keywords)}")

    if page > 1:
        params.append(f"page={page}")

    # Filtrer par type de contrat si nécessaire
    # params.append("contract_types[]=INTERNSHIP")
    # params.append("contract_types[]=APPRENTICESHIP")

    query_string = "&".join(params)
    return f"{JOBS_SEARCH}?{query_string}" if query_string else JOBS_SEARCH


# ============================================================
# COLLECT URLs
# ============================================================
async def collect_job_urls_from_wttj(page, label: str) -> List[str]:
    """
    Collecte les URLs des offres d'emploi sur WTTJ
    """
    await page.wait_for_timeout(2000)

    # Sélecteurs possibles pour WTTJ
    # WTTJ utilise des liens vers /fr/companies/{company}/jobs/{job_slug}
    selectors = [
        "a[href*='/jobs/']",
        "[data-testid='job-card'] a",
        ".job-card a",
        "article a[href*='/jobs/']",
    ]

    # Attendre qu'au moins un lien soit présent
    found = False
    for sel in selectors:
        try:
            await page.wait_for_selector(sel, timeout=10000)
            found = True
            break
        except Exception:
            continue

    if not found:
        print(f"   ⚠️ {label}: Aucun sélecteur de job trouvé")
        return []

    await page.wait_for_timeout(1500)

    # Extraire tous les liens
    links = await page.locator("a[href*='/jobs/']").all()
    urls: Set[str] = set()

    for link in links:
        try:
            href = await link.get_attribute("href")
            if not href:
                continue

            # Construire l'URL complète
            if href.startswith("/"):
                href = BASE + href

            # Filtrer uniquement les vraies pages d'offres
            if "/jobs/" in href and "/companies/" in href:
                # Nettoyer l'URL (enlever query params)
                clean_url = href.split("?")[0].split("#")[0]
                urls.add(clean_url)
        except Exception:
            continue

    return list(urls)


def extract_offer_id_from_wttj_url(url: str) -> Optional[str]:
    """
    Extrait l'ID de l'offre depuis l'URL WTTJ
    Format: /fr/companies/{company}/jobs/{job_slug}_{job_id}
    """
    # Chercher le pattern job_id à la fin de l'URL
    match = re.search(r'/jobs/[^/]+_([a-zA-Z0-9-]+)', url)
    if match:
        return match.group(1)

    # Sinon utiliser le hash de l'URL
    return sha1(url)


# ============================================================
# PAGINATION AUTOMATIQUE
# ============================================================
async def has_next_page(page) -> bool:
    """
    Vérifie si une page suivante existe
    WTTJ utilise généralement une pagination avec boutons
    """
    next_selectors = [
        'a[aria-label="Page suivante"]',
        'button[aria-label="Next page"]',
        'a:has-text("Suivant")',
        'button:has-text("Next")',
        '.pagination a[rel="next"]',
    ]

    for selector in next_selectors:
        try:
            btn = page.locator(selector).first
            count = await btn.count()
            if count > 0:
                is_disabled = await btn.get_attribute("disabled")
                if not is_disabled:
                    return True
        except Exception:
            continue

    return False


async def click_next_page(page) -> bool:
    """
    Clique sur le bouton page suivante
    """
    next_selectors = [
        'a[aria-label="Page suivante"]',
        'button[aria-label="Next page"]',
        'a:has-text("Suivant")',
        'button:has-text("Next")',
        '.pagination a[rel="next"]',
    ]

    for selector in next_selectors:
        try:
            btn = page.locator(selector).first
            count = await btn.count()

            if count > 0:
                is_disabled = await btn.get_attribute("disabled")
                if not is_disabled:
                    await btn.click(timeout=5000)
                    await page.wait_for_timeout(WAIT_AFTER_PAGE_MS)
                    return True
        except Exception:
            continue

    return False


# ============================================================
# DETAIL SCRAPE
# ============================================================
async def safe_text(page, selector: str, timeout=5000) -> Optional[str]:
    try:
        loc = page.locator(selector).first
        await loc.wait_for(state="visible", timeout=timeout)
        txt = await loc.text_content()
        return txt.strip() if txt else None
    except Exception:
        return None


async def scrape_wttj_job_detail(page, url: str, meta: dict) -> dict:
    """
    Scrape les détails d'une offre WTTJ - VERSION ROBUSTE
    """
    offer_id = extract_offer_id_from_wttj_url(url)

    try:
        # Navigation avec timeout généreux
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(3000)  # Attente pour le chargement JS

        # Essayer d'attendre les éléments principaux, mais ne pas bloquer
        try:
            # Attendre soit le titre, soit la description - timeout très court
            await page.wait_for_selector(
                "h1, [data-testid='job-title'], .job-title, article",
                timeout=3000  # Réduit de 8s à 3s
            )
        except Exception as e:
            # Pas grave, on continue
            pass

        # Extraction avec plusieurs sélecteurs possibles et timeouts TRÈS courts
        title = None
        for sel in ["h1", "[data-testid='job-title']", ".job-title", "h1.sc-"]:
            title = await safe_text(page, sel, timeout=2000)  # 2s au lieu de 3s
            if title:
                break

        company = None
        for sel in ["[data-testid='company-name']", ".company-name", "a[href*='/companies/'] h2",
                    "a[href*='/companies/']"]:
            company = await safe_text(page, sel, timeout=2000)
            if company:
                break

        location = None
        for sel in ["[data-testid='job-location']", ".location", "span:has-text('📍')", "[class*='location']"]:
            location = await safe_text(page, sel, timeout=2000)
            if location:
                break

        contract_type = None
        for sel in ["[data-testid='contract-type']", ".contract-type", "[class*='contract']"]:
            contract_type = await safe_text(page, sel, timeout=2000)
            if contract_type:
                break

        # Description - essayer plusieurs sélecteurs
        description = None
        for sel in [
            "[data-testid='job-description']",
            ".job-description",
            "article",
            "[class*='description']",
            "main article",
            "div[class*='JobDescription']"
        ]:
            description = await safe_text(page, sel, timeout=3000)  # 3s max
            if description and len(description) > 100:
                break

        # Salaire (optionnel) - timeout très court
        salary = None
        for sel in ["[data-testid='salary']", "[class*='salary']", "span:has-text('€')"]:
            salary = await safe_text(page, sel, timeout=1000)  # 1s
            if salary:
                break

        # Date de publication (optionnel) - timeout très court
        posted = None
        for sel in ["[data-testid='published-at']", "[class*='published']", "time"]:
            posted = await safe_text(page, sel, timeout=1000)  # 1s
            if posted:
                break

        # Vérifier qu'on a au moins le titre OU la description
        if not title and not description:
            print(f"   ⚠️ Aucune donnée extraite pour {offer_id} - {url}")
            # Sauvegarder le HTML pour debug
            try:
                html = await page.content()
                debug_file = OUT_DIR / f"debug_{offer_id}.html"
                debug_file.write_text(html[:50000], encoding='utf-8')  # Premiers 50KB
                print(f"   🐛 HTML sauvegardé : {debug_file}")
            except:
                pass

        return {
            "offer_id": offer_id,
            "url": url,
            "title": title or "N/A",
            "company": company or "N/A",
            "location": location or "N/A",
            "contract_type": contract_type or "N/A",
            "salary": salary or "N/A",
            "posted_at": posted or "N/A",
            "raw_text": description or "",
            "scraped_at": now_iso(),
            "source": "welcometothejungle",
            **meta,
        }

    except Exception as e:
        print(f"   ❌ Erreur scraping {offer_id}: {str(e)[:100]}")
        return {
            "offer_id": offer_id,
            "url": url,
            "title": "ERROR",
            "company": "ERROR",
            "location": "N/A",
            "contract_type": "N/A",
            "salary": "N/A",
            "posted_at": "N/A",
            "raw_text": "",
            "error": str(e)[:200],
            "scraped_at": now_iso(),
            "source": "welcometothejungle",
            **meta,
        }

# ============================================================
# Connexion PostgreSQL
# ============================================================

def insert_job_to_db(job: dict):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO raw_wttj_jobs
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

        print("🌐 Chargement de Welcome to the Jungle...")
        await page.goto(BASE, wait_until="networkidle", timeout=60000)
        await page.wait_for_timeout(2000)

        # ===== Phase 1: Collect URLs =====
        if phase == "collect":
            for sidx in range(int(state.get("search_idx", 0)), len(SEARCH_CONFIGS)):
                cfg = SEARCH_CONFIGS[sidx]
                label = cfg["label"]
                kw = cfg["keywords"]

                print("\n" + "=" * 70)
                print(f"📍 {sidx + 1}/{len(SEARCH_CONFIGS)}: {label}")
                print("=" * 70)

                # Aller sur la première page de recherche
                search_url = build_search_url(kw, page=1)
                await page.goto(search_url, wait_until="networkidle", timeout=60000)
                await page.wait_for_timeout(2000)

                page_num = 1

                # Boucle de pagination
                while page_num <= MAX_PAGES:
                    state.update({"search_idx": sidx, "page": page_num, "phase": "collect"})
                    save_state(state)

                    # Collecter les URLs
                    urls = await collect_job_urls_from_wttj(page, label)

                    if not urls:
                        print(f"   ⚠️ {label} page {page_num}: Aucune URL")
                        break

                    # Sauvegarder
                    new_count = 0
                    for u in urls:
                        if u not in seen_urls:
                            seen_urls.add(u)
                            append_jsonl(URLS_FILE, {
                                "url": u,
                                "offer_id": extract_offer_id_from_wttj_url(u),
                                "search_label": label,
                                "search_keywords": kw,
                                "page": page_num,
                                "collected_at": now_iso(),
                                "source": "welcometothejungle",
                            })
                            new_count += 1

                    print(f"   ✅ {label} page {page_num}: +{new_count} nouvelles offres (total: {len(seen_urls)})")

                    # Essayer de passer à la page suivante
                    has_next = await has_next_page(page)

                    if not has_next:
                        print(f"   ✓ {label}: Fin de la pagination")
                        break

                    clicked = await click_next_page(page)

                    if not clicked:
                        print(f"   ⚠️ {label}: Impossible de cliquer sur 'Suivant'")
                        break

                    page_num += 1
                    print(f"   ➡️  Page {page_num}...")

                # Reset page pour prochaine recherche
                state["page"] = 1
                save_state(state)

            # Phase suivante
            state["phase"] = "detail"
            state["detail_idx"] = 0
            save_state(state)

        # ===== Phase 2: Details =====
        urls_rows = read_jsonl(URLS_FILE)
        all_urls = [r["url"] for r in urls_rows if r.get("url")]

        print("\n" + "=" * 70)
        print(f"🧾 Phase DETAIL: {len(all_urls)} urls")
        print(f"📊 Déjà scrapés: {len(seen_details)}")
        print(f"📊 Restants: {len(all_urls) - len(seen_details)}")
        print("=" * 70)

        start_detail = int(state.get("detail_idx", 0))
        success_count = 0
        error_count = 0

        for i in range(start_detail, len(all_urls)):
            url = all_urls[i]
            offer_id = extract_offer_id_from_wttj_url(url)

            state["detail_idx"] = i
            state["phase"] = "detail"
            save_state(state)

            if offer_id in seen_details:
                print(f"⊘ {i + 1}/{len(all_urls)} - {offer_id} déjà scrapé")
                continue

            meta = {}
            try:
                meta = next(r for r in urls_rows if r.get("url") == url)
            except StopIteration:
                meta = {}

            try:
                data = await scrape_wttj_job_detail(page, url, {
                    "search_label": meta.get("search_label"),
                    "search_keywords": meta.get("search_keywords"),
                })
                insert_job_to_db(data)
                seen_details.add(data["offer_id"])
                success_count += 1

                title = (data.get('title') or 'Sans titre')[:60]
                company = (data.get('company') or 'N/A')[:30]
                contract = data.get('contract_type', 'N/A')
                print(f"✅ {i + 1}/{len(all_urls)} - {title} @ {company} ({contract})")

                await page.wait_for_timeout(800)

            except Exception as e:
                error_count += 1
                print(f"❌ {i + 1}/{len(all_urls)} - Erreur {offer_id}: {str(e)[:100]}")

                insert_job_to_db({
                    "offer_id": offer_id,
                    "url": url,
                    "error": str(e),
                    "scraped_at": now_iso(),
                    "source": "welcometothejungle",
                    "search_label": meta.get("search_label"),
                    "search_keywords": meta.get("search_keywords"),
                })
                seen_details.add(offer_id)

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