#!/usr/bin/env python3
"""
HelloWork Scraper - VERSION COMPLÈTE ET AMÉLIORÉE (URLs corrigées)
"""

from __future__ import annotations

import asyncio
import json
import csv
import re
import shutil
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus

from playwright.async_api import async_playwright

# ============================================================================
# CONFIGURATION
# ============================================================================

# Répertoire du script
SCRIPT_DIR = Path(__file__).resolve().parent

BASE_DIR = SCRIPT_DIR / "data" / "raw" / "hellowork"
BASE_DIR.mkdir(parents=True, exist_ok=True)

URLS_FILE = BASE_DIR / "hellowork_urls.jsonl"
DETAILS_FILE = BASE_DIR / "hellowork_details.jsonl"
STATE_FILE = BASE_DIR / "hellowork_state.json"
CSV_FILE = BASE_DIR / "hellowork_details.csv"

MAX_PAGES = 50
MAX_EMPTY_PAGES = 3
WAIT_AFTER_PAGE_MS = 4000
WAIT_BETWEEN_DETAILS_MS = 2000

SEARCH_CONFIG = [
    {"label": "data_scientist", "keywords": "data scientist"},
    {"label": "data_analyst", "keywords": "data analyst"},
    {"label": "data_engineer", "keywords": "data engineer"},
    {"label": "data_architect", "keywords": "data architect"},
    {"label": "ml", "keywords": "machine learning"},
    {"label": "bigdata", "keywords": "big data"},
    {"label": "bi", "keywords": "business intelligence"},
    {"label": "analyste", "keywords": "analyste données"},
    {"label": "stage_data", "keywords": "stage data"},
    {"label": "alternance_data", "keywords": "alternance data"},
]


# ============================================================================
# FONCTIONS UTILITAIRES
# ============================================================================

def read_jsonl(filepath: Path):
    """Lit un fichier JSONL et retourne une liste de dictionnaires"""
    if not filepath.exists():
        return []
    with filepath.open("r", encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def write_jsonl(filepath: Path, data):
    """Écrit des données dans un fichier JSONL"""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with filepath.open("w", encoding="utf-8") as f:
        for item in data:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")


def append_jsonl(filepath: Path, item: dict):
    """Ajoute une ligne à un fichier JSONL"""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with filepath.open("a", encoding="utf-8") as f:
        f.write(json.dumps(item, ensure_ascii=False) + "\n")


def save_state(state: dict):
    """Sauvegarde l'état du scraping"""
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with STATE_FILE.open("w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def load_state() -> dict:
    """Charge l'état du scraping"""
    if not STATE_FILE.exists():
        return {"phase": "collect", "search_index": 0, "page_num": 1}
    with STATE_FILE.open("r", encoding="utf-8") as f:
        return json.load(f)


def is_valid_job_url(url: str) -> bool:
    """
    Valide les vraies offres HelloWork.

    Format actuel observé :
      https://www.hellowork.com/fr-fr/emplois/74671705.html

    On accepte : /fr-fr/emplois/<digits>.html
    """
    if not url or not isinstance(url, str):
        return False

    # Normaliser en chemin relatif
    if url.startswith("https://www.hellowork.com"):
        path = url.split("https://www.hellowork.com", 1)[1]
    else:
        path = url

    # Pattern strict : /fr-fr/emplois/12345678.html
    return bool(re.match(r"^/fr-fr/emplois/\d+\.html$", path))


def build_search_url(keywords: str, location: str = "France", page: int | None = None) -> str:
    """Construit l'URL de recherche HelloWork"""
    base = "https://www.hellowork.com/fr-fr/emploi.html"
    k = quote_plus(keywords)
    l = quote_plus(location)

    if page and page > 1:
        return f"{base}?k={k}&l={l}&p={page}"
    else:
        return f"{base}?k={k}&l={l}"


# ============================================================================
# COLLECTE DES URLs
# ============================================================================

async def accept_cookies(page):
    """Accepte les cookies si nécessaire (simple mais efficace)"""
    try:
        await page.wait_for_timeout(1500)
        cookie_button = page.locator('button:has-text("Accepter")')
        if await cookie_button.count() > 0:
            await cookie_button.first.click()
            await page.wait_for_timeout(1000)
            print("   ✅ Cookies acceptés")
    except Exception:
        pass


async def collect_job_urls_from_hellowork(page, label: str, debug: bool = False):
    """
    Collecte les URLs d'offres sur une page de résultats HelloWork.
    Retourne une liste de dicts : {"url": full_url, "label": label}
    """
    try:
        # Attendre que la page soit chargée
        await page.wait_for_timeout(1500)

        # Nouveau sélecteur adapté : /fr-fr/emplois/<id>.html
        links = await page.locator('a[href*="/fr-fr/emplois/"][href$=".html"]').all()
        all_hrefs = []
        urls = []
        rejected = []

        for link in links:
            try:
                href = await link.get_attribute("href")
                if not href:
                    continue
                all_hrefs.append(href)

                if is_valid_job_url(href):
                    if href.startswith("http"):
                        full_url = href
                    else:
                        full_url = f"https://www.hellowork.com{href}"
                    urls.append({"url": full_url, "label": label})
                else:
                    if debug and len(rejected) < 10:
                        rejected.append(href)
            except Exception:
                continue

        print(f"   📊 {len(all_hrefs)} liens trouvés")
        print(f"   ✅ {len(urls)} vraies offres validées")

        if debug:
            print("   🔎 Exemples d'URLs VALIDE(S) :")
            for u in urls[:5]:
                print(f"      ✔ {u['url']}")
            if rejected:
                print("   🔎 Exemples d'URLs REJETÉES :")
                for r in rejected[:5]:
                    print(f"      ✖ {r}")

        return urls

    except Exception as e:
        print(f"   ❌ Erreur collecte: {e}")
        return []


async def scrape_urls_for_search(page, search_config: dict):
    """Scrape les URLs pour une recherche donnée"""
    label = search_config["label"]
    keywords = search_config["keywords"]

    # Charger les URLs déjà collectées (globales)
    existing_urls = read_jsonl(URLS_FILE)
    existing_url_set = {item["url"] for item in existing_urls}

    page_num = 1
    empty_pages_count = 0
    total_new_urls = 0

    while page_num <= MAX_PAGES:
        search_url = build_search_url(keywords, page=page_num)

        print(f"   🔗 Page {page_num}: {search_url}")

        try:
            await page.goto(search_url, wait_until="networkidle", timeout=60000)
            await accept_cookies(page)
            await page.wait_for_timeout(1000)

            print(f"   🔍 Collecte des URLs...")
            urls = await collect_job_urls_from_hellowork(
                page,
                label,
                debug=(page_num == 1)  # debug sur la 1re page de chaque recherche
            )

            # Filtrer les nouvelles URLs
            new_urls = [u for u in urls if u["url"] not in existing_url_set]

            if new_urls:
                for url_item in new_urls:
                    append_jsonl(URLS_FILE, url_item)
                    existing_url_set.add(url_item["url"])

                total_new_urls += len(new_urls)
                empty_pages_count = 0
                print(f"   ✅ Page {page_num}: +{len(new_urls)} nouvelles | Total: {total_new_urls} offres")
            else:
                empty_pages_count += 1
                print(f"   ✅ Page {page_num}: +0 nouvelles | Total: {total_new_urls} offres (page vide #{empty_pages_count})")

                if empty_pages_count >= MAX_EMPTY_PAGES:
                    print(f"   ⚠️  {MAX_EMPTY_PAGES} pages sans nouvelles offres - arrêt de la pagination pour {label}")
                    break

            page_num += 1
            await page.wait_for_timeout(WAIT_AFTER_PAGE_MS)

        except Exception as e:
            print(f"   ❌ Erreur page {page_num}: {e}")
            empty_pages_count += 1
            if empty_pages_count >= MAX_EMPTY_PAGES:
                print(f"   ⚠️ Trop d'erreurs/pages vides - arrêt pagination pour {label}")
                break
            page_num += 1


async def collect_all_urls():
    """Collecte toutes les URLs pour tous les mots-clés"""
    async with async_playwright() as p:
        # Tu peux passer à chromium ici si tu veux :
        # browser = await p.chromium.launch(headless=False)
        print("🌐 Lancement de Safari (WebKit)...")
        browser = await p.webkit.launch(headless=False)
        page = await browser.new_page()

        # Accepter les cookies une fois
        print("🌐 Chargement de HelloWork (homepage)...")
        await page.goto("https://www.hellowork.com/fr-fr/", wait_until="networkidle")
        await accept_cookies(page)

        # Scraper chaque recherche
        for i, search_config in enumerate(SEARCH_CONFIG, 1):
            print(f"\n{'=' * 70}")
            print(f"📍 {i}/{len(SEARCH_CONFIG)}: {search_config['label']} ({search_config['keywords']})")
            print(f"{'=' * 70}")

            await scrape_urls_for_search(page, search_config)

        await browser.close()


# ============================================================================
# EXTRACTION DES DÉTAILS
# ============================================================================

async def safe_get_text(page, selector: str, default: str = "") -> str:
    """Récupère le texte d'un élément de manière sécurisée"""
    try:
        element = page.locator(selector).first
        if await element.count() > 0:
            text = await element.text_content()
            return text.strip() if text else default
    except Exception:
        pass
    return default


async def scrape_job_details(page, url: str) -> dict:
    """Extrait les détails d'une offre d'emploi HelloWork"""
    try:
        await page.goto(url, wait_until="networkidle", timeout=60000)
        await page.wait_for_timeout(2000)

        # Extraire le titre
        title = await safe_get_text(page, "h1", "Non spécifié")

        # Extraire l'entreprise
        company = await safe_get_text(page, '[data-testid="job-company-name"]', "")
        if not company:
            company = await safe_get_text(page, ".company-name", "")
        if not company:
            company = await safe_get_text(page, '[class*="company"]', "Non spécifié")

        # Extraire la localisation
        location = await safe_get_text(page, '[data-testid="job-location"]', "")
        if not location:
            location = await safe_get_text(page, '[class*="location"]', "")
        if not location:
            location = await safe_get_text(page, '[class*="city"]', "Non spécifié")

        # Extraire le type de contrat
        contract_type = await safe_get_text(page, '[data-testid="job-contract-type"]', "")
        if not contract_type:
            contract_type = await safe_get_text(page, '[class*="contract"]', "Non spécifié")

        # Extraire le salaire
        salary = await safe_get_text(page, '[data-testid="job-salary"]', "")
        if not salary:
            salary = await safe_get_text(page, '[class*="salary"]', "Non spécifié")

        # Extraire la description
        description = await safe_get_text(page, '[data-testid="job-description"]', "")
        if not description:
            description = await safe_get_text(page, '[class*="description"]', "")
        if not description:
            description = await safe_get_text(page, "article", "Non disponible")

        # Extraire la date de publication
        published_date = await safe_get_text(page, '[data-testid="job-published-date"]', "")
        if not published_date:
            published_date = await safe_get_text(page, '[class*="date"]', "Non spécifié")

        # Tags / compétences (facultatif)
        tags = []
        try:
            tag_elements = await page.locator('[data-testid="job-tag"], [class*="tag"], [class*="skill"]').all()
            for tag_elem in tag_elements[:10]:
                tag_text = await tag_elem.text_content()
                if tag_text:
                    tags.append(tag_text.strip())
        except Exception:
            pass

        result = {
            "url": url,
            "title": title,
            "company": company,
            "location": location,
            "contract_type": contract_type,
            "salary": salary,
            "description": description[:500] if description else "",
            "published_date": published_date,
            "tags": ", ".join(tags) if tags else "",
            "scraped_at": datetime.now().isoformat(),
            "scraped": True,
            "error": None,
        }

        return result

    except Exception as e:
        return {
            "url": url,
            "title": None,
            "company": None,
            "location": None,
            "contract_type": None,
            "salary": None,
            "description": None,
            "published_date": None,
            "tags": None,
            "scraped_at": datetime.now().isoformat(),
            "scraped": False,
            "error": str(e),
        }


async def scrape_all_details():
    """Scrape les détails de toutes les URLs collectées"""
    urls_data = read_jsonl(URLS_FILE)
    details_data = read_jsonl(DETAILS_FILE)

    scraped_urls = {item["url"] for item in details_data}
    remaining = [u for u in urls_data if u["url"] not in scraped_urls]

    print(f"\n{'=' * 70}")
    print(f"🧾 Phase DETAIL: {len(urls_data)} urls")
    print(f"📊 Déjà scrapés: {len(scraped_urls)}")
    print(f"📊 Restants: {len(remaining)}")
    print(f"{'=' * 70}\n")

    if not remaining:
        print("✅ Tous les détails sont déjà collectés")
        return

    async with async_playwright() as p:
        print("🌐 Lancement de Safari pour extraction détails...")
        # Tu peux aussi mettre chromium ici si tu préfères :
        # browser = await p.chromium.launch(headless=False)
        browser = await p.webkit.launch(headless=False)
        page = await browser.new_page()

        success = 0
        errors = 0

        for i, url_item in enumerate(remaining, 1):
            url = url_item["url"]
            label = url_item["label"]

            print(f"\n📄 [{i}/{len(remaining)}] {label}")
            print(f"   🔗 {url}")

            details = await scrape_job_details(page, url)
            details["label"] = label

            append_jsonl(DETAILS_FILE, details)

            if details.get("scraped"):
                success += 1
                print(f"   ✅ Titre: {details.get('title', 'N/A')}")
                print(f"   ✅ Entreprise: {details.get('company', 'N/A')}")
                print(f"   ✅ Lieu: {details.get('location', 'N/A')}")
            else:
                errors += 1
                print(f"   ❌ Erreur: {details.get('error', 'Inconnue')}")

            await page.wait_for_timeout(WAIT_BETWEEN_DETAILS_MS)

        await browser.close()

        print(f"\n{'=' * 70}")
        print(f"✅ Phase DETAIL terminée:")
        print(f"   📊 Succès: {success}")
        print(f"   ❌ Erreurs: {errors}")
        print(f"   📊 Total: {len(scraped_urls) + success}")
        print(f"{'=' * 70}")


# ============================================================================
# EXPORT CSV
# ============================================================================

def export_to_csv():
    """Exporte les détails vers un CSV"""
    details = read_jsonl(DETAILS_FILE)

    if not details:
        print("⚠️  Aucune donnée à exporter")
        return

    # Dédupliquer par URL
    unique_details = {}
    for item in details:
        url = item.get("url")
        if url:
            unique_details[url] = item

    details = list(unique_details.values())

    if not details:
        print("⚠️  Aucune donnée valide à exporter")
        return

    fieldnames = [
        "url",
        "title",
        "company",
        "location",
        "contract_type",
        "salary",
        "description",
        "published_date",
        "tags",
        "label",
        "scraped",
        "error",
        "scraped_at",
    ]

    with CSV_FILE.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for item in details:
            writer.writerow(item)

    print(f"📤 CSV exporté: {CSV_FILE} ({len(details)} offres uniques)")
    print(f"📂 Chemin absolu : {CSV_FILE.resolve()}")


# ============================================================================
# FONCTION PRINCIPALE
# ============================================================================

async def main(force_restart: bool = False):
    """Fonction principale"""

    # Réinitialisation complète si demandé
    if force_restart:
        print("\n🔄 REDÉMARRAGE - Réinitialisation de l'état...")

        if BASE_DIR.exists():
            shutil.rmtree(BASE_DIR)
            print("   ✅ Dossier supprimé")

        BASE_DIR.mkdir(parents=True, exist_ok=True)
        print("   ✅ Dossier recréé\n")

    # Phase 1: Collecte des URLs
    print("\n" + "=" * 70)
    print("🚀 Phase 1: COLLECTE DES URLs")
    print("=" * 70)
    await collect_all_urls()

    save_state({"phase": "detail"})

    # Phase 2: Extraction des détails
    print("\n" + "=" * 70)
    print("🚀 Phase 2: EXTRACTION DES DÉTAILS")
    print("=" * 70)
    await scrape_all_details()

    save_state({"phase": "export"})

    # Phase 3: Export CSV
    print("\n" + "=" * 70)
    print("🚀 Phase 3: EXPORT CSV")
    print("=" * 70)
    export_to_csv()

    print("\n✅ Terminé")


# ============================================================================
# POINT D'ENTRÉE
# ============================================================================

if __name__ == "__main__":
    import sys

    force_restart = "--restart" in sys.argv or "--reset" in sys.argv

    if not force_restart:
        if STATE_FILE.exists():
            state = load_state()
            phase = state.get("phase", "collect")

            print("=" * 70)
            print("⚠️  DÉTECTION DE DONNÉES EXISTANTES")
            print("=" * 70)
            print(f"Phase: {phase}")

            if URLS_FILE.exists():
                urls_count = len(read_jsonl(URLS_FILE))
                print(f"URLs: {urls_count}")

            if DETAILS_FILE.exists():
                details_count = len(read_jsonl(DETAILS_FILE))
                print(f"Détails: {details_count}")

            print("\n" + "=" * 70)
            print("1. RECOMMENCER (efface l'état)")
            print("2. CONTINUER")
            print("=" * 70)

            choice = input("\nChoix (1 ou 2) [défaut: 1]: ").strip()

            if choice == "2":
                force_restart = False
                print("✅ Reprise...")
            else:
                force_restart = True
                print("🔄 Redémarrage...")
        else:
            force_restart = True
            print("🆕 Première exécution...")

    asyncio.run(main(force_restart=force_restart))
