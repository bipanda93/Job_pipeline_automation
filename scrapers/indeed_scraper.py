"""
Indeed Scraper avec undetected-chromedriver
Contourne Cloudflare et détections anti-bot
"""
import csv
import hashlib
import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# ============================================================
# CONFIG
# ============================================================
BASE = "https://fr.indeed.com"

SEARCH_CONFIGS = [
    {"keywords": "data scientist", "label": "data_scientist"},
    {"keywords": "data analyst", "label": "data_analyst"},
    {"keywords": "data engineer", "label": "data_engineer"},
    {"keywords": "data architecte", "label": "data_architect"},
    {"keywords": "machine learning", "label": "ml"},
    {"keywords": "big data", "label": "bigdata"},
    {"keywords": "business intelligence", "label": "bi"},
    {"keywords": "analyste données", "label": "analyste"},
    {"keywords": "data analytics engineer", "label": "analytics_engineer"}
]

LOCATION = "France"
SORT = "date"

RESULTS_PER_PAGE = 10
MAX_PAGES_PER_SEARCH = 150

OUT_DIR = Path("data/raw/indeed")
OUT_DIR.mkdir(parents=True, exist_ok=True)

URLS_FILE = OUT_DIR / "indeed_urls.jsonl"
DETAILS_FILE = OUT_DIR / "indeed_details.jsonl"
CSV_FILE = OUT_DIR / "indeed_details.csv"
STATE_FILE = OUT_DIR / "indeed_state.json"

JK_RE = re.compile(r"[?&]jk=([a-f0-9]{16})", re.IGNORECASE)

COOKIES_ACCEPTED = False


# ============================================================
# UTILS
# ============================================================
def now_iso() -> str:
    return datetime.now().isoformat()


def sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()


def append_jsonl(path: Path, data: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(data, ensure_ascii=False) + "\n")


def read_jsonl_unique_urls(path: Path) -> list[str]:
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


def read_seen_ids(path: Path) -> set[str]:
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
                continue
    return seen


def load_state() -> dict:
    if not STATE_FILE.exists():
        return {"searches_done": [], "current_search_idx": 0, "current_page": 0}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except:
        return {"searches_done": [], "current_search_idx": 0, "current_page": 0}


def save_state(state: dict):
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2))


def build_search_url(keywords: str, start: int) -> str:
    params = {"q": keywords, "l": LOCATION, "sort": SORT, "start": str(start)}
    return f"{BASE}/jobs?{urlencode(params)}"


def extract_jk(url: str) -> Optional[str]:
    m = JK_RE.search(url)
    return m.group(1) if m else None


def normalize_job_url(href: str) -> Optional[str]:
    if not href or "indeed." not in href:
        return None
    if href.startswith("/"):
        href = BASE + href
    jk = extract_jk(href)
    if jk:
        return f"{BASE}/viewjob?jk={jk}"
    if "/viewjob" in href:
        return href.split("#")[0]
    return None


def export_to_csv(jsonl_path: Path, csv_path: Path):
    if not jsonl_path.exists():
        print("⚠️ Pas de fichier à exporter")
        return

    by_id = {}
    with jsonl_path.open("r") as f:
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
        print("⚠️ Aucune offre à exporter")
        return

    fieldnames = ["offer_id", "title", "company", "location", "salary",
                  "contract_type", "url", "scraped_at", "source", "search_label",
                  "search_keywords", "error", "raw_text"]
    extra = sorted({k for r in rows for k in r.keys() if k not in fieldnames})
    fieldnames.extend(extra)

    with csv_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

    print(f"📤 CSV exporté: {len(rows)} offres uniques")


# ============================================================
# SELENIUM HELPERS - DÉTECTION CLOUDFLARE AMÉLIORÉE
# ============================================================
def accept_cookies(driver):
    global COOKIES_ACCEPTED
    if COOKIES_ACCEPTED:
        return

    selectors = [
        "//button[contains(text(), 'Accepter')]",
        "//button[contains(text(), 'Tout accepter')]",
        "//button[@id='onetrust-accept-btn-handler']",
        "//button[contains(@class, 'onetrust-close-btn-handler')]",
    ]

    for sel in selectors:
        try:
            btn = WebDriverWait(driver, 3).until(
                EC.element_to_be_clickable((By.XPATH, sel))
            )
            btn.click()
            time.sleep(1)
            COOKIES_ACCEPTED = True
            print("✓ Cookies acceptés")
            return
        except:
            continue

    COOKIES_ACCEPTED = True


def is_cloudflare_active(driver) -> bool:
    """
    Détecte si Cloudflare est VRAIMENT actif (pas juste du texte dans la page)
    """
    try:
        html = driver.page_source.lower()
        title = driver.title.lower()
        url = driver.current_url.lower()

        # Vérifier le titre - indicateur le plus fiable
        if "just a moment" in title or "checking your browser" in title:
            return True

        # Vérifier l'URL
        if "challenges.cloudflare.com" in url or "/cdn-cgi/challenge" in url:
            return True

        # Vérifier présence du challenge interactif (pas juste du texte)
        # Cloudflare a un div spécifique pour le challenge
        try:
            challenge_elem = driver.find_element(By.ID, "challenge-running")
            if challenge_elem:
                return True
        except:
            pass

        try:
            cf_wrapper = driver.find_element(By.ID, "cf-wrapper")
            if cf_wrapper and "just a moment" in html:
                return True
        except:
            pass

        # Si on trouve des offres d'emploi, c'est bon
        try:
            jobs = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/viewjob"], a[href*="jk="]')
            if len(jobs) > 3:
                return False  # Des offres = pas de Cloudflare actif
        except:
            pass

        # Texte "cloudflare" seul n'est pas suffisant (peut être dans footer)
        return False

    except:
        return False


def wait_for_cloudflare(driver, max_wait=30):
    """Attend que Cloudflare passe SI il est vraiment actif"""
    if not is_cloudflare_active(driver):
        return True  # Déjà passé

    print("⏳ Cloudflare actif, attente automatique (max 30s)...")
    for i in range(max_wait):
        time.sleep(1)

        if not is_cloudflare_active(driver):
            print(f"✅ Cloudflare passé après {i + 1}s")
            return True

        if i > 0 and i % 10 == 0:
            print(f"   ... encore {max_wait - i}s")

    print("⚠️ Timeout - intervention manuelle nécessaire")
    return False


def collect_job_urls(driver) -> list[str]:
    """Collecte les URLs d'offres"""
    urls = []
    try:
        # Attendre que les résultats se chargent
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'a[href*="/viewjob"], a[href*="jk="]'))
        )

        links = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/viewjob"], a[href*="jk="]')
        for link in links:
            try:
                href = link.get_attribute("href")
                full = normalize_job_url(href)
                if full:
                    urls.append(full)
            except:
                continue
    except TimeoutException:
        print("   ⚠️ Timeout: aucun résultat trouvé")
    except:
        pass

    return list(set(urls))


def safe_find_text(driver, selector, by=By.CSS_SELECTOR, timeout=5) -> Optional[str]:
    try:
        elem = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((by, selector))
        )
        return elem.text.strip() if elem.text else None
    except:
        return None


# ============================================================
# SCRAPING
# ============================================================
def scrape_one_search(driver, config: dict, state: dict, seen_urls: set) -> int:
    keywords = config["keywords"]
    label = config["label"]

    start_page = state.get("current_page", 0)
    new_count = 0
    consecutive_fails = 0

    print(f"\n🔍 Recherche: '{keywords}' (label={label})")
    print(f"   Reprise à la page {start_page + 1}")

    for page_idx in range(start_page, MAX_PAGES_PER_SEARCH):
        start = page_idx * RESULTS_PER_PAGE
        url = build_search_url(keywords, start)

        try:
            driver.get(url)
            time.sleep(2)

            accept_cookies(driver)

            # Vérifier Cloudflare INTELLIGEMMENT
            if is_cloudflare_active(driver):
                if not wait_for_cloudflare(driver, 30):
                    print("\n⚠️ Résous Cloudflare manuellement puis ENTRÉE...")
                    input()

            time.sleep(1)
            urls = collect_job_urls(driver)

            if not urls:
                consecutive_fails += 1
                print(f"   Page {page_idx + 1}: 0 URL (échecs: {consecutive_fails}/3)")

                if consecutive_fails >= 3:
                    print(f"   🛑 3 échecs consécutifs, arrêt de '{label}'")
                    break
                continue

            consecutive_fails = 0

            page_new = 0
            for u in urls:
                if u not in seen_urls:
                    seen_urls.add(u)
                    append_jsonl(URLS_FILE, {
                        "url": u,
                        "collected_at": now_iso(),
                        "source": "indeed",
                        "search_label": label,
                        "search_keywords": keywords,
                        "start": start,
                    })
                    page_new += 1
                    new_count += 1

            print(
                f"   Page {page_idx + 1}/{MAX_PAGES_PER_SEARCH} (start={start}): +{page_new} URLs (total={len(seen_urls)})")

            state["current_page"] = page_idx + 1
            save_state(state)

            time.sleep(1.5)

            if len(urls) < 5:
                print(f"   ✓ Peu de résultats ({len(urls)}), fin pour '{label}'")
                break

        except Exception as e:
            print(f"   ⚠️ Erreur page {page_idx + 1}: {e}")
            consecutive_fails += 1
            if consecutive_fails >= 3:
                break
            continue

    if label not in state["searches_done"]:
        state["searches_done"].append(label)
    state["current_page"] = 0
    save_state(state)

    print(f"   ✅ '{label}': {new_count} nouvelles URLs collectées")
    return new_count


def scrape_detail(driver, url: str, search_label: str = None, search_keywords: str = None) -> dict:
    jk = extract_jk(url)
    offer_id = jk or sha1(url)

    try:
        driver.get(url)
        time.sleep(2)

        accept_cookies(driver)

        # Vérifier Cloudflare intelligemment
        if is_cloudflare_active(driver):
            wait_for_cloudflare(driver, 20)

        title = safe_find_text(driver, "h1")
        company = (
                safe_find_text(driver, '[data-testid="inlineHeader-companyName"]') or
                safe_find_text(driver, 'div[data-company-name="true"]') or
                safe_find_text(driver, 'a[data-testid="company-name"]')
        )
        location = (
                safe_find_text(driver, '[data-testid="inlineHeader-companyLocation"]') or
                safe_find_text(driver, '[data-testid="job-location"]')
        )
        salary = (
                safe_find_text(driver, '[data-testid="jobsearch-JobInfoHeader-salaryText"]') or
                safe_find_text(driver, 'span:has-text("€")', By.CSS_SELECTOR, 3)
        )

        raw_text = safe_find_text(driver, "#jobDescriptionText", timeout=10)

        contract_type = None
        if raw_text:
            m = re.search(r"\b(CDI|CDD|Alternance|Stage|Intérim|Freelance)\b", raw_text, re.I)
            if m:
                contract_type = m.group(1)

        return {
            "offer_id": offer_id,
            "title": title,
            "company": company,
            "location": location,
            "salary": salary,
            "contract_type": contract_type,
            "url": url,
            "raw_text": raw_text,
            "scraped_at": now_iso(),
            "source": "indeed",
            "search_label": search_label,
            "search_keywords": search_keywords,
        }

    except Exception as e:
        return {
            "offer_id": offer_id,
            "url": url,
            "error": str(e),
            "scraped_at": now_iso(),
            "source": "indeed",
            "search_label": search_label,
            "search_keywords": search_keywords,
        }


# ============================================================
# MAIN
# ============================================================
def main():
    print("=" * 70)
    print("🚀 INDEED SCRAPER - UNDETECTED CHROMEDRIVER")
    print("=" * 70)

    state = load_state()
    existing_urls = read_jsonl_unique_urls(URLS_FILE)
    seen_urls = set(existing_urls)

    print(f"📦 URLs déjà collectées: {len(existing_urls)}")

    # Créer driver undetected
    options = uc.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    print("\n🌐 Lancement Chrome undetected...")
    driver = uc.Chrome(options=options)

    try:
        # Home pour cookies
        print("\n🏠 Visite de la page d'accueil Indeed...")
        driver.get(BASE)
        time.sleep(3)
        accept_cookies(driver)

        if is_cloudflare_active(driver):
            print("⏳ Cloudflare détecté sur la home...")
            if not wait_for_cloudflare(driver, 30):
                print("⚠️ Résous Cloudflare puis ENTRÉE...")
                input()

        print("✓ Prêt à scraper")

        # Phase 1: Collecte URLs
        start_idx = state.get("current_search_idx", 0)
        for i in range(start_idx, len(SEARCH_CONFIGS)):
            config = SEARCH_CONFIGS[i]
            label = config["label"]

            if label in state.get("searches_done", []):
                print(f"⊘ '{label}' déjà terminée, skip")
                continue

            state["current_search_idx"] = i
            save_state(state)
            scrape_one_search(driver, config, state, seen_urls)

        print(f"\n✅ Collecte URLs terminée: {len(seen_urls)} URLs uniques")

        # Phase 2: Scraping détails
        all_urls = read_jsonl_unique_urls(URLS_FILE)
        seen_details = read_seen_ids(DETAILS_FILE)

        url_meta = {}
        if URLS_FILE.exists():
            with URLS_FILE.open("r") as f:
                for line in f:
                    try:
                        o = json.loads(line)
                        u = o.get("url")
                        if u:
                            url_meta[u] = {
                                "search_label": o.get("search_label"),
                                "search_keywords": o.get("search_keywords"),
                            }
                    except:
                        continue

        print(f"\n📊 Phase 2: Scraping des détails")
        print(f"   URLs à scraper: {len(all_urls)}")
        print(f"   Déjà scrapées: {len(seen_details)}")

        scraped = 0
        skipped = 0

        for i, url in enumerate(all_urls, 1):
            oid = extract_jk(url) or sha1(url)

            if oid in seen_details:
                skipped += 1
                if skipped <= 5:
                    print(f"⊘ {i}/{len(all_urls)} - {oid} déjà scrapée")
                continue

            meta = url_meta.get(url, {})
            data = scrape_detail(driver, url, meta.get("search_label"), meta.get("search_keywords"))
            append_jsonl(DETAILS_FILE, data)

            if data.get("offer_id"):
                seen_details.add(data["offer_id"])

            scraped += 1
            title = (data.get("title") or "Sans titre")[:50]
            print(f"✓ {i}/{len(all_urls)} - {oid} - {title}")

            time.sleep(1.5)

            if scraped % 50 == 0:
                print(f"💾 Checkpoint: {scraped} offres scrapées")

        print(f"\n✅ {scraped} nouvelles offres scrapées")

    finally:
        print("\n🔒 Fermeture du navigateur...")
        driver.quit()

    export_to_csv(DETAILS_FILE, CSV_FILE)

    print("\n" + "=" * 70)
    print("✅ TERMINÉ")
    print(f"   📁 URLs: {URLS_FILE}")
    print(f"   📁 Détails: {DETAILS_FILE}")
    print(f"   📁 CSV: {CSV_FILE}")
    print(f"   📊 Total: {len(read_jsonl_unique_urls(URLS_FILE))} URLs | {len(read_seen_ids(DETAILS_FILE))} offres")
    print("=" * 70)


if __name__ == "__main__":
    main()