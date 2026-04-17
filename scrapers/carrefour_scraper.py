import logging
from typing import Optional
from playwright.sync_api import sync_playwright

URL = "https://www.carrefour.fr/r/informatique-bureau/ordinateurs-portables?noRedirect=0"

# ---------- LOGGING ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

SIZES = ["11 pouces", "12 pouces", "13 pouces", "14 pouces"]

"""
Application des filtres à partir d'une page PlayWright
:param page: la page playwrite
:param url: l'url de depart"""
# ---------- COOKIES ----------
def accept_cookies_carrefour(page) -> None:
    """
    Gère tous les cas Carrefour :
    - OneTrust
    - Modale "Tout accepter"
    """
    selectors = [
        "#onetrust-accept-btn-handler",
        "button:has-text('Tout accepter')"
    ]

    for sel in selectors:
        try:
            btn = page.locator(sel)
            if btn.count() > 0 and btn.first.is_visible():
                btn.first.click(force=True)
                logging.info("✔️ Cookies acceptés")
                page.wait_for_timeout(800)
                return
        except Exception:
            pass

    logging.info("⚠️ Cookies déjà acceptés ou invisibles")


# ---------- FILTRES TAILLE ÉCRAN ----------
def click_screen_size_filter(page, label_text: str) -> None:
    """
    Carrefour n’utilise PAS de checkbox native.
    On clique le <label> via JS (seule méthode fiable).
    """
    try:
        page.evaluate(
            """
            (text) => {
                const spans = [...document.querySelectorAll(
                    'span.checkbox-filters__option__label'
                )];

                const target = spans.find(
                    s => s.textContent.trim() === text
                );

                if (!target) throw 'span not found';

                const label = target.closest('label');
                if (!label) throw 'label not found';

                label.dispatchEvent(new MouseEvent('click', {
                    bubbles: true,
                    cancelable: true,
                    view: window
                }));
            }
            """,
            label_text
        )

        logging.info("✔️ Filtre coché : %s", label_text)
        page.wait_for_timeout(900)

    except Exception as e:
        logging.error("❌ Impossible de cocher '%s' : %s", label_text, e)


# ---------- EXTRACTIONS ----------
def extract_total_results(page) -> Optional[str]:
    try:
        txt = page.locator(
            "p.search-navigation__header-count"
        ).first.inner_text(timeout=5000)
        return txt.strip()
    except Exception:
        return None


def extract_label_taille_ecran(page) -> Optional[str]:
    try:
        return page.locator(
            "p.c-text",
            has_text="Taille type d'écran"
        ).first.inner_text(timeout=3000).strip()
    except Exception:
        return None


def extract_badge_filters(page) -> Optional[str]:
    """
    Le badge bleu existe parfois en double.
    On prend le premier visible.
    """
    try:
        badges = page.locator("span.c-badge--status-primary")
        for i in range(min(badges.count(), 10)):
            badge = badges.nth(i)
            if badge.is_visible():
                return badge.inner_text(timeout=2000).strip()
    except Exception:
        pass
    return None


# ---------- MAIN ----------
def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=200)
        page = browser.new_page()

        page.goto(URL, wait_until="domcontentloaded")

        # 1) Cookies
        accept_cookies_carrefour(page)

        # 2) Filtres
        for size in SIZES:
            click_screen_size_filter(page, size)

        # 3) Extraction finale (APRÈS filtres)
        total_results = extract_total_results(page)
        label_ecran = extract_label_taille_ecran(page)
        badge_filters = extract_badge_filters(page)

        tailles_fmt = ", ".join(s.replace(" pouces", '"') for s in SIZES)

        logging.info(
            "SCRAPING RESULT | Badge bleu=%s | %s=%s | Total produits=%s",
            badge_filters or "N/A",
            label_ecran or "Taille type d'écran",
            tailles_fmt,
            total_results or "N/A"
        )

        page.pause()
        browser.close()


if __name__ == "__main__":
    run()
