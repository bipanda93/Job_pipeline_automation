from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

URL = "https://www.cdiscount.com/informatique/r-pc+portable+14+pouces.html"


def accept_cookies(page):
    try:
        # Cas 1 : bouton direct (le plus fréquent)
        page.wait_for_selector(
            "#footer_tc_privacy_button_2",
            state="visible",
            timeout=15000
        )
        page.click("#footer_tc_privacy_button_2")
        print("✔️ Cookies acceptés (direct)")
        return
    except PWTimeoutError:
        pass

    # Cas 2 : bouton dans une iframe (fallback)
    for frame in page.frames:
        try:
            btn = frame.locator("#footer_tc_privacy_button_2")
            if btn.count() > 0:
                btn.click()
                print("✔️ Cookies acceptés (iframe)")
                return
        except:
            pass

    print("⚠️ Bouton cookies non trouvé (déjà accepté ?)")


def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            slow_mo=500
        )
        page = browser.new_page()

        page.goto(URL, wait_until="domcontentloaded")

        # --- 1) Cookies ---
        accept_cookies(page)

        # --- 2) Effacer tous les filtres ---
        try:
            page.get_by_text(
                "Effacer tous les filtres",
                exact=True
            ).click(timeout=10000)
            print("✔️ Filtres effacés")
        except:
            print("⚠️ Bouton 'Effacer tous les filtres' absent")

        # --- 3) Taille écran ---
        try:
            page.get_by_text('De 13" à 14"', exact=False).click(timeout=10000)
            print('✔️ Taille "13 à 14 pouces" sélectionnée')
        except:
            print('❌ Taille écran non trouvée')

        page.pause()
        browser.close()


if __name__ == "__main__":
    run()
