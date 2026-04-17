"""
- Fonction pour changer les critères
- fonction pour la loop item
- Fonction pour l'extraction de données
- Fonction pour exporter sous Excel
- Fonction pour la pagination
"""
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

URL = "https://www.cdiscount.com/informatique/r-pc+portable+14+pouces.html"

def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=600)
        page = browser.new_page()

        # Charger la page sans attendre "networkidle" (Cdiscount ne l’atteint jamais)
        page.goto(URL, wait_until="load")

        # Essayer de repérer le bouton cookies
        try:
            page.wait_for_selector("#footer_tc_privacy_button_2", timeout=15000)
        except PWTimeoutError:
            print("❌ Bouton 'Accepter' introuvable.")
            page.pause()
            return

        # Récupération du bouton
        btn = page.locator("#footer_tc_privacy_button_2")
        print("Texte bouton :", btn.inner_text())
        print("Visible ?", btn.is_visible())

        # Essayer de cliquer
        try:
            btn.click()
            print("✅ Clic normal effectué")
        except Exception:
            try:
                btn.click(force=True)
                print("⚠️ Clic forcé effectué")
            except Exception as e:
                print("❌ Impossible de cliquer :", e)

        # Pause pour inspection
        page.pause()
        browser.close()

if __name__ == "__main__":
    run()
