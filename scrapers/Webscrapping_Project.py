"""
- Fonction pour changer les critères
- fonction pour la loop item
- Fonction pour l'extraction de données
- Fonction pour exporter sous Excel
- Fonction pour la pagination
"""
from playwright.sync_api import sync_playwright

def main():
    url = "https://www.cdiscount.com/informatique/r-pc+portable+14+pouces.html?nav_menu=227%3A%3APC%20Portable%2012%22%20à%2014%22&nav=JTdCJTIybHBsciUyMjolN0IlMjJzZWxlY3RlZEZpbHRlcklkcyUyMjolNUIlMjJjYXRlZ29yeWNvZGVwYXRoLyU1QyUyMjBrLzBrMGMvMGswYzAxJTVDJTIyJTIyLCUyMlRhaWxsZSUyMGQnJUMzJUE5Y3Jhbi8lNUMlMjIlNUIxMzAwLDE0MDAlNUQlNUMlMjIlMjIsJTIyVGFpbGxlJTIwZCclQzMlQTljcmFuLyU1QjEzMDA7MTQwMCU1RCUyMiU1RCwlMjJzZWxlY3RlZFNvcnRpbmdJZCUyMjolNUIlMjJwZXJ0aW5lbmNlJTIyJTVELCUyMnBhZ2luZyUyMjolN0IlMjJwYWdlJTIyOjEsJTIycGFnZVNpemUlMjI6NDclN0QsJTIyc2VhcmNoV29yZCUyMjolMjJwYyUyMHBvcnRhYmxlJTIyLCUyMnVybCUyMjolMjJodHRwczovL3d3dy5jZGlzY291bnQuY29tL2luZm9ybWF0aXF1ZS9yLXBjK3BvcnRhYmxlKzE0K3BvdWNlcy5odG1sJTIyJTdELCUyMnF1aWNrRmFjZXRzJTIyOiU3QiUyMnNlbGVjdGVkRmFjZXRJZCUyMjpudWxsJTdEJTdE"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()

        page.goto(url)
        page.pause()

        browser.close()

if __name__ == "__main__":
    main()
