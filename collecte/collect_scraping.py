"""
Collecte des actualités énergétiques par web crawling + web scraping
(Actualités News Environnement, section Énergie, site public sans API officielle).
Étape 1 : crawling de la page catégorie pour repérer les URLs d'articles.
Étape 2 : scraping de chaque article pour en extraire titre, date et résumé
          (via les balises meta standardisées : og:title, article:published_time, description).
"""
import os
import re
import time
from datetime import datetime, timedelta, timezone

import requests
import psycopg2
from bs4 import BeautifulSoup

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "energiflow")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "energiflow")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "energiflow")

BASE_URL = "https://www.actualites-news-environnement.com"
LISTING_PATH = "/categorie/energie/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}
MAX_PAGES = 5         # garde-fou : la page 1 seule couvre déjà largement 7 jours d'actualité
POLITENESS_DELAY = 1  # secondes entre chaque requête, pour ne pas surcharger le site

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# URL d'article = un seul segment, au moins 3 mots séparés par des tirets
# (ex : /fin-arenh-vnu-bilan-six-mois-juillet-2026-electricite-france/)
# On exclut explicitement les pages qui ne sont pas des articles.
EXCLUDED_PREFIXES = ("categorie", "tag", "auteur", "cdn-cgi", "images", "_next", "api", "mentions-legales")
ARTICLE_PATH_RE = re.compile(r"^/([a-z0-9]+(?:-[a-z0-9]+){2,})/?$")


def fetch_listing_page(page_num: int) -> str | None:
    """Récupère le HTML d'une page de la liste des articles Énergie (crawling)."""
    url = f"{BASE_URL}{LISTING_PATH}"
    params = {"page": page_num} if page_num > 0 else {}
    response = SESSION.get(url, params=params, timeout=15)
    if response.status_code == 404:
        return None  # page au-delà du nombre disponible : on arrête proprement
    response.raise_for_status()
    return response.text


def extract_article_urls(html: str) -> set[str]:
    """Repère les URLs d'articles sur une page de liste (exclut catégories, tags, etc.)."""
    soup = BeautifulSoup(html, "html.parser")
    urls = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        path = href[len(BASE_URL):] if href.startswith(BASE_URL) else href
        if not path.startswith("/"):
            continue
        first_segment = path.strip("/").split("/")[0] if path.strip("/") else ""
        if first_segment in EXCLUDED_PREFIXES:
            continue
        if ARTICLE_PATH_RE.match(path):
            urls.add(BASE_URL + path)
    return urls


def fetch_article_details(url: str) -> dict | None:
    """Scrape la page d'un article : titre, date de publication, résumé (balises meta)."""
    try:
        response = SESSION.get(url, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        title_meta = soup.find("meta", property="og:title")
        titre = title_meta["content"].strip() if title_meta and title_meta.get("content") else None

        date_meta = soup.find("meta", property="article:published_time")
        if date_meta and date_meta.get("content"):
            raw_date = date_meta["content"].replace("Z", "+00:00")
            date_publication = datetime.fromisoformat(raw_date).date()
        else:
            date_publication = datetime.now(timezone.utc).date()  # repli si absent

        desc_meta = soup.find("meta", attrs={"name": "description"})
        contenu_resume = desc_meta["content"].strip() if desc_meta and desc_meta.get("content") else None

        if not titre:
            return None
        return {
            "url_source": url,
            "titre": titre,
            "date_publication": date_publication,
            "contenu_resume": contenu_resume,
        }
    except requests.RequestException as e:
        print(f"  ... échec du scraping de {url} : {e}")
        return None


def save_to_postgres(rows: list[dict]) -> None:
    """Crée la table si besoin et insère les articles (upsert sur url_source)."""
    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD,
    )
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS fact_actualites (
                    url_source        TEXT PRIMARY KEY,
                    date_publication  DATE NOT NULL,
                    titre             TEXT NOT NULL,
                    contenu_resume    TEXT
                );
            """)
            for r in rows:
                cur.execute("""
                    INSERT INTO fact_actualites (url_source, date_publication, titre, contenu_resume)
                    VALUES (%(url_source)s, %(date_publication)s, %(titre)s, %(contenu_resume)s)
                    ON CONFLICT (url_source) DO UPDATE
                    SET contenu_resume = EXCLUDED.contenu_resume;
                """, r)
    finally:
        conn.close()


if __name__ == "__main__":
    CUTOFF_DAYS = 30  # cette section publie ~2-3 articles/semaine : 7 jours serait souvent vide
    cutoff = (datetime.now(timezone.utc) - timedelta(days=CUTOFF_DAYS)).date()
    all_urls: set[str] = set()

    print(f"Crawling de {BASE_URL}{LISTING_PATH} ...")
    for page_num in range(MAX_PAGES):
        html = fetch_listing_page(page_num)
        if html is None:
            print(f"  page {page_num} indisponible, arrêt du crawling.")
            break
        page_urls = extract_article_urls(html)
        new_urls = page_urls - all_urls
        print(f"  page {page_num} : {len(page_urls)} articles trouvés, {len(new_urls)} nouveaux")
        if not new_urls:
            break
        all_urls |= new_urls
        time.sleep(POLITENESS_DELAY)

    print(f"{len(all_urls)} URLs d'articles au total. Scraping des détails...")
    articles = []
    for i, url in enumerate(all_urls, start=1):
        details = fetch_article_details(url)
        if details:
            print(f"  - {details['date_publication']} : {details['titre'][:60]}")
            articles.append(details)
        time.sleep(POLITENESS_DELAY)

    recent_articles = [a for a in articles if a["date_publication"] >= cutoff]
    print(f"{len(recent_articles)} articles retenus sur les {CUTOFF_DAYS} derniers jours (cutoff {cutoff}).")
    print("Exemple :", recent_articles[0] if recent_articles else "(aucun article)")

    save_to_postgres(recent_articles)
    print(f"{len(recent_articles)} articles insérés/mis à jour dans PostgreSQL (table fact_actualites).")