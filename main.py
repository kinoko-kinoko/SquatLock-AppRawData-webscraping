import argparse
import json
import random
import time
import requests
import os
from datetime import datetime
from typing import List, Dict, Any, Set, Optional
from urllib.parse import urlparse

# --- Constants ---

BASE_URL = "https://itunes.apple.com/{country}/rss/{feed_type}/limit={limit}/genre={genre_id}/json"
LOOKUP_URL = "https://itunes.apple.com/lookup?id={track_id}"

GAME_GENRE_IDS = [
    7001, 7002, 7003, 7004, 7005, 7006, 7009, 7011,
    7012, 7013, 7014, 7015, 7016, 7017, 7018, 7019, 6014
]

OTHER_GENRE_IDS = [
    6018, 6000, 6022, 6026, 6017, 6016, 6015, 6023,
    6027, 6013, 6012, 6021, 6020, 6011, 6010, 6009,
    6008, 6007, 6006, 6024, 6005, 6004, 6003, 6002, 6001
]

BUILTIN_COUNTRIES = [
    'us', 'cn', 'de', 'jp', 'in', 'gb', 'fr', 'it', 'br', 'ca', 'ru', 'mx',
    'au', 'kr', 'es', 'id', 'tr', 'nl', 'sa', 'ch', 'pl', 'tw', 'ar', 'be',
    'se', 'ie', 'ae', 'il', 'sg', 'th', 'at', 'no', 'vn', 'ph', 'bd', 'co',
    'my', 'za', 'dk', 'hk', 'ro', 'eg', 'cl', 'cz', 'ir', 'pt', 'fi', 'kz',
    'pe', 'gr'
]

# --- Scraping and Data Handling ---

def get_app_data(country_code: str, genre_id: int, feed_type: str, limit: int, processed_app_ids: Set[str]) -> List[Dict[str, Any]]:
    """Scrapes app data for a single feed URL."""
    url = BASE_URL.format(country=country_code, feed_type=feed_type, limit=limit, genre_id=genre_id)

    delay = random.uniform(2, 7)
    print(f"Waiting for {delay:.2f} seconds before fetching: {url}")
    time.sleep(delay)

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return []
    except json.JSONDecodeError:
        print(f"Error decoding JSON from {url}")
        return []

    entries = data.get("feed", {}).get("entry")
    if not entries:
        print(f"No entries found for {url}")
        return []

    new_apps = []
    for entry in entries:
        try:
            app_id = entry.get("id", {}).get("attributes", {}).get("im:id")
            bundle_id = entry.get("id", {}).get("attributes", {}).get("im:bundleId")

            if not app_id or app_id in processed_app_ids:
                continue

            name = entry.get("im:name", {}).get("label")
            icon_url = next((img["label"] for img in entry.get("im:image", []) if img.get("attributes", {}).get("height") == "100"), None)

            if name and icon_url and bundle_id:
                new_apps.append({"id": app_id, "bundle_id": bundle_id, "name": name, "icon_url": icon_url})
                processed_app_ids.add(app_id)
        except (AttributeError, TypeError) as e:
            print(f"Error parsing an entry from {url}: {e}")

    return new_apps

def save_to_json(data: List[Dict[str, Any]], filename: str, directory: str):
    """Saves the collected app data to a JSON file."""
    os.makedirs(directory, exist_ok=True)
    filepath = os.path.join(directory, filename)

    print(f"Saving data to {filepath}...")
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print("Save complete.")

# --- Enrich Mode Functions ---

def get_seller_url(track_id: str) -> Optional[str]:
    """Gets the sellerUrl from the iTunes Search API."""
    url = LOOKUP_URL.format(track_id=track_id)
    delay = random.uniform(3, 5)
    print(f"Waiting for {delay:.2f} seconds before fetching seller URL for track ID: {track_id}")
    time.sleep(delay)
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        if data.get("resultCount", 0) > 0 and "sellerUrl" in data["results"][0]:
            return data["results"][0]["sellerUrl"]
    except requests.RequestException as e:
        print(f"API request failed for track ID {track_id}: {e}")
    except (json.JSONDecodeError, IndexError) as e:
        print(f"Error parsing API response for track ID {track_id}: {e}")
    return None

def find_universal_links(seller_url: Optional[str], bundle_id: str) -> List[str]:
    """Finds universal links from the AASA file."""
    if not seller_url:
        return []

    try:
        domain = urlparse(seller_url).netloc
        if not domain:
            return []

        aasa_paths = [
            f"https://{domain}/.well-known/apple-app-site-association",
            f"https://{domain}/apple-app-site-association"
        ]

        for path in aasa_paths:
            try:
                response = requests.get(path, timeout=10)
                if response.status_code == 200:
                    aasa_data = response.json()
                    details = aasa_data.get("applinks", {}).get("details", [])
                    for detail in details:
                        app_id = detail.get("appID")
                        if isinstance(app_id, str) and app_id.endswith(bundle_id):
                            return detail.get("paths", [])
            except (requests.RequestException, json.JSONDecodeError):
                continue # Try the next path
    except Exception as e:
        print(f"An unexpected error occurred while finding universal links for {seller_url}: {e}")

    return []

# --- Mode-specific Functions ---

def run_giant_mode(country_code: str, limit: Optional[int]):
    """Runs the scraper for the giant catalog."""
    print(f"--- Running in GIANT mode for {country_code} ---")
    all_apps = []
    processed_ids = set()
    all_genre_ids = GAME_GENRE_IDS + OTHER_GENRE_IDS
    feed_types = ["topfreeapplications", "toppaidapplications"]

    for genre_id in all_genre_ids:
        for feed_type in feed_types:
            all_apps.extend(get_app_data(country_code, genre_id, feed_type, limit or 100, processed_ids))

    if all_apps:
        date_str = datetime.now().strftime("%Y%m%d")
        filename = f"catalog_{country_code}_RawData_{date_str}.json"
        save_to_json(all_apps, filename, "BigCatalogRawData")

def run_supplement_mode(country_code: str, limit: Optional[int]):
    """Runs the scraper for the supplement catalog."""
    print(f"--- Running in SUPPLEMENT mode for {country_code} ---")
    all_apps = []
    processed_ids = set()
    supplement_genres = GAME_GENRE_IDS + [6016, 6024, 6005, 6023, 6018]

    for genre_id in supplement_genres:
        all_apps.extend(get_app_data(country_code, genre_id, "topfreeapplications", limit or 100, processed_ids))

    if all_apps:
        date_str = datetime.now().strftime("%Y%m%d")
        filename = f"catalog_supplement_{country_code}_RawData_{date_str}.json"
        save_to_json(all_apps, filename, "SupplementCatalogRawData")

def run_builtin_mode(limit: Optional[int]):
    """Runs the scraper for the built-in catalog."""
    print("--- Running in BUILTIN mode ---")
    all_apps = []
    processed_ids = set()

    feeds_to_fetch = [
        {'genre_id': 6014, 'limit': 100, 'feed_type': 'toppaidapplications'},
        {'genre_id': 6014, 'limit': 100, 'feed_type': 'topfreeapplications'},
        {'genre_id': 6016, 'limit': 50, 'feed_type': 'topfreeapplications'},
        {'genre_id': 6005, 'limit': 50, 'feed_type': 'topfreeapplications'},
        {'genre_id': 6024, 'limit': 50, 'feed_type': 'topfreeapplications'},
        {'genre_id': 6018, 'limit': 50, 'feed_type': 'topfreeapplications'},
    ]

    for country in BUILTIN_COUNTRIES:
        for feed in feeds_to_fetch:
            # Note: The --limit from CLI will override the limit in the feed config for testing.
            fetch_limit = limit or feed['limit']
            all_apps.extend(get_app_data(country, feed['genre_id'], feed['feed_type'], fetch_limit, processed_ids))

    if all_apps:
        date_str = datetime.now().strftime("%Y%m%d")
        filename = f"BuiltInCatalog_RawData_{date_str}.json"
        save_to_json(all_apps, filename, "BuiltInCatalogRawData")

def run_enrich_mode(filepath: str, limit: Optional[int]):
    """Reads a JSON file, enriches it with seller and universal link data, and saves it."""
    print(f"--- Running in ENRICH mode for file: {filepath} ---")
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            apps_to_enrich = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error reading input file: {e}")
        return

    if limit:
        apps_to_enrich = apps_to_enrich[:limit]

    for app in apps_to_enrich:
        track_id = app.get("id")
        bundle_id = app.get("bundle_id")

        if not track_id or not bundle_id:
            app["sellerUrl"] = None
            app["universal_links"] = []
            continue

        seller_url = get_seller_url(track_id)
        app["sellerUrl"] = seller_url

        universal_links = find_universal_links(seller_url, bundle_id)
        app["universal_links"] = universal_links
        print(f"Enriched {app.get('name')}: sellerUrl: {seller_url is not None}, universal_links: {len(universal_links)} found.")

    # Save to a new file
    directory, original_filename = os.path.split(filepath)
    name, ext = os.path.splitext(original_filename)
    new_filename = f"{name}_enriched{ext}"
    save_to_json(apps_to_enrich, new_filename, directory)


def main():
    """Main function to parse arguments and run the specified mode."""
    parser = argparse.ArgumentParser(description="Scrape and enrich App Store data.")
    parser.add_argument("mode", choices=["giant", "supplement", "builtin", "enrich"], help="The mode to run.")
    parser.add_argument("argument", help="Country code for scrape modes, or filepath for enrich mode.")
    parser.add_argument("--limit", type=int, help="Limit the number of items processed for testing.")

    args = parser.parse_args()

    if args.mode in ["giant", "supplement"]:
        run_giant_mode(args.argument, args.limit) if args.mode == "giant" else run_supplement_mode(args.argument, args.limit)
    elif args.mode == "builtin":
        run_builtin_mode(args.limit)
    elif args.mode == "enrich":
        run_enrich_mode(args.argument, args.limit)

if __name__ == "__main__":
    main()