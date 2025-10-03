import argparse
import json
import random
import time
import requests
import os
from datetime import datetime
from typing import List, Dict, Any, Set

# --- Constants ---

BASE_URL = "https://itunes.apple.com/{country}/rss/{feed_type}/limit={limit}/genre={genre_id}/json"

GAME_GENRE_IDS = [
    7001, 7002, 7003, 7004, 7005, 7006, 7009, 7011,
    7012, 7013, 7014, 7015, 7016, 7017, 7018, 7019, 6014
]

OTHER_GENRE_IDS = [
    6018, 6000, 6022, 6026, 6017, 6016, 6015, 6023,
    6027, 6013, 6012, 6021, 6020, 6011, 6010, 6009,
    6008, 6007, 6006, 6024, 6005, 6004, 6003, 6002, 6001
]

# --- Scraping and Data Handling ---

def get_app_data(country_code: str, genre_id: int, feed_type: str, limit: int, processed_app_ids: Set[str]) -> List[Dict[str, Any]]:
    """
    Scrapes app data for a single feed URL.
    """
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
    """
    Saves the collected app data to a JSON file.
    """
    os.makedirs(directory, exist_ok=True)
    filepath = os.path.join(directory, filename)

    print(f"Saving data to {filepath}...")
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print("Save complete.")

# --- Mode-specific Functions ---

def run_giant_mode(country_code: str):
    """Runs the scraper for the giant catalog."""
    print(f"--- Running in GIANT mode for {country_code} ---")
    all_apps = []
    processed_ids = set()
    all_genre_ids = GAME_GENRE_IDS + OTHER_GENRE_IDS
    feed_types = ["topfreeapplications", "toppaidapplications"]

    for genre_id in all_genre_ids:
        for feed_type in feed_types:
            all_apps.extend(get_app_data(country_code, genre_id, feed_type, 100, processed_ids))

    if all_apps:
        date_str = datetime.now().strftime("%Y%m%d")
        filename = f"catalog_{country_code}_RawData_{date_str}.json"
        save_to_json(all_apps, filename, "BigCatalogRawData")

def run_supplement_mode(country_code: str):
    """Runs the scraper for the supplement catalog."""
    print(f"--- Running in SUPPLEMENT mode for {country_code} ---")
    all_apps = []
    processed_ids = set()
    supplement_genres = GAME_GENRE_IDS + [6016, 6024, 6005, 6023, 6018]

    for genre_id in supplement_genres:
        all_apps.extend(get_app_data(country_code, genre_id, "topfreeapplications", 100, processed_ids))

    if all_apps:
        date_str = datetime.now().strftime("%Y%m%d")
        filename = f"catalog_supplement_{country_code}_RawData_{date_str}.json"
        save_to_json(all_apps, filename, "SupplementCatalogRawData")

def run_builtin_mode():
    """Runs the scraper for the built-in catalog."""
    print("--- Running in BUILTIN mode ---")
    all_apps = []
    processed_ids = set()
    countries = ['us', 'cn', 'jp']
    # Genre ID and limit pairs
    feeds_to_fetch = {
        6014: 100, # Games
        6016: 50,  # Entertainment
        6005: 50,  # Social Networking
        6024: 50,  # Shopping
    }

    for country in countries:
        for genre_id, limit in feeds_to_fetch.items():
            all_apps.extend(get_app_data(country, genre_id, "topfreeapplications", limit, processed_ids))

    if all_apps:
        date_str = datetime.now().strftime("%Y%m%d")
        filename = f"BuiltInCatalog_RawData_{date_str}.json"
        save_to_json(all_apps, filename, "BuiltInCatalogRawData")


def main():
    """
    Main function to parse arguments and run the specified mode.
    """
    parser = argparse.ArgumentParser(
        description="Scrape app rankings from the Apple App Store RSS feeds."
    )
    parser.add_argument(
        "mode",
        choices=["giant", "supplement", "builtin"],
        help="The mode to run the scraper in."
    )
    parser.add_argument(
        "country_code",
        nargs='?',
        default=None,
        help="The two-letter ISO country code (required for 'giant' and 'supplement')."
    )
    args = parser.parse_args()

    if args.mode in ["giant", "supplement"] and not args.country_code:
        parser.error(f"--country_code is required for mode '{args.mode}'")

    if args.mode == "giant":
        run_giant_mode(args.country_code)
    elif args.mode == "supplement":
        run_supplement_mode(args.country_code)
    elif args.mode == "builtin":
        run_builtin_mode()

if __name__ == "__main__":
    main()