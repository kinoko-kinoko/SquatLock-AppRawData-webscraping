import argparse
import json
import random
import time
import requests
import os
from datetime import datetime
from typing import List, Dict, Any

# --- Constants ---

BASE_URL = "https://itunes.apple.com/{country}/rss/{feed_type}/limit=100/genre={genre_id}/json"

GAME_GENRE_IDS = [
    7001, 7002, 7003, 7004, 7005, 7006, 7009, 7011,
    7012, 7013, 7014, 7015, 7016, 7017, 7018, 7019, 6014
]

OTHER_GENRE_IDS = [
    6018, 6000, 6022, 6026, 6017, 6016, 6015, 6023,
    6027, 6013, 6012, 6021, 6020, 6011, 6010, 6009,
    6008, 6007, 6006, 6024, 6005, 6004, 6003, 6002, 6001
]

# Genres for the 'supplement' catalog
SUPPLEMENT_GENRE_IDS = GAME_GENRE_IDS + [
    6016, # Entertainment
    6024, # Shopping
    6005, # Social Networking
    6023, # Food & Drink
    6018  # Books
]

def get_app_data(country_code: str, genre_ids: List[int], feed_types: List[str]) -> List[Dict[str, Any]]:
    """
    Scrapes app data from the Apple RSS feeds for a given country, genres, and feed types.
    """
    all_apps = []
    processed_app_ids = set()

    print(f"Starting scraping for country: {country_code}")

    for genre_id in genre_ids:
        for feed_type in feed_types:
            url = BASE_URL.format(
                country=country_code,
                feed_type=feed_type,
                genre_id=genre_id
            )

            delay = random.uniform(2, 7)
            print(f"Waiting for {delay:.2f} seconds before fetching: {url}")
            time.sleep(delay)

            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                data = response.json()
            except requests.RequestException as e:
                print(f"Error fetching {url}: {e}")
                continue
            except json.JSONDecodeError:
                print(f"Error decoding JSON from {url}")
                continue

            entries = data.get("feed", {}).get("entry")
            if not entries:
                print(f"No entries found for {url}")
                continue

            for entry in entries:
                try:
                    app_id = entry.get("id", {}).get("attributes", {}).get("im:id")
                    bundle_id = entry.get("id", {}).get("attributes", {}).get("im:bundleId")

                    if not app_id or app_id in processed_app_ids:
                        continue

                    name = entry.get("im:name", {}).get("label")

                    icon_url = None
                    for image in entry.get("im:image", []):
                        if image.get("attributes", {}).get("height") == "100":
                            icon_url = image.get("label")
                            break

                    if name and icon_url and bundle_id:
                        all_apps.append({
                            "id": app_id,
                            "bundle_id": bundle_id,
                            "name": name,
                            "icon_url": icon_url
                        })
                        processed_app_ids.add(app_id)
                except (AttributeError, TypeError) as e:
                    print(f"Error parsing an entry from {url}: {e}")

    print(f"Finished scraping. Found {len(all_apps)} unique apps.")
    return all_apps

def save_to_json(data: List[Dict[str, Any]], country_code: str, directory: str, filename_prefix: str):
    """
    Saves the collected app data to a JSON file in the specified directory.
    """
    os.makedirs(directory, exist_ok=True)

    date_str = datetime.now().strftime("%Y%m%d")
    filename = f"{directory}/{filename_prefix}_{country_code}_RawData_{date_str}.json"

    print(f"Saving data to {filename}...")
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print("Save complete.")

def run_giant_mode(country_code: str):
    """Runs the scraper for the giant catalog."""
    print("--- Running in GIANT mode ---")
    all_genre_ids = GAME_GENRE_IDS + OTHER_GENRE_IDS
    feed_types = ["topfreeapplications", "toppaidapplications"]
    app_data = get_app_data(country_code, all_genre_ids, feed_types)
    if app_data:
        save_to_json(app_data, country_code, "BigCatalogRawData", "catalog")

def run_supplement_mode(country_code: str):
    """Runs the scraper for the supplement catalog."""
    print("--- Running in SUPPLEMENT mode ---")
    feed_types = ["topfreeapplications"]
    app_data = get_app_data(country_code, SUPPLEMENT_GENRE_IDS, feed_types)
    if app_data:
        save_to_json(app_data, country_code, "SupplementCatalogRawData", "catalog_supplement")

def main():
    """
    Main function to parse arguments and run the specified mode.
    """
    parser = argparse.ArgumentParser(
        description="Scrape app rankings from the Apple App Store RSS feeds."
    )
    parser.add_argument(
        "mode",
        choices=["giant", "supplement"],
        help="The mode to run the scraper in: 'giant' or 'supplement'."
    )
    parser.add_argument(
        "country_code",
        type=str,
        help="The two-letter ISO country code for the App Store to scrape (e.g., jp, us)."
    )
    args = parser.parse_args()

    if args.mode == "giant":
        run_giant_mode(args.country_code)
    elif args.mode == "supplement":
        run_supplement_mode(args.country_code)

if __name__ == "__main__":
    main()