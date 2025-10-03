import argparse
import json
import random
import time
import requests

# --- Constants ---

BASE_URL = "https://itunes.apple.com/{country}/rss/{feed_type}/limit=100/genre={genre_id}/json"

GAME_GENRE_IDS = [
    7001, 7002, 7003, 7004, 7005, 7006, 7009, 7011,
    7012, 7013, 7014, 7015, 7016, 7017, 7018, 7019,
    6014
]

OTHER_GENRE_IDS = [
    6018, 6000, 6022, 6026, 6017, 6016, 6015, 6023,
    6027, 6013, 6012, 6021, 6020, 6011, 6010, 6009,
    6008, 6007, 6006, 6024, 6005, 6004, 6003, 6002,
    6001
]

ALL_GENRE_IDS = GAME_GENRE_IDS + OTHER_GENRE_IDS
FEED_TYPES = ["topfreeapplications", "toppaidapplications"]

def get_app_data(country_code):
    """
    Scrapes app data from the Apple RSS feeds for a given country.
    """
    all_apps = []
    processed_app_ids = set()

    print(f"Starting scraping for country: {country_code}")

    for genre_id in ALL_GENRE_IDS:
        for feed_type in FEED_TYPES:
            # Construct URL
            url = BASE_URL.format(
                country=country_code,
                feed_type=feed_type,
                genre_id=genre_id
            )

            # Random delay between 2 and 7 seconds
            delay = random.uniform(2, 7)
            print(f"Waiting for {delay:.2f} seconds before fetching: {url}")
            time.sleep(delay)

            # Make HTTP request
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

            # Parse JSON response
            entries = data.get("feed", {}).get("entry")
            if not entries:
                print(f"No entries found for {url}")
                continue

            for entry in entries:
                try:
                    app_id = entry.get("id", {}).get("attributes", {}).get("im:id")
                    if not app_id or app_id in processed_app_ids:
                        continue

                    name = entry.get("im:name", {}).get("label")

                    icon_url = None
                    for image in entry.get("im:image", []):
                        if image.get("attributes", {}).get("height") == "100":
                            icon_url = image.get("label")
                            break

                    if name and icon_url:
                        all_apps.append({"name": name, "icon_url": icon_url})
                        processed_app_ids.add(app_id)

                except (AttributeError, TypeError) as e:
                    print(f"Error parsing an entry from {url}: {e}")
                    continue

    print(f"Finished scraping. Found {len(all_apps)} unique apps.")
    return all_apps

def save_to_json(data, country_code):
    """
    Saves the collected app data to a JSON file.
    """
    filename = f"app_rankings_{country_code}.json"
    print(f"Saving data to {filename}...")
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print("Save complete.")

def main():
    """
    Main function to run the scraper.
    """
    parser = argparse.ArgumentParser(
        description="Scrape top 100 free and paid apps from the Apple App Store RSS feeds."
    )
    parser.add_argument(
        "country_code",
        type=str,
        help="The two-letter ISO country code for the App Store to scrape (e.g., jp, us)."
    )
    args = parser.parse_args()

    app_data = get_app_data(args.country_code)

    if app_data:
        save_to_json(app_data, args.country_code)
    else:
        print("No data was collected. No file will be saved.")

if __name__ == "__main__":
    main()