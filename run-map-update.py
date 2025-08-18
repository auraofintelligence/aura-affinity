import os
import pandas as pd
import requests
import time
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load API key from environment variables (for local testing) or GitHub Secrets
load_dotenv()
API_KEY = os.getenv("GOOGLE_API_KEY")

# --- 1. CONFIGURATION ---
CITY_LIST_PATH = "curation/city-list.txt"
LOCKLIST_PATH = "curation/locklist.csv"
OUTPUT_CSV_PATH = "aura-affinity.csv"
TIMESTAMP_PATH = "last-run.txt"

def get_cities():
    """Reads the list of cities from the text file."""
    try:
        with open(CITY_LIST_PATH, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"Error: {CITY_LIST_PATH} not found. Please create it.")
        return []

def save_last_run_timestamp():
    """Saves the current UTC timestamp to a file."""
    with open(TIMESTAMP_PATH, "w") as f:
        # Using ISO 8601 format, which is easy for JavaScript to parse
        f.write(datetime.now(timezone.utc).isoformat())
    print(f"Saved timestamp to {TIMESTAMP_PATH}.")

# --- 2. DATA GATHERING (PLACES API) ---
def search_places(city):
    """Searches for businesses with 'Aura' in a given city."""
    print(f"Searching for 'Aura' in {city}...")
    all_places = []
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    query = f"business named Aura in {city}"
    params = {"query": query, "key": API_KEY}

    while True:
        try:
            response = requests.get(url, params=params)
            response.raise_for_status() # Raise an exception for bad status codes
            results = response.json()
        except requests.exceptions.RequestException as e:
            print(f"  Error searching in {city}: {e}")
            return [] # Stop searching this city on error

        all_places.extend(results.get('results', []))
        next_page_token = results.get('next_page_token')
        if not next_page_token:
            break

        time.sleep(2) # Important: Wait for the next page token to become valid
        params = {"pagetoken": next_page_token, "key": API_KEY}

    return all_places

def get_place_details(place_id):
    """Gets detailed information for a specific place."""
    print(f"  Getting details for place_id: {place_id[:10]}...")
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "place_id": place_id,
        "fields": "name,place_id,formatted_address,geometry,website,international_phone_number,types,address_components",
        "key": API_KEY
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json().get('result', {})
    except requests.exceptions.RequestException as e:
        print(f"  Error getting details for {place_id[:10]}: {e}")
        return {}

# --- 3. DATA PROCESSING AND CATEGORIZATION ---
def get_city_country_from_components(address_components):
    """Extracts city and country from address_components."""
    city = ''
    country = ''
    for component in address_components:
        if 'locality' in component['types']:
            city = component['long_name']
        if 'country' in component['types']:
            country = component['long_name']
    return city, country

def assign_category(types, name):
    """Assigns a custom category based on keywords."""
    name = name.lower()
    types_string = " ".join(types).lower()
    full_text = f"{name} {types_string}"

    # Check for Accommodation first because it can be more specific
    if any(kw in full_text for kw in ['hotel', 'resort', 'apartments', 'accommodation', 'villa', 'guesthouse', 'lodging', 'inn', 'suites']):
        return 'Accommodation'
    # Then check for Health & Wellbeing
    if any(kw in full_text for kw in ['spa', 'health', 'clinic', 'wellness', 'yoga', 'beauty', 'salon', 'therapies']):
        return 'Health & Wellbeing'
    # Then Creative Industries
    if any(kw in full_text for kw in ['design', 'studio', 'gallery', 'art', 'media', 'productions', 'creative', 'photography', 'fashion']):
        return 'Creative Industries'
    if any(kw in full_text for kw in ['events', 'planning', 'entertainment', 'lounge', 'nightclub']):
        return 'Event Management'
    # Then Cities & Developments with the new keyword
    if any(kw in full_text for kw in ['real estate', 'properties', 'developments', 'condominium', 'building', 'display village']):
        return 'Cities & Developments'
    if any(kw in full_text for kw in ['blockchain', 'crypto', 'web3']):
        return 'Blockchain'
    
    return 'Other' # Default category

# --- 4. MAIN WORKFLOW ---
def main():
    if not API_KEY:
        print("Error: GOOGLE_API_KEY is not set. Cannot run script.")
        return

    cities = get_cities()
    if not cities:
        print("No cities to process. Exiting.")
        return

    all_business_data = []
    for city in cities:
        places = search_places(city)
        for place in places:
            details = get_place_details(place.get('place_id'))
            if not details:
                continue

            lat = details.get('geometry', {}).get('location', {}).get('lat')
            lng = details.get('geometry', {}).get('location', {}).get('lng')
            city_name, country_name = get_city_country_from_components(details.get('address_components', []))

            business = {
                'place_id': details.get('place_id'),
                'name': details.get('name'),
                'category': assign_category(details.get('types', []), details.get('name', '')),
                'city': city_name,
                'country': country_name,
                'latitude': lat,
                'longitude': lng,
                'website': details.get('website', ''),
                'phone': details.get('international_phone_number', ''),
                'email': '',
                'social': '',
                'verified': False
            }
            all_business_data.append(business)

    if not all_business_data:
        print("No new business data found. Exiting.")
        save_last_run_timestamp()
        return

    df = pd.DataFrame(all_business_data)
    df.drop_duplicates(subset='place_id', inplace=True)
    df.reset_index(drop=True, inplace=True)
    df['id'] = df.index + 1

    try:
        locklist_df = pd.read_csv(LOCKLIST_PATH)
        if not locklist_df.empty:
            print("Applying manual overrides from locklist.csv...")
            df.set_index('place_id', inplace=True)
            locklist_df.set_index('place_id', inplace=True)
            df.update(locklist_df)
            df.reset_index(inplace=True)
    except FileNotFoundError:
        print("curation/locklist.csv not found. Skipping manual override.")

    final_columns = ['id', 'name', 'category', 'city', 'country', 'latitude', 'longitude', 'website', 'email', 'phone', 'social', 'verified']
    final_df = df.reindex(columns=final_columns) # Ensure columns are in the correct order
    final_df.to_csv(OUTPUT_CSV_PATH, index=False)

    print(f"\nProcess complete. Saved {len(final_df)} unique businesses to {OUTPUT_CSV_PATH}")
    save_last_run_timestamp()

if __name__ == "__main__":
    main()
