import os
import pandas as pd
import requests
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from google.cloud import translate_v2 as translate
import pycountry

# Load API key and initialize clients
load_dotenv()
API_KEY = os.getenv("GOOGLE_API_KEY")
translate_client = translate.Client()

# --- 1. CONFIGURATION ---
CITY_LIST_PATH = "curation/city-list.txt"
LOCKLIST_PATH = "curation/locklist.csv"
OUTPUT_CSV_PATH = "aura-affinity.csv"
TIMESTAMP_PATH = "last-run.txt"
SEARCH_KEYWORDS = ["Aura", "Chakra"] # Base keywords to translate

# --- Language Cache to avoid re-detecting language for the same country ---
language_cache = {}

def get_language_for_country(country_name):
    """Get the primary language code for a given country."""
    if country_name in language_cache:
        return language_cache[country_name]
    
    try:
        country = pycountry.countries.get(name=country_name)
        if country:
            lang = pycountry.languages.get(alpha_2=country.alpha_2)
            if lang:
                language_cache[country_name] = lang.alpha_2
                return lang.alpha_2
    except Exception:
        pass # Fallback for territories or complex names
    
    # Default fallback
    language_cache[country_name] = 'en'
    return 'en'

def translate_keywords(keywords, target_language):
    """Translate a list of keywords to the target language."""
    if target_language == 'en':
        return [] # No need to translate if the target is English
    try:
        results = translate_client.translate(keywords, target_language=target_language)
        return [result['translatedText'] for result in results]
    except Exception as e:
        print(f"  Could not translate keywords to {target_language}: {e}")
        return []

def get_cities():
    """Reads the list of cities and countries from the text file."""
    try:
        with open(CITY_LIST_PATH, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"Error: {CITY_LIST_PATH} not found. Please create it.")
        return []

def save_last_run_timestamp():
    """Saves the current UTC timestamp to a file."""
    with open(TIMESTAMP_PATH, "w") as f:
        f.write(datetime.now(timezone.utc).isoformat())
    print(f"Saved timestamp to {TIMESTAMP_PATH}.")

def search_places(query_location):
    """Searches for businesses using both English and translated keywords."""
    print(f"\nSearching in {query_location}...")
    
    country_name = query_location.split(',')[-1].strip()
    target_lang = get_language_for_country(country_name)
    
    translated_kws = translate_keywords(SEARCH_KEYWORDS, target_lang)
    all_search_terms = SEARCH_KEYWORDS + translated_kws
    
    all_places = []
    
    for term in set(all_search_terms): # Use set to avoid duplicate searches
        print(f"  Searching for term: '{term}'...")
        url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
        query = f"business named {term} in {query_location}"
        params = {"query": query, "key": API_KEY}

        while True:
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()
                results = response.json()
            except requests.exceptions.RequestException as e:
                print(f"    Error during search: {e}")
                break
            
            all_places.extend(results.get('results', []))
            next_page_token = results.get('next_page_token')
            if not next_page_token:
                break
            
            time.sleep(2)
            params = {"pagetoken": next_page_token, "key": API_KEY}
    
    return all_places

def get_place_details(place_id):
    # This function remains the same as before
    print(f"    Getting details for place_id: {place_id[:10]}...")
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "place_id": place_id,
        "fields": "name,place_id,formatted_address,geometry,website,international_phone_number,types,address_components,reviews",
        "key": API_KEY
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json().get('result', {})
    except requests.exceptions.RequestException as e:
        print(f"    Error getting details for {place_id[:10]}: {e}")
        return {}

def get_city_country_from_components(address_components):
    # This function remains the same
    city, country = '', ''
    for component in address_components:
        if 'locality' in component['types']:
            city = component['long_name']
        if 'country' in component['types']:
            country = component['long_name']
    return city, country

def assign_category(types, name):
    # This function remains the same
    name = name.lower()
    types_string = " ".join(types).lower()
    full_text = f"{name} {types_string}"
    if any(kw in full_text for kw in ['hotel', 'resort', 'apartments', 'accommodation', 'villa', 'guesthouse', 'lodging', 'inn', 'suites']):
        return 'Accommodation'
    if any(kw in full_text for kw in ['spa', 'health', 'clinic', 'wellness', 'yoga', 'beauty', 'salon', 'therapies']):
        return 'Health & Wellbeing'
    if any(kw in full_text for kw in ['design', 'studio', 'gallery', 'art', 'media', 'productions', 'creative', 'photography', 'fashion']):
        return 'Creative Industries'
    if any(kw in full_text for kw in ['events', 'planning', 'entertainment', 'lounge', 'nightclub']):
        return 'Event Management'
    if any(kw in full_text for kw in ['real estate', 'properties', 'developments', 'condominium', 'building', 'display village']):
        return 'Cities & Developments'
    if any(kw in full_text for kw in ['blockchain', 'crypto', 'web3']):
        return 'Blockchain'
    return 'Other'

def main():
    if not API_KEY:
        print("Error: GOOGLE_API_KEY is not set. Cannot run script.")
        return

    locations = get_cities()
    if not locations:
        print("No locations to process. Exiting.")
        return
            
    all_business_data = []
    for loc in locations:
        places = search_places(loc)
        for place in places:
            details = get_place_details(place.get('place_id'))
            if not details: continue
            
            lat = details.get('geometry', {}).get('location', {}).get('lat')
            lng = details.get('geometry', {}).get('location', {}).get('lng')
            city_name, country_name = get_city_country_from_components(details.get('address_components', []))
            first_review = details.get('reviews', [{}])[0].get('text', '')

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
                'email': '', 'social': '',
                'description': first_review,
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

    final_columns = ['id', 'name', 'category', 'city', 'country', 'latitude', 'longitude', 'website', 'email', 'phone', 'social', 'description', 'verified']
    final_df = df.reindex(columns=final_columns)
    final_df.to_csv(OUTPUT_CSV_PATH, index=False, encoding='utf-8')
    
    print(f"\nProcess complete. Saved {len(final_df)} unique businesses to {OUTPUT_CSV_PATH}")
    save_last_run_timestamp()

if __name__ == "__main__":
    main()
