import time
import csv
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlencode

BASE_URL = "https://helly-hansen.locally.com"
MAP_ENDPOINT = f"{BASE_URL}/stores/conversion_data"
STORE_ENDPOINT = f"{BASE_URL}/conversion/location/store/{{store_id}}?company_id=58&only_retailer_id=&service=store-locator&lang=en-us"
DEALER_ENDPOINT = f"{BASE_URL}/conversion/location/dealer/{{store_id}}?company_id=58&only_retailer_id=&service=store-locator&lang=en-us"

LATITUDE_RANGES = [(47.0, 30.0), (30, 27), (32.75, 32.75), (38.5, 38.5)]
LONGITUDE_RANGES = [(-13.0, 8.0), (-20, -12), (-17, -17), (-28.5, -28.5)]

BOX_SIZE = 1.5
OUTPUT_FILE = "helly_hansen_stores.csv"
REQUEST_DELAY = 0.5

MAP_PARAMS = {
    "has_data": "true",
    "company_id": "58",
    "category": "Sportswear or brandstore or outlet",
    "inline": "1",
    "map_ne_lat": "{ne_lat}",
    "map_ne_lng": "{ne_lng}",
    "map_sw_lat": "{sw_lat}",
    "map_sw_lng": "{sw_lng}",
    "map_center_lat": "{center_lat}",
    "map_center_lng": "{center_lng}",
    "map_distance_diag": "110.4776385381557",
    "sort_by": "proximity",
    "no_variants": "0",
    "dealers_company_id": "58",
    "only_store_id": "false",
    "uses_alt_coords": "false",
    "q": "false",
    "zoom_level": "8.867423876504883",
    "lang": "en-us",
}

FIELDS = [
    "name",
    "address",
    "has_enabled_affiliate",
    "phone",
    "city",
    "state",
    "country",
    "zip",
    "website_url",
    "maps_url",
    "lat",
    "lng",
]

HEADERS = {
    "name": "Nombre de la tienda",
    "address": "Dirección",
    "has_enabled_affiliate": "Afiliado",
    "phone": "Teléfono",
    "city": "Ciudad",
    "state": "Provincia",
    "country": "País",
    "zip": "Código postal",
    "website_url": "Página web",
    "maps_url": "Google Maps",
    "lat": "Latitud",
    "lng": "Longitud",
}


def build_map_url(box):
    """
    Build the map URL for fetching store markers within a bounding box.
    """
    center_lat, center_lng, ne_lat, ne_lng, sw_lat, sw_lng = box
    params = {
        k: v.format(
            center_lat=center_lat,
            center_lng=center_lng,
            ne_lat=ne_lat,
            ne_lng=ne_lng,
            sw_lat=sw_lat,
            sw_lng=sw_lng,
        )
        for k, v in MAP_PARAMS.items()
    }
    return f"{MAP_ENDPOINT}?{urlencode(params)}"


def extract_urls_from_html(html):
    """
    Extract website and Google Maps URLs from the store HTML.
    """
    soup = BeautifulSoup(html, "html.parser")
    website_tag = soup.select_one("span.store-info-subtitle a")
    maps_tag = soup.select_one("a.js-get-directions")

    website_url = website_tag["href"] if website_tag else ""
    maps_url = maps_tag["href"] if maps_tag else ""
    return website_url, maps_url


def fetch_store_data(store_id, marker):
    """
    Attempt to fetch detailed data for a given store by requesting both the
    store endpoint and the dealer endpoint, merging them into the marker dict.
    """
    try:
        store_resp = requests.get(STORE_ENDPOINT.format(store_id=store_id))
        store_resp.raise_for_status()
        store_data = store_resp.json()
        for k, v in store_data.items():
            marker.setdefault(k, v)

        return

    except Exception:
        print("Error fetching store data for store ID, trying dealer endpoint.")
        pass

    try:
        dealer_resp = requests.get(DEALER_ENDPOINT.format(store_id=store_id))
        dealer_resp.raise_for_status()
        dealer_data = dealer_resp.json()
        for k, v in dealer_data.items():
            marker.setdefault(k, v)

        return

    except Exception:
        pass


def write_csv_header(csv_writer):
    """
    Write the CSV header row using the HEADERS dictionary.
    """
    csv_writer.writerow([HEADERS.get(field, field) for field in FIELDS])


def write_csv_row(csv_writer, marker):
    """
    Write a single row of store data to the CSV.
    """
    csv_writer.writerow([marker.get(field, "") for field in FIELDS])


def generate_bounding_boxes():
    """
    Generate bounding boxes based on the LATITUDE_RANGES and LONGITUDE_RANGES.
    Yields tuples of:
        (center_lat, center_lng, ne_lat, ne_lng, sw_lat, sw_lng)
    """
    for (start_lat, end_lat), (start_lng, end_lng) in zip(
        LATITUDE_RANGES, LONGITUDE_RANGES
    ):
        center_lat = start_lat
        while center_lat >= end_lat:
            center_lng = start_lng
            while center_lng <= end_lng:
                ne_lat = center_lat + BOX_SIZE
                ne_lng = center_lng + BOX_SIZE
                sw_lat = center_lat - BOX_SIZE
                sw_lng = center_lng - BOX_SIZE

                yield center_lat, center_lng, ne_lat, ne_lng, sw_lat, sw_lng
                center_lng += BOX_SIZE

            center_lat -= BOX_SIZE


def main():
    seen_ids = set()
    wrote_header = False

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        for box in generate_bounding_boxes():
            url = build_map_url(box)

            try:
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()

            except Exception as e:
                print(f"Error fetching {url}: {e}")
                continue

            markers = data.get("markers", [])
            for marker in markers:
                store_id = marker.get("id")

                if store_id in seen_ids:
                    continue

                seen_ids.add(store_id)

                if marker.get("country") not in ["ES", "PT", "AD"]:
                    continue

                fetch_store_data(store_id, marker)

                store_html = marker.get("store_html", "")
                website_url, maps_url = extract_urls_from_html(store_html)
                affiliate = marker.get("has_enabled_affiliate", "")

                marker["website_url"] = website_url
                marker["maps_url"] = maps_url
                marker["has_enabled_affiliate"] = affiliate

                if not wrote_header:
                    write_csv_header(writer)
                    wrote_header = True

                write_csv_row(writer, marker)

            time.sleep(REQUEST_DELAY)


if __name__ == "__main__":
    main()
