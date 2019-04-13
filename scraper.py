import requests
from random import uniform
import time
import pandas as pd
from datetime import date
import json


class Scraper():
    """Scraper class contains the basic information that is needed for scraping.
    """
    # max_iter defines the max number of reqeust attempts.
    # If the max number of attempt is reached, the requests might be blocked.
    max_iter = 10
    search_url = 'https://classpass.com/_api/unisearch/v1/layout/web_search_page'
    search_categories = ['fitness', 'gym', 'wellness']
    search_request_header = {
        'authority': 'classpass.com',
        'method': 'POST',
        'path': '/_api/unisearch/v1/layout/web_search_page',
        'scheme': 'https',
        'accept': '*/*',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US,en',
        'content-type': 'application/json',
        # Token comes from the search page of classpass. It was hardcoded in `production` mode as the following value.
        'cp-authorization': 'Token 711016a6872c11e5a72022000bca01dd',
        'origin': 'https://classpass.com',
        'platform': 'web',
        'referer': 'https://classpass.com/search/ladera-heights-ca-usa/fitness-classes/4VvdcJFiBgT',
        'User-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36'
    }
    place_id_url = 'https://classpass.com/_api/unisearch/v1/location/autocomplete'
    place_id_request_header = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US, en',
        'Content-Type': 'application/json',
        'CP-Authorization': 'Token 711016a6872c11e5a72022000bca01dd',
        'Host': 'classpass.com',
        'Origin': 'https://classpass.com',
        'platform': 'web',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0 Win64 x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/17.17134'
    }
    rows_retrieved = 0

    def __init__(self, category_idx=0):
        """
        Constructor of the Scraper class
        Arguments:
            category_idx {int} -- The category in which we
            lon {float} -- The longitude of the center of the search. Default to lon of center LA.
        """
        self.session = requests.Session()
        self.category_idx = category_idx
        # We store the following information:
        # 'id': the venue_id
        # 'datetime': the date of the search. We record it because the rating and the rating total will change over time.
        # 'venue_name': the venue name
        # 'location_name': the location name
        # 'activities': an array of the activities/tags
        # 'display_rating_total': the total count of the rating
        # 'display_rating_average': the average rating
        # 'description': the description of the venue
        self.venues = pd.DataFrame(columns=[
                                   'id', 'datetime', 'venue_name', 'zipcode', 'location_name', 'activities', 'display_rating_total', 'display_rating_average', 'description'])

    @staticmethod
    def random_sleep():
        """Allows the program to sleep for random amount of time to avoid blocked by the site
        Raises:
            Exception -- If the provided url is not valid (HTTP 404) or the network condition is off, an exception will be thrown.
        Returns:
            None
        """
        time.sleep(uniform(0, 1))

    def get_place_id_from_zipcode(self, zipcode):
        """Given the zipcode of the are of interest, return place_id

        Arguments:
            zipcode {string} -- the zipcode of the area of interest
        """
        # the specific payload on class_pass autocomplete
        place_id_request_payload = {
            "autocomplete_type": "geocode", "query": str(zipcode)}
        place_id_resp = self.session.post(
            Scraper.place_id_url, headers=Scraper.search_request_header, data=json.dumps(
                place_id_request_payload), verify=True)
        if place_id_resp.status_code == 200:
            if len(json.loads(place_id_resp.text)['data']['predictions']) == 0:
                return None
            return json.loads(place_id_resp.text)['data']['predictions'][0]['place_id']
        print('ERROR with status code {}'.format(place_id_resp.status_code))
        print('HTTP response body {}'.format(place_id_resp.text))

    def get_lat_lon_from_place_id(self, place_id):
        """Given the place_id, it returns the lat and lon

        Arguments:
            place_id {string} -- the place_id that represents the center of the area of interest
        """
        lat_lon_resp = self.session.get(
            'https://classpass.com/_api/unisearch/v1/location/details/{}'.format(place_id), headers=Scraper.place_id_request_header, verify=False)
        if lat_lon_resp.status_code == 200:
            lat_lon_resp_data = json.loads(lat_lon_resp.text)['data']
            if not lat_lon_resp_data:
                return [None, None]
            return [lat_lon_resp_data['lat'], lat_lon_resp_data['lon']]
        print('ERROR with status code {}'.format(lat_lon_resp.status_code))
        print('HTTP response body {}'.format(lat_lon_resp.text))

    def get_search_results(self, lat, lon, place_id):
        """It return json data of the search results.
        """
        # sleep to avoid detection
        Scraper.random_sleep()
        search_request_payload = {
            'search_request': {
                'filters': {
                    'vertical': Scraper.search_categories[self.category_idx],
                    'date': date.today().strftime('%Y-%m-%d'),
                    'tag': [],
                    'lat': lat,
                    'lon': lon,
                    'place_id': place_id,
                    'result_type': 'VENUE'
                }
            }
        }
        # repeat the request for max_iter times just to avoid package loss or network glitches
        for _ in range(self.max_iter):
            Scraper.random_sleep()
            resp = self.session.post(
                Scraper.search_url, headers=self.search_request_header, data=json.dumps(
                    search_request_payload), verify=False)
            if resp.status_code == 200:
                results = json.loads(resp.text)[
                    'data']['modules']['web_search_results_01']['data']
                if 'venue_tab_items' not in results:
                    return None
                return results['venue_tab_items']
            print('ERROR with status code {}'.format(resp))
            print('HTTP response body {}'.format(resp.text))
        raise Exception(
            'Request failed {} times. It is probably blocked.'.format(self.max_iter))

    def append_search_results(self, data, zipcode, attach):
        """Given the data, it append each entry of the data to the existing dataframe in the scraper instance.

        Arguments:
            data {an array of json objects} -- each entry of the array is a json object containing the venue information
        """
        datetime = date.today().strftime('%Y-%m-%d')
        if not attach:
            venues = pd.DataFrame(columns=[
                'id', 'datetime', 'venue_name', 'zipcode', 'location_name', 'activities', 'display_rating_total', 'display_rating_average', 'description'])
        for entry in data:
            id = entry['venue_id']
            venue_name = entry['venue_name']
            location_name = entry['location_name'] if 'location_name' in entry else None
            activities = entry['activities'] if 'activities' in entry else None
            description = entry['description'] if 'description' in entry else None
            display_rating_total = entry['display_rating_total'] if 'display_rating_total' in entry else None
            display_rating_average = entry['display_rating_average'] if 'display_rating_average' in entry else None
            if attach:
                self.venues = self.venues.append({'id': id, 'datetime': datetime, 'venue_name': venue_name, 'zipcode': zipcode, 'location_name': location_name,
                                                  'activities': activities, 'display_rating_total': display_rating_total, 'display_rating_average': display_rating_average,
                                                  'description': description}, ignore_index=True)
            else:
                venues = venues.append(
                    {'id': id, 'datetime': datetime, 'venue_name': venue_name, 'zipcode': zipcode, 'location_name': location_name,
                     'activities': activities, 'display_rating_total': display_rating_total, 'display_rating_average': display_rating_average,
                     'description': description}, ignore_index=True)
        return

    def save_venues_to_pickle(self, path='scraped_venues.pkl'):
        """Save the current venues to a pickle file locally. The default path is 'scraped_venues.pkl'
        """
        pd.to_pickle(self.venues, path)
