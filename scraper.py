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
    url = 'https://classpass.com/_api/unisearch/v1/layout/web_search_page'
    search_categories = ['fitness', 'gym', 'wellness']
    request_header = {
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
        'User-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36'}

    def __init__(self, lat=34.0522342, lon=-118.2436849, category_idx=0):
        """
        Constructor of the Scraper class
        Arguments:
            lat {float} -- The latitude of the center of the search. Default to lat of center LA.
            lon {float} -- The longitude of the center of the search. Default to lon of center LA.
        """
        self.session = requests.Session()

        # We store the following information:
        # 'id': the venue_id
        # 'search_date': the date of the search. We record it because the rating and the rating total will change over time.
        # 'venue_name': the venue name
        # 'location_name': the location name
        # 'activities': an array of the activities/tags
        # 'display_rating_total': the total count of the rating
        # 'display_rating_average': the average rating
        # 'description': the description of the venue
        self.venues = pd.DataFrame(columns=[
                                   'id', 'search_date', 'venue_name', 'location_name', 'activities', 'display_rating_total', 'display_rating_average', 'description'])
        self.request_payload = {
            'search_request': {
                'filters': {
                    'vertical': Scraper.search_categories[category_idx],
                    'date': date.today().strftime('%Y-%m-%d'),
                    'tag': [],
                    'lat': lat,
                    'lon': lon,
                    'result_type': 'VENUE'
                }
            }
        }

    @staticmethod
    def random_sleep():
        """Allows the program to sleep for random amount of time to avoid blocked by the site
        Raises:
            Exception -- If the provided url is not valid (HTTP 404) or the network condition is off, an exception will be thrown.
        Returns:
            None
        """
        time.sleep(uniform(0, 2))

    def get_search_results(self):
        """It return json data of the search results.
        """
        # sleep to avoid detection
        Scraper.random_sleep()

        # repeat the request for max_iter times just to avoid package loss or network glitches
        for i in range(self.max_iter):
            Scraper.random_sleep()
            resp = self.session.post(
                Scraper.url, headers=self.request_header, data=json.dumps(
                    self.request_payload), verify=False)
            if resp.status_code == 200:
                return json.loads(resp.text)['data']['modules']['web_search_results_01']['data']['venue_tab_items']
            print('ERROR with status code {}'.format(resp))
            print('HTTP response body {}'.format(resp.text))
        raise Exception(
            'Request failed {} times. It is probably blocked.'.format(self.max_iter))
        return None

    def append_search_results(self, data):
        search_date = date.today().strftime('%Y-%m-%d')
        for entry in data:
            id = entry['venue_id']
            venue_name = entry['venue_name']
            location_name = entry['location_name'] if 'location_name' in entry else None
            activities = entry['activities'] if 'activities' in entry else None
            description = entry['description'] if 'description' in entry else None
            display_rating_total = entry['display_rating_total'] if 'display_rating_total' in entry else None
            display_rating_average = entry['display_rating_average'] if 'display_rating_average' in entry else None
            self.venues = self.venues.append(
                {'id': id, 'search_date': search_date, 'venue_name': venue_name, 'location_name': location_name,
                 'activities': activities, 'display_rating_total': display_rating_total, 'display_rating_average': display_rating_average,
                 'description': description}, ignore_index=True)
