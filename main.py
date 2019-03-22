from scraper import Scraper
import pandas as pd


def get_zipcode_from_console():
    zipcode = input()


if __name__ == '__main__':

    # create the scraper
    scraper = Scraper(lat=lat if lat != '' else None,
                      lon=lon if lon != '' else None, category_idx=0)

    data = scraper.get_search_results()
    scraper.append_search_results(data)

    print('Finished retrieving data!')
    print('Total rows retrieved: {}'.format(len(scraper.venues)))
