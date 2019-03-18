from scraper import Scraper
import pandas as pd

if __name__ == '__main__':
    # get the category, lat and lon to search
    lat = input(
        'What is the latitude of your center of searching? (type Enter to use the default)   ')
    lon = input(
        'What is the longitude of your center of searching? (type Enter to use the default)   ')
    while True:
        category_idx_str = input(
            'Which category do you want to search in? \n 1 - fitness, 2 - gym, 3 - wellness \n (type Enter to use the default(fitness))')
        if category_idx_str != '':
            category_idx = int(category_idx_str)
        else:
            category_idx = 1
        if type(category_idx) != type(1) or category_idx < 1 or category_idx > 3:
            print('Wrong choice of selection. Please try again.')
        break

    # create the scraper
    scraper = Scraper(lat=lat if lat != '' else None,
                      lon=lon if lon != '' else None, category_idx=category_idx)

    data = scraper.get_search_results()
    scraper.append_search_results(data)
    scraper.save_venues_to_pickle()
    print('Finished retrieving data!')
    print('Total rows retrieved: {}'.format(len(scraper.venues)))
