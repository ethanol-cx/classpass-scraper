from scraper import Scraper
import pandas as pd
from Databases.s3_transfer import s3_transfer
import sqlalchemy as db
import config
import time


def get_database_host_user_password():
    """Download the database credentials from aws s3. This part is specific and is subject to change
    on different bucket configuration.
    """
    credentials = s3_transfer(
        bucket_name=config.bucket_name,
        file_name=config.credential_name,
        direction='download',
        file_type='json')
    return [credentials['databases']['mysql']['aws']['host'],
            credentials['databases']['mysql']['aws']['user'],
            credentials['databases']['mysql']['aws']['password']]


def create_database_engine(user, password, host, database):
    """Create the mysql database engine.
    """
    try:
        engine = db.create_engine(
            'mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(user, password, host, database))
        return engine
    except:
        print('Exception: cannot create connection with database: {}'.format(database))


def get_all_zipcodes(conn, user, password, host):
    """Get all the zipcodes that we are interested in searching from the database.

    Arguments:
        conn {class 'sqlalchemy.engine.base.Connection'} -- an object of the database connection
        user {string} -- the user of the database
        password {string} -- the password
        host {string} -- the host of the database
    """
    zipcodes = pd.read_sql(
        'SELECT zipcode from geocoded_zips', con=conn)
    return zipcodes


if __name__ == '__main__':
    # record the start time of the program
    start_time = time.time()

    # get the database credentials from aws bucket
    [host, user, password] = get_database_host_user_password()

    # create the scraper; use only category_idx 0 for now ('fitness')
    scraper = Scraper(category_idx=0)

    # establish the database connection with the database zipcode_geocoded_data
    conn = create_database_engine(
        user, password, host, 'zipcode_geocoded_data').connect()

    # load the data for each zipcode of interest
    zipcodes = get_all_zipcodes(conn, user, password, host)
    for zipcode in zipcodes.values:
        print('Starting zipcode {} ...'.format(zipcode))
        place_id = scraper.get_place_id_from_zipcode(zipcode)
        [lat, lon] = scraper.get_lat_lon_from_place_id(place_id)
        data = scraper.get_search_results(lat, lon)
        if data:
            scraper.append_search_results(data)
            print('Finishing zipcode {} ...'.format(zipcode))
        else:
            print('No venues found in zipcode {} ...'.format(zipcode))

    print('Finished retrieving data!')
    print('Total rows retrieved: {}'.format(len(scraper.venues)))

    # stores the data into the dataset
    scraper.venues.to_sql('classpass_venues', create_database_engine(user, password, host, 'dev_test'), if_exists='append',
                          index=False, chunksize=3000, method='multi')
    print('Finished storing the data into the database: dev_test and the table: classpass_venues')
    print('Time used: {}'.format(time.time() - start_time))
