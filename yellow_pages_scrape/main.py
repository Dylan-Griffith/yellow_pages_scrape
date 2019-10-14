from bs4 import BeautifulSoup
import requests
import os
import csv
import pandas as pd
from collections import defaultdict


# TODO: 2 DONE only capture if 'glass' in title
# TODO:  3 only capture if company has email
# TODO: 4 remove duplicates

STATE = 'Kansas'

STATE_NAME = STATE + '_zip_codes.csv'

state_master_data = STATE + '_master_data.csv'

# finds directory for csv files
script_dir = os.path.dirname(__file__)
state_csv_files = os.path.join(script_dir, 'all_page_links')

master_data = os.path.join(script_dir, 'master_data')


def get_all_links(url):

    '''
    3
    Gets all post URLs from page URLs
    Go's through each page and grabs the 30 post href tags
    :param url:
    :return: list
    '''

    page_links = []
    try:
        response = requests.get(url)

    except:
        return page_links

    soup = BeautifulSoup(response.content, features="html.parser")

    raw_href = soup.find_all('div', attrs={"info"})
    for x in raw_href:
        if 'glass' in x.find('a').text.lower():
            page_links.append('https://www.yellowpages.com' + x.find('a').get('href'))
        else:
            print(x.find('a').text.lower())
            print('Glass not in title name. Not saving data...')
            print()

    return page_links


def scrape_page(url):
    '''
    4
    Scrpes each listing for info
    :param url:
    :return company_name, address, email
    '''
    response = requests.get(url)
    soup = BeautifulSoup(response.content, features="html.parser")
    # Company Name

    try:
        company_name = soup.find('div', attrs={"sales-info"}).text
    except:
        company_name = 'None Listed'

    # Finding Email is no email returns
    tags = soup.find_all('a', attrs={"email-business"})
    try:
        email = tags[0].get('href')
        email = email.split('mailto:')[1]

    except Exception as e:
        print('No email listed for {company}'.format(company=company_name))
        return

    # checks to see if email address already saved
    try:
        temp_df = pd.read_csv(os.path.join(master_data, state_master_data))
        if email in list(temp_df['email']):
            return
    except Exception as e:
        print('No data frame found. Creating files now....')

    # Address
    try:
        address = soup.find('h2', attrs={"address"}).text
    except:
        address = 'None Listed'

    save_data(company_name, address, email)
    print('Saved: {url}'.format(url=url))

    return company_name, address, email


def page_url_links(zip_code):
    """
    2
    Determines how many pages to search and gets all URL Pages
    :param zip_code:
    :return: list of urls formatted for page amount
    """
    url = 'https://www.yellowpages.com/search?search_terms=autoglass&geo_location_terms={zip_code}'.format(
        zip_code=zip_code)
    all_url_links = []
    try:
        response = requests.get(url)

    except Exception as e:
        print(e)

    if response:
        soup = BeautifulSoup(response.content, features="html.parser")
        all_url_links = []

        # find numbers of listings from main page
        results = soup.find('div', attrs={"pagination"}).text
        try:
            num_listings = int(results.split('We found')[1].split('results12345Next')[0])
        except:
            num_listings = int(results.split('We found')[1].split('results12345Next')[0].split('results')[0])

        if num_listings > 30:
            num_pages = round(num_listings / 30)
        else:
            num_pages = 1

        for page in range(1, num_pages + 1):
            url = 'https://www.yellowpages.com/search?search_terms=autoglass&geo_location_terms={zip_code}&page={page_num}'.format(
                zip_code=zip_code,
                page_num=page)
            all_url_links.append(url)

    return all_url_links


def get_every_page_link():
    """
    1
    Goes through formatted_zip.csv
    Finds the page url for every city and stores in csv file
    :return:
    """
    states = defaultdict(list)
    # loads states csv file and stores into dictionary called states
    df = pd.read_csv('formatted_zip.csv')
    df['zip_code'] = df['zip_code'].astype(str).str.zfill(5)
    for x in range(df.shape[0]):
        states[df.iloc[x]['state_name']].append(df.iloc[x]['zip_code'])

    url_df = pd.read_csv('zip_code_all_page_links.csv')

    for state, zip_codes in states.items():
        if state not in list(url_df['state']):
            for zip_code in zip_codes:
                print(state + ': ' + zip_code)
                links = page_url_links(zip_code)
                try:
                    for link in links:
                        with open('zip_code_all_page_links.csv', 'a', newline='') as fout:
                            writer = csv.writer(fout)
                            writer.writerow([link, state, zip_code, False])

                except Exception as e:
                    print(e)
                    print('ERROR: Did not save data for {}'.format(zip_code))


def save_data(company_name, address, email):
    """
    saves data tp csv file
    :param company_name:
    :param address:
    :param email:
    :return:
    """
    if not os.path.isfile(os.path.join(master_data, state_master_data)):
        with open(os.path.join(master_data, state_master_data), 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['company_name', 'address', 'email'])
    else:
        with open(os.path.join(master_data, state_master_data), 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([company_name, address, email])


def main():
    df = pd.read_csv(os.path.join(state_csv_files, STATE_NAME))
    print('loaded')

    for x in range(df.shape[0]):
        try:
            if df.iloc[x]['scraped'] == 'FALSE' or not df.iloc[x]['scraped']:
                print('scraping ' + df.iloc[x]['state'] + ': ' + str(df.iloc[x]['zip_code']))
                # gets all urls from page (up to 30 links)
                post_links = get_all_links(df.iloc[x]['url'])

                # goes through each url and scrapes business (name, address, email) and saves to csv
                for url in post_links:
                    scrape_page(url)
                # time.sleep(.25)
                df.loc[x, 'scraped'] = True
                print('edited')
                df.to_csv(os.path.join(state_csv_files, STATE_NAME))
        except Exception as e:
            print(e)


if __name__ == "__main__":
    main()
