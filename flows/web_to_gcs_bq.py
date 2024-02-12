from prefect import flow, task 
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import GcpCredentials, BigQueryWarehouse
import os
from urllib.parse import urlencode
from bs4 import BeautifulSoup as bs
import requests
from datetime import datetime, timedelta
import re
import pandas as pd
import numpy as np
import random
import time
from yaml import safe_load
import json 


def extract_rightmove_url(
    locationIdentifier="REGION^87490", 
    index=0,
    propertyTypes="bungalow,detached,flat,park-home,semi-detached,terraced", 
    maxDaysSinceAdded=1, 
    keywords="",
) -> str:
    """Generates a rightmove query URL from optional parameters. The default is set to:
    
        locationIdentifier is set to London
        index is set to 0, which represents the first page of results
        propertyTypes set to view bungalows, detached, flats, park-homes, semi-detached and terraced homes
        maxDaysSinceAdded set to 1, which is properties added in the last day
        keywords defaults to empty"""

    results_per_page=48
    
    maxDaysSinceAdded_valid = {'',1,3,7,14}
    if maxDaysSinceAdded not in maxDaysSinceAdded_valid:
        raise ValueError("results: maxDaysSinceAdded must be one of %r." % maxDaysSinceAdded_valid)

    params = {
        "locationIdentifier": locationIdentifier,
        "index": index,
        "propertyTypes": propertyTypes,
        "maxDaysSinceAdded": maxDaysSinceAdded,
        "numberOfPropertiesPerPage": results_per_page,
        "keywords": keywords,
    }

    # Generate the URL from the parameters given
    url = "https://www.rightmove.co.uk/property-for-sale/find.html?" + urlencode(params)
    return url


@task(name='extract_property_urls',
      log_prints=True,
      retries=3)
def get_rightmove_results(url: str, test: bool) -> list:
    """Takes a rightmove query URL and returns a list of
    property listing URLs."""
 
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    }
    search = requests.get(url, headers=headers)
    soup = bs(search.content, "html.parser")

    # Get the number of results from this query and find the number of
    # pages the query returns. First function returns a page with 48 results.
    result_count = int(soup.find("span", class_="searchHeader-resultCount").get_text().replace(',',''))
    page_count = int(round(result_count / 48, 0)) - 1
    # Ensure we're capturing the last pages of results.
    page_count += 1 if result_count % 48 > 0 else 0

    # Iterate through each page and fetch the URL
    property_links = []
   
    print(f"Scraping {page_count} pages for {result_count} results.")
    for page in range(0, page_count):
        
        if page == 0:
            continue
        else:
            url = extract_rightmove_url(index=page * 48)
            page = requests.get(url, headers=headers)
            soup = bs(page.content, "html.parser")
            time.sleep(random.uniform(2, 8))
            

        # Grabbing all property cards and slicing off the first listing, which is
        # always a "featured property" and may not be relevant to our search
        property_cards = soup.find_all("div", class_="l-searchResult is-list")
        property_cards = property_cards[1:]

        for card in property_cards:
            property_link = card.find(
                "a", class_="propertyCard-priceLink propertyCard-salePrice"
            ).attrs["href"]
            property_links.append(property_link)
        if test:
            break
    
    return property_links


@task(name='extract_rightmove_data',
      log_prints=True)
def scrape_page(url_suffixs: list) -> list:
    """Scrapes a list of rightmove pages for data. Returns data in the form of a list of dictionaries."""

    data_rows = []
    for suffix in url_suffixs:
        
        url_base = 'https://www.rightmove.co.uk'
        url = url_base + suffix

        headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        }

        try:
            page = requests.get(url, headers=headers, timeout=5)
        except requests.ConnectionError:
            t = random.uniform(30, 90)
            print(f"CONNECTION ERROR. Waiting {t} seconds.")
            time.sleep(t)
            page = requests.get(url, headers=headers, timeout=5)
        
        soup = bs(page.content, 'html.parser')

        # Assigning an ID to the property using the url 
        id = re.findall("([0-9]{7,15})", url)[0]
        id = int(id)

        # extracting the property listing description
        data = soup.find_all('div', class_="STw8udCxUaBUMfOOZu0iL _3nPVwR0HZYQah5tkVJHFh5")
        description = data[0].text

        # Extracting a JSON array from the HTML variable PAGE_MODEL, which contains much of the data we require
        script_elements = soup.find_all('script')
        page_model = [script.text for script in script_elements if 'PAGE_MODEL =' in script.text][0]
        data = page_model.split("PAGE_MODEL = ", 1)[1].strip()
        data = json.loads(data)
        data = data.get('propertyData')

        # Using the JSON array to extract a listing type (typically either added or reduced) and the date the listing was made
        listing = data.get("listingHistory", {}).get('listingUpdateReason', None)
        if listing != None:
            listing = listing.split(' ')
            listing_type = listing[0]
        else:
            listing_type = listing
        
        try:
            if listing[-1] == "today":
                Date = datetime.now().strftime("%Y-%m-%d")
            elif listing[-1] == "yesterday":
                Date = datetime.now() - timedelta(days=1)
                Date = Date.strftime("%Y-%m-%d")
            else:
                Date = listing[-1]
                Date = datetime.strptime(Date, "%d/%m/%Y").strftime("%Y-%m-%d")
        except:
            Date = None
            
        # Using the JSON array to extract more data
        price = data.get("prices", {}).get('primaryPrice', np.nan)
        price = int(re.sub('[£,]', '', price))
        
        address = data.get("address", {}).get('displayAddress')
        outcode = data.get("address", {}).get('outcode')
        incode = data.get("address", {}).get('incode')

        estate_agent = data.get('customer', {}).get('companyName')

        nearest_stations = data.get('nearestStations')

        try:
            nearest_station = nearest_stations[0].get('name')
            distance_from_nearest_station_miles = round(nearest_stations[0].get('distance',np.nan),2)
        except:
            nearest_station = None
            distance_from_nearest_station_miles = np.nan

        try:
            second_nearest_station = nearest_stations[1].get('name')
            distance_from_second_nearest_station_miles = round(nearest_stations[1].get('distance', np.nan),2)
        except:
            second_nearest_station = None
            distance_from_second_nearest_station_miles = np.nan

        try:
            third_nearest_station = nearest_stations[2].get('name')
            distance_from_third_nearest_station_miles = round(nearest_stations[2].get('distance', np.nan),2)
        except:
            third_nearest_station = None
            distance_from_third_nearest_station_miles = np.nan

        bedrooms = data.get('bedrooms', np.nan)
        if bedrooms == None:
            bedrooms = 0


        bathrooms = data.get('bathrooms', np.nan)
        if bathrooms == None:
            bathrooms = np.nan

        size = np.nan
        for unit in data.get('sizings', ()):
            if unit['unit'] == 'sqm':
                size = unit['minimumSize']
        
        tenure = data.get('tenure',{})
        tenure_type = tenure.get('tenureType')
        lease_length = tenure.get('yearsRemainingOnLease', np.nan)
        if lease_length == None or lease_length == 0 or lease_length =='0':
            lease_length = np.nan
       

        living_costs = data.get('livingCosts', {})
        ground_rent = living_costs.get('annualGroundRent')
        if ground_rent == None:
            ground_rent = np.nan

        service_charge = living_costs.get('annualServiceCharge')
        if service_charge == None:
            service_charge = np.nan

        property_type = 'N/A'
        try:
            property_type = [item['primaryText'] for item in data['infoReelItems'] if item['type'].lower() == 'property_type'][0]
        except:
            pass

        # Creating a dictionary using the variables assigned above. For each loop, a dictionary is created. Each dictionary represents a row in the dataframe
        # created in the clean() function below.
        row = {
            "id": id,
            # "testing": testing_keys,
            "Property_Link": url,
            "Address": address,
            "Outcode": outcode,
            "Incode": incode, 
            "Price": price,
            "Listing_Type": listing_type, # typically either added or reduced
            "Date": Date,
            "Property_Type": property_type, # don't really need this one tbh - could be fun to do analyses based on this though 
            "Size_Sqm": size, 
            "Bedrooms": bedrooms,
            "Bathrooms": bathrooms,
            "Ground_Rent": ground_rent,
            "Service_Charge": service_charge,
            "Tenure_Type": tenure_type,
            "Lease_Length_Years": lease_length,
            "Estate_Agent": estate_agent,
            "Nearest_Station": nearest_station,
            "Distance_From_Nearest_Station_Miles": distance_from_nearest_station_miles,
            "Second_Nearest_Station": second_nearest_station,
            "Distance_From_Second_Nearest_Station_Miles": distance_from_second_nearest_station_miles,
            "Third_Nearest_Station": third_nearest_station,
            "Distance_From_Third_Nearest_Station_Miles": distance_from_third_nearest_station_miles,
            "Description": description 
        }

        data_rows.append(row)
    return data_rows 


@task(name='clean_data_create_df',
      log_prints=True)
def clean(rows_list: list) -> pd.DataFrame:
    """Takes a list of property row dictionaries, returns a dataframe with a correct schema.
    Depends on dtypes.yaml."""

    df = pd.DataFrame(rows_list)
    # with open("./dtypes.yaml", "rb") as schema_yaml:
    #     schema = safe_load(schema_yaml)["raw_dtypes"]

    # try:
    #     df = df.astype(schema)
    # except BaseException as error:
    #     today = datetime.today().strftime("%Y-%m-%d")
    #     df.to_csv(f"/tmp/{today}_failed.csv")
    #     gcs_block = GcsBucket.load('london-properties')
    #     gcs_block.upload_from_path(
    #         from_path=f"/tmp/{today}_failed.csv",
    #         to_path=f"raw_daily_data/failed/{today}_daily_london_failed.csv",
    #     )
    #     raise Exception("The dataframe doesn't conform to the specified schema. Dataframe has been loaded to 'failed' directory in the GCS bucket.")

    return df


@task(name='save_to_gcp',
      log_prints=True)
def save_to_gcp(df: pd.DataFrame) -> None:
    """Take a Pandas DataFrame and save it to GCP in Parquet format"""

    today = datetime.today().strftime("%Y-%m-%d")
    df.to_parquet(f"/tmp/{today}.parquet")

    input_path = f"/tmp/{today}.parquet"
    output_path = f"raw_daily_data/succeeded/{today}.parquet"

    gcs_block = GcsBucket.load("london-properties")
    gcs_block.upload_from_path(
        from_path=input_path,
        to_path=output_path
    )

    os.remove(input_path)


@task(name='external_table',
      log_prints=True)
def create_external_table():
    """Creates an external table, if it doesn't already exist, pointing to the bucket storage location
    where the daily raw london-properties parquet files are stored"""

    gcp_credentials = GcpCredentials.load('london-properties-analysis')

    with BigQueryWarehouse(gcp_credentials=gcp_credentials) as warehouse:
        warehouse.execute(
            """CREATE EXTERNAL TABLE IF NOT EXISTS properties_dataset.raw_london_properties 
            OPTIONS (format = 'PARQUET', uris=['gs://london_property_data_evident-display-410312/raw_daily_data/succeeded/*.parquet'])"""
        )


@flow(name='ingest_gcp',
      log_prints=True)
def main_flow(test: bool = True):

    url = extract_rightmove_url()
    properties_urls = get_rightmove_results(url, test)
    rows_list = scrape_page(properties_urls)
    df = clean(rows_list)
    save_to_gcp(df)

    # creating the external table to point to the parquet files, if it doesn't already exist
    create_external_table()




if __name__ == '__main__':

    main_flow()



