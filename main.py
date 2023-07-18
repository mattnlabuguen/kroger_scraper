import csv
import json
import logging
import os
import random
import requests
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, date
from dataclasses import dataclass
from typing import List

import pandas as pd
from fake_useragent import UserAgent
from win10toast import ToastNotifier

# Lock for synchronization
lock = threading.Lock()
logging.basicConfig(filename=f'logs/{date.today()}.txt', level=logging.ERROR, datefmt='%Y-%m-%d %H:%M:%S',
                    format='%(asctime)s %(levelname)-8s %(message)s')
user_agent = UserAgent(browsers=['edge', 'chrome', 'safari'])
toaster = ToastNotifier()


@dataclass
class KrogerData:
    Ecommerce: str
    CityName: str
    StateAbbrev: str
    ZipCode: str
    Delivery: str
    DeliveryGrocery: list
    DeliveryRestaurants: list
    DeliveryAll: list
    Pickup: str
    PickupGrocery: list
    PickupRestaurants: list
    PickupAll: list


def postal_code_formatter(code: int) -> str:
    postal_code = str(code)
    if len(postal_code) == 4:  # Prepend 0 to postal codes with only 4 digits.
        postal_code = f'0{code}'

    return postal_code


def download_data(postal_code: str) -> str:
    try:
        api_url = 'https://www.kroger.com/atlas/v1/modality/options'
        default_headers = {
            'authority': 'www.kroger.com',
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/json',
            'origin': 'https://www.kroger.com',
            'referer': 'https://www.kroger.com/',
            'user-agent': user_agent.random
        }
        payload = {
            'address': {
                'postalCode': postal_code
            }
        }
        status_code = None
        response = requests.post(api_url, timeout=random.randint(10, 100), headers=default_headers,
                                 data=json.dumps(payload))
        if response:
            raw_data = response.text
            status_code = response.status_code

            if response.ok or response.status_code in [200, 201]:
                print(f"Download success: {postal_code}")
            elif status_code == 400:
                invalid_msg = f"Postal code is invalid - {postal_code}"
                logging.error(invalid_msg)
                raise Exception(invalid_msg)

            return raw_data
        else:
            toaster.show_toast('Download data error', 'Download failure exception', duration=3)
            raise Exception(f'Download failure exception')

    except Exception as e:
        logging.error(str(f'download_data() error: {str(e)} - {postal_code}'))


def transform_data(details: dict, raw_data: str) -> List[KrogerData]:
    data = {
        'Ecommerce': 'Kroger',
        'CityName': details.get('NAME', None),
        'StateAbbrev': details.get('RG_ABBREV', None),
        'ZipCode': details.get('ID', None),
        'Delivery': 'No',  # By default, set value to No.
        'DeliveryGrocery': [],
        'DeliveryRestaurants': [],
        'DeliveryAll': [],
        'Pickup': 'No',  # By default, set value to No.
        'PickupGrocery': [],
        'PickupRestaurants': [],
        'PickupAll': [],
    }

    try:
        json_data = json.loads(raw_data)
        if 'data' in json_data:
            if 'modalityOptions' in json_data['data']:
                modality_options = json_data['data']['modalityOptions']
                data = check_modality_options(data, modality_options)

                if 'storeDetails' in modality_options:
                    store_details = modality_options['storeDetails']
                    data = get_modality_brands(data, store_details)

        return list(data.values())

    except json.decoder.JSONDecodeError:
        toaster.show_toast('Error', 'JSON Decode Error', duration=3)
        logging.error('transform_data() error: Downloaded data could not be parsed into JSON format')
    except Exception as e:
        toaster.show_toast('Transform data error', str(e), duration=3)
        logging.error(f'transform_data() error: {str(e)}')


def check_modality_options(data: dict, modality_options: dict) -> dict:
    options_to_check = ['DELIVERY', 'PICKUP']
    for option in options_to_check:
        if option in modality_options and modality_options[option]:
            data[option.capitalize()] = 'Yes'
        else:
            data[option.capitalize()] = 'No'

    return data


def get_modality_brands(data: dict, store_details: dict) -> dict:
    kroger_brand_list = [
        'Baker’s', 'City Market', 'Dillons', 'Food 4 Less', 'Foods Co', 'Fred Meyer', 'Fry’s',
        'Gerbes', 'Harris Teeter', 'Jay C Food Store', 'King Soopers', 'Kroger', 'Mariano’s',
        'Metro Market', 'Pay-Less Super Markets', 'Pick’n Save', 'QFC', 'Ralphs', 'Ruler',
        'Smith’s Food and Drug'
    ]

    if data['Pickup'] == 'Yes':
        for store in store_details:
            if 'PICKUP' in store['allowedModalities']:
                banner = store['banner']
                highest_similarity = 0
                closest_brand = None

                for brand in kroger_brand_list:
                    similarity = jaccard_similarity(banner, brand)
                    if similarity > highest_similarity:
                        highest_similarity = similarity
                        closest_brand = brand

                if closest_brand:
                    if closest_brand not in data['PickupGrocery']:
                        data['PickupGrocery'].append(closest_brand)

                    if closest_brand not in data['PickupAll']:
                        data['PickupAll'].append(closest_brand)

    return data


def jaccard_similarity(s1: str, s2: str) -> float:
    """
    This method takes the measure of similarity between two strings by converting them into sets.
    :param s2: String to be compared to s1 to check how similar they are to each other.
    :param s1: String to be compared to s2 to check how similar they are to each other.
    :return: Returns a float value that determines how similar both strings are.
    """
    set1 = set(s1)
    set2 = set(s2)

    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))

    return intersection / union


def write_to_file(file_path: str, data: list, mode: str = 'w'):
    with lock:
        with open(file_path, mode, newline='') as file:
            writer = csv.writer(file)
            writer.writerow(data)


def extract_csv_data(input_file: str = 'input/USZipCodesXLS.csv',
                     output_file: str = 'output/Kroger-US-Full.csv') -> dict:
    input_df = pd.read_csv(input_file)
    output_df = pd.read_csv(output_file)
    existing_postal_codes = output_df['ZipCode'].to_list()

    csv_data = input_df[~input_df['ID'].isin(existing_postal_codes)].to_dict(orient='records')
    print(f'{len(existing_postal_codes)} postal codes filtered out')

    return csv_data

    # states = df['RG_NAME'].unique().tolist()
    # existing_postal_codes = []
    #
    # for state in states:
    #     state_file = f'output/{state}.csv'
    #     state_path = os.path.expanduser(f'{state_file}')
    #     if os.path.exists(state_path):
    #         state_df = pd.read_csv(state_file)
    #         existing_postal_codes.extend(state_df['ZipCode'].to_list())
    #
    # df = df[~df['ID'].isin(existing_postal_codes)]  # Filter out postal codes that have finished
    # print(f'{len(existing_postal_codes)} postal codes filtered out')
    #
    # grouped_data_by_state = df.groupby('RG_NAME').apply(lambda x: x.to_dict('records')).to_dict()
    # return grouped_data_by_state


def process_data(details: dict, output_file: str = 'output/Kroger-US-Full.csv'):
    postal_code = postal_code_formatter(details.get('ID', None))
    raw_data = download_data(postal_code)

    if raw_data:
        data = transform_data(details, raw_data)
        write_to_file(output_file, data, 'a')
        print(f"Written to file: {postal_code}")

        delay = random.randint(5, 15)
        print(f'Delaying for {delay} seconds.')
        time.sleep(delay)
    else:
        logging.error(f"Process failed: {details['ID']}")


def initialize_output_file(filename: str = 'output/Kroger-US-Full.csv'):
    if not os.path.exists('output'):
        os.mkdir('output')

    initial_file = pd.DataFrame(columns=['Ecommerce', 'CityName', 'StateAbbrev', 'ZipCode', 'Delivery',
                                         'DeliveryGrocery', 'DeliveryRestaurants', 'DeliveryAll', 'Pickup',
                                         'PickupGrocery', 'PickupRestaurants', 'PickupAll'])
    initial_file.to_csv(filename, mode='w', index=False)


def main():
    initialize_output_file()
    csv_data = extract_csv_data()

    with ThreadPoolExecutor(max_workers=12) as executor:
        for row in csv_data:
            executor.submit(process_data, row, 'output/Kroger-US-Full.csv')
            time.sleep(random.randint(1, 3))


def test():
    initialize_output_file('output/test_output.csv')
    test_row = {
        'OBJECTID': 11212,
        'ID': 36804,
        'NAME': 'Opelika',
        'RG_NAME': 'Alabama	',
        'RG_ABBREV': 'AL',
    }

    process_data(test_row, 'output/test_output.csv')


if __name__ == '__main__':
    main()
