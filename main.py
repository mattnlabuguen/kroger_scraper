import csv
import json
import os
import random
import requests
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, date
from logging import Logger

import pandas as pd
from fake_useragent import UserAgent

from logger import initialize_logger

lock = threading.Lock()
max_retries = 3
max_workers = 12
retry_delay_range = (5, 15)
worker_delay_range = (1, 3)
timeout_range = (10, 100)
user_agent = UserAgent(browsers=['edge', 'chrome', 'safari'])
output_columns = ['Ecommerce', 'CityName', 'StateAbbrev', 'ZipCode', 'Delivery',
                  'DeliveryGrocery', 'DeliveryRestaurants', 'DeliveryAll', 'Pickup',
                  'PickupGrocery', 'PickupRestaurants', 'PickupAll']
state_list = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut',
              'Delaware', 'District of Columbia', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois',
              'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts',
              'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada',
              'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota',
              'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina',
              'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington',
              'West Virginia', 'Wisconsin', 'Wyoming']


class KrogerScraper:
    def __init__(self, postal_code_input, store_input, output_file, kroger_logger):
        self.postal_code_data = pd.read_csv(postal_code_input)
        self.store_data = pd.read_csv(store_input, encoding='ISO-8859-1')
        self.output_file = output_file
        self.logger = kroger_logger

        self.api_url = 'https://www.kroger.com/atlas/v1/modality/options'
        self.headers = {
            'authority': 'www.kroger.com',
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/json',
            'origin': 'https://www.kroger.com',
            'referer': 'https://www.kroger.com/'
        }

        # Setup default directories for output and log files.
        default_directories = ['output', 'logs']
        for directory in default_directories:
            if not os.path.exists(directory):
                os.mkdir(directory)

                if directory == 'output':
                    self.initialize_output_file()

    def run(self):
        filtered_postal_code_list = self.filter_postal_codes()
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for postal_code_details in filtered_postal_code_list:
                executor.submit(self.process_postal_code, postal_code_details)
                worker_delay = random.randint(worker_delay_range[0], worker_delay_range[1])
                time.sleep(worker_delay)

    def test(self, postal_code: int = None):
        if postal_code:  # For testing individual postal codes.
            postal_df = self.postal_code_data
            postal_code_details = postal_df.loc[postal_df['ID'] == postal_code].to_dict(orient='records')[0]
            self.process_postal_code(postal_code_details)
        else:
            filtered_postal_code_list = self.filter_postal_codes(state_filter=['California'])
            for postal_code_details in filtered_postal_code_list:
                self.process_postal_code(postal_code_details)

    def initialize_output_file(self):
        initial_file = pd.DataFrame(columns=output_columns)
        initial_file.to_csv(self.output_file, mode='w', index=False)

    def process_postal_code(self, postal_code_details: dict):
        postal_code = self.postal_code_formatter(postal_code_details.get('ID', None))
        kroger_postal_data = self.download_data(postal_code)

        if kroger_postal_data:
            transformed_data = self.transform_data(kroger_postal_data, postal_code_details)
            self.write_to_file(self.output_file, transformed_data, 'a')

            delay = random.randint(5, 15)
            self.logger.info(f'Delaying for {delay} seconds.')
            time.sleep(delay)

    def filter_postal_codes(self, state_filter=None) -> dict:
        postal_code_data = self.postal_code_data
        if state_filter is None or not state_filter:
            state_filter = state_list

        output_df = pd.read_csv(self.output_file)
        existing_postal_codes = output_df['ZipCode'].to_list()

        filtered_data = postal_code_data[(
                (~postal_code_data['ID'].isin(existing_postal_codes)) &  # Filter out any already existing postal code.
                (postal_code_data['RG_NAME'].isin(state_filter))  # Filter out states not specified in the list.
        )].to_dict(orient='records')

        self.logger.info(f'{len(existing_postal_codes)} postal codes filtered out')
        return filtered_data

    def download_data(self, postal_code: str, retries: int = 0) -> dict:
        payload = {'address': {'postalCode': postal_code}}
        kroger_postal_data = None

        try:
            self.headers['User-Agent'] = user_agent.random  # Randomize user agent per request
            response = requests.post(self.api_url, timeout=random.randint(timeout_range[0], timeout_range[1]),
                                     headers=self.headers, data=json.dumps(payload))
            response.raise_for_status()
            kroger_postal_data = response.json()
            self.logger.info(f'Request success for postal code {postal_code}')

        except requests.exceptions.HTTPError as e:
            self.logger.error(f'Request failed for postal code {postal_code}: {str(e)}')
            if 'Client Error' in str(e):
                # A status code of 400 may indicate that the postal code is invalid, or simply unaccounted for by Kroger
                kroger_postal_data = {'errors': None}

            elif retries < max_retries:
                retry_delay = random.randint(retry_delay_range[0], retry_delay_range[1])
                self.logger.info(f'Delaying by {retry_delay} seconds before retrying for postal code {postal_code}')
                time.sleep(retry_delay)
                kroger_postal_data = self.download_data(postal_code, retries + 1)

        except requests.exceptions.RequestException as e:
            self.logger.error(f'Error while processing postal code {postal_code}: {str(e)}')

        except requests.exceptions.JSONDecodeError as e:
            self.logger.error(f'Data returned by postal code {postal_code} does not contain valid JSON: {str(e)}')

        finally:
            return kroger_postal_data

    def transform_data(self, kroger_postal_data: dict, postal_code_details: dict) -> list:
        output_data = {
            'Ecommerce': 'Kroger',
            'CityName': postal_code_details.get('NAME', None),
            'StateAbbrev': postal_code_details.get('RG_ABBREV', None),
            'ZipCode': postal_code_details.get('ID', None),
            'Delivery': 'No',  # By default, set value to No.
            'DeliveryGrocery': [],
            'DeliveryRestaurants': [],
            'DeliveryAll': [],
            'Pickup': 'No',  # By default, set value to No.
            'PickupGrocery': [],
            'PickupRestaurants': [],
            'PickupAll': [],
        }

        if 'data' in kroger_postal_data:
            if 'modalityOptions' in kroger_postal_data['data']:
                modality_options = kroger_postal_data['data']['modalityOptions']
                output_data.update(self.check_modality_options(modality_options))

                if output_data['Delivery'] == 'Yes':
                    delivery_store_brands = self.get_store_brands(modality_options, 'Delivery')
                    output_data.update(dict.fromkeys(['DeliveryGrocery', 'DeliveryAll'], delivery_store_brands))

                if output_data['Pickup'] == 'Yes':
                    pickup_store_brands = self.get_store_brands(modality_options, 'Pickup')
                    output_data.update(dict.fromkeys(['PickupGrocery', 'PickupAll'], pickup_store_brands))

        return list(output_data.values())

    def get_store_brands(self, modality_options: dict, mode: str) -> list:
        store_brands = []
        store_ids = []
        store_df = self.store_data

        if mode == 'Delivery':
            if 'fulfillment' in modality_options['DELIVERY']:
                store_ids = modality_options['DELIVERY']['fulfillment']
        elif mode == 'Pickup':
            if 'storeDetails' in modality_options:
                store_details = modality_options['storeDetails']
                store_ids = [store['locationId'] for store in store_details]

        for store_id in store_ids:
            try:
                store_df['StoreNumber'] = store_df['StoreNumber'].str.replace('-', '').astype(str)
                brand = store_df.loc[store_df['StoreNumber'] == store_id, 'ChainName'].iloc[0]
                if brand not in store_brands:
                    store_brands.append(brand)
            except IndexError as e:
                self.logger.error(f'Store with ID: {store_id} is not on the list.')
                pass

        return store_brands

    @staticmethod
    def check_modality_options(modality_options: dict) -> dict:
        data = {}
        options_to_check = ['DELIVERY', 'PICKUP']
        for option in options_to_check:
            if option in modality_options and modality_options[option]:
                data[option.capitalize()] = 'Yes'
            else:
                data[option.capitalize()] = 'No'

        return data

    @staticmethod
    def write_to_file(output_file: str, data: list, mode: str = 'a'):
        with lock:
            with open(output_file, mode, newline='') as file:
                writer = csv.writer(file)
                writer.writerow(data)

    @staticmethod
    def postal_code_formatter(code: int) -> str:
        postal_code = str(code)
        if len(postal_code) == 4:  # Prepend 0 to postal codes with only 4 digits.
            postal_code = f'0{code}'

        return postal_code


if __name__ == '__main__':
    postal_code_input_file = 'input/USZipCodesXLS.csv'
    store_input_file = 'input/Kroger-Store-List.csv'
    output = 'output/Kroger-US-Full.csv'

    log_file = f"logs/{datetime.today().strftime('%Y-%m-%d')}.log"
    logger: Logger = initialize_logger('KrogerScraper', log_file)

    scraper = KrogerScraper(postal_code_input_file, store_input_file, output, logger)
    scraper.run()
