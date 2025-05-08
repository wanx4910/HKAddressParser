import argparse
import aiohttp
import asyncio
import logging
import os
import pandas as pd
import re
import sys
import time

from components import connection, util


def parse_response(response, input_address):
    """
    Parses the OGCIO API response and extracts relevant address information.

    This function takes the OGCIO API response and the input address, processes them,
    and returns a dictionary containing parsed address components in both
    Chinese and English.

    Args:
        response (dict): The raw response from the OGCIO API.
        input_address (str): The original input address string.

    Returns:
        parsed_response_dict (dict): A dictionary containing parsed address components including:
            - input_address (str): The original input address.
            - OGCIO_score (int): The score from OGCIO, if available.
            - OGCIO_CHI_* (str): Chinese address components (Region, District, Estate, etc.).
            - OGCIO_ENG_* (str): English address components (Region, District, Estate, etc.).

    Raises:
        Exception: If there's an error during parsing, it prints an error message
                   with the line number and exception details.

    Note:
        This function relies on helper functions `flattenOGCIO` and `ParseAddress`,
        which are not defined within this function and should be imported or
        defined elsewhere in the code.
    """
    try:
        flatten_result = util.flattenOGCIO(response)
        # print(flatten_result)
        result = util.ParseAddress(flatten_result, input_address)

        score = (int(result['OGCIO_score']) if result.get('OGCIO_score', {}) else 0)

        # CHN
        chi_region = (str(result['chi']['Region']) if result.get('chi', {}).get('Region', {}) else '')
        chi_district = (
            str(result['chi']['ChiDistrict']['DcDistrict']) if result.get('chi', {}).get('ChiDistrict', {}).get(
                'DcDistrict') else '')
        chi_estate = (str(result['chi']['ChiEstate']['EstateName']) if result.get('chi', {}).get('ChiEstate', {}).get(
            'EstateName') else '')
        chi_building_name = (str(result['chi']['BuildingName']) if result.get('chi', {}).get('BuildingName') else '')
        chi_street_name = (
            str(result['chi']['ChiStreet']['StreetName']) if result.get('chi', {}).get('ChiStreet', {}).get(
                'StreetName') else '')
        chi_building_no = (
            str(result['chi']['ChiStreet']['BuildingNoFrom']) if result.get('chi', {}).get('ChiStreet', {}).get(
                'BuildingNoFrom') else '')
        chi_block = (str(result['chi']['ChiBlock']['BlockNo']) if result.get('chi', {}).get('ChiBlock', {}).get(
            'BlockNo') else '')

        # ENG
        eng_region = (str(result['eng']['Region']) if result.get('eng', {}).get('Region', {}) else '')
        eng_district = (
            str(result['eng']['EngDistrict']['DcDistrict']) if result.get('eng', {}).get('EngDistrict', {}).get(
                'DcDistrict') else '')
        eng_estate = (str(result['eng']['EngEstate']['EstateName']) if result.get('eng', {}).get('EngEstate', {}).get(
            'EstateName') else '')
        eng_building_name = (str(result['eng']['BuildingName']) if result.get('eng', {}).get('BuildingName') else '')
        eng_street_name = (
            str(result['eng']['EngStreet']['StreetName']) if result.get('eng', {}).get('EngStreet', {}).get(
                'StreetName') else '')
        eng_building_no = (
            str(result['eng']['EngStreet']['BuildingNoFrom']) if result.get('eng', {}).get('EngStreet', {}).get(
                'BuildingNoFrom') else '')
        eng_block = (str(result['eng']['EngBlock']['BlockNo']) if result.get('eng', {}).get('EngBlock', {}).get(
            'BlockNo') else '')

        parsed_response_dict = {'input_address': input_address,
                                'score': score,
                                'CHI_Region': chi_region,
                                'chi_district': chi_district,
                                'chi_estate': chi_estate,
                                'OGCIO_CHI_BuildingName': chi_building_name,
                                'OGCIO_CHI_StreetName': chi_street_name,
                                'OGCIO_CHI_BuildingNo': chi_building_no,
                                'OGCIO_CHI_Block': chi_block,
                                'OGCIO_ENG_Region': eng_region,
                                'OGCIO_ENG_District': eng_district,
                                'OGCIO_ENG_Estate': eng_estate,
                                'OGCIO_ENG_BuildingName': eng_building_name,
                                'OGCIO_ENG_StreetName': eng_street_name,
                                'OGCIO_ENG_BuildingNo': eng_building_no,
                                'OGCIO_ENG_Block': eng_block}

        return parsed_response_dict
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        parse_response_error = f"addresses_fetcher.py:parse_response():line_num={exc_tb.tb_lineno},exception={e}"
        print(parse_response_error)
        logger.error(parse_response_error)


async def get_data(semaphore, session, url, header_info, params, raw_address, max_retries=10, sleep_multiplier=2):
    """
    Asynchronously fetches data from a specified URL with retry mechanism.

    This function attempts to retrieve JSON data from a given URL using an aiohttp session.
    It implements a retry mechanism with exponential backoff in case of failures.

    Args:
        semaphore (asyncio.Semaphore): A semaphore to limit concurrent requests.
        session (aiohttp.ClientSession): An aiohttp client session for making requests.
        url (str): The URL to fetch data from.
        header_info (dict): Headers to be sent with the request.
        params (dict): Query parameters for the request.
        raw_address (str): The original address string used for logging purposes.
        max_retries (int, optional): Maximum number of retry attempts. Defaults to 10.
        sleep_multiplier (int, optional): Multiplier for exponential backoff. Defaults to 2.

    Returns:
        data (dict or None): Parsed response data if successful, None otherwise.

    Raises:
        Exception: Catches and logs any exceptions that occur during the process.

    Note:
        - The function uses exponential backoff for retries.
        - It parses the response using a separate 'parse_response' function if 'SuggestedAddress' is present.
        - Exceptions are caught, logged, and printed with line numbers for debugging.
    """
    try:
        response_data = None
        retries = 0
        while response_data is None and retries < max_retries:
            try:
                async with semaphore:
                    async with session.get(url, headers=header_info, params=params) as response:
                        response.raise_for_status()
                        if response:
                            response_data = await response.json()
                            # print(raw_address, flush=True)
                            # print(response_data, flush=True)
                        else:
                            print('No data')
                            logger.warning(f"{raw_address} could not be fetched from {url}")
            except Exception as e:
                await asyncio.sleep(1 if retries == 0 else sleep_multiplier * retries)
                retries += 1
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fetching_logic_error_msg = f"addresses_fetcher:get_data():line_num={exc_tb.tb_lineno},exception={e}"
                print(fetching_logic_error_msg)
                logger.error(fetching_logic_error_msg)

        if retries == max_retries:
            print('Max Retries Exceeded')

        if 'SuggestedAddress' in response_data:
            data = parse_response(response_data['SuggestedAddress'], raw_address)
        else:
            data = None
        return data
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        get_data_error_msg = f"addresses_fetcher.py:get_data():line_num={exc_tb.tb_lineno},exception={e}"
        print(get_data_error_msg)
        logger.error(get_data_error_msg)


async def get_data_from_addresses(addresses):
    """
    Asynchronously fetches data for a list of addresses from a specified API.

    This function takes a list of addresses, processes them, and makes concurrent API requests
    to retrieve data for each address. It uses a throttled client session to manage the rate
    of requests and implements error handling.

    Args:
        addresses (list): A list of address strings to process and fetch data for.

    Returns:
        result_data (list): A list of results containing the data fetched for each address. The structure
              of each result depends on the API response.

    Raises:
        Exception: If an error occurs during the execution of the function. The error details,
                   including the line number and exception message, are printed to the console.

    Note:
        - The function uses a semaphore to limit concurrent requests to 5.
        - The API rate is limited to 20 requests per second.
        - Addresses that are not strings are skipped.
        - The 'removeFloor' function is assumed to be defined elsewhere and is used to
          preprocess each address string.
    """
    try:
        headers = {
            "Accept": "application/json",
            "Accept-Language": "en,zh-Hant",
            "Accept-Encoding": "gzip"
        }
        base_url = "https://www.als.gov.hk/lookup?"
        tasks = []
        sem = asyncio.Semaphore(20)
        async with connection.ThrottledClientSession(rate_limit=20) as session:
            for address in addresses:
                if type(address) == str:
                    input_address = util.removeFloor(address)
                    params = {
                        "q": input_address,
                        "n": 1
                    }
                    tasks.append(get_data(sem, session, base_url, headers, params, address))
                else:
                    continue
            result_data = await asyncio.gather(*tasks)
        return result_data
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        get_address_data_error = f"addresses_fetcher.py:get_data_from_addresses():line_num={exc_tb.tb_lineno},exception={e}"
        print(get_address_data_error)
        logger.error(get_address_data_error)





if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-ip', '--input_path', dest='INPUT_PATH', help='Path to the input data file')
    parser.add_argument('-op', '--output_path', dest='OUTPUT_PATH', help='Path to the output data file')
    parser.add_argument('-lp', '--log_path', dest='LOG_PATH', help='Path to the log file')
    parser.add_argument('--si', '--start_index', dest='START_INDEX', help='Index of file to start queries')
    parser.add_argument('--ei', '--stop_index', dest='STOP_INDEX', help='Index of file to stop queries')
    args = parser.parse_args()
    INPUT_PATH = args.INPUT_PATH
    OUTPUT_PATH = args.OUTPUT_PATH
    LOG_PATH = args.LOG_PATH
    START_INDEX = args.START_INDEX
    STOP_INDEX = args.STOP_INDEX

    logging.basicConfig(filename=f'{LOG_PATH}addresses_fetcher.log', level=logging.INFO, filemode='w')
    logger = logging.getLogger(__name__)

    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.options.display.float_format = '{:.0f}'.format
    pd.set_option("display.precision", 10)
    pd.set_option('display.float_format', lambda x: '%16.2f' % x)
    pd.set_option('max_colwidth', None)
    pd.set_option('mode.chained_assignment', None)

    try:
        addresses_df = pd.read_csv(f"{INPUT_PATH}")
        st = time.time()

        if START_INDEX and STOP_INDEX:
            addresses_df_partial = addresses_df[START_INDEX:STOP_INDEX]
            addresses = addresses_df_partial['address'].to_list()
        else:
            addresses = addresses_df['address'].to_list()

        print(f"Amount of addresses: {len(addresses)}")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        process_addresses = loop.run_until_complete(get_data_from_addresses(addresses))
        loop.close()
        print(f'Fetching from OGCIO endpoint takes: {time.time() - st:.1f} secs, of len={len(addresses)}')
        cleaned_process_addresses = list(filter(lambda item: item is not None, process_addresses))
        result_address_df = pd.DataFrame(cleaned_process_addresses)
        result_address_df.to_csv(f'{OUTPUT_PATH}scanned_addresses.csv', index=False)

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        main_func_error = f'addresses_fetcher.py:main():line_num={exc_tb.tb_lineno},exception={e}'
        print(main_func_error)
        logger.error(main_func_error)