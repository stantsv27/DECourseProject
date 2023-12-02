from environs import Env
import requests
from typing import List, Dict, Any


env = Env()
env.read_env()
AUTH_TOKEN = env('API_AUTH_TOKEN')
URL = 'https://fake-api-vycpfa6oca-uc.a.run.app/sales'


def get_sales(date: str) -> List[Dict[str, Any]]:
    """
    Get data from sales API for specified date.

    :param date: date retrieve the data from
    :return: list of records
    """
    counter: int = 1
    res: List = []

    while True:

        try:
            response = requests.get(
                    url=URL,
                    params={'date': date, 'page': counter},
                    headers={'Authorization': AUTH_TOKEN},
                )

            counter += 1

            print("Response status code:", response.status_code)
            if response.status_code == 200:
                res += response.json()
            else:
                break
        except ValueError:
            print('Got ValueError')
            break

    return res
