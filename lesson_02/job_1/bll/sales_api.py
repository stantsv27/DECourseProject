from lesson_02.job_1.dal import local_disk
from lesson_02.job_1.dal import sales_api
import os


def save_sales_to_local_disk(date: str, raw_dir: str) -> None:
    """
    Get sales from api and save it on disk

    :param date: date retrieve the data from API
    :param raw_dir: path to save sales data
    :return: None
    """
    file_path = os.path.join(raw_dir, f'{date}.json')

    json_content = sales_api.get_sales(date)

    if not os.path.exists(raw_dir):
        os.makedirs(raw_dir)
        print('new dirs created')

    local_disk.save_to_disk(json_content, file_path)
