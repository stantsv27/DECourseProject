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

    json_content = sales_api.get_sales(date)

    if not os.path.exists(raw_dir):
        os.makedirs(raw_dir)
        print('new dirs created')

    files_in_dir = os.listdir(raw_dir)

    if len(files_in_dir) > 0:
        print('there are files in path dir')
        for filename in files_in_dir:
            os.remove(os.path.join(raw_dir, filename))
        print('files removed')

    file_path = os.path.join(raw_dir, f'sales_{date}.json')

    local_disk.save_to_disk(json_content, file_path)
