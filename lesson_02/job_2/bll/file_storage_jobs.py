from typing import List, Dict, Any
from lesson_02.job_2.dal.local_disk import read_json, save_to_disk_as_avro
import os


def sales_json_to_avro(stg_dir: str, raw_dir: str) -> None:
    """
    Read stg_dir json files and write to raw_dir as avro files

    :param stg_dir: avro files path
    :param raw_dir: json files path
    :return: None
    """
    schema: Dict[str, Any] = {
        "type": "record",
        "name": "Sales",
        "fields": [
            {"name": "client", "type": "string"},
            {"name": "purchase_date", "type": "string"},
            {"name": "product", "type": "string"},
            {"name": "price", "type": "int"}
        ]
    }

    files: List[str] = os.listdir(raw_dir)

    for file in files:

        if not file[-4:] == 'json':
            print('file is not json extension')
            continue

        json_content: List[Dict[str, Any]] = read_json(os.path.join(raw_dir, file))

        file = file[:-4] + 'avro'

        if not os.path.exists(stg_dir):
            print('stg_dir does not exists')
            os.makedirs(stg_dir)
            print('stg_dir created')

        save_to_disk_as_avro(json_content, schema, os.path.join(stg_dir, file))
