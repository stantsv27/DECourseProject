from typing import List, Dict, Any
import fastavro
import json


def save_to_disk_as_avro(json_content: List[Dict[str, Any]], schema: Dict[str, Any], path: str) -> None:
    """
    Save avro file to Disk

    :param json_content: list of records
    :param schema: file schema for avro
    :param path: path to save json file
    :return: None
    """
    with open(path, 'wb') as avro_file:
        fastavro.writer(avro_file, schema, json_content)
    print('File created')


def read_json(path: str) -> List[Dict[str, Any]]:
    """
    Read json files in directory
    :param path: path to read json files
    :return: json data
    """

    with open(path, 'r') as json_file:
        json_content = json.load(json_file)
    print('json file was read')

    return json_content
