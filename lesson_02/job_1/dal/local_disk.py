from typing import List, Dict, Any
import json


def save_to_disk(json_content: List[Dict[str, Any]], path: str) -> None:
    """
    Save json file to Disk

    :param json_content: list of records
    :param path: path to save json file
    :return: None
    """
    with open(path, 'w') as outfile:
        outfile.write(json.dumps(json_content))
    print('File created')
