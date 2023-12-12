"""
This file contains the controller that accepts command via HTTP
and trigger business logic layer
"""
from flask import Flask, request
from flask import typing as flask_typing
from environs import Env
import os
from lesson_02.job_2.bll.file_storage_jobs import sales_json_to_avro

env = Env()
env.read_env()
AUTH_TOKEN = env("API_AUTH_TOKEN")

if not AUTH_TOKEN:
    print("AUTH_TOKEN environment variable must be set")


app = Flask(__name__)


@app.route('/', methods=['POST'])
def main() -> flask_typing.ResponseReturnValue:
    """
    Controller that accepts command via HTTP and
    trigger business logic layer

    Proposed POST body in JSON:
    {
      "stg_dir: "/path/to/my_dir/stg/sales/2022-08-09",
      "raw_dir": "/path/to/my_dir/raw/sales/2022-08-09"
    }
    """
    input_data: dict = request.json
    stg_dir = input_data.get('stg_dir')
    raw_dir = input_data.get('raw_dir')

    if not stg_dir:
        return {
            "message": "stg_dir parameter missed",
        }, 400

    if not raw_dir:
        return {
            "message": "raw_dir parameter missed",
        }, 400

    if not os.path.exists(raw_dir):
        return {
            "message": "directory raw_dir does not exists",
        }, 400

    sales_json_to_avro(stg_dir=stg_dir, raw_dir=raw_dir)

    return {
               "message": "Data successfully recorded to stg_dir",
           }, 201


if __name__ == "__main__":
    app.run(debug=True, host="localhost", port=8082)
