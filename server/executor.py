from typing import List
from uuid import uuid4
import subprocess

USER_CODE_PATH = "static/user_code"
USER_DATA_PATH = "static/user_data"
SPARK_PROGRAM_PATH = "static/Entrypoint"
EMSDK_PATH = "/home/ubuntu/emsdk"


def execute(args):
    program = args['program']
    data_path = args['data']
    operation = args['operation']
    function_name = args['functionName']
    input_column_names = args['inputColumnNames']
    output_column_name = args['outputColumnName']

    program_path = write_program(program)
    java_args = ['java', SPARK_PROGRAM_PATH,
                 "-c", program_path,
                 "--data", data_path,
                 "--schema", get_schema_path(data_path),
                 "--operation", operation,
                 "--function", function_name,
                 "--input", list_as_csv(input_column_names),
                 "--emsdk", EMSDK_PATH]
    if output_column_name is not None:
        java_args.extend(["--output", output_column_name])

    process = subprocess.Popen(
        java_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()


def list_data_files():
    process = subprocess.Popen(
        ["ls", USER_DATA_PATH], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    return stdout.split()


def get_schema(file_name: str):
    schema_path = get_schema_path(f"{USER_DATA_PATH}/{file_name}")
    with open(schema_path, "r") as f:
        schema_str = f.readline()
        return [{"name": entry.split()[0], "type": entry.split()[1]} for entry in schema_str.split(',')]


def write_program(program: str) -> str:
    path = f"{USER_CODE_PATH}/{uuid4()}.c"
    with open(path, "w") as f:
        f.write(program)
    return path


def get_schema_path(data_path: str):
    idx = data_path.rfind('.')
    return f"{data_path[:idx]}_schema.txt"


def list_as_csv(inp: List[str]):
    return inp.join(',')
