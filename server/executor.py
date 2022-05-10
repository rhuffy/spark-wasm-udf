from typing import List
from uuid import uuid4
import subprocess

USER_CODE_PATH = "static/user_code"
USER_DATA_PATH = "static/user_data"
SPARK_PROGRAM_PATH = "static/Entrypoint"
EMSDK_PATH = "/home/ubuntu/emsdk"
JAVA_ROOT = "../"


def execute(args):
    program = args['program']
    data_path = args['data']
    operation = args['operation']
    function_name = args['function_name']
    input_column_names = args['input_column_names']
    output_column_name = args['output_column_name']
    output_column_type = args['output_column_type']

    program_path = write_program(program)
    program_args = ["-c", f'server/{program_path}',
                 "--data", data_path,
                 "--schema", get_schema_path(data_path),
                 "--operation", operation,
                 "--function", function_name,
                 "--input", list_as_csv(input_column_names),
                 "--emsdk", EMSDK_PATH]
    if output_column_name is not None:
        program_args.extend(["--output", output_column_name, "--outputType", output_column_type])
    args_str = ' '.join(program_args)
    gradle_command = ' '.join(['./gradlew', 'run', f'--args=\"{args_str}\"'])

    print(gradle_command)
    process = subprocess.Popen(
        gradle_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=JAVA_ROOT, shell=True)
    stdout, stderr = process.communicate()
    print(stdout)
    print(stderr)


def list_data_files():
    process = subprocess.Popen(
        ["ls", USER_DATA_PATH], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    all_file_names = stdout.decode('UTF-8').split()

    return list(filter(lambda s: '.csv' in s, all_file_names))


def get_schema(file_name: str):
    print(f'Get schema for file {file_name}')
    schema_path = get_schema_path(f"{USER_DATA_PATH}/{file_name}")
    with open(schema_path, "r") as f:
        schema_str = f.readline()
        print(schema_str)
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
    return ','.join(inp)
