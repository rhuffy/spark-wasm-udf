from typing import List
from uuid import uuid4
from os.path import exists
from glob import glob
import subprocess
import json


USER_CODE_PATH = "static/user_code"
USER_DATA_PATH = "static/user_data"
SPARK_PROGRAM_PATH = "static/Entrypoint"
EMSDK_PATH = "/home/ubuntu/emsdk"
EMCC = EMSDK_PATH + "/emscripten/main/emcc"
JAVA_ROOT = "../"


def execute(args):
    exec_id = str(uuid4())

    program = args['program']
    data_path = args['data']
    operation = args['operation']
    function_name = args['function_name']
    input_column_names = args['input_column_names']
    output_column_name = args['output_column_name']
    output_column_type = args['output_column_type']

    program_path = write_program(program, exec_id)
    wasm_path = compile_c(program_path, function_name)
    program_args = ["--wasm", f'server/{wasm_path}',
                 "--data", data_path,
                 "--schema", get_schema_path(data_path),
                 "--operation", operation,
                 "--function", function_name,
                 "--input", list_as_csv(input_column_names)]
    if output_column_name is not None:
        program_args.extend(["--output", output_column_name, "--outputType", output_column_type])
    args_str = ' '.join(program_args)
    gradle_command = ' '.join(['./gradlew', 'run', f'--args=\"{args_str}\"'])

    print(gradle_command)
    subprocess.Popen(
        gradle_command, stdout=subprocess.PIPE, cwd=JAVA_ROOT, shell=True)

    return exec_id


def compile_c(src_file, exported_function):
    wasm_file = replace_extension(src_file, ".wasm")
    print(f'Compiling {wasm_file}...')
    process = subprocess.Popen([
        EMCC, '--no-entry', src_file,
        '-o', wasm_file,
        f'-sEXPORTED_FUNCTIONS=_{exported_function}'])
    process.wait()
    if not exists(wasm_file):
        raise RuntimeError('Failed to compile WASM')

    return wasm_file


def get_result_if_exists(id):
    if exists(f'{USER_DATA_PATH}/{id}/_SUCCESS'):
        for name in glob(f'{USER_DATA_PATH}/{id}/part*json'):
            data = []
            with open(name, 'r') as f:
                for line in f:
                    data.append(json.loads(line))
            return data


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


def write_program(program: str, exec_id: str) -> str:
    path = f"{USER_CODE_PATH}/{exec_id}.c"
    with open(path, "w") as f:
        f.write(program)
    return path


def get_schema_path(data_path: str):
    return replace_extension(data_path, "_schema.txt")


def replace_extension(file_path: str, new_extension: str):
    idx = file_path.rfind('.')
    return file_path[:idx] + new_extension


def list_as_csv(inp: List[str]):
    return ','.join(inp)
