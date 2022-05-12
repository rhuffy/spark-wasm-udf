In the frontend directory (`spark-wasm-udf-app`)

- Install dependencies with `npm install`
- Run frontend locally with `npm start`

On your server, install python dependencies with `pip install -r requirements.txt`

Download Java dependencies with `./download_libs.sh`

Install Emscripten using the instructions here: https://emscripten.org/docs/getting_started/downloads.html

Update `EMSDK_PATH` in `server/executor.py`

Start the server with `./run_server.sh`
