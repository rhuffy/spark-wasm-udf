#!/bin/sh

./gradlew run --args='--wasm demo/udf.wasm --data two_columns_1000000.csv --schema two_columns_1000000_schema.txt --operation MAP --function add --input col1,col2 --output result --outputType INTEGER'