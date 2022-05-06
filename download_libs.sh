#!/bin/sh

mkdir -p libs
cd libs

case "$(uname -s)" in

   Darwin)
     wget https://github.com/wasmerio/wasmer-java/releases/download/0.3.0/wasmer-jni-amd64-darwin-0.3.0.jar
     ;;

   Linux)
     wget https://github.com/wasmerio/wasmer-java/releases/download/0.3.0/wasmer-jni-amd64-linux-0.3.0.jar
     ;;
esac
