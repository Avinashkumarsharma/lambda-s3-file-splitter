#!/bin/bash
# Currently not being used as Lambda has a aws provided Pandas Layer
export LIB_DIR="./build/python"
rm -rf ${LIB_DIR} && mkdir -p ${LIB_DIR}
pip3 install -r requirements.txt -t ${LIB_DIR}
cd ./build/python
rm -r *.whl *.dist-info __pycache__
cd ..
rm -rf python-lambda-layer.zip
zip -r ./python-lambda-layer.zip ./python
rm -rf ${LIB_DIR}