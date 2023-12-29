#!/bin/bash

mkdir -p output/dist

export LC_ALL="en_US.UTF-8"
VERSION=$(awk -F"'" '$1 ~ /SDK_VERSION/ {print $2}' pymochow/__init__.py)
TARGET="mochow-python-sdk-$VERSION"
mkdir $TARGET
cp -rf pymochow example setup.py README.txt $TARGET
zip -r ${TARGET}.zip $TARGET -x "*.pyc" "*.pyo"
cp ${TARGET}.zip output/

PYTHON=python
[ -f /home/cmc/opt/pyenv/versions/2.7.14/bin/python2.7 ] && PYTHON=/home/cmc/opt/pyenv/versions/2.7.14/bin/python2.7

echo "using Python=$PYTHON"

$PYTHON setup.py bdist_wheel --universal
$PYTHON setup.py sdist
mv -fv dist/* output/dist/
