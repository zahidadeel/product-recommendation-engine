#! /usr/bin/env bash

echo "Packaging virtual environment using venv-pack"
venv-pack -fo venv.tar.gz

export PYSPARK_DRIVER_PYTHON=python
export PYSPARK_PYTHON=./venv/bin/python
