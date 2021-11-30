#! /usr/bin/env bash

# install virtualenv module if it doesn't exist
pip3 freeze | grep virtualenv || pip3 install virtualenv

echo "[!]. Creating virtual environment"
virtualenv venv
source venv/bin/activate

echo "[!]. Installing dependencies"
pip3 install -r requirements.txt

echo "[!]. Environment setup has completed"
