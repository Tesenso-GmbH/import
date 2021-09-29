# Data Import

This project contains some useful scripts and tools to upload data to Tesenso IoT Cloud from (hopefully) various sources.

## Getting started

* Install [Poetry](https://python-poetry.org/)
* Clone the repository
* Install dependencies:
```sh
poetry install
```
* Run the cli app:
```sh
poetry run python tb_import/cli.py --help
```

Alternatively, build the pip package and install locally:
```sh
poetry build
pip install .
```
