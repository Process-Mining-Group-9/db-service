[![MyPy Check](https://github.com/Process-Mining-Group-9/db_service/actions/workflows/mypy.yml/badge.svg)](https://github.com/Process-Mining-Group-9/db_service/actions/workflows/mypy.yml)

# Database API Service

Stores event logs, and exposes endpoints to add and retrieve data via a REST API.

## Installing and running

Install the required packages using ```pip install -r requirements.txt```.

```cd``` into the ```src``` directory and run ```uvicorn main:app --port 8000 --reload --access-log```