[![MyPy Check](https://github.com/Process-Mining-Group-9/db_service/actions/workflows/mypy.yml/badge.svg)](https://github.com/Process-Mining-Group-9/db_service/actions/workflows/mypy.yml)

# Database API Service

Stores event logs, and exposes endpoints to add and retrieve data via a REST API.

# Installation and Running

1. Download [PostgreSQL](https://www.postgresql.org/download/) and set up a local server. Update the ```.env``` file with your own configuration.
2. Check that your Python version is relatively new (```python --version```). Version 3.10 is used in production.
3. Create a virtual environment using ```python -m venv venv``` and activate it:
   1. On Linux: ```source venv\bin\activate```
   2. On Windows: ```\venv\Scripts\activate```
4. Install the required packages using ```pip install -r requirements.txt```.
5. The environment variables in ```.env``` need to be available to the program when running it. These are the options:
   1. Install the [Heroku CLI](https://devcenter.heroku.com/articles/heroku-cli) and run ```heroku local``` to start the application
   2. When using an IDE like PyCharm, create a run configuration and copy-paste the contents of ```.env``` into the environment variables section

## Type Checking

Use ```mypy src``` to type-check the code for type violations.