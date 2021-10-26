from fastapi import FastAPI, HTTPException, Request
from starlette.responses import RedirectResponse
from pypika import Query, Table
from typing import Optional, Tuple
from custom_logging import CustomizeLogger
from pathlib import Path
from mqtt_event import MqttEvent, from_dict
import sqlite3 as sqlite
import logging
import uvicorn
import os
import yaml

config: dict = yaml.safe_load(open('../config.yaml'))
logger = logging.getLogger(__name__)


def create_app() -> FastAPI:
    fastapi_app = FastAPI(title='DbService', debug=False)
    custom_logger = CustomizeLogger.make_logger(Path('../logging_config.json'))
    fastapi_app.logger = custom_logger
    return fastapi_app


app: FastAPI = create_app()


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def get_db_connection(name: str, create: bool) -> Tuple[sqlite.Connection, sqlite.Cursor]:
    os.makedirs(config['dir'], exist_ok=True)
    db_file = os.path.join(config['dir'], name + '.db')

    if not create and not os.path.isfile(db_file):
        raise HTTPException(status_code=404, detail=f'Database with name \'{name}\' not found')

    db = sqlite.connect(db_file, timeout=10)
    db.row_factory = dict_factory
    c = db.cursor()
    c.execute('CREATE TABLE IF NOT EXISTS events (timestamp INTEGER, process TEXT, activity TEXT, payload TEXT)')
    db.commit()
    return db, c


@app.get('/')
async def root(request: Request):
    # request.app.logger.info('Redirecting to documentation.') # This is how custom logging can be inserted
    return RedirectResponse(url='/docs')


@app.post('/events/add')
async def add_event(event: MqttEvent):
    db, c = get_db_connection(event.source, create=True)
    query = Query.into('events').insert(event.timestamp, event.process, event.activity, event.payload)
    c.execute(str(query))
    db.commit()
    db.close()


@app.get('/events')
async def get_logs() -> list:
    files = []
    for file in os.listdir(config['dir']):
        if file.endswith('.db'):
            files.append(file.replace('.db', ''))
    return files


@app.get('/events/{log}')
async def get_events(log: str, process: Optional[str] = None, activity: Optional[str] = None) -> list[MqttEvent]:
    db, c = get_db_connection(log, create=False)
    events = Table('events')
    query = Query.from_(events).select('rowid', 'timestamp', 'process', 'activity', 'payload')
    if process:
        query = query.where(events.process == process)
    if activity:
        query = query.where(events.activity == activity)
    c.execute(str(query))
    data = [from_dict(row) for row in c.fetchall()]
    db.close()
    return data


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
