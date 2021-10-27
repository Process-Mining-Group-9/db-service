from fastapi import FastAPI, HTTPException, Request
from starlette.responses import RedirectResponse
from fastapi_utils.tasks import repeat_every
from mqtt_event import MqttEvent, from_dict
from custom_logging import CustomizeLogger
from typing import Optional, Tuple, Dict
from pypika import Query, Table
from queue import Queue
import sqlite3 as sqlite
import logging
import uvicorn
import yaml
import os

config: dict = yaml.safe_load(open('../config.yaml'))
logger = logging.getLogger(__name__)
new_event_queue: Dict[str, Queue[MqttEvent]] = dict()


def create_app() -> FastAPI:
    """Create a FastAPI instance for this application."""
    fastapi_app = FastAPI(title='DbService', debug=False)
    custom_logger = CustomizeLogger.make_logger(config['log'])
    fastapi_app.logger = custom_logger
    return fastapi_app


def dict_factory(cursor, row):
    """Used by sqlite3 to convert results to dictionaries"""
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


app: FastAPI = create_app()


def get_db_connection(name: str, create: bool) -> Tuple[sqlite.Connection, sqlite.Cursor]:
    """Create and return a connection and cursor to the specified database, and create it if needed."""
    os.makedirs(config['dir'], exist_ok=True)
    db_file = os.path.join(config['dir'], name + '.db')

    if not create and not os.path.isfile(db_file):
        raise HTTPException(status_code=404, detail=f'Database with name \'{name}\' not found')

    db = sqlite.connect(db_file, timeout=10)
    db.row_factory = dict_factory
    c = db.cursor()
    c.execute('CREATE TABLE IF NOT EXISTS events (timestamp REAL, process TEXT, activity TEXT, payload TEXT)')
    db.commit()
    return db, c


@app.get('/')
async def root():
    """Redirect to the interactive Swagger documentation on root."""
    return RedirectResponse(url='/docs')


@app.on_event('startup')
@repeat_every(seconds=10, wait_first=True, raise_exceptions=True)
def insert_queued_events():
    logging.info(f'Checking queues of outstanding new events to be inserted.')
    for key, queue in new_event_queue.items():
        events: list[Tuple] = []
        while not queue.empty():
            events.append(queue.get(block=True, timeout=1).to_tuple())
        if events:
            try:
                db, c = get_db_connection(key, create=True)
                c.executemany('INSERT INTO events (timestamp, process, activity, payload) VALUES (?,?,?,?)', events)
                db.commit()
                db.close()
                logging.info(f'Inserted {len(events)} new events to the "{key}" database.')
            except Exception as e:
                logging.error(e)


@app.post('/events/add')
async def add_event(request: Request, event: MqttEvent):
    """Add a new event."""
    if not event.source:
        raise HTTPException(status_code=400, detail=f'Source value must be set')

    if event.source not in new_event_queue:
        new_event_queue[event.source] = Queue()
    new_event_queue[event.source].put(event, block=True, timeout=1)


@app.get('/events')
async def get_logs() -> list:
    """Get a list of all available event log databases."""
    files = []
    for file in os.listdir(config['dir']):
        if file.endswith('.db'):
            files.append(file.replace('.db', ''))
    return files


@app.get('/events/{log}')
async def get_events(log: str, process: Optional[str] = None, activity: Optional[str] = None) -> list[MqttEvent]:
    """Get all event logs in a specific database, with optional filters."""
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
