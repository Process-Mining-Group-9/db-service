from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import RedirectResponse
from fastapi_utils.tasks import repeat_every
from mqtt_event import MqttEvent, from_dict
from custom_logging import CustomizeLogger
from typing import Optional, Tuple, Dict
from psycopg.rows import dict_row
from pypika import Query, Table
from queue import Queue
import psycopg
import logging
import uvicorn
import os

logger = logging.getLogger(__name__)
new_event_queue: Dict[str, Queue[MqttEvent]] = dict()


def create_app() -> FastAPI:
    """Create a FastAPI instance for this application."""
    fastapi_app = FastAPI(title='DbService', debug=False)
    custom_logger = CustomizeLogger.make_logger()
    fastapi_app.logger = custom_logger
    fastapi_app.add_middleware(CORSMiddleware, allow_credentials=True, allow_origins=['*'], allow_methods=['*'], allow_headers=['*'])
    return fastapi_app


app: FastAPI = create_app()


def get_db_connection(table: Optional[str], create: bool) -> Tuple[psycopg.Connection, psycopg.Cursor]:
    """Create and return a connection and cursor to the specified database, and create it if needed."""
    db = psycopg.connect(os.environ['DATABASE_URL'], row_factory=dict_row)
    c = db.cursor()

    if table:
        if not create and not bool(c.execute('SELECT * FROM information_schema.tables WHERE table_name=%s', (table,))):
            raise HTTPException(status_code=404, detail=f'Database with name \'{table}\' not found')

        c.execute(f'CREATE TABLE IF NOT EXISTS {table} (id serial PRIMARY KEY, timestamp REAL, process TEXT, activity TEXT, payload TEXT)')
        db.commit()

    return db, c


@app.get('/')
async def root():
    """Redirect to the interactive Swagger documentation on root."""
    return RedirectResponse(url='/docs')


@app.on_event('startup')
@repeat_every(seconds=10, wait_first=True, raise_exceptions=True)
def insert_queued_events():
    logging.debug(f'Checking queues of outstanding new events to be inserted.')
    for key, queue in new_event_queue.items():
        events: list[Tuple] = []
        while not queue.empty():
            events.append(queue.get(block=True, timeout=1).to_tuple())
        if events:
            try:
                db, c = get_db_connection(key, create=True)
                c.executemany(f'INSERT INTO {key} (timestamp, process, activity, payload) VALUES (%s,%s,%s,%s)', events)
                db.commit()
                db.close()
                logging.info(f'Inserted {len(events)} new events to the "{key}" database.')
            except Exception as e:
                logging.error(e)


@app.post('/events/add')
async def add_event(request: Request, event: MqttEvent):
    """Add a new event."""
    if 'x-secret' not in request.headers.keys() or request.headers['x-secret'] != os.environ['SECRET']:
        raise HTTPException(status_code=403, detail=f'Access denied. Secret did not match.')

    if not event.source:
        raise HTTPException(status_code=400, detail=f'Source value must be set')

    if event.source not in new_event_queue:
        new_event_queue[event.source] = Queue()
    new_event_queue[event.source].put(event, block=True, timeout=1)


@app.get('/events')
async def get_logs() -> list:
    """Get a list of all available event log databases."""
    db, c = get_db_connection(None, create=False)
    logs = c.execute("SELECT * FROM information_schema.tables WHERE table_schema='public'").fetchall()
    return [row['table_name'] for row in logs]


@app.get('/events/{log}')
async def get_events(log: str, process: Optional[str] = None, activity: Optional[str] = None) -> list[MqttEvent]:
    """Get all event logs in a specific database, with optional filters."""
    db, c = get_db_connection(log, create=False)
    table = Table(log)
    query = Query.from_(table).select('id', 'timestamp', 'process', 'activity', 'payload')
    if process:
        query = query.where(table.process == process)
    if activity:
        query = query.where(table.activity == activity)
    c.execute(str(query))
    data = [from_dict(row) for row in c.fetchall()]
    db.close()
    return data

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
