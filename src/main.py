from fastapi import FastAPI, HTTPException
from pypika import Query, Table, Field
from mqtt_event import MqttEvent
from typing import Optional
import sqlite3 as sqlite
import uvicorn
import os
import datetime
import yaml

config: dict = yaml.safe_load(open('../config.yaml'))
app: FastAPI = FastAPI()


def get_db_connection(name: str, create: bool) -> (sqlite.Connection, sqlite.Cursor):
    os.makedirs(config['dir'], exist_ok=True)
    db_file = os.path.join(config['dir'], name + '.db')

    if not create and not os.path.isfile(db_file):
        raise HTTPException(status_code=404, detail=f'Database with name \'{name}\' not found')

    db = sqlite.connect(db_file, timeout=10)
    c = db.cursor()
    c.execute('CREATE TABLE IF NOT EXISTS events (timestamp TEXT, process TEXT, activity TEXT, payload TEXT)')
    db.commit()
    return db, c


def insert_event(event: MqttEvent, db: sqlite.Connection, c: sqlite.Cursor):
    events = Table('events')
    query = Query.into(events).insert(datetime.datetime.utcnow().isoformat(), event.process, event.activity, event.payload)
    c.execute(str(query))
    db.commit()


def query_events(c: sqlite.Cursor, process: Optional[str] = None, activity: Optional[str] = None) -> list:
    events = Table('events')
    query = Query.from_(events).select('*')

    if process:
        query = query.where(events.process == process)

    if activity:
        query = query.where(events.activity == activity)

    c.execute(str(query))
    return c.fetchall()


@app.post("/events/add")
async def add_event(event: MqttEvent):
    db, c = get_db_connection(event.source, create=True)
    insert_event(event, db, c)
    db.close()


@app.get("/events/{log}")
async def get_events(log: str, process: Optional[str] = None, activity: Optional[str] = None):
    db, c = get_db_connection(log, create=False)
    data = query_events(c, process, activity)
    db.close()
    return data


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
