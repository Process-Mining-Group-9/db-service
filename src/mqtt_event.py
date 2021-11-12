from typing import Optional
from pydantic import BaseModel


def from_dict(d: dict):
    return MqttEvent(id=d['id'], timestamp=d['timestamp'], process=d['process'],
                     activity=d['activity'], payload=d['payload'])


class MqttEvent(BaseModel):
    id: Optional[int] = None
    timestamp: float
    base: Optional[str] = None
    source: Optional[str] = None
    process: str
    activity: str
    payload: Optional[str] = None

    def to_dict(self) -> dict:
        return {'id': self.id, 'timestamp': self.timestamp, 'base': self.base, 'source': self.source,
                'process': self.process, 'activity': self.activity, 'payload': self.payload}

    def to_tuple(self) -> tuple:
        return self.timestamp, self.process, self.activity, self.payload

    def __str__(self) -> str:
        return f'{self.timestamp}: {self.base}/{self.source}/{self.process}/{self.activity}: {self.payload}'
