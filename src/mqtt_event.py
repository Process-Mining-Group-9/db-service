from typing import Optional
from pydantic import BaseModel


class MqttEvent(BaseModel):
    base: str
    source: str
    process: str
    activity: str
    payload: Optional[str] = None

    def __str__(self):
        return f'{self.base}/{self.source}/{self.process}/{self.activity}: {self.payload}'
