from typing import Optional
from pydantic import BaseModel


class MqttEvent(BaseModel):
    base: str
    source: str
    process: str
    activity: str
    payload: Optional[str] = None
