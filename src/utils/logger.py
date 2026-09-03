import json
import logging
from datetime import datetime

class StructuredLogger:
    """
    Structured JSON logger for CloudWatch/Datadog integration.
    """
    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            self.logger.addHandler(handler)

    def _format(self, level: str, message: str, extra: dict = None) -> str:
        payload = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": level,
            "message": message,
            **(extra or {})
        }
        return json.dumps(payload)

    def info(self, message: str, extra: dict = None):
        self.logger.info(self._format("INFO", message, extra))

    def error(self, message: str, extra: dict = None):
        self.logger.error(self._format("ERROR", message, extra))