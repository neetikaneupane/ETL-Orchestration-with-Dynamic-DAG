from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class BaseDestination(ABC):
    """Abstract base for all data destinations."""

    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    def load(self, rows: List[Dict[str, Any]], context: Optional[dict] = None) -> int:
        """Load rows to the destination.

        Returns the number of rows loaded.
        """
        ...

    @abstractmethod
    def close(self):
        ...
