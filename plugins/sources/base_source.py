from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple


class BaseSource(ABC):
    """Abstract base for all data sources."""

    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    def extract(self, context: Optional[dict] = None) -> Tuple[List[Dict[str, Any]], List[str]]:
        """Extract data from the source.

        Returns (list of row dicts, list of column names).
        """
        ...

    @abstractmethod
    def get_row_count(self) -> int:
        ...

    @abstractmethod
    def close(self):
        ...
