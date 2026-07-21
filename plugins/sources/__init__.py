from .base_source import BaseSource
from .postgres_source import PostgresSource

SOURCE_REGISTRY = {
    "postgres": PostgresSource,
}


def get_source(source_type: str, config: dict) -> BaseSource:
    cls = SOURCE_REGISTRY.get(source_type)
    if cls is None:
        raise ValueError(
            f"Unsupported source type: {source_type}. Available: {list(SOURCE_REGISTRY.keys())}"
        )
    return cls(config)


__all__ = ["BaseSource", "PostgresSource", "get_source"]
