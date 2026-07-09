from .base_destination import BaseDestination
from .s3_destination import S3Destination

DESTINATION_REGISTRY = {
    "s3": S3Destination,
}

def get_destination(dest_type: str, config: dict) -> BaseDestination:
    cls = DESTINATION_REGISTRY.get(dest_type)
    if cls is None:
        raise ValueError(f"Unsupported destination type: {dest_type}. Available: {list(DESTINATION_REGISTRY.keys())}")
    return cls(config)

__all__ = ["BaseDestination", "S3Destination", "get_destination"]
