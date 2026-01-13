# SwotDB/__init__.py
from .src.index import SWOTSpatialIndex
from .src.query import query_swot_data

__all__ = [
    "SWOTSpatialIndex",
    "query_swot_data",
]