# SwotDB
Efficiently subset and query SWOT data from the source NetCDFs in a manner that is scalable and doesn't require duplicating the dataset in a different file format.

Build index:

`python swotdb.py build --data-dir /path/to/swot/data --index-file swot_index_filename`

Use index:

```python
from src.index import SWOTSpatialIndex
from src.query import *
import pandas as pd

swot_index_filename = 'swot_index.pkl'
lat_min = 33
lat_max = 32
lon_min = 295
lon_max = 305
time_start = pd.Timestamp('2024-09-21')
time_end = pd.Timestamp('2024-09-28')

index = SWOTSpatialIndex.load(swot_index_filename)

ds = query_swot_data(
    index,
    lat_min, 
    lat_max, 
    lon_min, 
    lon_max,
    time_start=time_start, 
    time_end=time_end,
    variables=['ssha_unfiltered'],
)
```