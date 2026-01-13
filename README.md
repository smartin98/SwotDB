# SwotDB
Efficiently subset and query SWOT data from the source NetCDFs in a manner that is scalable and doesn't require duplicating the dataset in a different file format.

## How it works

Each SWOT file has along- and across-swath coordinates ordered by time along the swath but which are unstructured in lat-lon. To subset data in a small lat-lon-time bounding box, without needlessly opening NetCDF files to check whether any points are in the bounding box, we split each file into smaller logical "tiles" (without alterring the nc files) and build a .pkl index file with coordinate bounding boxes for each tile in each file across the whole dataset. Building this index is a one-time cost (takes ~1 hr on single CPU for ~3 years of SWOT data) which then allows efficient spatiotemporal querying by only opening files containing tiles that overlap the domain of interest. An existing index can be updated as new data become available.

## Basic usage

Build index:

`python swotdb.py build --data-dir /path/to/swot/data --index-file swot_index_filename`

Update index:

`python swotdb.py build --data-dir /path/to/swot/data --index-file swot_index_filename --load-existing`

Use index:

```python
from src.index import SWOTSpatialIndex
from src.query import *
import pandas as pd

swot_index_filename = 'swot_index.pkl'
lat_min = 33
lat_max = 43
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

## Advanced usage

Update the base data path for the index (e.g. if index created on different server to deployment):

`python swotdb.py remap --index-file swot_index --new-base-path /different/path/to/swot/data`