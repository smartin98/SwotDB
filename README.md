# SwotDB
Efficiently subset and query SWOT data from the source NetCDFs in a manner that is scalable and doesn't require duplicating the dataset in a different file format.

Create index:

`python swotdb.py build --data-dir /path/to/swot/data --index-file swot_index_filename`
