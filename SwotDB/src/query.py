from SwotDB.src.index import SWOTSpatialIndex
import xarray as xr
import numpy as np
import pandas as pd
from pathlib import Path
from collections import defaultdict

def merge_line_ranges(line_ranges):
    """
    Merge overlapping/adjacent line ranges into contiguous slices
    
    Input: [(0, 500), (500, 1000), (2000, 2500)]
    Output: [slice(0, 1000), slice(2000, 2500)]
    """
    if not line_ranges:
        return []
    
    # Sort by start line
    sorted_ranges = sorted(line_ranges)
    
    merged = []
    current_start, current_end = sorted_ranges[0]
    
    for start, end in sorted_ranges[1:]:
        if start <= current_end:  # Overlapping or adjacent
            current_end = max(current_end, end)
        else:  # Gap - start new range
            merged.append(slice(current_start, current_end))
            current_start, current_end = start, end
    
    # Add the last range
    merged.append(slice(current_start, current_end))
    
    return merged


def query_swot_data(index, lat_min, lat_max, lon_min, lon_max,
                    time_start=None, time_end=None,
                    variables=['ssha_unfiltered'],
                    ):
    """
    Query SWOT data within bounds, preserving (num_lines, num_pixels) structure
    
    Returns entire swath lines where ANY pixel intersects the query box
    
    Args:
        lat_min, lat_max, lon_min, lon_max: Spatial bounds
        time_start, time_end: Temporal bounds (optional, as pd.Timestamp)
        variables: List of variables to load
        index_file: Path to index file
    
    Returns:
        xarray Dataset with (num_lines, num_pixels) dimensions
    """
    
    # Query for relevant tiles
    tiles = index.query(lat_min, lat_max, lon_min, lon_max, 
                       time_start, time_end)
    
    print(f"Found {len(tiles)} relevant tiles")
    
    # Group tiles by file
    tiles_by_file = defaultdict(list)
    for tile in tiles:
        tiles_by_file[tile['file']].append(tile['line_range'])
    
    print(f"Spanning {len(tiles_by_file)} unique files")
    
    # Load and filter data
    datasets = []
    
    for filepath, line_ranges in tiles_by_file.items():
        ds = xr.open_dataset(filepath)
        
        # Merge line ranges into contiguous slices
        merged_slices = merge_line_ranges(line_ranges)
        
        print(f"  {Path(filepath).name}: {len(line_ranges)} tiles → {len(merged_slices)} slices")
        
        # Load all relevant slices
        file_datasets = []
        for line_slice in merged_slices:
            ds_slice = ds.isel(num_lines=line_slice)
            ds_slice = ds_slice[variables + ['latitude', 'longitude', 'time']]
            
            # Find lines where ANY pixel intersects the query box
            lat = ds_slice.latitude
            lon = ds_slice.longitude
            
            # For each line, check if ANY pixel is in the box
            line_mask = (
                ((lat >= lat_min) & (lat <= lat_max) & 
                 (lon >= lon_min) & (lon <= lon_max))
                .any(dim='num_pixels')  # True if ANY pixel in the line is in bounds
            )
            
            # Select only lines that have at least one pixel in bounds
            ds_filtered = ds_slice.isel(num_lines=line_mask)
            
            if ds_filtered.sizes['num_lines'] > 0:
                file_datasets.append(ds_filtered)
        
        # Concatenate slices from this file
        if file_datasets:
            file_data = xr.concat(file_datasets, dim='num_lines')
            datasets.append(file_data)
        
        ds.close()
    
    # Concatenate all results
    if datasets:
        result = xr.concat(datasets, dim='num_lines')
        return result
    else:
        return None