import xarray as xr
import numpy as np
import pandas as pd
from pathlib import Path
from shapely.geometry import box
import geopandas as gpd
from rtree import index as rtree_index
import pickle
import shutil

class SWOTSpatialIndex:
    """Spatial index for SWOT swath data with auto-save capability"""
    
    def __init__(self, index_file='swot_index', tile_size=493, autosave_interval=100):
        # Remove .pkl extension if provided
        self.index_file = index_file.replace('.pkl', '')
        self.metadata_file = f"{self.index_file}_metadata.pkl"
        
        # Create new in-memory index
        self.spatial_idx = rtree_index.Index()
        self.metadata = {}
        self.file_counter = 0
        self.indexed_files = set()  # Track which files have been indexed
        self.tile_size = tile_size  # Store as instance attribute
        self.base_path = None  # Original base path (set when first file added)
        
        # Auto-save settings
        self.autosave_interval = autosave_interval  # Save every N files
        self.files_since_save = 0
        
    def add_file(self, filepath, tile_size=None):
        """
        Add a SWOT file to the index
        tile_size: number of lines per tile (if None, uses instance default)
        """
        filepath_str = str(filepath)
        
        # Set base_path from first file if not set
        if self.base_path is None:
            self.base_path = str(Path(filepath).parent)
        
        # Skip if already indexed
        if filepath_str in self.indexed_files:
            print(f"  Skipping {Path(filepath).name} (already indexed)")
            return
        
        # Use instance tile_size if not specified
        if tile_size is None:
            tile_size = self.tile_size
        
        ds = xr.open_dataset(filepath)
        
        # Get time bounds
        time_min = pd.Timestamp(ds.time.min().values)
        time_max = pd.Timestamp(ds.time.max().values)
        
        num_lines = ds.sizes['num_lines']
        num_pixels = ds.sizes['num_pixels']
        
        # Create tiles along the swath
        for tile_start in range(0, num_lines, tile_size):
            tile_end = min(tile_start + tile_size, num_lines)
            
            # Get bounding box for this tile
            lat_tile = ds.latitude.isel(num_lines=slice(tile_start, tile_end))
            lon_tile = ds.longitude.isel(num_lines=slice(tile_start, tile_end))
            
            lat_min, lat_max = float(lat_tile.min()), float(lat_tile.max())
            lon_min, lon_max = float(lon_tile.min()), float(lon_tile.max())
            
            # Handle dateline crossing
            if lon_max - lon_min > 180:
                # Swath crosses dateline, split into two tiles
                self._add_tile(filepath_str, tile_start, tile_end, 
                              lat_min, lat_max, lon_min, 180, 
                              time_min, time_max)
                self._add_tile(filepath_str, tile_start, tile_end,
                              lat_min, lat_max, -180, lon_max - 360,
                              time_min, time_max)
            else:
                self._add_tile(filepath_str, tile_start, tile_end,
                              lat_min, lat_max, lon_min, lon_max,
                              time_min, time_max)
        
        # Mark file as indexed
        self.indexed_files.add(filepath_str)
        
        ds.close()
        
        # Auto-save check
        self.files_since_save += 1
        if self.autosave_interval > 0 and self.files_since_save >= self.autosave_interval:
            self._autosave()
        
    def _add_tile(self, filepath, line_start, line_end,
                  lat_min, lat_max, lon_min, lon_max,
                  time_min, time_max):
        """Add a single tile to the spatial index"""
        
        tile_id = self.file_counter
        self.file_counter += 1
        
        # R-tree expects (minx, miny, maxx, maxy)
        bbox = (lon_min, lat_min, lon_max, lat_max)
        
        # Insert into spatial index
        self.spatial_idx.insert(tile_id, bbox)
        
        # Store metadata
        self.metadata[tile_id] = {
            'file': str(filepath),
            'line_range': (line_start, line_end),
            'bbox': bbox,
            'time_range': (time_min, time_max)
        }
    
    def _autosave(self):
        """Internal auto-save without resetting counter"""
        temp_file = f"{self.metadata_file}.tmp"
        
        try:
            # Save to temporary file first
            with open(temp_file, 'wb') as f:
                pickle.dump({
                    'metadata': self.metadata,
                    'file_counter': self.file_counter,
                    'indexed_files': self.indexed_files,
                    'tile_size': self.tile_size,
                    'base_path': self.base_path
                }, f)
            
            # Move temporary file to actual file (atomic operation)
            shutil.move(temp_file, self.metadata_file)
            
            print(f"  [Auto-saved: {len(self.indexed_files)} files, {len(self.metadata)} tiles]")
            self.files_since_save = 0
            
        except Exception as e:
            print(f"  [Auto-save failed: {e}]")
            if Path(temp_file).exists():
                Path(temp_file).unlink()
    
    def add_files_from_directory(self, directory, pattern='*.nc', tile_size=None):
        """
        Add all matching files from a directory
        Skips files that are already indexed
        """
        files = list(Path(directory).glob(pattern))
        new_files = [f for f in files if str(f) not in self.indexed_files]
        
        print(f"Found {len(files)} total files, {len(new_files)} new files to index")
        if self.autosave_interval > 0:
            print(f"Auto-save enabled: every {self.autosave_interval} files")
        
        for i, filepath in enumerate(new_files, 1):
            print(f"[{i}/{len(new_files)}] Indexing {filepath.name}")
            self.add_file(filepath, tile_size=tile_size)
        
        print(f"Added {len(new_files)} files to index")
        
        # Final save if there were any changes since last auto-save
        if self.files_since_save > 0:
            print("Performing final save...")
            self.save()
    
    def query(self, lat_min, lat_max, lon_min, lon_max, 
              time_start=None, time_end=None):
        """
        Query the index for tiles within bounds
        Returns list of dicts with file info
        """
        bbox = (lon_min, lat_min, lon_max, lat_max)
        
        # Get candidate tiles from spatial index
        candidate_ids = list(self.spatial_idx.intersection(bbox))
        
        # Filter by time if provided
        results = []
        for tile_id in candidate_ids:
            if tile_id not in self.metadata:
                continue
                
            meta = self.metadata[tile_id]
            
            # Time filter
            if time_start and meta['time_range'][1] < time_start:
                continue
            if time_end and meta['time_range'][0] > time_end:
                continue
            
            results.append({
                'file': meta['file'],
                'line_range': meta['line_range'],
                'bbox': meta['bbox']
            })
        
        return results
    
    def set_base_path(self, new_base_path):
        """
        Update the base path for all indexed files
        Useful when moving the index to a different machine/location
        
        Example:
            index = SWOTSpatialIndex.load('swot_index')
            index.set_base_path('/new/data/location')
        """
        if self.base_path is None:
            print("Warning: Original base path not set")
            return
        
        old_base = Path(self.base_path)
        new_base = Path(new_base_path)
        
        print(f"Remapping paths:")
        print(f"  Old base: {old_base}")
        print(f"  New base: {new_base}")
        
        # Update metadata with new paths
        updated_count = 0
        for tile_id, meta in self.metadata.items():
            old_path = Path(meta['file'])
            # Get relative path from old base
            try:
                rel_path = old_path.relative_to(old_base)
                new_path = new_base / rel_path
                meta['file'] = str(new_path)
                updated_count += 1
            except ValueError:
                # Path is not relative to old_base, skip
                print(f"Warning: Could not remap {old_path}")
        
        # Update indexed_files set
        new_indexed_files = set()
        for old_path in self.indexed_files:
            try:
                rel_path = Path(old_path).relative_to(old_base)
                new_path = new_base / rel_path
                new_indexed_files.add(str(new_path))
            except ValueError:
                new_indexed_files.add(old_path)
        
        self.indexed_files = new_indexed_files
        self.base_path = str(new_base)
        
        print(f"Remapped {updated_count} tile paths")
    
    def save(self):
        """Save index to disk"""
        # Save all data to pickle (including R-tree bboxes for reconstruction)
        with open(self.metadata_file, 'wb') as f:
            pickle.dump({
                'metadata': self.metadata,
                'file_counter': self.file_counter,
                'indexed_files': self.indexed_files,
                'tile_size': self.tile_size,
                'base_path': self.base_path
            }, f)
        
        print(f"Index saved to {self.metadata_file}")
        print(f"  - {len(self.metadata)} tiles")
        print(f"  - {len(self.indexed_files)} files indexed")
        print(f"  - Tile size: {self.tile_size} lines")
        print(f"  - Base path: {self.base_path}")
        
        # Reset auto-save counter
        self.files_since_save = 0
    
    @classmethod
    def load(cls, index_file='swot_index', new_base_path=None, autosave_interval=10):
        """
        Load index from disk and rebuild R-tree
        
        Args:
            index_file: Path to index file
            new_base_path: If provided, remap all file paths to this new base
            autosave_interval: Auto-save interval for future operations (default: 10)
        """
        index_file = index_file.replace('.pkl', '')
        metadata_file = f"{index_file}_metadata.pkl"
        
        if not Path(metadata_file).exists():
            raise FileNotFoundError(f"Index file not found: {metadata_file}")
        
        with open(metadata_file, 'rb') as f:
            data = pickle.load(f)
        
        # Get tile_size from saved data (default to 493 for old indices)
        tile_size = data.get('tile_size', 493)
        
        # Create new instance
        idx = cls(index_file, tile_size=tile_size, autosave_interval=autosave_interval)
        idx.metadata = data['metadata']
        idx.file_counter = data['file_counter']
        idx.indexed_files = data.get('indexed_files', set())
        idx.base_path = data.get('base_path', None)
        
        # Rebuild R-tree from metadata
        print(f"Rebuilding spatial index from {len(idx.metadata)} tiles...")
        for tile_id, meta in idx.metadata.items():
            idx.spatial_idx.insert(tile_id, meta['bbox'])
        
        print(f"Index loaded: {len(idx.metadata)} tiles from {len(idx.indexed_files)} files")
        print(f"  - Tile size: {idx.tile_size} lines")
        print(f"  - Auto-save interval: {idx.autosave_interval} files")
        if idx.base_path:
            print(f"  - Original base path: {idx.base_path}")
        
        # Remap paths if requested
        if new_base_path is not None:
            idx.set_base_path(new_base_path)
        
        return idx
    
    def get_stats(self):
        """Get index statistics"""
        return {
            'num_tiles': len(self.metadata),
            'num_files': len(self.indexed_files),
            'tile_size': self.tile_size,
            'base_path': self.base_path,
            'autosave_interval': self.autosave_interval,
            'files': sorted(self.indexed_files)
        }