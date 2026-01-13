"""
Command-line interface for SWOT spatial indexing and querying
"""

import argparse
import pandas as pd
from pathlib import Path

from SwotDB import SWOTSpatialIndex, query_swot_data


def build_index(args):
    """Build or update spatial index"""
    print(f"Building index: {args.index_file}")
    print(f"Tile size: {args.tile_size} lines")

    if args.load_existing and Path(f"{args.index_file}_metadata.pkl").exists():
        print("Loading existing index...")
        index = SWOTSpatialIndex.load(args.index_file)
    else:
        print("Creating new index...")
        index = SWOTSpatialIndex(args.index_file, tile_size=args.tile_size)

    index.add_files_from_directory(
        args.data_dir,
        pattern=args.pattern,
        tile_size=args.tile_size,
    )

    index.save()

    stats = index.get_stats()
    print(f"\nIndex Statistics:")
    print(f"  Files indexed: {stats['num_files']}")
    print(f"  Total tiles: {stats['num_tiles']}")
    print(f"  Tile size: {stats['tile_size']} lines")
    print(f"  Base path: {stats['base_path']}")


def query_index(args):
    """Query spatial index"""
    print(f"Querying index: {args.index_file}")

    index = SWOTSpatialIndex.load(args.index_file)

    time_start = pd.Timestamp(args.time_start) if args.time_start else None
    time_end = pd.Timestamp(args.time_end) if args.time_end else None

    variables = args.variables.split(",") if args.variables else ["ssha_unfiltered"]

    print("Query bounds:")
    print(f"  Latitude: {args.lat_min} to {args.lat_max}")
    print(f"  Longitude: {args.lon_min} to {args.lon_max}")
    if time_start:
        print(f"  Time start: {time_start}")
    if time_end:
        print(f"  Time end: {time_end}")
    print(f"  Variables: {variables}")

    data = query_swot_index(
        index,
        lat_min=args.lat_min,
        lat_max=args.lat_max,
        lon_min=args.lon_min,
        lon_max=args.lon_max,
        time_start=time_start,
        time_end=time_end,
        variables=variables,
    )

    if data is not None:
        print("\nQuery Results:")
        print(f"  Lines: {data.sizes['num_lines']}")
        print(f"  Pixels: {data.sizes['num_pixels']}")
        print(f"  Variables: {list(data.data_vars)}")

        if args.output:
            print(f"\nSaving to {args.output}")
            data.to_netcdf(args.output)
            print("Done!")
        else:
            print("\nDataset preview:")
            print(data)
    else:
        print("\nNo data found matching query criteria")


def info_index(args):
    """Show index information"""
    print(f"Loading index: {args.index_file}")

    index = SWOTSpatialIndex.load(args.index_file)
    stats = index.get_stats()

    print("\nIndex Information:")
    print(f"  Index file: {args.index_file}_metadata.pkl")
    print(f"  Files indexed: {stats['num_files']}")
    print(f"  Total tiles: {stats['num_tiles']}")
    print(f"  Tile size: {stats['tile_size']} lines")
    print(f"  Base path: {stats['base_path']}")

    if args.list_files:
        print("\nIndexed files:")
        for i, f in enumerate(stats["files"], 1):
            print(f"  {i}. {Path(f).name}")


def remap_paths(args):
    """Remap file paths to new base directory"""
    print(f"Loading index: {args.index_file}")

    index = SWOTSpatialIndex.load(args.index_file)

    print(f"\nRemapping paths to: {args.new_base_path}")
    index.set_base_path(args.new_base_path)

    print("\nSaving updated index...")
    index.save()
    print("Done!")


def main():
    parser = argparse.ArgumentParser(
        description="SWOT Spatial Index – Build and query spatial indices for SWOT data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    build_parser = subparsers.add_parser("build", help="Build or update spatial index")
    build_parser.add_argument("--data-dir", required=True)
    build_parser.add_argument("--index-file", default="swot_index")
    build_parser.add_argument("--tile-size", type=int, default=493)
    build_parser.add_argument("--pattern", default="*.nc")
    build_parser.add_argument("--load-existing", action="store_true")
    build_parser.set_defaults(func=build_index)

    query_parser = subparsers.add_parser("query", help="Query spatial index")
    query_parser.add_argument("--index-file", default="swot_index")
    query_parser.add_argument("--lat-min", type=float, required=True)
    query_parser.add_argument("--lat-max", type=float, required=True)
    query_parser.add_argument("--lon-min", type=float, required=True)
    query_parser.add_argument("--lon-max", type=float, required=True)
    query_parser.add_argument("--time-start")
    query_parser.add_argument("--time-end")
    query_parser.add_argument("--variables", default="ssha_unfiltered")
    query_parser.add_argument("--output", "-o")
    query_parser.set_defaults(func=query_index)

    info_parser = subparsers.add_parser("info", help="Show index information")
    info_parser.add_argument("--index-file", default="swot_index")
    info_parser.add_argument("--list-files", action="store_true")
    info_parser.set_defaults(func=info_index)

    remap_parser = subparsers.add_parser("remap", help="Remap file paths")
    remap_parser.add_argument("--index-file", required=True)
    remap_parser.add_argument("--new-base-path", required=True)
    remap_parser.set_defaults(func=remap_paths)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
