import argparse
from pathlib import Path

import dask.array as da
import zarr
from ome_zarr.io import parse_url
from tifffile import TiffWriter


def convert_zarr_to_big_tiff(*, src: Path, dst: Path, tile_size: int):
    options = dict(tile=(tile_size, tile_size), compression="zlib")

    root = zarr.open_group(store=parse_url(src, mode="r").store)
    pyramid = [da.from_zarr(root.store, key) for key in root.keys()]
    n_levels = len(pyramid)

    with TiffWriter(dst, bigtiff=True) as tif:
        tif.write(pyramid[0], subifds=n_levels - 1, **options)
        for level in range(1, n_levels):
            tif.write(pyramid[level], subfiletype=1, **options)


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", type=str, default=None)
    parser.add_argument("--dst", type=str, default=None)
    parser.add_argument("--tile-size", type=int, default=None)

    args = parser.parse_args()

    convert_zarr_to_big_tiff(
        src=Path(args.src),
        dst=Path(args.dst),
        tile_size=args.tile_size,
    )
