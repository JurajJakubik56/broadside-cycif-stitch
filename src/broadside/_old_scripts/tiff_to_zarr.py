import argparse
from pathlib import Path

import dask.array as da
import zarr
from numcodecs import Blosc
from ome_zarr.io import parse_url
from ome_zarr.writer import write_multiscale
from tifffile import tifffile


def tiff_to_zarr(*, src: Path, dst: Path, tile_size: int):
    zgroup = zarr.open(tifffile.imread(src, aszarr=True), mode="r")
    pyramid_src = [
        da.from_zarr(zgroup[int(dataset["path"])])
        for dataset in zgroup.attrs["multiscales"][0]["datasets"]
    ]

    store_dst = parse_url(dst, mode="w").store
    root_dst = zarr.group(store=store_dst, overwrite=True)
    root_dst.attrs.clear()

    # we need to specify the dimension separator as "/", and not "."
    # ome-zarr uses "/" as its specification, and may change!

    write_multiscale(
        pyramid=pyramid_src,
        group=root_dst,
        axes=["c", "y", "x"],
        chunks=(1, tile_size, tile_size),
        storage_options=dict(compressor=Blosc(), dimension_separator="/"),
    )


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", type=str, required=True)
    parser.add_argument("--dst", type=str, required=True)
    parser.add_argument("--tile-size", type=int, required=True)

    args = parser.parse_args()

    # unfortunately, there is a serialization error when constructing a dask cluster with
    # write_multiscale; since this is a pretty fast step, we leave it out
    tiff_to_zarr(
        src=Path(args.src),
        dst=Path(args.dst),
        tile_size=args.tile_size,
    )
