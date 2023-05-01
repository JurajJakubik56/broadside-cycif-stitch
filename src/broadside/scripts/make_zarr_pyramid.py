import argparse
import glob
import math
import warnings
from pathlib import Path

import dask.array as da
import zarr
from datatree import DataTree
from multiscale_spatial_image import to_multiscale, Methods
from numcodecs import Blosc
from ome_zarr.io import parse_url
from ome_zarr.writer import write_multiscale
from spatial_image import to_spatial_image
from tifffile import ZarrTiffStore, tifffile


def make_zarr_pyramid(
    *,
    tile_size: int,
    max_top_level_size: int,
    downscale: int,
    src_pattern: str,
    dst: Path,
):
    # collect dask arrays from tiff files (list of 2D YX images)
    paths = sorted(glob.glob(src_pattern))
    arrays = []
    for path in paths:
        store: ZarrTiffStore = tifffile.imread(path, aszarr=True)
        zgroup: zarr.Array = zarr.open(store=store, mode="r")
        arr = da.from_zarr(zgroup, chunks=(tile_size, tile_size))
        arrays.append(arr)
    arrays = da.stack(arrays, axis=0)

    # compute pyramid dimensions
    if max_top_level_size < 1:
        warnings.warn(f"Max top level size is less than 1; setting to 1")
        max_top_level_size = 1

    max_base_shape = max([max(s.shape) for s in arrays])
    n_levels_to_make = math.ceil(math.log2(max_base_shape / max_top_level_size))
    scale_factors = [dict(x=downscale, y=downscale) for _ in range(n_levels_to_make)]

    # use multiscale-spatial-image to generate the downsampling
    image = to_spatial_image(arrays, dims=("c", "y", "x"))
    multiscale: DataTree = to_multiscale(
        image, scale_factors=scale_factors, method=Methods.XARRAY_COARSEN
    )

    # collect dask arrays from datatree
    pyramid_src = []
    for level in multiscale.values():
        pyramid_src.append(level["image"].data)

    # prepare destination
    store_dst = parse_url(dst, mode="w").store
    root_dst = zarr.group(store=store_dst, overwrite=True)
    root_dst.attrs.clear()

    # write pyramid into destination
    write_multiscale(
        pyramid=pyramid_src,
        group=root_dst,
        axes=list(("c", "y", "x")),
        chunks=(1, tile_size, tile_size),
        storage_options=dict(compressor=Blosc(), dimension_separator="/"),
    )


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dst", type=str, required=True)
    parser.add_argument("--src-pattern", type=str, required=True)
    parser.add_argument("--tile-size", type=int, required=True)
    parser.add_argument("--max-top-level-size", type=int, required=True)
    parser.add_argument("--downscale", type=int, required=True)

    args = parser.parse_args()

    make_zarr_pyramid(
        tile_size=args.tile_size,
        max_top_level_size=args.max_top_level_size,
        downscale=args.downscale,
        src_pattern=args.src_pattern,
        dst=Path(args.dst),
    )
