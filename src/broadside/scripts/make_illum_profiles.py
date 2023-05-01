import argparse
from pathlib import Path
from typing import Callable

import dask
import dask.array as da
import numpy as np
import numpy.typing as npt
import tifffile
from ome_types import from_xml
from pybasic import compute_illum_profiles
from skimage import img_as_float
from skimage.transform import resize
from tifffile import TiffReader

from broadside.adjustments.hot_pixels import get_remove_hot_pixels_func_cyx
from broadside.utils.io import read_paths
from broadside.utils.parallel import dask_session


def read_image(
    path: Path,
    *,
    shape: tuple[int, int, int],
    transforms_before_resize: list[Callable] = None,
) -> npt.NDArray:
    image = tifffile.imread(path, maxworkers=1)
    image = img_as_float(image)

    if transforms_before_resize is not None:
        for tr in transforms_before_resize:
            image = tr(image)

    image = resize(image, shape)
    return image


def read_images(
    paths: list[Path],
    *,
    working_size: int,
    transforms_before_resize: list[Callable] = None,
) -> npt.NDArray:
    im_rep = tifffile.imread(paths[0], maxworkers=1)
    assert im_rep.ndim == 3
    n_channels = im_rep.shape[0]
    shape = (n_channels, working_size, working_size)

    # load, transform, and stack using dask
    images = [
        dask.delayed(read_image)(
            path, shape=shape, transforms_before_resize=transforms_before_resize
        )
        for path in paths
    ]
    images = [da.from_delayed(i, shape=shape, dtype=np.float64) for i in images]
    images = da.stack(images)
    images = images.compute()

    assert images.shape == (len(paths), n_channels, working_size, working_size)
    return images


def make_illumination_profiles(
    tile_paths: list[Path],
    *,
    working_size: int,
    flatfield_dst: Path,
    darkfield_dst: Path,
    compute_darkfield: bool,
    dark_dir: Path,
):
    """
    When reading in tiles for computing illumination profiles, we just pick the first
    tile and consider its timestamp as the one we use for the dark image, from which we
    compute the hot pixel locations.

    the locations of the hot pixels do not change much, as far as I can tell...
    """
    with TiffReader(tile_paths[0]) as reader:
        ome = from_xml(reader.ome_metadata, parser="lxml")
        ts = ome.images[0].acquisition_date.timestamp()

    remove_hot_pixels = get_remove_hot_pixels_func_cyx(ts, dark_dir=dark_dir)
    image_stack = read_images(
        tile_paths,
        working_size=working_size,
        transforms_before_resize=[remove_hot_pixels],
    )

    src_shape = tifffile.imread(tile_paths[0], maxworkers=1).shape
    n_channels = src_shape[0]

    delayeds = [
        dask.delayed(compute_illum_profiles)(
            image_stack[:, ch], compute_darkfield=compute_darkfield
        )
        for ch in range(n_channels)
    ]
    profiles = dask.compute(*delayeds)

    # collect profiles into flat and dark, then stack and save
    flatfields = []
    darkfields = []

    flatfield: npt.NDArray
    darkfield: npt.NDArray
    for flatfield, darkfield in profiles:
        flatfields.append(flatfield)
        darkfields.append(darkfield)

    flatfield = np.stack(flatfields)
    darkfield = np.stack(darkfields)

    flatfield = resize(flatfield, output_shape=src_shape)
    darkfield = resize(darkfield, output_shape=src_shape)

    tifffile.imwrite(flatfield_dst, flatfield)
    tifffile.imwrite(darkfield_dst, darkfield)


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tiles-path", type=str, required=True)
    parser.add_argument("--flatfield-path", type=str, required=True)
    parser.add_argument("--darkfield-path", type=str, required=True)
    parser.add_argument("--darkfield", action="store_true")
    parser.add_argument("--no-darkfield", dest="darkfield", action="store_false")
    parser.set_defaults(darkfield=False)
    parser.add_argument("--dark-dir", type=str, required=True)
    parser.add_argument("--working-size", type=int, required=True)

    parser.add_argument("--n-cpus", type=int, default=None)
    parser.add_argument("--memory-limit", type=str, default=None)
    parser.add_argument("--dask-report-filename", type=str, default=None)

    args = parser.parse_args()

    # parse nextflow-specific arguments
    tile_paths = read_paths(Path(args.tiles_path))
    assert len(tile_paths) >= 1

    # dask config
    with dask_session(
        memory_limit=args.memory_limit,
        n_cpus=args.n_cpus,
        dask_report_filename=args.dask_report_filename,
    ):
        make_illumination_profiles(
            tile_paths=tile_paths,
            flatfield_dst=Path(args.flatfield_path),
            darkfield_dst=Path(args.darkfield_path),
            compute_darkfield=args.darkfield,
            dark_dir=Path(args.dark_dir),
            working_size=args.working_size,
        )
