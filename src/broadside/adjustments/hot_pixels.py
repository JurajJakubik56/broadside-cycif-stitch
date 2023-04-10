from functools import lru_cache
from pathlib import Path
from typing import Callable

import numpy as np
import numpy.typing as npt
from numba import njit
from skimage.util import img_as_float
from tifffile import tifffile

from broadside.config import re_dark_timestamps
from broadside.utils.search import find_nearest


@njit()
def get_neighbor_inds():
    """
    For numba's nopython compilation to work, globals are not allowed, so we use this
    instead to provide neighbor indexes.
    """
    return {
        4: np.array(
            [
                (-1, 0),
                (1, 0),
                (0, -1),
                (0, 1),
            ]
        ),
        8: np.array(
            [
                (-1, -1),
                (-1, 0),
                (-1, 1),
                (0, -1),
                # (0, 0)
                (0, 1),
                (1, -1),
                (1, 0),
                (1, 1),
            ]
        ),
    }


@lru_cache()
def get_dark_timestamps(dark_dir: Path) -> list[float]:
    # get timestamps from path filenames
    timestamps = []
    for p in dark_dir.iterdir():
        is_match = re_dark_timestamps.match(p.name)
        if not is_match:
            continue
        timestamps.append(float(is_match["ts"]))

    # should not happen
    assert len(timestamps) > 0, "No dark images found!"

    return timestamps


def get_remove_hot_pixels_func_cyx(
    ts: float, *, pct=99.99, connectivity=8, method="median", dark_dir: Path
) -> Callable:
    # validate inputs
    assert 0 <= pct <= 100
    assert connectivity in get_neighbor_inds().keys()

    if method == "median":
        method_func = np.median
    elif method == "mean":
        method_func = np.mean
    else:
        raise RuntimeError(f"Unsupported method {method}")

    # get dark image with the closest timestamp
    timestamps = get_dark_timestamps(dark_dir)
    if ts is None:
        ts = sorted(timestamps)[-1]
    else:
        ts = find_nearest(ts, np.array(timestamps))

    # read in image
    dark_im: npt.NDArray = tifffile.imread(dark_dir / f"dark-{int(ts)}.ome.tiff")
    dark_im = img_as_float(dark_im)

    # compute locations of hot pixels in dark image
    median = np.median(dark_im, axis=0)
    thresh = np.percentile(median.ravel(), pct).item()
    dark_inds = np.array(list(zip(*np.where(median > thresh))))

    # slightly modified so that it can be compiled by numba
    @njit()
    def func(stack: npt.NDArray) -> npt.NDArray:
        assert stack.ndim == 3

        neighbor_inds = get_neighbor_inds()[connectivity]
        # we only make one copy of the cache array because we only need one;
        # we iterate through all neighbors anyway, so we know it will be completely
        # replaced
        cache = np.empty(shape=len(neighbor_inds), dtype=float)

        h, w = stack.shape[1], stack.shape[2]

        for i in range(stack.shape[0]):
            for iy, ix in dark_inds:
                for i_n, (dy, dx) in enumerate(neighbor_inds):
                    diy = iy + dy
                    dix = ix + dx

                    # if a neighbor index is outside the array, use the other side;
                    # TODO: can this be improved?
                    if diy <= -1:
                        diy = 1
                    elif diy >= h:
                        diy = h - 2

                    if dix <= -1:
                        dix = 1
                    elif dix >= w:
                        dix = w - 2

                    cache[i_n] = stack[i, diy, dix]
                stack[i, iy, ix] = method_func(cache)
        return stack

    return func


def get_remove_hot_pixels_func_yx(
    ts: float, *, pct=99.99, connectivity=8, method="median", dark_dir: Path
) -> Callable:
    # validate inputs
    assert 0 <= pct <= 100
    assert connectivity in get_neighbor_inds().keys()

    if method == "median":
        method_func = np.median
    elif method == "mean":
        method_func = np.mean
    else:
        raise RuntimeError(f"Unsupported method {method}")

    # get dark image with the closest timestamp
    timestamps = get_dark_timestamps(dark_dir)
    if ts is None:
        ts = sorted(timestamps)[-1]
    else:
        ts = find_nearest(ts, np.array(timestamps))

    # read in image
    dark_im: npt.NDArray = tifffile.imread(dark_dir / f"dark-{int(ts)}.ome.tiff")
    dark_im = img_as_float(dark_im)

    # compute locations of hot pixels in dark image
    median = np.median(dark_im, axis=0)
    thresh = np.percentile(median.ravel(), pct).item()
    dark_inds = np.array(list(zip(*np.where(median > thresh))))

    # slightly modified so that it can be compiled by numba
    @njit()
    def func(im: npt.NDArray) -> npt.NDArray:
        assert im.ndim == 2

        neighbor_inds = get_neighbor_inds()[connectivity]
        # we only make one copy of the cache array because we only need one;
        # we iterate through all neighbors anyway, so we know it will be completely
        # replaced
        cache = np.empty(shape=len(neighbor_inds), dtype=float)

        h, w = im.shape[0:2]

        for iy, ix in dark_inds:
            for i_n, (dy, dx) in enumerate(neighbor_inds):
                diy = iy + dy
                dix = ix + dx

                # if a neighbor index is outside the array, use the other side;
                # TODO: can this be improved?
                if diy <= -1:
                    diy = 1
                elif diy >= h:
                    diy = h - 2

                if dix <= -1:
                    dix = 1
                elif dix >= w:
                    dix = w - 2

                cache[i_n] = im[diy, dix]
            im[iy, ix] = method_func(cache)
        return im

    return func
