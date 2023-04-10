import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import numpy as np
import numpy.typing as npt
import pandas as pd
from ome_types.model import Pixels
from skimage.transform import SimilarityTransform, warp, EuclideanTransform

from broadside.config import re_filter, re_wavelength
from broadside.utils.geoms import Point2D
from broadside.utils.search import find_nearest

"""
The warp order is 3 as it gives a reasonable tradeoff between 1 (fast but not so 
accurate) and 5 (accurate but ~8x slower than 1).
"""
WARP_ORDER = 3


def scale_image(im: npt.NDArray, scale: float) -> npt.NDArray:
    h, w = im.shape[-2:]
    dx = (scale - 1) * w / 2
    dy = (scale - 1) * h / 2
    tr = SimilarityTransform(scale=scale, translation=[-dx, -dy])
    return warp(im, tr.inverse, mode="edge", order=WARP_ORDER)


def shift_image(im: npt.NDArray, shift: Point2D) -> npt.NDArray:
    tr = EuclideanTransform(translation=[shift.x, shift.y])
    return warp(im, tr.inverse, mode="edge", order=WARP_ORDER)


@lru_cache
def get_scales_shifts(ts: float, *, scales_shifts_dir: Path) -> tuple[dict, dict]:
    re_csv = re.compile("(?P<ts>[0-9]+).csv")
    timestamps = []
    for p in scales_shifts_dir.iterdir():
        is_match = re_csv.match(p.name)
        if not is_match:
            continue
        timestamps.append(int(is_match["ts"]))

    ts = find_nearest(ts, np.array(timestamps))
    path = scales_shifts_dir / f"{ts}.csv"
    df = pd.read_csv(path, dtype=dict(cube="category"))

    # smooth chromatic aberration
    # for cube in df["cube"].unique():
    #     ind = df["cube"] == cube
    #     df.loc[ind, "scale"] = (
    #         df.loc[ind, "scale"].rolling(5, center=True, min_periods=1).mean()
    #     )

    # for cube in df["cube"].unique():
    #     ind = df["cube"] == cube
    #     for col in ["shift_x", "shift_y"]:
    #         df.loc[ind, col] = (
    #             df.loc[ind, col].rolling(3, center=True, min_periods=1).mean()
    #         )

    scales = {}
    shifts = {}
    for cube in df["cube"].unique():
        df_cube = df[df["cube"] == cube]
        for wavelength in df_cube["wavelength"].unique():
            df_wav = df_cube[df_cube["wavelength"] == wavelength]
            scale = df_wav["scale"].item()
            try:
                scales[cube][wavelength] = scale
            except KeyError:
                scales[cube] = {wavelength: scale}

            shift = Point2D(
                x=df_wav["shift_x"].item(),
                y=df_wav["shift_y"].item(),
            )
            try:
                shifts[cube][wavelength] = shift
            except KeyError:
                shifts[cube] = {wavelength: shift}

    return scales, shifts


@dataclass(frozen=True)
class SpectralBand:
    cube: str
    wavelength: int
    name: str


def get_spectral_bands(pixels: Pixels) -> list[SpectralBand]:
    bands = []
    for i, channel in enumerate(pixels.channels):
        em_refs = channel.light_path.emission_filter_ref

        cube_matches = [
            re_filter.match(ref.id)
            for ref in em_refs
            if re_wavelength.match(ref.id) is None
        ]
        cube_matches = [m for m in cube_matches if m]
        cube = cube_matches[0]["cube"]

        wave_matches = [re_wavelength.match(ref.id) for ref in em_refs]
        wave_matches = [m for m in wave_matches if m]
        wavelength = int(wave_matches[0]["wavelength"])

        bands.append(SpectralBand(cube=cube, wavelength=wavelength, name=channel.name))
    return bands
